import { randomUUID } from 'node:crypto';
import { performance } from 'node:perf_hooks';
import { SOURCE_CONCURRENCY_LIMITS } from '../core/concurrency.js';
import { LITE_PRIORITY_RANK } from './types.js';
import { parseCanonicalChapterKey } from './chapter-identity.js';
import { STORED_MAPPING_ALIAS_FIELDS, STORED_MAPPING_TAG_FIELDS } from './mapping-metadata.js';
/**
 * Older catalog passes could put two legacy mappings for one canonical chapter
 * in a single UPSERT. PostgreSQL/YSQL rejects that statement before its
 * conflict clause can decide either row. Keep the exact durable reason
 * narrowly scoped so recovery gives only those code-bug failures one clean
 * retry.
 */
export const DUPLICATE_CANONICAL_BATCH_REASON = 'ON CONFLICT DO UPDATE command cannot affect row a second time';
// A provider 429 can durably defer a large sole-route backlog in one event.
// Once its deadline expires, recovery must not take tens of minutes merely
// because the generic retry pass is intentionally small. Keep re-admission
// bounded per source, work and recovery turn so a large work cannot hide other
// confirmed chapter gaps or immediately recreate the rate limit.
const SOURCE_COOLDOWN_RECOVERY_SOURCES_PER_TURN = 8;
const SOURCE_COOLDOWN_RECOVERY_TASKS_PER_SOURCE = 8;
const SOURCE_COOLDOWN_RECOVERY_CANDIDATE_WORKS = 64;
const SOURCE_COOLDOWN_RECOVERY_TASKS_PER_WORK = 2;
/** Reuse an abundant executable-P1 reserve measurement for this long. */
const EXECUTABLE_P1_RESERVE_CACHE_MS = 180_000;
// Historical refetch owns one P2 seat while the importer is serving chapter
// work. Looking across eight providers merely to fill that one seat made the
// producer's six-tier candidate CTE the dominant YSQL workload. A durable
// source cursor lets a short horizon cover every provider without repeatedly
// scanning the catalogue. V5 reserves one of four seats for a popular
// aliasless work, so probing three cursor sources lets a healthy refill fill
// all four seats instead of leaving one idle behind the old two-source cap.
const SOURCE_METADATA_SOURCE_PROBE_LIMIT = 3;
const METADATA_REPLAY_V4_ENABLED_KEY = 'metadata_replay_v4_enabled';
// V4 replays evidence already stored in YSQL. Once it has exhausted that
// finite input, this version marker switches the existing source-refetch lane
// to a real, one-pass observation of every confirmed active mapping. Keeping
// the marker on the durable enrichment row makes coverage measurable without
// turning an old NO_ALIASES classification into a permanent conclusion.
const METADATA_ALIAS_COVERAGE_V5_VERSION = 5;
const METADATA_CROSS_SOURCE_DISCOVERY_V7_PREFIX = 'lite:metadata:cross-source:v7:';
// A source that was temporarily unavailable is not evidence that a work has
// no aliases.  Revisit it sparingly after the first catalog-wide pass, but
// cap the recovery path so an external outage can never become an endless
// metadata loop.
const METADATA_CROSS_SOURCE_DEFERRED_RECHECK_LIMIT = 2;
const METADATA_REPLAY_V4_EXCLUSIVE_MODE_KEY = 'metadata_replay_v4_exclusive_mode';
export const METADATA_TURBO_EXCLUSIVE_MODE_KEY = 'metadata_turbo_exclusive_mode';
const METADATA_REPLAY_V4_SOURCE_DONE_PREFIX = 'metadata_replay_v4_source_done:';
const METADATA_REPLAY_V4_MAPPING_CURSOR_PREFIX = 'metadata_replay_v4_mapping_cursor:';
const METADATA_REPLAY_V4_SOURCE_CURSOR_KEY = 'metadata_replay_v4_source_cursor';
const METADATA_REPLAY_V4_SOURCE_BURST_PREFIX = 'metadata_replay_v4_source_burst:';
// P0 observation is a separate, one-seat service.  Persisting its provider
// frontier prevents a deep watch backlog from one source from making another
// eligible source wait until that backlog has drained.
const P0_MONITOR_SOURCE_CURSOR = 'p0_monitor';
// This cursor is intentionally separate from the monitor *claim* cursor.
// Claim rotation decides which existing watch is fetched next; coverage
// rotation decides which provider receives its next missing durable watch.
// Sharing them let a deep backlog on one provider repeatedly steer both
// decisions, even though the two operations have different fairness goals.
const P0_MONITOR_COVERAGE_CURSOR = 'p0_monitor_coverage';
// The source cursor decides provider fairness; this second, per-provider
// cursor decides which mapping slice is inspected next.  Without it, an
// almost-complete source would repeatedly scan every already-covered mapping
// merely to discover there was no missing watch.  Persisted settings keep the
// scan resumable across restarts without adding a hot queue-table index.
const P0_MONITOR_COVERAGE_MAPPING_CURSOR_PREFIX = 'p0_monitor_coverage_mapping:';
const P0_MONITOR_COVERAGE_SCAN_MULTIPLIER = 8;
const METADATA_REPLAY_V4_INTERVAL_MS = 20_000;
const METADATA_REPLAY_V4_WINDOWS_PER_SOURCE_TURN = 4;
const METADATA_REPLAY_V4_SOURCE_BURST_LIMIT = 3;
// Sources with usable stored metadata are sparse. Twelve indexed windows are
// still a bounded serial probe (at most 1,536 mappings per producer turn),
// but avoid spending several minutes rotating across empty providers before
// the next eligible local merge is found. Task execution remains one-at-a-
// time and retains the 20-second due-time fence.
const METADATA_REPLAY_V4_SOURCE_WINDOWS_PER_TURN = 12;
// The exclusive campaign scans three bounded source slices per producer turn.
// The active-work partial index keeps each successor anti-join index-backed;
// without it, twelve slices serialized full task-table scans. Cursors remain
// durable, so the scan only reduces idle time and never skips a mapping.
const METADATA_REPLAY_V4_EXCLUSIVE_SOURCE_WINDOWS_PER_TURN = 12;
const sourceExecutionEligibilitySql = (alias) => `(
  (COALESCE(${alias}.status,'ACTIVE') IN ('ACTIVE','DEGRADED','RECOVERING') OR
   (${alias}.status='COOLDOWN' AND ${alias}.cooldown_until IS NOT NULL AND ${alias}.cooldown_until<=now()))
  AND (${alias}.blocked_reason IS NULL
    OR ${alias}.blocked_details->>'probe_success'='true'
    OR ${alias}.blocked_details->>'recovered_at' IS NOT NULL)
)`;
const priorityName = (value) => ['STAFF', 'P0', 'P1', 'P2'][value] || 'P2';
const asTask = (row) => ({
    id: row.id, taskType: row.task_type, workId: row.work_id, workKey: row.work_key,
    chapterKey: row.chapter_key, priority: priorityName(row.priority), basePriority: priorityName(row.base_priority),
    state: row.state, idempotencyKey: row.idempotency_key, nextRunAt: new Date(row.next_run_at),
    attempts: row.attempts, maxAttempts: row.max_attempts, leaseOwner: row.lease_owner,
    leaseUntil: row.lease_until ? new Date(row.lease_until) : null, reason: row.reason,
    monitoring: Boolean(row.monitoring),
    payload: row.payload || {}, checkpoint: row.checkpoint || {},
    createdAt: row.created_at ? new Date(row.created_at) : undefined,
});
/** Direct YSQL persistence. Network, adapters and Telegram are never invoked here. */
export class LiteRepository {
    db;
    sourceMetadataDb;
    // This is only a local maintenance hint. Durable task state remains the
    // authority; the runtime uses the hint to avoid a second catalog turn when
    // the materialised P1 import reserve is already abundant.
    historicalP1ReserveAbundant = false;
    // The executable-P1 reserve count correlates every READY import with its
    // route/source health. Production ASH showed it as the single largest YSQL
    // CPU consumer (~22%) while the reserve held thousands of executable rows.
    // An abundant answer is stable for minutes, so reuse it briefly; a low
    // reserve is never cached and is always re-measured on the next turn.
    manifestP1ReserveCache = null;
    coolingSources = new Map();
    p1FocusWorkKeys = [];
    p1FocusMeta = new Map();
    p1FocusTargetSize = 18;
    p1FocusRefreshedAt = null;
    p1CompletedWorksCount = 0;
    p1CompletedWorkIds = [];
    p1PrimaryClaims = 0;
    p1SecondaryClaims = 0;
    lastHistoricalP1CatalogSource = '';
    p1BacklogCache = null;
    p0MonitorCursorEnsured = false;
    sourcePolicyCache = new Map();
    lastHealthyRecordAt = new Map();
    hasAbundantHistoricalP1Reserve() { return this.historicalP1ReserveAbundant; }
    invalidateP1BacklogCache() { this.p1BacklogCache = null; }
    invalidateSourcePolicyCache(source) {
        if (source) {
            this.sourcePolicyCache.delete(source);
            this.lastHealthyRecordAt.delete(source);
        }
        else {
            this.sourcePolicyCache.clear();
            this.lastHealthyRecordAt.clear();
        }
    }
    getActiveCoolingSources() {
        const now = Date.now();
        const active = [];
        for (const [source, entry] of this.coolingSources.entries()) {
            if (entry.until > now)
                active.push(source);
            else
                this.coolingSources.delete(source);
        }
        return active;
    }
    clearNonRateLimitedCoolingSource(source) {
        const existing = this.coolingSources.get(source);
        if (existing && !(existing.rateLimited && existing.until > Date.now())) {
            this.coolingSources.delete(source);
        }
    }
    /**
     * Source-backed historical metadata is deliberately allowed one tiny,
     * isolated database seat.  It cannot borrow the chapter claim pool during a
     * busy wave, while its own producer/claim/finish operations remain strictly
     * serial through the runtime's lane gates.
     */
    constructor(db, sourceMetadataDb = db) {
        this.db = db;
        this.sourceMetadataDb = sourceMetadataDb;
    }
    /**
     * Enrol a small, source-rotated slice of existing published ongoing works
     * into the durable P0 observation lane.
     *
     * Older Lite releases only made a recurring watch as a side effect of an
     * initial P2 catalog pass.  Existing mapped works that predated Lite (or
     * whose first pass completed before that rule) therefore never received a
     * P0 check at all.  A healthy monitor executor cannot discover a release
     * for a work it was never asked to observe.
     *
     * This is deliberately a producer, not a second claim path: it creates at
     * most a small number of `monitoring=true` CATALOG rows for one operational
     * source.  Those rows remain invisible to normal P1/P2 claims and use the
     * existing two-permit P0 monitor lane.  No network operation or long-lived
     * transaction occurs here.
     */
    /** Enrol newly confirmed mappings without waiting for the coverage cursor. */
    async enrollConfirmedP0Mappings(workId, mappings) {
        if (!mappings.length)
            return 0;
        const result = await this.sourceMetadataDb.query(`
      WITH input AS (
        SELECT DISTINCT * FROM unnest($2::text[], $3::text[]) AS v(source, source_work_id)
      ), eligible AS (
        SELECT m.source, m.source_work_id, m.work_id
        FROM input i
        JOIN importer_work_mappings m ON m.source=i.source AND m.source_work_id=i.source_work_id AND m.work_id=$1::uuid
        JOIN works w ON w.id=m.work_id
        JOIN importer_sources s ON s.id=m.source
        LEFT JOIN importer_lite_source_health h ON h.source=s.id
        WHERE m.sync_status IN ('WAITING_ADMISSION','SYNCED','ACTIVE','PENDING')
          AND COALESCE(w.status,'ONGOING') <> 'CANCELLED'
          AND s.enabled=true AND COALESCE(s.chapter_ingestion_enabled,true)=true
          AND COALESCE(s.status,'ACTIVE') IN ('ACTIVE','DEGRADED','RECOVERING','COOLDOWN')
          AND COALESCE(h.health,'HEALTHY') NOT IN ('BROKEN','DISABLED')
          AND m.source NOT IN ('mangaball','mangadex','mangafire')
          AND NOT (m.source='nexus' AND m.source_work_id !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')
          AND NOT EXISTS (
            SELECT 1 FROM importer_lite_tasks t
            WHERE t.idempotency_key IN ('lite:catalog:'||m.source||':'||m.source_work_id,
                                       'lite:p0-monitor:'||m.source||':'||m.source_work_id)
              AND t.state IN ('READY','RUNNING','RETRY_WAIT')
          )
      )
      INSERT INTO importer_lite_tasks
        (task_type,work_id,work_key,priority,base_priority,idempotency_key,next_run_at,max_attempts,payload,checkpoint,monitoring)
      SELECT 'CATALOG', e.work_id, 'work:'||e.work_id::text,
        CASE WHEN EXISTS (SELECT 1 FROM importer_staff_requests r WHERE r.work_id=e.work_id
          AND r.status IN ('QUEUED','IMPORTING','RETRYING','ACTIVE')) THEN 0 ELSE 3 END,
        3, 'lite:p0-monitor:'||e.source||':'||e.source_work_id,
        now()+interval '1 minute',32,
        jsonb_build_object('source',e.source,'sourceWorkId',e.source_work_id,'p0MonitorCoverage',true),
        jsonb_build_object('workId',e.work_id::text,'enrolledBy','verified-cross-source'),true
      FROM eligible e ON CONFLICT(idempotency_key) DO NOTHING`, [workId, mappings.map((m) => m.source), mappings.map((m) => m.sourceWorkId)]);
        return result.rowCount || 0;
    }
    async refillP0MonitorCoverage(limit = 16) {
        const maximum = Math.max(1, Math.min(256, Math.floor(limit)));
        const client = await this.sourceMetadataDb.connect();
        try {
            await client.query('BEGIN');
            // Coverage work is optional while the old monitor lane keeps serving
            // existing watches. A safe statement timeout protects against brief lock waits.
            await client.query(`SET LOCAL statement_timeout = '8000ms'`);
            let cursor = await client.query(`
        SELECT last_source FROM importer_lite_metadata_source_cursor
        WHERE cursor_name=$1 FOR UPDATE`, [P0_MONITOR_COVERAGE_CURSOR]);
            if (!cursor.rows[0]) {
                await client.query(`INSERT INTO importer_lite_metadata_source_cursor(cursor_name,last_source)
          VALUES($1,NULL) ON CONFLICT(cursor_name) DO NOTHING`, [P0_MONITOR_COVERAGE_CURSOR]);
                cursor = await client.query(`
          SELECT last_source FROM importer_lite_metadata_source_cursor
          WHERE cursor_name=$1 FOR UPDATE`, [P0_MONITOR_COVERAGE_CURSOR]);
            }
            const lastSource = cursor.rows[0]?.last_source || '';
            // There are only a few dozen providers.  Pick one source first and run
            // the mapping query only for that source; a global anti-join over every
            // work was the kind of catalogue scan that previously hurt YSQL.
            // Enrol durable monitors for all enabled chapter-ingestion sources even
            // if a provider is temporarily cooling down; claimMonitor enforces
            // runnable health and cooldown boundaries at check execution time.
            const selected = await client.query(`
        WITH after_cursor AS (
          SELECT s.id AS source
          FROM importer_sources s
          LEFT JOIN importer_lite_source_health h ON h.source=s.id
          WHERE s.enabled=true AND s.id>$1::text
            AND s.id NOT IN ('mangaball','mangadex','mangafire')
            AND COALESCE(s.status,'ACTIVE') IN ('ACTIVE','DEGRADED','RECOVERING','COOLDOWN')
            AND COALESCE(s.chapter_ingestion_enabled,true)=true
            AND COALESCE(h.health,'HEALTHY') NOT IN ('BROKEN','DISABLED')
          ORDER BY s.id LIMIT 1
        ), wrapped AS (
          SELECT s.id AS source
          FROM importer_sources s
          LEFT JOIN importer_lite_source_health h ON h.source=s.id
          WHERE s.enabled=true AND s.id<=$1::text
            AND s.id NOT IN ('mangaball','mangadex','mangafire')
            AND COALESCE(s.status,'ACTIVE') IN ('ACTIVE','DEGRADED','RECOVERING','COOLDOWN')
            AND COALESCE(s.chapter_ingestion_enabled,true)=true
            AND COALESCE(h.health,'HEALTHY') NOT IN ('BROKEN','DISABLED')
          ORDER BY s.id LIMIT 1
        )
        SELECT source FROM after_cursor
        UNION ALL SELECT source FROM wrapped
        LIMIT 1`, [lastSource]);
            const source = selected.rows[0]?.source || null;
            if (!source) {
                await client.query('COMMIT');
                return { source: null, admitted: 0, revived: 0 };
            }
            const mappingCursorKey = `${P0_MONITOR_COVERAGE_MAPPING_CURSOR_PREFIX}${source}`;
            let mappingCursor = await client.query(`
        SELECT value FROM settings WHERE key=$1 FOR UPDATE`, [mappingCursorKey]);
            if (!mappingCursor.rows[0]) {
                await client.query(`INSERT INTO settings(key,value) VALUES($1,'')
          ON CONFLICT(key) DO NOTHING`, [mappingCursorKey]);
                mappingCursor = await client.query(`
          SELECT value FROM settings WHERE key=$1 FOR UPDATE`, [mappingCursorKey]);
            }
            const lastSourceWorkId = mappingCursor.rows[0]?.value || '';
            // Inspect a small deterministic slice, not only currently-missing
            // rows. Advancing over healthy rows is what makes a 30k catalogue stay
            // bounded after coverage reaches 100%.
            const scanLimit = Math.min(512, maximum * P0_MONITOR_COVERAGE_SCAN_MULTIPLIER);
            const loadMappingWindow = async (after) => client.query(`
        SELECT m.source_work_id, m.work_id::text AS work_id
        FROM importer_work_mappings m
        JOIN works w ON w.id=m.work_id
        WHERE m.source=$1 AND m.source_work_id>$2
          AND m.work_id IS NOT NULL
          AND m.sync_status IN ('WAITING_ADMISSION','SYNCED','ACTIVE','PENDING')
          AND COALESCE(w.status,'ONGOING') <> 'CANCELLED'
          AND NOT (m.source='nexus' AND m.source_work_id !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')
        ORDER BY m.source, m.source_work_id LIMIT $3`, [source, after, scanLimit]);
            const scanRows = await loadMappingWindow(lastSourceWorkId);
            const wrapped = scanRows.rowCount === 0 && lastSourceWorkId !== '';
            const effectiveAfter = wrapped ? '' : lastSourceWorkId;
            const effectiveScanRows = wrapped ? await loadMappingWindow(effectiveAfter) : scanRows;
            const lastScanned = effectiveScanRows.rows.at(-1)?.source_work_id || '';
            let admitted = 0;
            let revived = 0;
            let nextMappingCursor = lastScanned;
            if (effectiveScanRows.rows.length > 0) {
                const lookupKeys = [];
                for (const row of effectiveScanRows.rows) {
                    const swid = String(row.source_work_id);
                    lookupKeys.push(`lite:catalog:${source}:${swid}`, `lite:p0-monitor:${source}:${swid}`);
                }
                const existingRows = await client.query(`SELECT id, idempotency_key, state, monitoring, priority
            FROM importer_lite_tasks
            WHERE idempotency_key = ANY($1::text[])`, [lookupKeys]);
                const existingByKey = new Map(existingRows.rows.map((r) => [String(r.idempotency_key), r]));
                const missing = [];
                for (const row of effectiveScanRows.rows) {
                    const sourceWorkId = String(row.source_work_id);
                    const workId = String(row.work_id);
                    const catalogTask = existingByKey.get(`lite:catalog:${source}:${sourceWorkId}`);
                    const monitorTask = existingByKey.get(`lite:p0-monitor:${source}:${sourceWorkId}`);
                    const hasActiveMonitor = Boolean((monitorTask && monitorTask.monitoring && (monitorTask.state === 'READY' || monitorTask.state === 'RUNNING' || monitorTask.state === 'RETRY_WAIT'))
                        || (catalogTask && catalogTask.monitoring && (catalogTask.state === 'READY' || catalogTask.state === 'RUNNING' || catalogTask.state === 'RETRY_WAIT')));
                    if (hasActiveMonitor)
                        continue;
                    // Never race an actively running catalog task.
                    if (catalogTask && catalogTask.state === 'RUNNING')
                        continue;
                    // Pending STAFF/P0/P1 work must retain its priority before this
                    // producer can reuse the catalog identity as a P2 observation.
                    if (catalogTask && Number(catalogTask.priority) < 3
                        && (catalogTask.state === 'READY' || catalogTask.state === 'RETRY_WAIT'))
                        continue;
                    if (monitorTask) {
                        missing.push({ sourceWorkId, workId, reviveTaskId: String(monitorTask.id) });
                    }
                    else if (catalogTask && !(catalogTask.state === 'BLOCKED' && !catalogTask.monitoring)) {
                        missing.push({ sourceWorkId, workId, reviveTaskId: String(catalogTask.id) });
                    }
                    else {
                        // Preserve a blocked onboarding task and its diagnostic reason;
                        // its state cannot be revived by the guarded UPDATE below.
                        missing.push({ sourceWorkId, workId, reviveTaskId: null });
                    }
                }
                const toProcess = missing.slice(0, maximum);
                // Never advance past uncovered rows in the same scan window when
                // missing.length exceeds maximum; resume right after the last enrolled
                // row so 100% of eligible mappings are enrolled on the first pass.
                nextMappingCursor = missing.length > maximum
                    ? toProcess.at(-1).sourceWorkId
                    : lastScanned;
                const enrolledAt = new Date().toISOString();
                const shouldStagger = !(process.env.NODE_ENV === 'test' || process.env.VITEST) && toProcess.length > 16;
                const toRevive = toProcess.filter((item) => item.reviveTaskId !== null);
                const toInsert = toProcess.filter((item) => item.reviveTaskId === null);
                if (toRevive.length > 0) {
                    const reopened = await client.query(`
            WITH input AS (
              SELECT * FROM unnest($1::uuid[], $2::uuid[], $3::text[], $4::jsonb[], $5::jsonb[], $6::int[])
                AS v(task_id, work_id, work_key, payload_patch, checkpoint_patch, offset_sec)
            )
            UPDATE importer_lite_tasks t SET
              work_id=i.work_id, work_key=i.work_key, priority=3, base_priority=3,
              state='READY', next_run_at=now() + make_interval(secs => i.offset_sec), attempts=0, lease_owner=NULL,
              lease_until=NULL, reason=NULL, monitoring=true,
              payload=t.payload || i.payload_patch,
              checkpoint=t.checkpoint || i.checkpoint_patch, updated_at=now()
            FROM input i
            WHERE t.id=i.task_id
              AND (
                (t.monitoring=false AND (t.state='DONE' OR (t.state IN ('READY','RETRY_WAIT') AND t.priority=3)))
                OR (t.monitoring=true AND t.state IN ('DONE','BLOCKED'))
              )
            RETURNING t.id`, [
                        toRevive.map((item) => item.reviveTaskId),
                        toRevive.map((item) => item.workId),
                        toRevive.map((item) => `work:${item.workId}`),
                        toRevive.map((item) => JSON.stringify({ source, sourceWorkId: item.sourceWorkId, p0MonitorCoverage: true })),
                        toRevive.map(() => JSON.stringify({ p0MonitorCoverageEnrolledAt: enrolledAt })),
                        toRevive.map((_, idx) => shouldStagger ? (idx % 20) * 30 : 0),
                    ]);
                    revived += reopened.rowCount || 0;
                }
                if (toInsert.length > 0) {
                    const inserted = await client.query(`
            INSERT INTO importer_lite_tasks
              (task_type,work_id,work_key,priority,base_priority,idempotency_key,next_run_at,max_attempts,payload,checkpoint,monitoring)
            SELECT 'CATALOG', v.work_id, v.work_key, 3, 3, v.idem_key, now() + make_interval(secs => v.offset_sec), 32, v.payload, v.checkpoint, true
            FROM unnest($1::uuid[], $2::text[], $3::text[], $4::jsonb[], $5::jsonb[], $6::int[])
              AS v(work_id, work_key, idem_key, payload, checkpoint, offset_sec)
            ON CONFLICT(idempotency_key) DO NOTHING
            RETURNING id`, [
                        toInsert.map((item) => item.workId),
                        toInsert.map((item) => `work:${item.workId}`),
                        toInsert.map((item) => `lite:p0-monitor:${source}:${item.sourceWorkId}`),
                        toInsert.map((item) => JSON.stringify({ source, sourceWorkId: item.sourceWorkId, p0MonitorCoverage: true })),
                        toInsert.map(() => JSON.stringify({ p0MonitorCoverageEnrolledAt: enrolledAt })),
                        toInsert.map((_, idx) => shouldStagger ? (idx % 20) * 30 : 0),
                    ]);
                    admitted += inserted.rowCount || 0;
                }
            }
            // Advance cursor to next provider when this provider has completed its current missing window.
            // Retaining the cursor on this provider while missing.length > maximum allows backlogged
            // providers to drain rapidly in consecutive bounded batches.
            if (effectiveScanRows.rows.length === 0 || (admitted === 0 && revived === 0) || (effectiveScanRows.rows.length > 0 && nextMappingCursor === lastScanned)) {
                await client.query(`UPDATE importer_lite_metadata_source_cursor
          SET last_source=$2,updated_at=now() WHERE cursor_name=$1`, [P0_MONITOR_COVERAGE_CURSOR, source]);
            }
            // `settings` is a legacy shared table.  Production intentionally has
            // only the key/value contract, whereas isolated test databases may add
            // an `updated_at` convenience column.  Coverage cursors must not turn
            // that optional test-only column into a P0 availability dependency.
            await client.query(`UPDATE settings SET value=$2 WHERE key=$1`, [mappingCursorKey, nextMappingCursor]);
            await client.query('COMMIT');
            return { source, admitted, revived };
        }
        catch (error) {
            await client.query('ROLLBACK').catch(() => { });
            throw error;
        }
        finally {
            client.release();
        }
    }
    /**
     * Admit at most two low-priority source-backed metadata reads at a time.
     * Selection rotates across sources, honors source cooldown/eligibility, and
     * records PENDING durably before inserting an idempotent catalog task.
     */
    async refillSourceMetadataEnrichment(maxPending = 2, sourceProbeLimit, preferredSource) {
        const client = await this.sourceMetadataDb.connect();
        try {
            await client.query('BEGIN');
            // A completed V4 is a durable handoff, not evidence that aliases were
            // discovered. Version five therefore reuses this carefully bounded
            // source lane to fetch every confirmed mapping once from its provider.
            // Before that handoff, retain the old two-seat v3 behaviour unchanged.
            const replay = await client.query(`SELECT value FROM settings
        WHERE key=$1 LIMIT 1`, [METADATA_REPLAY_V4_ENABLED_KEY]);
            const turbo = await client.query(`SELECT value FROM settings
        WHERE key=$1 LIMIT 1`, [METADATA_TURBO_EXCLUSIVE_MODE_KEY]);
            const isTurbo = process.env.METADATA_TURBO_MODE === 'true' || turbo.rows[0]?.value === 'true' || (() => {
                try {
                    return JSON.parse(turbo.rows[0]?.value || '{}')?.enabled === true;
                }
                catch {
                    return false;
                }
            })();
            const coverageVersion = replay.rows[0]?.value === 'complete' ? METADATA_ALIAS_COVERAGE_V5_VERSION : 3;
            const isCoverageV5 = coverageVersion === METADATA_ALIAS_COVERAGE_V5_VERSION;
            const defaultProbeLimit = isCoverageV5 ? SOURCE_METADATA_SOURCE_PROBE_LIMIT : 8;
            const probeLimit = Math.max(1, Math.min(defaultProbeLimit, sourceProbeLimit ?? defaultProbeLimit));
            const ceiling = Math.max(1, Math.min(isCoverageV5 ? (isTurbo ? 48 : 8) : 2, maxPending));
            const sourceTaskPrefix = `lite:metadata:source:v${coverageVersion}:`;
            const stateEligible = isCoverageV5
                ? `(status.source IS NULL OR status.version<${METADATA_ALIAS_COVERAGE_V5_VERSION}
            OR status.aliases_state='PENDING' OR status.tags_state='PENDING'
            OR ((status.aliases_state='EXTERNAL_BLOCKED' OR status.tags_state='EXTERNAL_BLOCKED')
              AND status.attempt_count<4 AND COALESCE(status.next_retry_at,'-infinity'::timestamptz)<=now()))`
                : `(status.source IS NULL OR status.aliases_state='PENDING' OR status.tags_state='PENDING'
            OR (status.version<3 AND (status.aliases_state='PARSER_ERROR' OR status.tags_state='PARSER_ERROR'))
            OR ((status.aliases_state='EXTERNAL_BLOCKED' OR status.tags_state='EXTERNAL_BLOCKED')
              AND COALESCE(status.next_retry_at,'-infinity'::timestamptz)<=now()))`;
            const pending = await client.query(`SELECT count(*)::int AS count FROM (
        SELECT 1
        FROM importer_lite_tasks
        WHERE task_type='CATALOG'
          AND state IN ('READY','RUNNING','RETRY_WAIT')
          AND (state='READY'
          -- A rolling deploy can leave the old owner in RUNNING until the
          -- generic reconciler receives a main-pool turn. That expired lease
          -- cannot execute and must not consume the one protected P2 seat.
          OR (state='RUNNING' AND lease_until>now())
          OR (state='RETRY_WAIT' AND next_run_at<=now()+interval '30 seconds'))
          AND payload ? 'metadataSourceRefresh'
        LIMIT $1
      ) AS bounded_pending`, [ceiling]);
            if (Number(pending.rows[0]?.count || 0) >= ceiling) {
                await client.query('COMMIT');
                return 0;
            }
            const admissionLimit = ceiling - Number(pending.rows[0]?.count || 0);
            const priorityCandidates = [];
            // Keep one of the protected V5 seats focused on a genuinely popular
            // aliasless public work. The published/views index makes this a bounded
            // lookup, while the remaining seats retain the fair per-source cursor
            // so the long tail cannot starve.
            if (isCoverageV5 && admissionLimit > 0) {
                await client.query('SAVEPOINT v5_popular_alias_candidate');
                try {
                    await client.query(`SET LOCAL statement_timeout = '2500ms'`);
                    const priority = await client.query(`
            WITH popular AS (
              -- Read a small leading window through idx_works_published_views
              -- before touching mappings. Ranking the full joined catalog was
              -- slow enough to silently skip popular aliasless titles.
              SELECT w.id,w.views_total
              FROM works w
              WHERE w.published=true AND COALESCE(cardinality(w.aliases),0)=0
              ORDER BY w.views_total DESC LIMIT 128
            ), candidates AS (
              SELECT m.source,m.source_work_id,m.work_id,popular.views_total,
              true AS selected_aliasless,
              NOT EXISTS (SELECT 1 FROM work_tags wt WHERE wt.work_id=m.work_id AND COALESCE(wt.system_generated,false)=false) AS selected_tagless
              FROM popular
              CROSS JOIN LATERAL (
                SELECT m.source,m.source_work_id,m.work_id
                FROM importer_work_mappings m
                JOIN importer_sources s ON s.id=m.source
                LEFT JOIN importer_lite_source_health health ON health.source=s.id
                LEFT JOIN importer_lite_metadata_enrichment status
                  ON status.source=m.source AND status.source_work_id=m.source_work_id
                WHERE m.work_id=popular.id
                  AND m.sync_status IN ('WAITING_ADMISSION','SYNCED','ACTIVE')
                  AND s.enabled=true AND ${sourceExecutionEligibilitySql('s')}
                  AND COALESCE(health.health,'HEALTHY') NOT IN ('BROKEN','DISABLED')
                  AND (health.cooldown_until IS NULL OR health.cooldown_until<=now())
                  AND ${stateEligible}
                  AND NOT EXISTS (SELECT 1 FROM importer_lite_tasks task
                    WHERE task.idempotency_key IN (
                      'lite:metadata:source:v3:' || m.source || ':' || m.source_work_id,
                      'lite:metadata:source:v5:' || m.source || ':' || m.source_work_id
                    ) AND task.state IN ('READY','RUNNING','RETRY_WAIT'))
                ORDER BY m.source,m.source_work_id LIMIT 1
              ) m
            )
            SELECT source,source_work_id,work_id,selected_aliasless,selected_tagless
            FROM candidates ORDER BY views_total DESC,source,source_work_id LIMIT 1`);
                    await client.query('RELEASE SAVEPOINT v5_popular_alias_candidate');
                    if (priority.rows[0])
                        priorityCandidates.push(priority.rows[0]);
                }
                catch {
                    await client.query('ROLLBACK TO SAVEPOINT v5_popular_alias_candidate');
                }
                finally {
                    await client.query(`SET LOCAL statement_timeout = '12000ms'`);
                }
            }
            // Advance the source frontier even if every mapping in this small
            // horizon was already classified.  Otherwise an exhausted provider at
            // the front would be revisited forever and hide healthy providers that
            // still have pending mappings.  The row lock makes this crash-safe and
            // keeps a replacement refill from racing the regular producer tick.
            const cursor = await client.query(`
        SELECT last_source FROM importer_lite_metadata_source_cursor
        WHERE cursor_name='source_refetch' FOR UPDATE`);
            const lastSource = cursor.rows[0]?.last_source || null;
            // Do not ORDER BY a cursor CASE across every source. On YSQL that made
            // the EXISTS below run once for every provider (32 in production) just
            // to choose two. The two ordered index walks preserve the same circular
            // cursor semantics: take sources after the cursor, then wrap only when
            // the first walk did not fill this bounded horizon.
            // Do not preflight mappings here: even an indexed EXISTS is multiplied
            // by every provider under a contended YSQL cluster. The following
            // per-source, index-backed candidate window is the authoritative check.
            const eligibleSourceSelect = (cursorPredicate) => `
        SELECT s.id AS source
        FROM importer_sources s
        LEFT JOIN importer_lite_source_health health ON health.source=s.id
        WHERE s.enabled=true AND ${cursorPredicate} AND ${sourceExecutionEligibilitySql('s')}
          AND COALESCE(health.health,'HEALTHY') NOT IN ('BROKEN','DISABLED')
          AND (health.cooldown_until IS NULL OR health.cooldown_until<=now())
        ORDER BY s.id LIMIT $2`;
            // A source that just completed a healthy refetch is known to have paid
            // the admission and adapter costs already. Keep it in this *single*
            // next bounded selection so a deep, productive source is not revisited
            // only after a full cursor lap through providers whose historical rows
            // are already classified. The normal cursor probe remains alongside it:
            // this is a small throughput hint, never a replacement for durable
            // source fairness and it remains safe to lose on restart.
            const preferred = typeof preferredSource === 'string' && preferredSource.trim()
                ? await client.query(`
            SELECT s.id AS source
            FROM importer_sources s
            LEFT JOIN importer_lite_source_health health ON health.source=s.id
            WHERE s.id=$1 AND s.enabled=true AND ${sourceExecutionEligibilitySql('s')}
              AND COALESCE(health.health,'HEALTHY') NOT IN ('BROKEN','DISABLED')
              AND (health.cooldown_until IS NULL OR health.cooldown_until<=now())
            LIMIT 1`, [preferredSource.trim()])
                : { rows: [] };
            // The fair cursor can legitimately cross several fully-covered
            // providers before reaching a source with historical V5 debt. Add one
            // indexed backlog hint *after* the cursor slice: it fills otherwise
            // idle seats but can never displace the normal rotating sources. The
            // per-source candidate query below remains the authoritative active-
            // mapping validation; duplicating that EXISTS here defeated the narrow
            // backlog index under YSQL pressure.
            const backlogPreferred = isCoverageV5
                ? await client.query(`
            SELECT status.source
            FROM importer_lite_metadata_enrichment status
            JOIN importer_sources s ON s.id=status.source
            JOIN works w ON w.id=status.work_id AND w.published=true
            LEFT JOIN importer_lite_source_health health ON health.source=s.id
            WHERE status.version<${METADATA_ALIAS_COVERAGE_V5_VERSION}
              AND s.enabled=true AND ${sourceExecutionEligibilitySql('s')}
              AND COALESCE(health.health,'HEALTHY') NOT IN ('BROKEN','DISABLED')
              AND (health.cooldown_until IS NULL OR health.cooldown_until<=now())
            ORDER BY status.updated_at,status.source,status.source_work_id
            LIMIT 1`)
                : { rows: [] };
            const sourceProbe = await client.query(`
        WITH after_cursor AS (
          ${eligibleSourceSelect(lastSource ? 's.id>$1::text' : '$1::text IS NULL')}
        ), wrapped AS (
          ${eligibleSourceSelect(lastSource ? 's.id<=$1::text' : '$1::text IS NOT NULL')}
        )
        SELECT source FROM after_cursor
        UNION ALL
        SELECT source FROM wrapped
        LIMIT $2`, [lastSource, probeLimit]);
            const probeSources = [...new Set([
                    ...preferred.rows.map((row) => String(row.source)),
                    ...sourceProbe.rows.map((row) => String(row.source)),
                    ...backlogPreferred.rows.map((row) => String(row.source)),
                ])];
            if (!probeSources.length) {
                await client.query('COMMIT');
                return 0;
            }
            const nextSourceCursor = sourceProbe.rows.at(-1)?.source || probeSources.at(-1);
            await client.query(`UPDATE importer_lite_metadata_source_cursor
        SET last_source=$2,updated_at=now() WHERE cursor_name=$1`, ['source_refetch', nextSourceCursor]);
            // A single cross-source/LATERAL ranking query regularly crossed YSQL's
            // read timeout once its task history grew. Keep the same bounded
            // per-source cursor semantics, but issue one indexed source window at
            // a time. A pathological provider is skipped by a savepoint instead of
            // stalling every other V5 candidate behind it.
            const candidates = [];
            const candidateCapacity = Math.max(1, admissionLimit - priorityCandidates.length);
            const perSourceLimit = isTurbo ? candidateCapacity : 1;
            for (const source of probeSources) {
                const needed = perSourceLimit;
                await client.query('SAVEPOINT v5_source_candidate');
                try {
                    await client.query(`SET LOCAL statement_timeout = '6000ms'`);
                    if (!isCoverageV5) {
                        const v3Candidate = await client.query(`
              WITH history AS (
                SELECT task.created_at AS last_admitted_at,
                  task.payload->>'sourceWorkId' AS last_source_work_id
                FROM importer_lite_tasks task
                WHERE task.task_type='CATALOG' AND task.payload ? 'metadataSourceRefresh'
                  AND task.payload->>'source'=$1
                ORDER BY task.created_at DESC LIMIT 1
              ), productivity AS (
                SELECT
                  EXISTS (SELECT 1 FROM importer_lite_tasks rich
                    WHERE rich.task_type='CATALOG' AND rich.state='DONE'
                      AND rich.payload ? 'metadataSourceRefresh' AND rich.payload->>'source'=$1
                      AND rich.checkpoint->>'aliasDelta' ~ '^[1-9][0-9]*$') AS alias_productive_source,
                  EXISTS (SELECT 1 FROM importer_lite_tasks rich
                    WHERE rich.task_type='CATALOG' AND rich.state='DONE'
                      AND rich.payload ? 'metadataSourceRefresh' AND rich.payload->>'source'=$1
                      AND rich.checkpoint->>'tagDelta' ~ '^[1-9][0-9]*$') AS tag_productive_source
              ), after_cursor AS (
                SELECT m.source,m.source_work_id,m.work_id,
                  COALESCE(cardinality(w.aliases),0)=0 AS selected_aliasless,
                  NOT EXISTS (SELECT 1 FROM work_tags wt WHERE wt.work_id=m.work_id AND COALESCE(wt.system_generated,false)=false) AS selected_tagless,
                  0 AS cursor_turn
                FROM importer_work_mappings m
                JOIN works w ON w.id=m.work_id AND w.published=true
                LEFT JOIN importer_lite_metadata_enrichment status
                  ON status.source=m.source AND status.source_work_id=m.source_work_id
                LEFT JOIN history ON true
                WHERE m.source=$1 AND m.work_id IS NOT NULL
                  AND m.sync_status IN ('WAITING_ADMISSION','SYNCED','ACTIVE')
                  AND (history.last_source_work_id IS NULL OR m.source_work_id>history.last_source_work_id)
                  AND ${stateEligible}
                ORDER BY m.source_work_id LIMIT 32
              ), wrapped AS (
                SELECT m.source,m.source_work_id,m.work_id,
                  COALESCE(cardinality(w.aliases),0)=0 AS selected_aliasless,
                  NOT EXISTS (SELECT 1 FROM work_tags wt WHERE wt.work_id=m.work_id AND COALESCE(wt.system_generated,false)=false) AS selected_tagless,
                  1 AS cursor_turn
                FROM importer_work_mappings m
                JOIN works w ON w.id=m.work_id AND w.published=true
                LEFT JOIN importer_lite_metadata_enrichment status
                  ON status.source=m.source AND status.source_work_id=m.source_work_id
                JOIN history ON history.last_source_work_id IS NOT NULL
                WHERE m.source=$1 AND m.work_id IS NOT NULL
                  AND m.sync_status IN ('WAITING_ADMISSION','SYNCED','ACTIVE')
                  AND m.source_work_id<=history.last_source_work_id
                  AND ${stateEligible}
                ORDER BY m.source_work_id LIMIT 32
              )
              SELECT candidate.source,candidate.source_work_id,candidate.work_id,
                candidate.selected_aliasless,candidate.selected_tagless,
                productivity.alias_productive_source,productivity.tag_productive_source,
                history.last_admitted_at
              FROM (SELECT * FROM after_cursor UNION ALL SELECT * FROM wrapped) candidate
              CROSS JOIN productivity
              LEFT JOIN history ON true
              ORDER BY candidate.selected_aliasless DESC,candidate.selected_tagless DESC,
                candidate.cursor_turn,candidate.source_work_id
              LIMIT $2
            `, [source, needed]);
                        for (const r of v3Candidate.rows)
                            candidates.push(r);
                    }
                    else {
                        const hist = await client.query(`
              SELECT task.payload->>'sourceWorkId' AS last_source_work_id
              FROM importer_lite_tasks task
              WHERE task.task_type='CATALOG' AND task.payload ? 'metadataSourceRefresh'
                AND task.payload->>'source'=$1
              ORDER BY task.created_at DESC LIMIT 1
            `, [source]);
                        const lastSourceWorkId = hist.rows[0]?.last_source_work_id || null;
                        const fwd = await client.query(`
              SELECT m.source,m.source_work_id,m.work_id,
                COALESCE(cardinality(w.aliases),0)=0 AS selected_aliasless,
                false AS selected_tagless
              FROM importer_work_mappings m
              JOIN works w ON w.id=m.work_id AND w.published=true
              LEFT JOIN importer_lite_metadata_enrichment status
                ON status.source=m.source AND status.source_work_id=m.source_work_id
              WHERE m.source=$1 AND ($2::text IS NULL OR m.source_work_id > $2::text) AND m.work_id IS NOT NULL
                AND m.sync_status IN ('WAITING_ADMISSION','SYNCED','ACTIVE')
                AND ${stateEligible}
              ORDER BY m.source_work_id LIMIT $3
            `, [source, lastSourceWorkId, needed]);
                        for (const r of fwd.rows)
                            candidates.push(r);
                        if (fwd.rows.length < needed && lastSourceWorkId) {
                            const wrapNeeded = needed - fwd.rows.length;
                            const wrap = await client.query(`
                SELECT m.source,m.source_work_id,m.work_id,
                  COALESCE(cardinality(w.aliases),0)=0 AS selected_aliasless,
                  false AS selected_tagless
                FROM importer_work_mappings m
                JOIN works w ON w.id=m.work_id AND w.published=true
                LEFT JOIN importer_lite_metadata_enrichment status
                  ON status.source=m.source AND status.source_work_id=m.source_work_id
                WHERE m.source=$1 AND m.source_work_id <= $2 AND m.work_id IS NOT NULL
                  AND m.sync_status IN ('WAITING_ADMISSION','SYNCED','ACTIVE')
                  AND ${stateEligible}
                ORDER BY m.source_work_id LIMIT $3
              `, [source, lastSourceWorkId, wrapNeeded]);
                            for (const r of wrap.rows)
                                candidates.push(r);
                        }
                    }
                    await client.query('RELEASE SAVEPOINT v5_source_candidate');
                }
                catch {
                    await client.query('ROLLBACK TO SAVEPOINT v5_source_candidate');
                }
                finally {
                    await client.query(`SET LOCAL statement_timeout = '12000ms'`);
                }
            }
            candidates.sort((left, right) => {
                const leftAdmitted = left.last_admitted_at ? new Date(left.last_admitted_at).getTime() : -1;
                const rightAdmitted = right.last_admitted_at ? new Date(right.last_admitted_at).getTime() : -1;
                return Number(right.selected_aliasless) - Number(left.selected_aliasless)
                    || Number(Boolean(right.alias_productive_source)) - Number(Boolean(left.alias_productive_source))
                    || Number(right.selected_tagless) - Number(left.selected_tagless)
                    || Number(Boolean(right.tag_productive_source)) - Number(Boolean(left.tag_productive_source))
                    || (leftAdmitted - rightAdmitted)
                    || String(left.source).localeCompare(String(right.source));
            });
            const allDeduplicatedCandidates = [...new Map([...priorityCandidates, ...candidates].map((candidate) => [`${candidate.source}:${candidate.source_work_id}`, candidate])).values()];
            // Candidate windows are deliberately source-indexed.  Correlating an
            // active-task lookup for every mapping in those windows made YSQL scan
            // the queue until its 2.5 s guard expired, despite viable V5 debt.
            // The idempotency index makes one bounded lookup for this tiny admitted
            // batch both faster and equivalent: active v3/v5 siblings are excluded
            // before INSERT, and the unique key remains the final race-safe fence.
            const candidateTaskKeys = allDeduplicatedCandidates.flatMap((candidate) => {
                const source = String(candidate.source);
                const sourceWorkId = String(candidate.source_work_id);
                return [
                    `lite:metadata:source:v3:${source}:${sourceWorkId}`,
                    `lite:metadata:source:v5:${source}:${sourceWorkId}`,
                ];
            });
            const activeCandidateKeys = candidateTaskKeys.length
                ? await client.query(`SELECT idempotency_key
            FROM importer_lite_tasks
            WHERE idempotency_key=ANY($1::text[])
              AND state IN ('READY','RUNNING','RETRY_WAIT')`, [candidateTaskKeys])
                : { rows: [] };
            const activeCandidateKeySet = new Set(activeCandidateKeys.rows.map((row) => row.idempotency_key));
            const insertableCandidates = allDeduplicatedCandidates.filter((candidate) => {
                const source = String(candidate.source);
                const sourceWorkId = String(candidate.source_work_id);
                return !activeCandidateKeySet.has(`lite:metadata:source:v3:${source}:${sourceWorkId}`)
                    && !activeCandidateKeySet.has(`lite:metadata:source:v5:${source}:${sourceWorkId}`);
            }).slice(0, admissionLimit);
            let admitted = 0;
            for (const row of insertableCandidates) {
                const source = String(row.source);
                const sourceWorkId = String(row.source_work_id);
                const workId = String(row.work_id);
                await client.query(`INSERT INTO importer_lite_metadata_enrichment(source,source_work_id,work_id)
          VALUES($1,$2,$3::uuid) ON CONFLICT(source,source_work_id) DO UPDATE SET
            work_id=excluded.work_id,updated_at=now()`, [source, sourceWorkId, workId]);
                const workKey = `work:${workId}`;
                const inserted = await client.query(`INSERT INTO importer_lite_tasks
          (task_type,work_id,work_key,priority,base_priority,idempotency_key,next_run_at,max_attempts,payload,checkpoint,monitoring)
          VALUES('CATALOG',$1::uuid,$2,3,3,$3,now(),32,$4::jsonb,'{}'::jsonb,false)
          ON CONFLICT(idempotency_key) DO NOTHING RETURNING work_key`, [
                    workId, workKey, `${sourceTaskPrefix}${source}:${sourceWorkId}`,
                    JSON.stringify({ source, sourceWorkId, metadataSourceRefresh: true, metadataEnrichmentVersion: coverageVersion }),
                ]);
                admitted += inserted.rowCount || 0;
            }
            await client.query('COMMIT');
            return admitted;
        }
        catch (error) {
            await client.query('ROLLBACK').catch(() => { });
            throw error;
        }
        finally {
            client.release();
        }
    }
    async recordSourceMetadataEnrichment(input) {
        const client = await this.sourceMetadataDb.connect();
        try {
            await client.query('BEGIN');
            if (input.rawMetadata && Object.keys(input.rawMetadata).length) {
                await client.query(`UPDATE importer_work_mappings
          SET metadata=COALESCE(metadata,'{}'::jsonb) || $3::jsonb
          WHERE source=$1 AND source_work_id=$2 AND work_id=$4::uuid`, [input.source, input.sourceWorkId, JSON.stringify(input.rawMetadata), input.workId]);
            }
            await client.query(`INSERT INTO importer_lite_metadata_enrichment
        (source,source_work_id,work_id,aliases_state,tags_state,aliases_added,tags_added,attempt_count,
        last_checked_at,next_retry_at,last_error,version,updated_at)
        VALUES($1,$2,$3::uuid,$4,$5,$6,$7,1,now(),$8,$9,$10,now())
        ON CONFLICT(source,source_work_id) DO UPDATE SET work_id=excluded.work_id,
          aliases_state=excluded.aliases_state,tags_state=excluded.tags_state,
          aliases_added=importer_lite_metadata_enrichment.aliases_added+excluded.aliases_added,
          tags_added=importer_lite_metadata_enrichment.tags_added+excluded.tags_added,
          attempt_count=CASE WHEN importer_lite_metadata_enrichment.version<excluded.version THEN 1
            ELSE importer_lite_metadata_enrichment.attempt_count+1 END,
          last_checked_at=now(),next_retry_at=excluded.next_retry_at,last_error=excluded.last_error,
        version=excluded.version,updated_at=now()
        WHERE excluded.version>=importer_lite_metadata_enrichment.version`, [input.source, input.sourceWorkId, input.workId, input.aliasesState, input.tagsState,
                Math.max(0, input.aliasesAdded || 0), Math.max(0, input.tagsAdded || 0), input.nextRetryAt || null, input.error || null,
                Math.max(3, Math.trunc(Number(input.version) || 3))]);
            await client.query('COMMIT');
        }
        catch (error) {
            await client.query('ROLLBACK').catch(() => { });
            throw error;
        }
        finally {
            client.release();
        }
    }
    /** Commit a successful source refresh, its durable classification, source
     * health and task completion together. This avoids two YSQL transactions
     * between one external metadata fetch and the next available task. */
    async completeSourceMetadataRefresh(task, workerId, input, checkpoint) {
        const transactionStartedAt = performance.now();
        const client = await this.sourceMetadataDb.connect();
        try {
            await client.query('BEGIN');
            await client.query(`UPDATE importer_work_mappings
        SET metadata=COALESCE(metadata,'{}'::jsonb) || $3::jsonb,last_synced_at=now()
        WHERE source=$1 AND source_work_id=$2 AND work_id=$4::uuid`, [input.source, input.sourceWorkId, JSON.stringify(input.rawMetadata), input.workId]);
            await client.query(`INSERT INTO importer_lite_metadata_enrichment
        (source,source_work_id,work_id,aliases_state,tags_state,aliases_added,tags_added,attempt_count,
        last_checked_at,next_retry_at,last_error,version,updated_at)
        VALUES($1,$2,$3::uuid,$4,$5,$6,$7,1,now(),NULL,NULL,$8,now())
        ON CONFLICT(source,source_work_id) DO UPDATE SET work_id=excluded.work_id,
          aliases_state=excluded.aliases_state,tags_state=excluded.tags_state,
          aliases_added=importer_lite_metadata_enrichment.aliases_added+excluded.aliases_added,
          tags_added=importer_lite_metadata_enrichment.tags_added+excluded.tags_added,
          attempt_count=CASE WHEN importer_lite_metadata_enrichment.version<excluded.version THEN 1
            ELSE importer_lite_metadata_enrichment.attempt_count+1 END,
          last_checked_at=now(),next_retry_at=NULL,last_error=NULL,
          version=excluded.version,updated_at=now()
        WHERE excluded.version>=importer_lite_metadata_enrichment.version`, [input.source, input.sourceWorkId, input.workId, input.aliasesState, input.tagsState,
                Math.max(0, input.aliasesAdded), Math.max(0, input.tagsAdded), Math.max(3, Math.trunc(Number(input.version) || 3))]);
            await client.query(`INSERT INTO importer_lite_source_health(source,health,cooldown_until,consecutive_failures,last_error)
        VALUES($1,'HEALTHY',NULL,0,NULL)
        ON CONFLICT(source) DO UPDATE SET health='HEALTHY',cooldown_until=NULL,
          consecutive_failures=0,last_error=NULL,updated_at=now()
        WHERE NOT (importer_lite_source_health.health='RATE_LIMITED'
          AND importer_lite_source_health.cooldown_until>now())`, [input.source]);
            const completed = await client.query(`UPDATE importer_lite_tasks
        SET state='DONE',reason=NULL,checkpoint=checkpoint || $3::jsonb,work_id=$4::uuid,
          attempts=0,lease_owner=NULL,lease_until=NULL,updated_at=now()
        WHERE id=$1::uuid AND state='RUNNING' AND lease_owner=$2 AND lease_until>now()
        RETURNING id`, [task.id, workerId, JSON.stringify({
                    ...checkpoint,
                    metadataTiming: { ...(checkpoint.metadataTiming || {}),
                        metadataFinalizeBeforeCommitMs: Math.round(performance.now() - transactionStartedAt) },
                }), input.workId]);
            if (completed.rowCount !== 1)
                throw new Error('METADATA_TASK_LEASE_LOST');
            await this.refreshReadyWork(client, task.workKey);
            await client.query('COMMIT');
            return true;
        }
        catch (error) {
            await client.query('ROLLBACK').catch(() => { });
            throw error;
        }
        finally {
            client.release();
        }
    }
    /**
     * Incrementally replays explicit aliases/genres already stored on legacy
     * mappings.  It is deliberately a tiny P2 producer: no chapter is claimed,
     * no source is contacted, and at most two catalog tasks may be outstanding.
     * The task itself performs the normal protected metadata merge, preserving
     * manual fields and tag provenance rules.
     */
    async refillStoredMetadataEnrichment(maxPending = 2) {
        const ceiling = Math.max(1, Math.min(4, maxPending));
        const metaDb = this.sourceMetadataDb || this.db;
        const client = await metaDb.connect();
        try {
            await client.query('BEGIN');
            const pending = await client.query(`SELECT count(*)::int AS count FROM (
        SELECT 1 FROM importer_lite_tasks
        WHERE task_type='CATALOG' AND state IN ('READY','RUNNING','RETRY_WAIT')
          AND (payload->>'metadataEnrichmentVersion') IN ('2','3')
        LIMIT $1
      ) AS bounded_v23`, [ceiling]);
            if (Number(pending.rows[0]?.count || 0) >= ceiling) {
                await client.query('COMMIT');
                return 0;
            }
            // Select one source seat per turn. Use a recursive index skip-scan on
            // active mappings plus a bounded index-only per-source probe before
            // reading `m.metadata` JSONB blobs, and resolve source_history via an
            // indexed LATERAL lookup instead of scanning importer_lite_tasks.
            const candidate = await client.query(`WITH RECURSIVE distinct_sources AS (
        (SELECT m.source
         FROM importer_work_mappings m
         WHERE m.work_id IS NOT NULL
           AND m.sync_status IN ('WAITING_ADMISSION','SYNCED','ACTIVE')
         ORDER BY m.source ASC
         LIMIT 1)
        UNION ALL
        SELECT next_src.source
        FROM distinct_sources prev
        CROSS JOIN LATERAL (
          SELECT m.source
          FROM importer_work_mappings m
          WHERE m.work_id IS NOT NULL
            AND m.sync_status IN ('WAITING_ADMISSION','SYNCED','ACTIVE')
            AND m.source > prev.source
          ORDER BY m.source ASC
          LIMIT 1
        ) next_src
      ), active_v23_sources AS MATERIALIZED (
        SELECT DISTINCT t.payload->>'source' AS source
        FROM importer_lite_tasks t
        WHERE t.task_type='CATALOG' AND t.state IN ('READY','RUNNING','RETRY_WAIT')
          AND (t.payload->>'metadataEnrichmentVersion') IN ('2','3')
      ), candidates AS (
        SELECT cand.*
        FROM distinct_sources ds
        CROSS JOIN LATERAL (
          SELECT m.source,m.source_work_id,m.work_id,m.updated_at,m.id,evidence.alias_evidence,
            CASE WHEN evidence.legacy_pt_tags THEN 3 ELSE 2 END AS metadata_version,
            (COALESCE(array_length(w.aliases,1),0)=0 AND evidence.alias_evidence) AS missing_alias_evidence
          FROM (
            (SELECT m_idx.source, m_idx.source_work_id
             FROM importer_work_mappings m_idx
             WHERE m_idx.source=ds.source
               AND m_idx.work_id IS NOT NULL
               AND m_idx.sync_status IN ('WAITING_ADMISSION','SYNCED','ACTIVE')
               AND NOT EXISTS (
                 SELECT 1 FROM importer_lite_tasks t
                 WHERE t.idempotency_key='lite:metadata:v2:' || m_idx.source || ':' || m_idx.source_work_id
               )
             ORDER BY m_idx.source_work_id ASC
             LIMIT 16)
            UNION
            (SELECT m_idx.source, m_idx.source_work_id
             FROM importer_work_mappings m_idx
             WHERE ds.source='mangotoons'
               AND m_idx.source=ds.source
               AND m_idx.work_id IS NOT NULL
               AND m_idx.sync_status IN ('WAITING_ADMISSION','SYNCED','ACTIVE')
               AND NOT EXISTS (
                 SELECT 1 FROM importer_lite_tasks t
                 WHERE t.idempotency_key='lite:metadata:v3:' || m_idx.source || ':' || m_idx.source_work_id
               )
             ORDER BY m_idx.source_work_id ASC
             LIMIT 16)
          ) idx
          JOIN importer_work_mappings m ON m.source=idx.source AND m.source_work_id=idx.source_work_id
          JOIN works w ON w.id=m.work_id
          CROSS JOIN LATERAL (
            SELECT
              EXISTS (
                SELECT 1 FROM jsonb_each(m.metadata) AS metadata_entry(key,value)
                WHERE metadata_entry.key = ANY(ARRAY['alternativeTitles','alternative_titles','aliases','alternativeNames','alternative_names',
                                                      'originalTitle','original_title','nativeTitle','native_title','englishTitle','english_title',
                                                      'romanizedTitle','romanized_title','genres','tags','categories']::text[])
                  AND CASE jsonb_typeof(metadata_entry.value)
                    WHEN 'array' THEN jsonb_array_length(metadata_entry.value)>0
                    WHEN 'string' THEN COALESCE(NULLIF(lower(btrim(metadata_entry.value #>> '{}')),''),'') NOT IN ('[]','{}','null')
                    WHEN 'object' THEN metadata_entry.value<>'{}'::jsonb
                    ELSE false
                  END
              ) AS metadata_evidence,
              EXISTS (
                SELECT 1 FROM jsonb_each(m.metadata) AS alias_entry(key,value)
                WHERE alias_entry.key = ANY(ARRAY['alternativeTitles','alternative_titles','aliases','alternativeNames','alternative_names',
                                                  'originalTitle','original_title','nativeTitle','native_title','englishTitle','english_title',
                                                  'romanizedTitle','romanized_title']::text[])
                  AND CASE jsonb_typeof(alias_entry.value)
                    WHEN 'array' THEN jsonb_array_length(alias_entry.value)>0
                    WHEN 'string' THEN COALESCE(NULLIF(lower(btrim(alias_entry.value #>> '{}')),''),'') NOT IN ('[]','{}','null')
                    ELSE false
                  END
              ) AS alias_evidence,
              EXISTS (
                SELECT 1
                FROM jsonb_each(m.metadata) AS tag_field(key,value)
                CROSS JOIN LATERAL jsonb_array_elements(
                  CASE WHEN jsonb_typeof(tag_field.value)='array' THEN tag_field.value ELSE '[]'::jsonb END
                ) AS tag_entry(value)
                WHERE tag_field.key=ANY(ARRAY['genres','tags','categories']::text[])
                  AND jsonb_typeof(tag_entry.value)='object'
                  AND NULLIF(btrim(tag_entry.value->>'nome'),'') IS NOT NULL
              ) AS legacy_pt_tags
          ) evidence
          WHERE m.work_id IS NOT NULL AND w.published=true
            AND m.sync_status IN ('WAITING_ADMISSION','SYNCED','ACTIVE')
            AND evidence.metadata_evidence
            AND NOT EXISTS (
              SELECT 1 FROM importer_lite_tasks t
              WHERE t.idempotency_key='lite:metadata:v' || CASE WHEN evidence.legacy_pt_tags THEN '3' ELSE '2' END
                || ':' || m.source || ':' || m.source_work_id
            )
          ORDER BY (COALESCE(array_length(w.aliases,1),0)=0 AND evidence.alias_evidence) DESC,
            evidence.alias_evidence DESC, m.updated_at, m.id
          LIMIT 1
        ) cand
        WHERE NOT EXISTS (SELECT 1 FROM active_v23_sources a WHERE a.source = ds.source)
      )
      SELECT candidates.source,candidates.source_work_id,candidates.work_id,candidates.metadata_version
      FROM candidates
      LEFT JOIN LATERAL (
        SELECT max(t.updated_at) AS enriched_at
        FROM importer_lite_tasks t
        WHERE t.payload ? 'metadataEnrichment'
          AND (t.payload->>'source') = candidates.source
      ) source_history ON true
      ORDER BY candidates.missing_alias_evidence DESC,
        source_history.enriched_at NULLS FIRST,candidates.updated_at,candidates.source LIMIT 1`);
            const row = candidate.rows[0];
            if (!row) {
                await client.query('COMMIT');
                return 0;
            }
            const idempotencyKey = `lite:metadata:v${row.metadata_version}:${row.source}:${row.source_work_id}`;
            const inserted = await client.query(`INSERT INTO importer_lite_tasks
        (task_type,work_id,work_key,priority,base_priority,idempotency_key,next_run_at,max_attempts,payload,checkpoint,monitoring)
        VALUES ('CATALOG',$1::uuid,$2,$3,$3,$4,now(),4,$5::jsonb,'{}'::jsonb,false)
        ON CONFLICT(idempotency_key) DO NOTHING RETURNING work_key`, [
                row.work_id, `work:${row.work_id}`, LITE_PRIORITY_RANK.P2, idempotencyKey,
                JSON.stringify({ source: row.source, sourceWorkId: row.source_work_id, metadataEnrichment: true, metadataEnrichmentVersion: row.metadata_version }),
            ]);
            if (inserted.rowCount)
                await this.refreshReadyWork(client, inserted.rows[0].work_key);
            await client.query('COMMIT');
            return inserted.rowCount || 0;
        }
        catch (error) {
            await client.query('ROLLBACK').catch(() => { });
            throw error;
        }
        finally {
            client.release();
        }
    }
    /**
     * Version 4 is an explicitly enabled historical campaign, separate from
     * the older parser-only replays.  It combines every provider-declared
     * alias/tag field with the already-attached secondary source title, but it
     * never uses either value to match a different work. Source-local durable
     * cursors and one outstanding P2 task preserve idempotency and source
     * rotation without competing with chapter admission or rescanning the
     * full catalog. The runtime chooses the due-time fence for normal versus
     * explicitly enabled exclusive operation.
     */
    async refillStoredMetadataEnrichmentV4(maxPending = 2, minimumIntervalMs = METADATA_REPLAY_V4_INTERVAL_MS, sourceWindowsPerTurn = METADATA_REPLAY_V4_SOURCE_WINDOWS_PER_TURN) {
        // Exclusive mode can safely keep four mappings from *different canonical
        // works* in flight. The candidate query below excludes the work_id of an
        // outstanding V4 task; per-work serialization in the runtime is the
        // final correctness fence.
        const ceiling = Math.max(1, Math.min(4, maxPending));
        const client = await this.sourceMetadataDb.connect();
        try {
            await client.query('BEGIN');
            const enabled = await client.query(`SELECT value FROM settings WHERE key=$1 LIMIT 1`, [METADATA_REPLAY_V4_ENABLED_KEY]);
            if (enabled.rows[0]?.value !== 'true') {
                await client.query('COMMIT');
                return 0;
            }
            const pending = await client.query(`SELECT count(*)::int AS count
        FROM importer_lite_tasks
        WHERE task_type='CATALOG' AND state IN ('READY','RUNNING','RETRY_WAIT')
          AND payload->>'metadataEnrichmentVersion'='4'`);
            if (Number(pending.rows[0]?.count || 0) >= ceiling) {
                await client.query('COMMIT');
                return 0;
            }
            const fields = [...STORED_MAPPING_ALIAS_FIELDS, ...STORED_MAPPING_TAG_FIELDS];
            let rows = [];
            let selectedBurst = 0;
            let exhaustedAllSources = false;
            // Source and mapping cursors both ride existing narrow indexes. One
            // source window is inspected per producer turn, so neither a JSON
            // predicate nor a provider with many mappings can degrade into a full
            // catalog scan. A turn probes at most twelve windows in three bounded
            // four-window source slices: a sparse large provider advances far
            // enough to find deep evidence, while rotation still prevents it from
            // monopolising the historical lane.
            const sourceCursor = await client.query(`SELECT value FROM settings WHERE key=$1 LIMIT 1`, [METADATA_REPLAY_V4_SOURCE_CURSOR_KEY]);
            let sourceAfter = sourceCursor.rows[0]?.value || null;
            let activeSourceId = null;
            let activeSourceWindows = 0;
            const sourceWindows = Math.max(1, Math.min(METADATA_REPLAY_V4_SOURCE_WINDOWS_PER_TURN, Math.trunc(sourceWindowsPerTurn)));
            for (let probe = 0; probe < sourceWindows && rows.length === 0; probe++) {
                let sourceId = activeSourceId;
                if (!sourceId) {
                    const source = await client.query(`
            WITH after_cursor AS (
              SELECT s.id FROM importer_sources s
              WHERE s.enabled=true AND COALESCE(s.status,'ACTIVE') IN ('ACTIVE','DEGRADED','RECOVERING')
                AND s.id>$2::text
                AND NOT EXISTS (SELECT 1 FROM settings completed
                  WHERE completed.key=$1 || s.id AND completed.value='complete')
              ORDER BY s.id LIMIT 1
            ), wrapped AS (
              SELECT s.id FROM importer_sources s
              WHERE NOT EXISTS (SELECT 1 FROM after_cursor)
                AND s.enabled=true AND COALESCE(s.status,'ACTIVE') IN ('ACTIVE','DEGRADED','RECOVERING')
                AND NOT EXISTS (SELECT 1 FROM settings completed
                  WHERE completed.key=$1 || s.id AND completed.value='complete')
              ORDER BY s.id LIMIT 1
            ) SELECT id FROM after_cursor UNION ALL SELECT id FROM wrapped LIMIT 1`, [METADATA_REPLAY_V4_SOURCE_DONE_PREFIX, sourceAfter || '']);
                    sourceId = source.rows[0]?.id || null;
                    if (!sourceId) {
                        exhaustedAllSources = true;
                        break;
                    }
                    sourceAfter = sourceId;
                    activeSourceId = sourceId;
                    activeSourceWindows = 0;
                    await client.query(`INSERT INTO settings(key,value) VALUES($1,$2)
            ON CONFLICT(key) DO UPDATE SET value=excluded.value`, [METADATA_REPLAY_V4_SOURCE_CURSOR_KEY, sourceId]);
                }
                const cursor = await client.query(`SELECT value FROM settings WHERE key=$1 LIMIT 1`, [`${METADATA_REPLAY_V4_MAPPING_CURSOR_PREFIX}${sourceId}`]);
                // Keep the existing three-mapping source burst, but select that
                // bounded set in one indexed scan. Repeating one-row scans mostly
                // spends time on Discloud-to-YSQL round trips rather than merging
                // metadata, while the partition below still excludes same-work rows.
                const burstState = await client.query(`SELECT value FROM settings WHERE key=$1 LIMIT 1`, [`${METADATA_REPLAY_V4_SOURCE_BURST_PREFIX}${sourceId}`]);
                const sourceBurst = Math.max(0, Number(burstState.rows[0]?.value || 0) || 0) % METADATA_REPLAY_V4_SOURCE_BURST_LIMIT;
                const batchLimit = Math.max(1, Math.min(ceiling - Number(pending.rows[0]?.count || 0), METADATA_REPLAY_V4_SOURCE_BURST_LIMIT - sourceBurst));
                const candidate = await client.query(`
          WITH scanned AS (
            SELECT m.source,m.source_work_id,m.work_id,m.source_slug,m.source_title,m.metadata,
              EXISTS (
                SELECT 1 FROM jsonb_each(m.metadata) evidence(key,value)
                WHERE evidence.key=ANY($1::text[]) AND (
                  (jsonb_typeof(evidence.value)='string' AND btrim(evidence.value #>> '{}')<>'') OR
                  (jsonb_typeof(evidence.value)='array' AND jsonb_array_length(evidence.value)>0) OR
                  (jsonb_typeof(evidence.value)='object' AND evidence.value<>'{}'::jsonb)
                )
              ) AS has_explicit_metadata
            FROM importer_work_mappings m
            WHERE m.source=$2 AND m.work_id IS NOT NULL
              AND m.sync_status IN ('WAITING_ADMISSION','SYNCED','ACTIVE')
              AND ($3::text IS NULL OR m.source_work_id>$3)
            ORDER BY m.source_work_id LIMIT 128
          ), eligible AS (
            SELECT m.source,m.source_work_id,m.work_id::text,
              CASE WHEN m.has_explicit_metadata THEN 'EXPLICIT_METADATA' ELSE 'SECONDARY_SOURCE_TITLE' END AS reason
            FROM scanned m JOIN works w ON w.id=m.work_id AND w.published=true
            WHERE (m.has_explicit_metadata OR (
              lower(regexp_replace(btrim(m.source_title), '[[:space:]]+', ' ', 'g'))
                <> lower(regexp_replace(btrim(w.title), '[[:space:]]+', ' ', 'g'))
              AND lower(btrim(COALESCE(m.source_title,''))) NOT IN ('unknown','n/a','none','null','undefined','untitled','sem titulo','sem título')
              AND btrim(COALESCE(m.source_title,'')) !~ '^[0-9]+$'
              AND NOT (m.source_title IS NOT DISTINCT FROM m.source_slug
                AND btrim(COALESCE(m.source_title,'')) ~ '^[a-z0-9]+([-_][a-z0-9]+)+$')
            ))
              AND NOT EXISTS (SELECT 1 FROM importer_lite_tasks existing
                WHERE existing.idempotency_key='lite:metadata:v4:' || m.source || ':' || m.source_work_id)
              AND NOT EXISTS (SELECT 1 FROM importer_lite_tasks active_work
                WHERE active_work.task_type='CATALOG'
                  AND active_work.state IN ('READY','RUNNING','RETRY_WAIT')
                  AND active_work.payload->>'metadataEnrichmentVersion'='4'
                  AND active_work.work_id=m.work_id)
          ), candidate AS (
            SELECT source,source_work_id,work_id,reason
            FROM (
              SELECT eligible.*,row_number() OVER (PARTITION BY work_id ORDER BY source_work_id) AS work_rank
              FROM eligible
            ) distinct_works
            WHERE work_rank=1
            ORDER BY source_work_id LIMIT $4
          ), tail AS (SELECT max(source_work_id) AS tail_source_work_id FROM scanned)
          SELECT candidate.source,candidate.source_work_id,candidate.work_id,candidate.reason,tail.tail_source_work_id
          FROM tail LEFT JOIN candidate ON true
          ORDER BY candidate.source_work_id NULLS LAST`, [fields, sourceId, cursor.rows[0]?.value || null, batchLimit]);
                const tailSourceWorkId = candidate.rows[0]?.tail_source_work_id || null;
                if (!tailSourceWorkId) {
                    await client.query(`INSERT INTO settings(key,value) VALUES($1,'complete')
            ON CONFLICT(key) DO UPDATE SET value='complete'`, [`${METADATA_REPLAY_V4_SOURCE_DONE_PREFIX}${sourceId}`]);
                    activeSourceId = null;
                    activeSourceWindows = 0;
                    continue;
                }
                rows = candidate.rows.flatMap((result) => (result.source && result.source_work_id && result.work_id && result.reason
                    ? [{ source: result.source, source_work_id: result.source_work_id, work_id: result.work_id, reason: result.reason }]
                    : []));
                if (rows.length === 0) {
                    await client.query(`INSERT INTO settings(key,value) VALUES($1,$2)
            ON CONFLICT(key) DO UPDATE SET value=excluded.value`, [`${METADATA_REPLAY_V4_MAPPING_CURSOR_PREFIX}${sourceId}`, tailSourceWorkId]);
                    activeSourceWindows += 1;
                    if (activeSourceWindows >= METADATA_REPLAY_V4_WINDOWS_PER_SOURCE_TURN) {
                        activeSourceId = null;
                        activeSourceWindows = 0;
                    }
                    continue;
                }
                selectedBurst = sourceBurst;
            }
            if (rows.length === 0) {
                if (exhaustedAllSources) {
                    // Completion is durable and prevents an exhausted historical cursor
                    // from repeatedly re-reading the mapping catalog. New onboarding does
                    // not rely on this campaign: it enriches during normal cataloguing.
                    await client.query(`UPDATE settings SET value='complete'
            WHERE key=$1 AND value='true'`, [METADATA_REPLAY_V4_ENABLED_KEY]);
                }
                await client.query('COMMIT');
                return 0;
            }
            const lastCompleted = await client.query(`SELECT max(updated_at) AS completed_at
        FROM importer_lite_tasks WHERE idempotency_key LIKE 'lite:metadata:v4:%' AND state='DONE'`);
            const lastAt = lastCompleted.rows[0]?.completed_at ? new Date(lastCompleted.rows[0].completed_at).getTime() : 0;
            // Normal runtime calls retain their conservative 5–20s fences. The
            // lower bound exists solely for the explicitly persisted, reversible
            // exclusive campaign, where chapter claims and source maintenance are
            // parked before a faster local replay is admitted.
            const intervalMs = Math.max(500, Math.min(60_000, Math.trunc(minimumIntervalMs)));
            const nextRunAt = new Date(Math.max(Date.now(), lastAt + intervalMs));
            const inserted = [];
            for (const row of rows) {
                const idempotencyKey = `lite:metadata:v4:${row.source}:${row.source_work_id}`;
                const result = await client.query(`INSERT INTO importer_lite_tasks
          (task_type,work_id,work_key,priority,base_priority,idempotency_key,next_run_at,max_attempts,payload,checkpoint,monitoring)
          VALUES('CATALOG',$1::uuid,$2,3,3,$3,$4,4,$5::jsonb,'{}'::jsonb,false)
          ON CONFLICT(idempotency_key) DO NOTHING RETURNING work_key`, [
                    row.work_id, `work:${row.work_id}`, idempotencyKey, nextRunAt,
                    JSON.stringify({ source: row.source, sourceWorkId: row.source_work_id, metadataEnrichment: true,
                        metadataEnrichmentVersion: 4, metadataReplayReason: row.reason }),
                ]);
                if (result.rowCount)
                    inserted.push({ source: row.source, sourceWorkId: row.source_work_id });
            }
            if (inserted.length) {
                const source = inserted[0].source;
                await client.query(`INSERT INTO settings(key,value) VALUES($1,$2)
          ON CONFLICT(key) DO UPDATE SET value=excluded.value`, [`${METADATA_REPLAY_V4_MAPPING_CURSOR_PREFIX}${source}`, inserted[inserted.length - 1].sourceWorkId]);
                // A source is selected fairly by cursor, but moving to a different
                // provider after every single hit leaves sparse cursor scans idle for
                // most of the 20-second fence. Retain at most three consecutive
                // mappings from a source, then resume the normal round-robin.
                const burstKey = `${METADATA_REPLAY_V4_SOURCE_BURST_PREFIX}${source}`;
                const burst = selectedBurst + inserted.length;
                if (burst < METADATA_REPLAY_V4_SOURCE_BURST_LIMIT) {
                    const predecessor = await client.query(`SELECT max(id) AS id FROM importer_sources
            WHERE enabled=true AND COALESCE(status,'ACTIVE') IN ('ACTIVE','DEGRADED','RECOVERING') AND id<$1`, [source]);
                    await client.query(`INSERT INTO settings(key,value) VALUES($1,$2)
            ON CONFLICT(key) DO UPDATE SET value=excluded.value`, [METADATA_REPLAY_V4_SOURCE_CURSOR_KEY, predecessor.rows[0]?.id || '']);
                }
                await client.query(`INSERT INTO settings(key,value) VALUES($1,$2)
          ON CONFLICT(key) DO UPDATE SET value=excluded.value`, [burstKey, String(burst >= METADATA_REPLAY_V4_SOURCE_BURST_LIMIT ? 0 : burst)]);
                // V4 has a dedicated durable claimant. Projecting it into the
                // generic chapter-ready index would spend an extra expensive
                // refresh on every mapping without making the task more runnable.
            }
            await client.query('COMMIT');
            return inserted.length;
        }
        catch (error) {
            await client.query('ROLLBACK').catch(() => { });
            throw error;
        }
        finally {
            client.release();
        }
    }
    /** The V4 campaign owns the one isolated metadata seat while it has debt. */
    async isStoredMetadataReplayV4Enabled() {
        const result = await this.sourceMetadataDb.query(`SELECT value FROM settings WHERE key=$1 LIMIT 1`, [METADATA_REPLAY_V4_ENABLED_KEY]);
        return result.rows[0]?.value === 'true';
    }
    /** A persisted, reversible operator mode for the finite V4 campaign. */
    async storedMetadataReplayV4ExclusiveMode() {
        const result = await this.sourceMetadataDb.query(`SELECT value FROM settings WHERE key=$1 LIMIT 1`, [METADATA_REPLAY_V4_EXCLUSIVE_MODE_KEY]);
        const value = String(result.rows[0]?.value || 'false').trim();
        if (value === 'true')
            return { enabled: true, intervalMs: 2_000 };
        try {
            const parsed = JSON.parse(value);
            if (parsed?.enabled !== true)
                return { enabled: false, intervalMs: 2_000 };
            return { enabled: true, intervalMs: Math.max(500, Math.min(5_000, Math.trunc(Number(parsed.intervalMs) || 2_000))) };
        }
        catch {
            return { enabled: false, intervalMs: 2_000 };
        }
    }
    /** Completion is durable, idempotent and makes a restart resume normal work. */
    async disableStoredMetadataReplayV4ExclusiveMode() {
        await this.sourceMetadataDb.query(`INSERT INTO settings(key,value) VALUES($1,$2)
      ON CONFLICT(key) DO UPDATE SET value=excluded.value`, [
            METADATA_REPLAY_V4_EXCLUSIVE_MODE_KEY,
            JSON.stringify({ enabled: false, completedAt: new Date().toISOString() }),
        ]);
    }
    /** A persisted, reversible operator mode for the finite V5 metadata & alias campaign. */
    async metadataTurboExclusiveMode() {
        if (process.env.METADATA_TURBO_MODE === 'true') {
            return { enabled: true };
        }
        const result = await this.sourceMetadataDb.query(`SELECT value FROM settings WHERE key=$1 LIMIT 1`, [METADATA_TURBO_EXCLUSIVE_MODE_KEY]);
        const value = String(result.rows[0]?.value || 'false').trim();
        if (value === 'true')
            return { enabled: true };
        try {
            const parsed = JSON.parse(value);
            return {
                enabled: parsed?.enabled === true,
                discoveryConcurrency: typeof parsed?.discoveryConcurrency === 'number' ? parsed.discoveryConcurrency : undefined,
                searchConcurrency: typeof parsed?.searchConcurrency === 'number' ? parsed.searchConcurrency : undefined,
                metadataGateCapacity: typeof parsed?.metadataGateCapacity === 'number' ? parsed.metadataGateCapacity : undefined,
                persistenceCapacity: typeof parsed?.persistenceCapacity === 'number' ? parsed.persistenceCapacity : undefined,
            };
        }
        catch {
            return { enabled: false };
        }
    }
    async setMetadataTurboExclusiveMode(enabled, options) {
        await this.sourceMetadataDb.query(`INSERT INTO settings(key,value) VALUES($1,$2)
      ON CONFLICT(key) DO UPDATE SET value=excluded.value`, [
            METADATA_TURBO_EXCLUSIVE_MODE_KEY,
            JSON.stringify({ enabled, ...options, updatedAt: new Date().toISOString() }),
        ]);
    }
    async disableMetadataTurboExclusiveMode() {
        await this.setMetadataTurboExclusiveMode(false);
    }
    /**
     * Materialise only chapters already positively confirmed by a durable source
     * manifest.  Historical catalog tasks are useful for obtaining new
     * manifests, but a completed old catalog task must not make its confirmed
     * missing rows invisible forever.  This producer never infers a numeric
     * gap, never contacts a source, and remains bounded by both work and source
     * so one large title/provider cannot monopolise P1 admission.
     */
    async refillManifestProvenP1Imports(readyFloor = 64, batchLimit = 16, forceRepair = false) {
        const floor = Math.max(1, Math.min(512, readyFloor));
        const maximum = Math.max(1, Math.min(forceRepair ? 600 : 32, batchLimit));
        const cachedReserve = this.manifestP1ReserveCache;
        const reserveFromCache = !forceRepair && cachedReserve && cachedReserve.floor === floor
            && cachedReserve.count >= floor && Date.now() - cachedReserve.at < EXECUTABLE_P1_RESERVE_CACHE_MS;
        const reserve = reserveFromCache ? { rows: [{ count: String(cachedReserve.count) }] } : await this.db.query(`SELECT count(*)::int AS count FROM (
      SELECT 1 FROM importer_lite_tasks t
      WHERE t.state='READY' AND t.monitoring=false AND t.priority=2
        AND t.task_type='IMPORT_CHAPTER' AND t.next_run_at<=now()
      LIMIT $1
    ) AS bounded_ready`, [floor * 2]);
        const current = Number(reserve.rows[0]?.count || 0);
        if (!reserveFromCache)
            this.manifestP1ReserveCache = { at: Date.now(), floor, count: current };
        if (!forceRepair && current >= floor * 2) {
            return 0;
        }
        // A healthy executable P1 reserve must remain dominant, but it cannot
        // permanently hide a confirmed historical manifest simply because an old
        // catalog task has already completed. Admit only a tiny repair seat in
        // that case; the normal claim projection still provides work/source
        // fairness and the durable idempotency key makes the producer inert after
        // each concrete row is represented.
        const limit = forceRepair ? maximum : current >= floor
            ? Math.min(4, maximum)
            : Math.min(maximum, floor - current);
        const candidates = await this.db.query(`WITH bounded AS (
        SELECT cs.work_id::text,cs.canonical_chapter_key,cs.source,cs.language,cs.edition,cs.order_key,
          cs.source_rank,cs.updated_at
        FROM importer_lite_chapter_sources cs
        JOIN works w ON w.id=cs.work_id
        JOIN importer_sources src ON src.id=cs.source
        WHERE cs.confirmed=true AND cs.has_chapter=true
          AND src.enabled=true
          AND COALESCE(src.status,'ACTIVE')<>'DISABLED'
          AND COALESCE(src.chapter_ingestion_enabled,true)=true
          AND NOT EXISTS (
            SELECT 1 FROM importer_lite_chapters chapter
            WHERE chapter.work_id=cs.work_id AND chapter.canonical_chapter_key=cs.canonical_chapter_key
          )
          AND NOT EXISTS (
            SELECT 1 FROM importer_lite_tasks task
            WHERE task.idempotency_key='lite:chapter:' || cs.work_id::text || ':' || cs.canonical_chapter_key
          )
        ORDER BY cs.updated_at,cs.work_id,cs.canonical_chapter_key,cs.source_rank,cs.source
        LIMIT GREATEST(256,$1)
      ), canonical_routes AS (
        SELECT DISTINCT ON (work_id,canonical_chapter_key) *
        FROM bounded
        ORDER BY work_id,canonical_chapter_key,source_rank,source
      ), source_fair AS (
        SELECT *,
          row_number() OVER (PARTITION BY source ORDER BY updated_at,work_id,canonical_chapter_key) AS source_position
        FROM canonical_routes
      ), fair AS (
        SELECT *,
          row_number() OVER (PARTITION BY work_id ORDER BY updated_at,order_key,canonical_chapter_key) AS work_position
        FROM source_fair
      )
      SELECT * FROM fair
      WHERE ${forceRepair ? 'true' : 'source_position<=2 AND work_position<=2'}
      ORDER BY updated_at,source_position,work_position,source,work_id,order_key
      LIMIT $1`, [limit]);
        const tasks = [];
        for (const row of candidates.rows) {
            const chapterKey = String(row.canonical_chapter_key);
            const parsed = parseCanonicalChapterKey(chapterKey);
            // A malformed historical key is not trusted as task input. It remains
            // visible in the manifest for diagnosis instead of creating a task that
            // could collapse identity or call a provider with an invented number.
            if (!parsed)
                continue;
            tasks.push({
                taskType: 'IMPORT_CHAPTER',
                workId: String(row.work_id),
                workKey: `work:${row.work_id}`,
                chapterKey,
                priority: 'P1',
                idempotencyKey: `lite:chapter:${row.work_id}:${chapterKey}`,
                payload: {
                    displayNumber: parsed.displayNumber,
                    chapterTitle: parsed.title,
                    chapterKind: parsed.kind,
                    orderKey: String(row.order_key || ''),
                    language: String(row.language || parsed.language),
                    edition: String(row.edition || parsed.edition),
                    manifestP1Repair: true,
                },
            });
        }
        return (await this.enqueueMany(tasks)).inserted;
    }
    /** One-shot resumable repair for a known confirmed-manifest backlog. It
     * bypasses only the ordinary reserve quota, never evidence checks or task
     * idempotency, and still inserts in bounded 128-row transactions.
     */
    async materializeConfirmedMissingP1Tasks(maximum = 600) {
        const limit = Math.max(1, Math.min(10_000, maximum));
        let inserted = 0;
        for (let turn = 0; turn < Math.ceil(limit / 128); turn++) {
            const count = await this.refillManifestProvenP1Imports(1, Math.min(128, limit - inserted), true);
            inserted += count;
            if (count === 0 || inserted >= limit)
                break;
        }
        return inserted;
    }
    async refreshReadyWork(client, workKey) {
        await client.query('SELECT public.importer_lite_refresh_ready_work($1)', [workKey]);
    }
    async refreshReadyWorks(client, workKeys) {
        const unique = [...new Set([...workKeys].map((k) => String(k || '').trim()).filter(Boolean))];
        if (unique.length === 0)
            return;
        if (unique.length === 1) {
            await this.refreshReadyWork(client, unique[0]);
            return;
        }
        await client.query('SELECT public.importer_lite_refresh_ready_work(wk) FROM unnest($1::text[]) AS wk', [unique]);
    }
    async enqueue(input) {
        const client = await this.db.connect();
        try {
            await client.query('BEGIN');
            // STAFF remains an overlay. A newly-materialized task inherits an
            // already-active staff request rather than waiting for another event.
            const staff = input.workId ? await client.query(`SELECT 1 FROM importer_staff_requests
         WHERE work_id=$1::uuid AND status IN ('QUEUED','IMPORTING','RETRYING','ACTIVE') LIMIT 1`, [input.workId]) : { rowCount: 0 };
            const priority = staff.rowCount ? 0 : LITE_PRIORITY_RANK[input.priority];
            const result = await client.query(`INSERT INTO importer_lite_tasks
          (task_type,work_id,work_key,chapter_key,priority,base_priority,idempotency_key,next_run_at,max_attempts,payload,checkpoint,monitoring)
         VALUES ($1,$2::uuid,$3,$4,$5,$6,$7,$8,$9,$10::jsonb,$11::jsonb,$12)
         ON CONFLICT (idempotency_key) DO NOTHING RETURNING *`, [input.taskType, input.workId || null, input.workKey, input.chapterKey || null, priority,
                LITE_PRIORITY_RANK[input.priority], input.idempotencyKey, input.nextRunAt || new Date(),
                input.maxAttempts || 8, JSON.stringify(input.payload || {}), JSON.stringify(input.checkpoint || {}), Boolean(input.monitoring)]);
            // Startup's `lite:discover:*` seeds are claimed by claimDueDiscovery
            // directly from importer_lite_tasks. Avoid running the expensive
            // ready-work stored procedure inside every boot transaction, but retain
            // the established projection refresh for all normal enqueue callers.
            if (!input.skipReadyWorkRefresh)
                await this.refreshReadyWork(client, input.workKey);
            await client.query('COMMIT');
            if (result.rowCount)
                this.invalidateP1BacklogCache();
            return { inserted: Boolean(result.rowCount), task: result.rows[0] ? asTask(result.rows[0]) : null };
        }
        catch (error) {
            await client.query('ROLLBACK').catch(() => { });
            throw error;
        }
        finally {
            client.release();
        }
    }
    /**
     * Cataloguing can reveal hundreds of chapters at once.  Insert their durable
     * work in one short transaction, then refresh each affected ready-work row
     * once.  It intentionally does no network work and never claims anything.
     */
    async enqueueMany(inputs) {
        // A provider can expose the same canonical chapter through more than one
        // source entry. The catalog persists every source route as an alternative,
        // but executable work has one idempotency key per canonical chapter. Keep
        // the queue batch compact even when a caller has not already coalesced it.
        const uniqueInputs = [...new Map(inputs.map((input) => [input.idempotencyKey, input])).values()];
        if (uniqueInputs.length === 0)
            return { inserted: 0, insertedKeys: new Set() };
        const client = await this.db.connect();
        try {
            await client.query('BEGIN');
            const workIds = [...new Set(uniqueInputs.map((input) => input.workId).filter((id) => Boolean(id)))];
            const staff = workIds.length ? await client.query(`SELECT DISTINCT work_id::text AS work_id
        FROM importer_staff_requests WHERE work_id=ANY($1::uuid[])
          AND status IN ('QUEUED','IMPORTING','RETRYING','ACTIVE')`, [workIds]) : { rows: [] };
            const staffWorkIds = new Set(staff.rows.map((row) => String(row.work_id)));
            let inserted = 0;
            const insertedKeys = new Set();
            const insertedWorkKeys = new Set();
            // Keep far below PostgreSQL's parameter limit even if a source returns
            // an unusually large manifest.
            for (let offset = 0; offset < uniqueInputs.length; offset += 500) {
                const batch = uniqueInputs.slice(offset, offset + 500);
                const values = [];
                const rows = batch.map((input, index) => {
                    const start = index * 12;
                    values.push(input.taskType, input.workId || null, input.workKey, input.chapterKey || null, staffWorkIds.has(String(input.workId)) ? 0 : LITE_PRIORITY_RANK[input.priority], LITE_PRIORITY_RANK[input.priority], input.idempotencyKey, input.nextRunAt || new Date(), input.maxAttempts || 8, JSON.stringify(input.payload || {}), JSON.stringify(input.checkpoint || {}), Boolean(input.monitoring));
                    return `($${start + 1},$${start + 2}::uuid,$${start + 3},$${start + 4},$${start + 5},$${start + 6},$${start + 7},$${start + 8},$${start + 9},$${start + 10}::jsonb,$${start + 11}::jsonb,$${start + 12})`;
                });
                const result = await client.query(`INSERT INTO importer_lite_tasks
          (task_type,work_id,work_key,chapter_key,priority,base_priority,idempotency_key,next_run_at,max_attempts,payload,checkpoint,monitoring)
          VALUES ${rows.join(',')}
          ON CONFLICT (idempotency_key) DO NOTHING RETURNING idempotency_key, work_key`, values);
                inserted += result.rowCount || 0;
                for (const row of result.rows) {
                    insertedKeys.add(String(row.idempotency_key));
                    insertedWorkKeys.add(String(row.work_key));
                }
            }
            await this.refreshReadyWorks(client, insertedWorkKeys);
            await client.query('COMMIT');
            if (inserted > 0)
                this.invalidateP1BacklogCache();
            return { inserted, insertedKeys };
        }
        catch (error) {
            await client.query('ROLLBACK').catch(() => { });
            throw error;
        }
        finally {
            client.release();
        }
    }
    async claim(workerId, leaseSeconds, options) {
        const lease = `${Math.max(30, leaseSeconds)} seconds`;
        const useFocused = Boolean(options && ((options.focusWorkKeys && options.focusWorkKeys.length > 0)
            || options.preferFocus !== undefined
            || (options.excludedSources && options.excludedSources.length > 0)
            || options.chaptersOnly));
        const result = useFocused
            ? await this.db.query('SELECT * FROM public.importer_lite_claim_task_focused($1,$2::interval,$3::text[],$4::boolean,$5::text[],$6::boolean)', [
                workerId,
                lease,
                options?.focusWorkKeys?.length ? options.focusWorkKeys : null,
                options?.preferFocus ?? true,
                options?.excludedSources?.length ? options.excludedSources : null,
                Boolean(options?.chaptersOnly),
            ])
            : await this.db.query('SELECT * FROM public.importer_lite_claim_task($1,$2::interval)', [workerId, lease]);
        const task = result.rows[0] ? asTask(result.rows[0]) : null;
        if (task && task.priority === 'P1' && (task.taskType === 'IMPORT_CHAPTER' || task.taskType === 'PUBLISH')) {
            if (this.p1FocusWorkKeys.includes(task.workKey))
                this.p1PrimaryClaims++;
            else
                this.p1SecondaryClaims++;
        }
        return task;
    }
    /**
     * Dedicated source-rotated claim for READY historicalP1Backfill CATALOG tasks
     * across operational sources so multi-source P1 manifests materialize without
     * waiting behind hundreds of operation_rank=1 chapter imports on hot sources.
     */
    async claimHistoricalP1Catalog(workerId, leaseSeconds) {
        const metaDb = this.sourceMetadataDb || this.db;
        const result = await metaDb.query(`
      WITH runnable_candidates AS (
        SELECT
          t.id,
          t.work_key,
          t.payload->>'source' AS source,
          t.created_at
        FROM importer_lite_tasks t
        JOIN importer_sources s ON s.id = t.payload->>'source'
        LEFT JOIN importer_lite_source_health h ON h.source = s.id
        WHERE t.state IN ('READY', 'RUNNING', 'RETRY_WAIT')
          AND t.state = 'READY'
          AND t.monitoring = false
          AND t.priority = 2
          AND t.task_type = 'CATALOG'
          AND t.payload ? 'historicalP1Backfill'
          AND t.next_run_at <= now()
          AND s.enabled = true
          AND s.id NOT IN ('mangaball','mangadex','mangafire')
          AND COALESCE(s.status, 'ACTIVE') <> 'DISABLED'
          AND COALESCE(s.chapter_ingestion_enabled, true) = true
          AND ${sourceExecutionEligibilitySql('s')}
          AND COALESCE(h.health, 'HEALTHY') NOT IN ('BROKEN', 'DISABLED')
          AND (h.cooldown_until IS NULL OR h.cooldown_until <= now())
        ORDER BY
          CASE WHEN (t.payload->>'source') > $3 THEN 0 ELSE 1 END,
          t.payload->>'source',
          t.created_at,
          t.id
        LIMIT 24
      ), picked AS (
        SELECT rc.id
        FROM runnable_candidates rc
        JOIN importer_lite_tasks locked ON locked.id = rc.id
        ORDER BY
          CASE WHEN rc.source > $3 THEN 0 ELSE 1 END,
          rc.source,
          rc.created_at,
          rc.id
        LIMIT 1
        FOR UPDATE OF locked SKIP LOCKED
      ), claimed AS (
        UPDATE importer_lite_tasks target
        SET state = 'RUNNING',
            lease_owner = $1,
            lease_until = now() + $2::interval,
            attempts = target.attempts + 1,
            updated_at = now()
        FROM picked
        WHERE target.id = picked.id
        RETURNING target.*
      )
      SELECT * FROM claimed`, [workerId, `${Math.max(30, leaseSeconds)} seconds`, this.lastHistoricalP1CatalogSource]);
        const task = result.rows[0] ? asTask(result.rows[0]) : null;
        if (task) {
            if (typeof task.payload.source === 'string') {
                this.lastHistoricalP1CatalogSource = task.payload.source;
            }
            await metaDb.query('SELECT public.importer_lite_refresh_ready_work($1)', [task.workKey]).catch(() => { });
        }
        return task;
    }
    /**
     * Dynamically maintain the Primary P1 Focus Group (~15-20 works, expanding
     * with real admission capacity) favoring near-completion works + queue-age
     * fairness + multi-source diversity, while detecting completed P1 works.
     */
    async refreshP1FocusGroup(options = {}) {
        const desiredSize = Math.max(12, Math.min(48, Math.floor(options.targetSize || 24)));
        const maxPerSource = Math.max(2, Math.min(16, Math.floor(options.maxPerSource || 6)));
        const sourceTiers = options.sourceTiers;
        const maxFastPerSource = Math.max(maxPerSource, Math.ceil(desiredSize * 0.55));
        const maxSlowPerSource = Math.min(maxPerSource, 2);
        const maxTotalSlowInFocus = Math.max(3, Math.floor(desiredSize * 0.22));
        const degradedSources = new Set();
        const maxForSource = (source) => {
            if (degradedSources.has(source))
                return 1;
            if (!sourceTiers)
                return maxPerSource;
            const tier = sourceTiers.get(source) || 'NORMAL';
            if (tier === 'FAST') {
                const sourceMaxChapters = SOURCE_CONCURRENCY_LIMITS[source]?.maxChapters ?? 6;
                return Math.max(maxPerSource, Math.min(maxFastPerSource, sourceMaxChapters + 2));
            }
            if (tier === 'SLOW')
                return maxSlowPerSource;
            return maxPerSource;
        };
        const speedRankFor = (source) => {
            if (degradedSources.has(source))
                return 2;
            if (!sourceTiers)
                return 1;
            const tier = sourceTiers.get(source) || 'NORMAL';
            return tier === 'FAST' ? 0 : tier === 'NORMAL' ? 1 : 2;
        };
        this.p1FocusTargetSize = desiredSize;
        const metaDb = this.sourceMetadataDb || this.db;
        const horizonIso = new Date(Date.now() + 35_000).toISOString();
        const currentFocusKeys = this.p1FocusWorkKeys || [];
        const candidatesRes = await metaDb.query(`
      WITH current_focus_works AS (
        SELECT rw.work_key,
               NULLIF(substring(rw.work_key from 6), '')::uuid AS work_id,
               rw.next_run_at,
               rw.last_claimed_at
        FROM importer_lite_ready_works rw
        WHERE rw.priority = 2
          AND rw.operation_rank <= 1
          AND rw.work_key = ANY($2::text[])
          AND rw.next_run_at <= $1::timestamptz
      ), candidate_ready_works AS (
        SELECT rw.work_key,
               NULLIF(substring(rw.work_key from 6), '')::uuid AS work_id,
               rw.next_run_at,
               rw.last_claimed_at
        FROM importer_lite_ready_works rw
        WHERE rw.priority = 2
          AND rw.operation_rank <= 1
          AND rw.work_key LIKE 'work:%'
          AND rw.next_run_at <= $1::timestamptz
        ORDER BY rw.last_claimed_at ASC NULLS FIRST, rw.next_run_at ASC, rw.work_key ASC
        LIMIT 256
      ), ready_p1_works AS MATERIALIZED (
        SELECT * FROM current_focus_works
        UNION
        SELECT * FROM candidate_ready_works
      ), active_sources AS MATERIALIZED (
        SELECT src.id AS source,
          (src.enabled = true
            AND COALESCE(src.status, 'ACTIVE') <> 'DISABLED'
            AND COALESCE(src.chapter_ingestion_enabled, true) = true
            AND ${sourceExecutionEligibilitySql('src')}
            AND COALESCE(sh.health, 'HEALTHY') NOT IN ('BROKEN', 'DISABLED')
            AND (sh.cooldown_until IS NULL OR sh.cooldown_until <= now())
          ) AS is_runnable,
          (COALESCE(sh.consecutive_failures, 0) >= 3) AS is_degraded
        FROM importer_sources src
        LEFT JOIN importer_lite_source_health sh ON sh.source = src.id
      ), work_stats AS MATERIALIZED (
        SELECT
          rpw.work_key,
          rpw.work_id,
          count(t.id)::int AS remaining_chapters,
          min(t.created_at) AS oldest_task_at,
          (array_agg(t.chapter_key ORDER BY t.priority ASC, t.next_run_at ASC, t.created_at ASC))[1] AS frontier_chapter_key
        FROM ready_p1_works rpw
        JOIN importer_lite_tasks t
          ON t.work_key = rpw.work_key
         AND t.state = 'READY'
         AND t.priority = 2
         AND t.monitoring = false
         AND t.task_type IN ('IMPORT_CHAPTER', 'PUBLISH')
        GROUP BY rpw.work_key, rpw.work_id
      ), route_stats AS MATERIALIZED (
        SELECT
          ws.work_key,
          count(cs.source) > 0 AS has_recorded_routes,
          array_agg(cs.source ORDER BY cs.source_rank, cs.source) FILTER (WHERE s.is_runnable = true) AS runnable_sources,
          array_agg(cs.source ORDER BY cs.source_rank, cs.source) FILTER (WHERE s.is_runnable = true AND NOT s.is_degraded) AS healthy_runnable_sources
        FROM work_stats ws
        LEFT JOIN importer_lite_chapter_sources cs
          ON cs.work_id = ws.work_id
         AND cs.canonical_chapter_key = ws.frontier_chapter_key
         AND cs.confirmed = true
         AND COALESCE(cs.has_chapter, true) = true
        LEFT JOIN active_sources s ON s.source = cs.source
        GROUP BY ws.work_key
      )
      SELECT
        ws.work_key,
        ws.work_id::text AS work_id,
        ws.remaining_chapters,
        ws.oldest_task_at,
        rs.runnable_sources,
        rs.healthy_runnable_sources,
        COALESCE(rs.has_recorded_routes, false) AS has_recorded_routes
      FROM work_stats ws
      LEFT JOIN route_stats rs ON rs.work_key = ws.work_key
      WHERE ws.remaining_chapters > 0
    `, [horizonIso, currentFocusKeys]);
        const now = Date.now();
        const candidateByKey = new Map();
        for (const row of candidatesRes.rows) {
            const hasRecordedRoutes = Boolean(row.has_recorded_routes);
            const runnableList = Array.isArray(row.runnable_sources)
                ? row.runnable_sources.map((item) => String(item || '')).filter(Boolean)
                : (row.runnable_source ? [String(row.runnable_source)] : []);
            if (hasRecordedRoutes && runnableList.length === 0)
                continue;
            const hasHealthyColumn = row.healthy_runnable_sources !== undefined;
            const healthyList = Array.isArray(row.healthy_runnable_sources)
                ? row.healthy_runnable_sources.map((item) => String(item || '')).filter(Boolean)
                : [];
            const isDegraded = hasHealthyColumn && runnableList.length > 0 && healthyList.length === 0;
            const effectivePool = healthyList.length > 0 ? healthyList : runnableList;
            const runnableSource = effectivePool.length > 0
                ? [...effectivePool].sort((a, b) => speedRankFor(a) - speedRankFor(b))[0]
                : null;
            const source = runnableSource || 'unattributed';
            if (isDegraded && source !== 'unattributed')
                degradedSources.add(source);
            const remaining = Math.max(1, Number(row.remaining_chapters || 1));
            const oldestMs = row.oldest_task_at ? new Date(row.oldest_task_at).getTime() : now;
            const ageHours = Math.max(0, (now - oldestMs) / 3_600_000);
            const completionScore = remaining <= 3 ? 120
                : remaining <= 7 ? 85
                    : remaining <= 15 ? 55
                        : remaining <= 30 ? 30
                            : remaining <= 60 ? 15
                                : 0;
            const fairnessScore = Math.min(35, ageHours * 1.5);
            const speedBonus = isDegraded
                ? -25
                : (sourceTiers ? (speedRankFor(source) === 0 ? 22 : speedRankFor(source) === 2 ? -12 : 0) : 0);
            candidateByKey.set(String(row.work_key), {
                workKey: String(row.work_key),
                workId: String(row.work_id),
                source,
                isDegraded,
                remainingChapters: remaining,
                oldestTaskMs: oldestMs,
                score: completionScore + fairnessScore + speedBonus,
            });
        }
        // Detect previous Focus works that exited because their backlog completed.
        const exitedFocusKeys = [...this.p1FocusMeta.keys()].filter((key) => !candidateByKey.has(key));
        if (exitedFocusKeys.length > 0) {
            const stillPending = await metaDb.query(`
        SELECT DISTINCT t.work_key
        FROM importer_lite_tasks t
        WHERE t.work_key = ANY($1::text[])
          AND t.task_type IN ('IMPORT_CHAPTER', 'PUBLISH')
          AND t.state IN ('RUNNING', 'READY', 'RETRY_WAIT')
          AND t.monitoring = false
          AND (t.state <> 'RETRY_WAIT' OR t.attempts < t.max_attempts)`, [exitedFocusKeys]);
            const pendingSet = new Set(stillPending.rows.map((r) => String(r.work_key)));
            for (const exitedKey of exitedFocusKeys) {
                if (!pendingSet.has(exitedKey)) {
                    const prev = this.p1FocusMeta.get(exitedKey);
                    this.p1CompletedWorksCount++;
                    if (prev?.workId && !this.p1CompletedWorkIds.includes(prev.workId)) {
                        this.p1CompletedWorkIds.push(prev.workId);
                        if (this.p1CompletedWorkIds.length > 100)
                            this.p1CompletedWorkIds.shift();
                    }
                }
            }
        }
        const nextFocus = [];
        const selectedSet = new Set();
        const sourceCounts = new Map();
        let totalSlowCount = 0;
        // 1. Keep sticky Focus works that remain runnable so works drain continuously
        //    to completion without thrashing every chapter.
        for (const prevKey of this.p1FocusWorkKeys) {
            if (nextFocus.length >= desiredSize)
                break;
            const cand = candidateByKey.get(prevKey);
            if (!cand)
                continue;
            const prevMeta = this.p1FocusMeta.get(prevKey);
            const enteredAt = prevMeta?.enteredAt || now;
            const isSlow = Boolean((sourceTiers && speedRankFor(cand.source) === 2) || cand.isDegraded);
            const stickyRotateMs = isSlow ? 12 * 60_000 : 25 * 60_000;
            const stickyRemainingThreshold = isSlow ? 25 : 35;
            // Rotate out long backlogs only when fresher near-done alternatives wait.
            // Works close to completion (<= 15 chapters remaining) remain sticky until finished.
            if (cand.remainingChapters > 15 && now - enteredAt > stickyRotateMs && cand.remainingChapters > stickyRemainingThreshold && candidateByKey.size > desiredSize) {
                continue;
            }
            if (isSlow && totalSlowCount >= maxTotalSlowInFocus)
                continue;
            const currentSourceCount = sourceCounts.get(cand.source) || 0;
            if (currentSourceCount >= maxForSource(cand.source))
                continue;
            selectedSet.add(prevKey);
            sourceCounts.set(cand.source, currentSourceCount + 1);
            if (isSlow)
                totalSlowCount++;
            nextFocus.push({
                workKey: cand.workKey,
                workId: cand.workId,
                source: cand.source,
                remainingChapters: cand.remainingChapters,
                enteredAt,
            });
        }
        // 2. Fill remaining Focus slots favoring fast throughput + unrepresented sources + completion potential + age fairness.
        const remainingCandidates = [...candidateByKey.values()].filter((c) => !selectedSet.has(c.workKey));
        while (nextFocus.length < desiredSize && remainingCandidates.length > 0) {
            let bestIdx = -1;
            let bestScore = -Infinity;
            for (let i = 0; i < remainingCandidates.length; i++) {
                const cand = remainingCandidates[i];
                const isSlow = Boolean((sourceTiers && speedRankFor(cand.source) === 2) || cand.isDegraded);
                if (isSlow && totalSlowCount >= maxTotalSlowInFocus)
                    continue;
                const srcCount = sourceCounts.get(cand.source) || 0;
                if (srcCount >= maxForSource(cand.source))
                    continue;
                const diversityBonus = srcCount === 0 ? 25 : srcCount === 1 ? 10 : 0;
                const effectiveScore = cand.score + diversityBonus;
                if (effectiveScore > bestScore) {
                    bestScore = effectiveScore;
                    bestIdx = i;
                }
            }
            if (bestIdx === -1)
                break;
            const chosen = remainingCandidates.splice(bestIdx, 1)[0];
            selectedSet.add(chosen.workKey);
            sourceCounts.set(chosen.source, (sourceCounts.get(chosen.source) || 0) + 1);
            if ((sourceTiers && speedRankFor(chosen.source) === 2) || chosen.isDegraded)
                totalSlowCount++;
            nextFocus.push({
                workKey: chosen.workKey,
                workId: chosen.workId,
                source: chosen.source,
                remainingChapters: chosen.remainingChapters,
                enteredAt: now,
            });
        }
        // 3. Never leave capacity idle if fewer sources have eligible works: relax per-source cap for FAST/NORMAL
        //    sources first using source-count penalty so no single FAST source monopolizes relaxed slots,
        //    keeping SLOW sources bounded by maxTotalSlowInFocus while any FAST/NORMAL works remain.
        if (nextFocus.length < desiredSize && remainingCandidates.length > 0) {
            while (nextFocus.length < desiredSize) {
                let bestIdx = -1;
                let bestEffectiveScore = -Infinity;
                for (let i = 0; i < remainingCandidates.length; i++) {
                    const cand = remainingCandidates[i];
                    if (selectedSet.has(cand.workKey))
                        continue;
                    const isSlow = Boolean((sourceTiers && speedRankFor(cand.source) === 2) || cand.isDegraded);
                    if (isSlow)
                        continue;
                    const count = sourceCounts.get(cand.source) || 0;
                    const effectiveScore = cand.score - count * 4;
                    if (effectiveScore > bestEffectiveScore
                        || (effectiveScore === bestEffectiveScore && bestIdx >= 0 && cand.oldestTaskMs < remainingCandidates[bestIdx].oldestTaskMs)) {
                        bestEffectiveScore = effectiveScore;
                        bestIdx = i;
                    }
                }
                if (bestIdx === -1)
                    break;
                const chosen = remainingCandidates.splice(bestIdx, 1)[0];
                selectedSet.add(chosen.workKey);
                sourceCounts.set(chosen.source, (sourceCounts.get(chosen.source) || 0) + 1);
                nextFocus.push({
                    workKey: chosen.workKey,
                    workId: chosen.workId,
                    source: chosen.source,
                    remainingChapters: chosen.remainingChapters,
                    enteredAt: now,
                });
            }
            if (nextFocus.length < desiredSize) {
                remainingCandidates.sort((a, b) => b.score - a.score || a.oldestTaskMs - b.oldestTaskMs);
                for (const chosen of remainingCandidates) {
                    if (nextFocus.length >= desiredSize)
                        break;
                    if (selectedSet.has(chosen.workKey))
                        continue;
                    const isSlow = Boolean(sourceTiers && speedRankFor(chosen.source) === 2);
                    if (isSlow && totalSlowCount >= maxTotalSlowInFocus && nextFocus.length > 0)
                        continue;
                    selectedSet.add(chosen.workKey);
                    sourceCounts.set(chosen.source, (sourceCounts.get(chosen.source) || 0) + 1);
                    if (isSlow)
                        totalSlowCount++;
                    nextFocus.push({
                        workKey: chosen.workKey,
                        workId: chosen.workId,
                        source: chosen.source,
                        remainingChapters: chosen.remainingChapters,
                        enteredAt: now,
                    });
                }
            }
        }
        // Prioritize works with fewest remaining chapters at the front of the focus group
        nextFocus.sort((a, b) => a.remainingChapters - b.remainingChapters);
        this.p1FocusWorkKeys = nextFocus.map((item) => item.workKey);
        this.p1FocusMeta = new Map(nextFocus.map((item) => [item.workKey, item]));
        this.p1FocusRefreshedAt = new Date(now).toISOString();
        return this.getP1FocusGroupSnapshot();
    }
    getP1FocusWorkKeys() {
        return this.p1FocusWorkKeys;
    }
    getP1FocusGroupSnapshot() {
        const focusSources = {};
        for (const entry of this.p1FocusMeta.values()) {
            focusSources[entry.source] = (focusSources[entry.source] || 0) + 1;
        }
        return {
            targetSize: this.p1FocusTargetSize,
            focusWorkCount: this.p1FocusWorkKeys.length,
            focusWorkKeys: [...this.p1FocusWorkKeys],
            focusSources,
            primaryClaims: this.p1PrimaryClaims,
            secondaryClaims: this.p1SecondaryClaims,
            completedWorksSinceBoot: this.p1CompletedWorksCount,
            recentCompletedWorkIds: [...this.p1CompletedWorkIds],
            refreshedAt: this.p1FocusRefreshedAt,
        };
    }
    /**
     * Determines whether executable or recoverable P1 historical backlog exists
     * so P2 new-work discovery/onboarding remains subordinated to P1 without
     * ever starving permanently on terminal BLOCKED or disabled-source rows.
     */
    async hasExecutableOrRecoverableP1Backlog(forceFresh = false) {
        const now = Date.now();
        if (!forceFresh && this.p1BacklogCache && now - this.p1BacklogCache.at < 30_000) {
            return { pending: this.p1BacklogCache.pending, reason: this.p1BacklogCache.reason };
        }
        const metaDb = this.sourceMetadataDb || this.db;
        const probe = await metaDb.query(`
      SELECT COALESCE(
        (
          SELECT 'P1_RUNNING'
          FROM importer_lite_tasks t
          WHERE t.state = 'RUNNING'
            AND t.task_type IN ('IMPORT_CHAPTER', 'PUBLISH')
            AND t.monitoring = false
            AND t.priority = 2
            AND (t.lease_until IS NULL OR t.lease_until > now())
          LIMIT 1
        ),
        (
          SELECT 'P1_READY_CHAPTER'
          FROM (
            SELECT work_key FROM importer_lite_ready_works
            WHERE priority = 2 AND operation_rank <= 1
            LIMIT 32
          ) rw
          JOIN importer_lite_tasks t
            ON t.work_key = rw.work_key
           AND t.state = 'READY'
          LEFT JOIN works w ON w.id = t.work_id
          LEFT JOIN media cov ON cov.id = w.cover_id
          WHERE t.monitoring = false
            AND t.priority = 2
            AND t.task_type IN ('IMPORT_CHAPTER', 'PUBLISH')
            AND (
              t.work_id IS NULL
              OR (
                NULLIF(btrim(w.title), '') IS NOT NULL
                AND NULLIF(btrim(w.slug), '') IS NOT NULL
                AND cov.storage_ready = true
                AND cov.bytes >= 1500
              )
            )
            AND NOT (
              t.task_type = 'IMPORT_CHAPTER'
              AND t.work_id IS NOT NULL
              AND t.chapter_key IS NOT NULL
              AND public.importer_lite_manifest_p1_route_deferred(t.work_id, t.chapter_key)
            )
          LIMIT 1
        ),
        (
          SELECT 'P1_RECOVERABLE_RETRY'
          FROM importer_lite_tasks t
          LEFT JOIN works w ON w.id = t.work_id
          LEFT JOIN media cov ON cov.id = w.cover_id
          WHERE t.state IN ('RUNNING', 'RETRY_WAIT')
            AND t.state = 'RETRY_WAIT'
            AND t.monitoring = false
            AND t.priority = 2
            AND t.task_type IN ('IMPORT_CHAPTER', 'PUBLISH')
            AND t.attempts < t.max_attempts
            AND (
              t.work_id IS NULL
              OR (
                NULLIF(btrim(w.title), '') IS NOT NULL
                AND NULLIF(btrim(w.slug), '') IS NOT NULL
                AND cov.storage_ready = true
                AND cov.bytes >= 1500
              )
            )
            AND NOT (
              t.task_type = 'IMPORT_CHAPTER'
              AND t.work_id IS NOT NULL
              AND t.chapter_key IS NOT NULL
              AND public.importer_lite_manifest_p1_route_deferred(t.work_id, t.chapter_key)
            )
          LIMIT 1
        ),
        (
          SELECT 'P1_HISTORICAL_CATALOG'
          FROM importer_lite_tasks t
          JOIN importer_sources s ON s.id = t.payload->>'source'
          LEFT JOIN importer_lite_source_health h ON h.source = s.id
          WHERE t.state IN ('READY', 'RUNNING', 'RETRY_WAIT')
            AND t.monitoring = false
            AND t.priority = 2
            AND t.task_type = 'CATALOG'
            AND t.payload ? 'historicalP1Backfill'
            AND (t.state = 'RUNNING' OR t.attempts < t.max_attempts)
            AND s.enabled = true
            AND s.id NOT IN ('mangaball', 'mangadex', 'mangafire')
            AND COALESCE(s.status, 'ACTIVE') <> 'DISABLED'
            AND COALESCE(s.chapter_ingestion_enabled, true) = true
            AND ${sourceExecutionEligibilitySql('s')}
            AND COALESCE(h.health, 'HEALTHY') NOT IN ('BROKEN', 'DISABLED')
          LIMIT 1
        )
      ) AS reason
    `);
        const reason = probe.rows[0]?.reason ? String(probe.rows[0].reason) : null;
        const pending = reason !== null;
        this.p1BacklogCache = { at: now, pending, reason };
        return { pending, reason };
    }
    /**
     * Checks whether an existing work currently has pending unpublished chapters
     * on operational routes so newly detected P0 chapters are incorporated into
     * that work's P1 canonical sequence instead of publishing out of order.
     */
    async hasPendingWorkChapterBacklog(workId) {
        const metaDb = this.sourceMetadataDb || this.db;
        const result = await metaDb.query(`
      WITH unimported_keys AS MATERIALIZED (
        SELECT cs_idx.canonical_chapter_key
        FROM (
          SELECT DISTINCT canonical_chapter_key
          FROM importer_lite_chapter_sources
          WHERE work_id = $1::uuid AND confirmed = true
        ) cs_idx
        WHERE NOT EXISTS (
          SELECT 1 FROM importer_lite_chapters ch
          WHERE ch.work_id = $1::uuid
            AND ch.canonical_chapter_key = cs_idx.canonical_chapter_key
        )
      )
      SELECT (
        EXISTS (
          SELECT 1
          FROM importer_lite_tasks t
          WHERE t.work_key = ('work:' || $1::text)
            AND t.task_type IN ('IMPORT_CHAPTER', 'PUBLISH')
            AND t.state IN ('RUNNING', 'READY', 'RETRY_WAIT')
            AND t.monitoring = false
            AND (t.state <> 'RETRY_WAIT' OR t.attempts < t.max_attempts)
            AND NOT (
              t.task_type = 'IMPORT_CHAPTER'
              AND t.chapter_key IS NOT NULL
              AND public.importer_lite_manifest_p1_route_deferred(t.work_id, t.chapter_key)
            )
        )
        OR EXISTS (
          SELECT 1
          FROM importer_lite_tasks t
          WHERE t.work_key = ('work:' || $1::text)
            AND t.state IN ('READY', 'RUNNING', 'RETRY_WAIT')
            AND t.monitoring = false
            AND t.task_type = 'CATALOG'
            AND t.base_priority = 2
            AND NOT (t.payload ? 'metadataEnrichment')
            AND (t.state <> 'RETRY_WAIT' OR t.attempts < t.max_attempts)
        )
        OR (
          EXISTS (
            SELECT 1 FROM chapters pub
            WHERE pub.work_id = $1::uuid AND pub.published_at IS NOT NULL
          )
          AND EXISTS (
            SELECT 1
            FROM unimported_keys uk
            JOIN importer_lite_chapter_sources cs
              ON cs.work_id = $1::uuid
             AND cs.canonical_chapter_key = uk.canonical_chapter_key
             AND cs.confirmed = true
            JOIN importer_sources s ON s.id = cs.source
            LEFT JOIN importer_lite_source_health h ON h.source = s.id
            WHERE COALESCE(cs.has_chapter, true) = true
              AND s.enabled = true
              AND COALESCE(s.status, 'ACTIVE') <> 'DISABLED'
              AND COALESCE(s.chapter_ingestion_enabled, true) = true
              AND ${sourceExecutionEligibilitySql('s')}
              AND COALESCE(h.health, 'HEALTHY') NOT IN ('BROKEN', 'DISABLED')
              AND NOT EXISTS (
                SELECT 1 FROM chapters c
                WHERE c.work_id = $1::uuid
                  AND c.canonical_chapter_key = uk.canonical_chapter_key
                  AND c.published_at IS NOT NULL
              )
              AND NOT EXISTS (
                SELECT 1 FROM importer_lite_tasks blk
                WHERE blk.idempotency_key = 'lite:chapter:' || $1::text || ':' || uk.canonical_chapter_key
                  AND blk.state = 'BLOCKED'
              )
          )
        )
      ) AS pending`, [workId]);
        return Boolean(result.rows[0]?.pending);
    }
    /** Source-backed metadata owns a dedicated fast lane and never waits behind chapter claims. */
    async claimSourceMetadataRefresh(workerId, leaseSeconds) {
        const result = await this.sourceMetadataDb.query(`
      WITH picked AS (
        SELECT task.id
        FROM importer_lite_tasks task
        WHERE task.task_type='CATALOG'
          AND task.state IN ('READY','RUNNING','RETRY_WAIT')
          AND task.payload ? 'metadataSourceRefresh'
          AND (
            (task.state IN ('READY','RETRY_WAIT') AND task.next_run_at<=now())
            OR (task.state='RUNNING' AND task.lease_until<now())
          )
        ORDER BY task.next_run_at,task.id
        LIMIT 1
        FOR UPDATE SKIP LOCKED
      )
      UPDATE importer_lite_tasks task
      SET state='RUNNING',lease_owner=$1,lease_until=now()+$2::interval,
          attempts=task.attempts+1,updated_at=now()
      FROM picked
      WHERE task.id=picked.id
      RETURNING task.*
    `, [workerId, `${Math.max(30, leaseSeconds)} seconds`]);
        return result.rows[0] ? asTask(result.rows[0]) : null;
    }
    /**
     * Whole-catalog alias discovery must not wait behind an unbounded chapter
     * P1 reserve.  It owns one explicit, low-rate source-metadata lane; the
     * normal task claim remains the authority for all chapter/catalog work.
     */
    async claimCrossSourceMetadataDiscovery(workerId, leaseSeconds) {
        const dbToUse = (process.env.METADATA_TURBO_MODE === 'true' && this.sourceMetadataDb && this.sourceMetadataDb !== this.db && (this.sourceMetadataDb.waitingCount || 0) > 0 && (this.db.idleCount || 0) > 0)
            ? this.db
            : this.sourceMetadataDb;
        const result = await dbToUse.query(`
      WITH picked AS (
        SELECT /*+ IndexScan(t importer_lite_tasks_v7_claim_idx) */ t.id
        FROM importer_lite_tasks t
        WHERE t.task_type='CATALOG'
          AND t.state = ANY (ARRAY['READY', 'RUNNING', 'RETRY_WAIT'])
          AND t.idempotency_key >= $1 AND t.idempotency_key < $2
          AND (
            (t.state IN ('READY','RETRY_WAIT') AND t.next_run_at<=now())
            OR (t.state='RUNNING' AND t.lease_until<now())
          )
        ORDER BY t.next_run_at ASC, t.id ASC
        LIMIT 1
        FOR UPDATE SKIP LOCKED
      )
      UPDATE importer_lite_tasks target
      SET state='RUNNING',lease_owner=$3,lease_until=now()+$4::interval,
          attempts=target.attempts+1,updated_at=now()
      FROM picked
      WHERE target.id=picked.id
      RETURNING target.*`, [
            METADATA_CROSS_SOURCE_DISCOVERY_V7_PREFIX,
            `${METADATA_CROSS_SOURCE_DISCOVERY_V7_PREFIX}~`,
            workerId,
            `${Math.max(30, leaseSeconds)} seconds`
        ]);
        return result.rows[0] ? asTask(result.rows[0]) : null;
    }
    /**
     * One first-pass catalog claim is kept independent from the chapter queue.
     * It is limited by the runtime to a single slot and only applies before a
     * canonical work exists, so a sustained P1 repair reserve cannot make new
     * work onboarding wait forever.
     */
    async claimNewWorkCatalog(workerId, leaseSeconds) {
        // Keep the runnable-source fence in this single atomic statement rather
        // than depending solely on a stored function migration. The native app
        // deploy does not execute migrations, and an old cooled source must not
        // consume the only first-catalog seat before a healthy new discovery.
        // This remains a short claim-only YSQL operation: no adapter or network
        // work occurs until after the returned lease has committed.
        const metaDb = this.sourceMetadataDb || this.db;
        const result = await metaDb.query(`
      WITH picked AS (
        SELECT task.id
        FROM importer_lite_tasks task
        JOIN importer_sources source ON source.id=task.payload->>'source'
        LEFT JOIN importer_lite_source_health health ON health.source=source.id
        WHERE task.task_type='CATALOG'
          AND task.work_id IS NULL
          AND task.state='READY'
          AND task.monitoring=false
          AND task.next_run_at<=now()
          AND source.enabled=true
          AND (COALESCE(source.status,'ACTIVE') IN ('ACTIVE','DEGRADED','RECOVERING')
            OR (source.status='COOLDOWN' AND source.cooldown_until IS NOT NULL AND source.cooldown_until<=now()))
          AND COALESCE(health.health,'HEALTHY') NOT IN ('BROKEN','DISABLED')
          AND (source.cooldown_until IS NULL OR source.cooldown_until<=now())
          AND (health.cooldown_until IS NULL OR health.cooldown_until<=now())
          AND NOT (task.payload ? 'metadataEnrichment')
          AND NOT (task.payload ? 'metadataSourceRefresh')
        ORDER BY task.created_at,task.id
        LIMIT 1 FOR UPDATE OF task SKIP LOCKED
      )
      UPDATE importer_lite_tasks task
      SET state='RUNNING',lease_owner=$1,lease_until=now()+$2::interval,
        attempts=task.attempts+1,updated_at=now()
      FROM picked
      WHERE task.id=picked.id
      RETURNING task.*`, [workerId, `${Math.max(30, leaseSeconds)} seconds`]);
        return result.rows[0] ? asTask(result.rows[0]) : null;
    }
    /**
     * Claims one due DISCOVER task independent of P1 chapter reserve so active
     * sources continuously feed the unmapped NEW_WORK onboarding lane without
     * waiting for the entire historical chapter backlog to drain.
     */
    async claimDueDiscovery(workerId, leaseSeconds) {
        const metaDb = this.sourceMetadataDb || this.db;
        const result = await metaDb.query(`
      WITH picked AS (
        SELECT task.id
        FROM importer_lite_tasks task
        JOIN importer_sources source ON source.id=task.payload->>'source'
        LEFT JOIN importer_lite_source_health health ON health.source=source.id
        WHERE task.idempotency_key >= 'lite:discover:' AND task.idempotency_key < 'lite:discover:~'
          AND task.task_type='DISCOVER'
          AND (
            (task.state IN ('READY','RETRY_WAIT') AND task.next_run_at<=now())
            OR (task.state='RUNNING' AND task.lease_until<now())
          )
          AND source.enabled=true
          AND COALESCE(source.catalog_discovery_enabled,true)
          AND (COALESCE(source.status,'ACTIVE') IN ('ACTIVE','DEGRADED','RECOVERING')
            OR (source.status='COOLDOWN' AND source.cooldown_until IS NOT NULL AND source.cooldown_until<=now()))
          AND COALESCE(health.health,'HEALTHY') NOT IN ('BROKEN','DISABLED')
          AND (source.cooldown_until IS NULL OR source.cooldown_until<=now())
          AND (health.cooldown_until IS NULL OR health.cooldown_until<=now())
        ORDER BY task.next_run_at,task.updated_at,task.id
        LIMIT 1
      )
      UPDATE importer_lite_tasks task
      SET state='RUNNING',lease_owner=$1,lease_until=now()+$2::interval,
        attempts=task.attempts+1,updated_at=now()
      FROM picked
      WHERE task.id=picked.id
        AND (
          (task.state IN ('READY','RETRY_WAIT') AND task.next_run_at<=now())
          OR (task.state='RUNNING' AND task.lease_until<now())
        )
      RETURNING task.*`, [workerId, `${Math.max(30, leaseSeconds)} seconds`]);
        return result.rows[0] ? asTask(result.rows[0]) : null;
    }
    /**
     * Look up already-mapped source_work_id -> work_id in one indexed batch so
     * discovery only enqueues unmapped CATALOG tasks (work_id IS NULL) for
     * genuinely new works.
     */
    async existingSourceWorkMappings(source, sourceWorkIds) {
        const ids = [...new Set(sourceWorkIds.map((id) => String(id || '').trim()).filter(Boolean))];
        if (!source || !ids.length)
            return new Map();
        const metaDb = this.sourceMetadataDb || this.db;
        const result = await metaDb.query(`SELECT source_work_id, work_id::text AS work_id
       FROM importer_work_mappings
       WHERE source=$1 AND source_work_id=ANY($2::text[]) AND work_id IS NOT NULL`, [source, ids]);
        const map = new Map();
        for (const row of result.rows) {
            map.set(String(row.source_work_id), String(row.work_id));
        }
        return map;
    }
    /**
     * Extend a live task lease without changing its owner or state.  Catalog
     * work deliberately performs provider HTTP outside a transaction and a new
     * work can also make a bounded complementary-source pass, so its wall time
     * can legitimately exceed the initial 90-second claim window.  The owner
     * and unexpired-lease predicates retain the normal fencing guarantees: a
     * stale runtime can never revive a task reclaimed by another worker.
     */
    async renewLease(task, workerId, leaseSeconds) {
        const result = await this.db.query(`UPDATE importer_lite_tasks
       SET lease_until=now()+$3::interval,updated_at=now()
       WHERE id=$1::uuid AND state='RUNNING' AND lease_owner=$2 AND lease_until>now()`, [task.id, workerId, `${Math.max(30, leaseSeconds)} seconds`]);
        return result.rowCount === 1;
    }
    /** Product-level transitions, counted once per work rather than per mapping. */
    async metadataProductSnapshot() {
        const result = await this.db.query(`
      SELECT count(*) FILTER (WHERE aliases_activated_at IS NOT NULL)::int AS aliases_activated_works,
        count(*) FILTER (WHERE tags_activated_at IS NOT NULL)::int AS tags_activated_works
      FROM importer_lite_metadata_product_metrics`);
        return {
            aliasesActivatedWorks: Number(result.rows[0]?.aliases_activated_works || 0),
            tagsActivatedWorks: Number(result.rows[0]?.tags_activated_works || 0),
        };
    }
    /**
     * P0 observation has one durable, explicitly bounded lane.  It is never
     * selected by normal claims, so unchanged watches cannot starve P1/P2.
     *
     * The old stored function selected the globally oldest watch. A provider
     * with hundreds of overdue watches could therefore hide every other source
     * for hours even while this lane itself remained healthy. This short
     * transaction locks one durable source cursor, claims the oldest due watch
     * for the next source in its round-robin, and advances only when the claim
     * succeeds. It neither changes chapter priority nor admits media work.
     */
    async claimMonitor(workerId, leaseSeconds) {
        return (await this.claimMonitorBatch(workerId, leaseSeconds, 1))[0] || null;
    }
    /** A small, fenced batch amortizes the source cursor and SQL round trip. */
    async claimMonitorBatch(workerId, leaseSeconds, limit, preferOldest = false, busySources = []) {
        const maximum = Math.max(1, Math.min(8, Math.floor(limit) || 1));
        const metaDb = this.sourceMetadataDb || this.db;
        if (!this.p0MonitorCursorEnsured) {
            await metaDb.query(`INSERT INTO importer_lite_metadata_source_cursor(cursor_name,last_source)
         VALUES($1,NULL) ON CONFLICT(cursor_name) DO NOTHING`, [P0_MONITOR_SOURCE_CURSOR]);
            this.p0MonitorCursorEnsured = true;
        }
        const cleanBusySources = Array.isArray(busySources)
            ? busySources.map((s) => String(s || '').trim()).filter(Boolean)
            : [];
        // Single-statement atomic round-robin monitor claim: locks the durable
        // cursor row, selects the next due provider head using index seek across
        // active sources in importer_sources, claims the task, and advances the cursor
        // in one fast (<10ms) YSQL round trip using importer_lite_tasks_p0_source_due_idx.
        // Sources currently at their in-flight concurrency limit are skipped so other providers never starve.
        const result = await metaDb.query(`
      WITH cur AS (
        SELECT last_source FROM importer_lite_metadata_source_cursor
        WHERE cursor_name=$1 FOR UPDATE
      ), ordered_sources AS MATERIALIZED (
        SELECT s.id AS source
        FROM importer_sources s
        LEFT JOIN importer_lite_source_health h ON h.source = s.id
        LEFT JOIN cur ON true
        WHERE s.enabled = true
          AND s.id NOT IN ('mangaball','mangadex','mangafire')
          AND ${sourceExecutionEligibilitySql('s')}
          AND COALESCE(s.chapter_ingestion_enabled, true) = true
          AND COALESCE(h.health, 'HEALTHY') NOT IN ('BROKEN', 'DISABLED')
          AND (h.cooldown_until IS NULL OR h.cooldown_until <= now())
          AND ($5::text[] IS NULL OR cardinality($5::text[]) = 0 OR NOT (s.id = ANY($5::text[])))
        ORDER BY CASE WHEN s.id > COALESCE(cur.last_source, '') THEN 0 ELSE 1 END, s.id
      ), head AS MATERIALIZED (
        SELECT os.source
        FROM ordered_sources os
        CROSS JOIN LATERAL (
          SELECT t.id,t.priority,t.next_run_at FROM importer_lite_tasks t
          WHERE t.state = 'READY' AND t.task_type = 'CATALOG' AND t.monitoring = true
            AND (t.payload->>'source') = os.source AND t.next_run_at <= now()
          ORDER BY t.priority ASC, t.next_run_at ASC LIMIT 1
        ) t
        ${preferOldest ? 'ORDER BY t.priority ASC, t.next_run_at ASC, os.source ASC' : ''}
        LIMIT 1
      ), picked AS MATERIALIZED (
        SELECT t.id, head.source FROM head
        CROSS JOIN LATERAL (
          SELECT t.id FROM importer_lite_tasks t
          WHERE t.state = 'READY' AND t.task_type = 'CATALOG' AND t.monitoring = true
            AND (t.payload->>'source') = head.source AND t.next_run_at <= now()
          ORDER BY t.priority ASC, t.next_run_at ASC LIMIT $4
        ) t
      ), claimed AS (
        UPDATE importer_lite_tasks task
        SET state='RUNNING',lease_owner=$2,lease_until=now()+$3::interval,
            attempts=attempts+1,updated_at=now()
        FROM picked
        WHERE task.id=picked.id AND task.state='READY'
        RETURNING task.*
      ), advanced AS (
        UPDATE importer_lite_metadata_source_cursor cursor
        SET last_source=head.source,updated_at=now()
        FROM head
        WHERE cursor.cursor_name=$1
          AND EXISTS (SELECT 1 FROM claimed)
        RETURNING cursor.last_source
      )
      SELECT * FROM claimed`, [P0_MONITOR_SOURCE_CURSOR, workerId, `${Math.max(30, leaseSeconds)} seconds`, maximum, cleanBusySources]);
        return result.rows.map(asTask);
    }
    /**
     * Claims one already-durable, no-network metadata replay.  This deliberately
     * is not a second chapter-admission policy: it can only advance the bounded
     * v2 enrichment rows created from metadata we already have, and is invoked
     * at a much lower cadence than normal recovery.  Without this narrow drain,
     * a permanently healthy P1 backlog would make historical aliases and tags
     * wait forever despite requiring neither a source slot nor a chapter worker.
     */
    async claimStoredMetadataReplay(workerId, leaseSeconds, metadataVersion) {
        const db = metadataVersion === 4 ? this.sourceMetadataDb : this.db;
        const result = await db.query(`WITH picked AS (
        SELECT id
        FROM importer_lite_tasks
        WHERE task_type='CATALOG' AND state IN ('READY','RETRY_WAIT') AND monitoring=false
          AND priority=$1 AND next_run_at<=now()
          AND payload ? 'metadataEnrichment'
          AND ($4::text IS NULL OR payload->>'metadataEnrichmentVersion'=$4::text)
        ORDER BY next_run_at,created_at,id
        LIMIT 1 FOR UPDATE SKIP LOCKED
      )
      UPDATE importer_lite_tasks task
      SET state='RUNNING',lease_owner=$2,
          lease_until=now()+$3::interval,attempts=attempts+1,updated_at=now()
      FROM picked WHERE task.id=picked.id
      RETURNING task.*`, [LITE_PRIORITY_RANK.P2, workerId, `${Math.max(30, leaseSeconds)} seconds`,
            metadataVersion === undefined ? null : String(metadataVersion)]);
        return result.rows[0] ? asTask(result.rows[0]) : null;
    }
    /** Recover one expired v4 lease without waiting for the chapter-pool reconciler. */
    async reclaimExpiredStoredMetadataReplayV4() {
        const client = await this.sourceMetadataDb.connect();
        try {
            await client.query('BEGIN');
            const recovered = await client.query(`WITH picked AS (
          SELECT id,work_key,attempts,max_attempts
          FROM importer_lite_tasks
          WHERE task_type='CATALOG' AND state='RUNNING' AND lease_until<now()
            AND payload->>'metadataEnrichmentVersion'='4'
          ORDER BY lease_until LIMIT 1 FOR UPDATE SKIP LOCKED
        ) UPDATE importer_lite_tasks task
        SET state=CASE WHEN picked.attempts>=picked.max_attempts THEN 'BLOCKED' ELSE 'READY' END,
            reason=CASE WHEN picked.attempts>=picked.max_attempts
              THEN 'LEASE_EXPIRED_MAX_ATTEMPTS' ELSE 'LEASE_EXPIRED' END,
            lease_owner=NULL,lease_until=NULL,next_run_at=CASE WHEN picked.attempts>=picked.max_attempts
              THEN task.next_run_at ELSE now() END,updated_at=now()
        FROM picked WHERE task.id=picked.id
        RETURNING task.work_key`, []);
            for (const row of recovered.rows)
                await this.refreshReadyWork(client, row.work_key);
            await client.query('COMMIT');
            return recovered.rowCount || 0;
        }
        catch (error) {
            await client.query('ROLLBACK').catch(() => { });
            throw error;
        }
        finally {
            client.release();
        }
    }
    /**
     * A bounded view of sources that can serve an actual next-priority chapter
     * import. One task is sampled per ready work, rather than the first N task
     * rows, so a single large work cannot hide other ready works. Historical P1
     * catalog work is admitted by the ready-work projection separately; it must
     * not consume a chapter slot until it has materialised a real chapter.
     *
     * The sample is outside claim; durable claim remains authority for task
     * existence, priority and work-level round-robin.
     */
    async readyImportSourceRows(limit = 128, db = this.db) {
        const maximum = Math.max(1, Math.min(256, limit));
        const hasFocus = this.p1FocusWorkKeys.length > 0;
        const focusOrderSql = hasFocus
            ? 'CASE WHEN rw.work_key = ANY($2::text[]) THEN 0 ELSE 1 END,'
            : '';
        const params = hasFocus ? [maximum, this.p1FocusWorkKeys] : [maximum];
        const result = await db.query(`WITH candidate_works AS (
        SELECT rw.work_key,rw.priority
        FROM importer_lite_ready_works rw
        WHERE rw.operation_rank<=1 AND rw.next_run_at<=now()
          -- Match the claim function's in-flight work fence.  A later P0
          -- task is deliberately visible in the durable projection while an
          -- earlier P1 chapter for the same work runs, but it is not an
          -- executable source alternative yet. Including it here could make
          -- the fairness sample contain only that blocked source and leave
          -- unrelated P1 workers holding admission while they wait for a
          -- saturated per-source slot.
          AND NOT EXISTS (
            SELECT 1
            FROM importer_lite_tasks active_work
            WHERE active_work.work_key=rw.work_key
              AND active_work.task_type IN ('IMPORT_CHAPTER','PUBLISH')
              AND active_work.state='RUNNING'
          )
        ORDER BY rw.priority,${focusOrderSql}rw.last_claimed_at NULLS FIRST,rw.next_run_at,rw.work_key
        LIMIT $1
      ), raw_candidates AS MATERIALIZED (
        SELECT DISTINCT ON (rw.work_key) t.id,t.work_id,t.chapter_key,rw.priority
        FROM candidate_works rw
        JOIN importer_lite_tasks t
          ON t.work_key=rw.work_key AND t.state='READY' AND t.monitoring=false
         AND t.priority=rw.priority AND t.task_type='IMPORT_CHAPTER' AND t.next_run_at<=now()
        ORDER BY rw.work_key,t.next_run_at,t.created_at
      ), import_routes AS (
        -- Pick the priority lane only after routing evidence is available.
        -- A durable P0 task may be READY while its confirmed route is in a
        -- source cooldown or has been retired. It remains first for the
        -- claim path and will be retried safely, but it cannot be a fairness
        -- alternative yet. Letting that empty lane hide routable P1 work
        -- keeps P1 admission slots waiting on saturated sources.
        SELECT c.id,c.priority,cs.source,cs.source_rank,cs.created_at
        FROM raw_candidates c
        JOIN works w ON w.id=c.work_id
        JOIN media cover ON cover.id=w.cover_id
        JOIN importer_lite_chapter_sources cs
          ON cs.work_id=c.work_id AND cs.canonical_chapter_key=c.chapter_key
        JOIN importer_sources s ON s.id=cs.source
        LEFT JOIN importer_lite_source_health h ON h.source=cs.source
        WHERE NULLIF(btrim(w.title),'') IS NOT NULL
          AND NULLIF(btrim(w.slug),'') IS NOT NULL
          AND cover.storage_ready=true AND cover.bytes>=1500
          AND cs.confirmed=true AND cs.has_chapter=true
          AND s.enabled=true
          AND COALESCE(s.chapter_ingestion_enabled,true)
          AND ${sourceExecutionEligibilitySql('s')}
          AND COALESCE(h.health,'HEALTHY') NOT IN ('BROKEN','DISABLED')
          AND (h.cooldown_until IS NULL OR h.cooldown_until<=now())
      ), lane AS (
        SELECT min(priority) AS priority FROM import_routes
      )
      -- P0 remains the durable claim priority.  Its first saturated source
      -- waiter is reserved by the executor, but subsequent P0 claims must be
      -- able to yield to a routable P1 source instead of holding every chapter
      -- admission seat behind that one provider.  Include P1 only in this
      -- *local fairness view* when P0 is the live lane; STAFF remains
      -- exclusive and the claim function remains the priority authority.
      --
      -- One route per executable work, ordered with the same source-rank
      -- preference as the executor. Keeping the rows (rather than a DISTINCT
      -- source list) lets the local fair-share gate distinguish a provider
      -- with one runnable work from one with a real ready-work backlog.
      SELECT DISTINCT ON (routes.id) routes.source
      FROM import_routes routes
      JOIN lane ON (
        routes.priority=lane.priority
        OR (lane.priority=1 AND routes.priority=2)
      )
      ORDER BY routes.id, routes.source_rank, routes.created_at, routes.source`, params);
        return result.rows.map((row) => String(row.source));
    }
    async readyImportSources(limit = 128, db = this.db) {
        return [...new Set(await this.readyImportSourceRows(limit, db))].sort();
    }
    /**
     * A bounded source demand view for the local fairness gate.  The count is
     * deliberately one executable frontier per work, never raw chapters, so a
     * deep single-work P1 backlog cannot claim capacity reserved for unrelated
     * works or sources.
     */
    async readyImportSourceDemand(limit = 128, db = this.db) {
        const demand = new Map();
        for (const source of await this.readyImportSourceRows(limit, db)) {
            demand.set(source, (demand.get(source) || 0) + 1);
        }
        return demand;
    }
    /**
     * Recent public publication attribution. It reads the durable publication
     * event rather than task DONE, so media storage or a stale task can never
     * inflate throughput/diversity metrics.
     */
    async publicationDiversitySnapshot(windowSeconds = 60) {
        const seconds = Math.max(10, Math.min(3_600, Math.floor(windowSeconds)));
        const result = await this.db.query(`WITH events AS (
        SELECT source,work_id,priority
        FROM importer_publication_events
        WHERE transition_at>=now()+($1::int * interval '1 second')
      ), source_counts AS (
        SELECT source,count(*)::int AS chapters,count(DISTINCT work_id)::int AS works
        FROM events WHERE source IS NOT NULL GROUP BY source
      ), work_counts AS (
        SELECT work_id,count(*)::int AS chapters FROM events WHERE work_id IS NOT NULL GROUP BY work_id
      ), priority_counts AS (
        SELECT priority,count(*)::int AS chapters,count(DISTINCT work_id)::int AS works
        FROM events GROUP BY priority
      )
      SELECT
        (SELECT count(*)::int FROM events) AS chapters,
        (SELECT count(*)::int FROM events WHERE source IS NOT NULL) AS attributed_chapters,
        (SELECT count(DISTINCT source)::int FROM events WHERE source IS NOT NULL) AS unique_sources,
        (SELECT count(DISTINCT work_id)::int FROM events WHERE work_id IS NOT NULL) AS unique_works,
        (SELECT COALESCE(max(chapters),0)::int FROM source_counts) AS top_source_chapters,
        (SELECT COALESCE(sum(chapters),0)::int FROM (SELECT chapters FROM source_counts ORDER BY chapters DESC LIMIT 3) top_sources) AS top_three_source_chapters,
        (SELECT COALESCE(max(chapters),0)::int FROM work_counts) AS top_work_chapters,
        (SELECT COALESCE(sum(chapters),0)::int FROM (SELECT chapters FROM work_counts ORDER BY chapters DESC LIMIT 3) top_works) AS top_three_work_chapters,
        (SELECT COALESCE(jsonb_agg(jsonb_build_object('source',source,'chapters',chapters,'works',works) ORDER BY chapters DESC,source),'[]'::jsonb) FROM source_counts) AS by_source,
        (SELECT COALESCE(jsonb_agg(jsonb_build_object('priority',priority,'chapters',chapters,'works',works) ORDER BY priority),'[]'::jsonb) FROM priority_counts) AS by_priority`, [-seconds]);
        const row = result.rows[0] || {};
        const chapters = Number(row.chapters || 0);
        const share = (value) => chapters ? Math.round(Number(value || 0) * 10_000 / chapters) / 100 : 0;
        return {
            windowSeconds: seconds,
            chapters,
            attributedChapters: Number(row.attributed_chapters || 0),
            uniqueSources: Number(row.unique_sources || 0),
            uniqueWorks: Number(row.unique_works || 0),
            topSourceSharePercent: share(row.top_source_chapters),
            topThreeSourcesSharePercent: share(row.top_three_source_chapters),
            topWorkSharePercent: share(row.top_work_chapters),
            topThreeWorksSharePercent: share(row.top_three_work_chapters),
            bySource: row.by_source || [],
            byPriority: row.by_priority || [],
        };
    }
    /**
     * Source fairness is an admission yield, not a retry: no provider request
     * was started and consuming retry budget would eventually block healthy
     * work. Moving the work briefly behind its peers lets a source with an
     * available permit proceed without changing durable priority.
     */
    async yieldForSourceFairness(task, workerId, delayMs = 750) {
        const clampedDelay = Math.max(50, Math.min(5_000, delayMs));
        const result = await this.db.query('SELECT public.importer_lite_yield_task($1::uuid, $2, $3::int) AS yielded', [task.id, workerId, clampedDelay]);
        return result.rows[0]?.yielded === true;
    }
    /**
     * Discovery is a low-cost producer, never a reason to monopolize P2.  This
     * bounded startup repair only defers due DISCOVER rows; catalog and chapter
     * tasks are untouched.  Discovery tasks are claimed directly without
     * ready-work projections.
     */
    async staggerReadyDiscoveries(spacingMs, limit = 64) {
        const candidates = await this.db.query(`SELECT id FROM importer_lite_tasks
       WHERE task_type='DISCOVER' AND state='READY' AND next_run_at<=now()
       ORDER BY created_at,id LIMIT $1`, [Math.max(1, Math.min(limit, 256))]);
        if (!candidates.rows.length)
            return 0;
        const base = Date.now();
        const step = Math.max(1_000, spacingMs);
        const ids = candidates.rows.map((row) => row.id);
        const runAts = candidates.rows.map((_, index) => new Date(base + index * step));
        const result = await this.db.query(`UPDATE importer_lite_tasks AS t
       SET next_run_at = v.next_run_at, updated_at = now()
       FROM (SELECT unnest($1::uuid[]) AS id, unnest($2::timestamptz[]) AS next_run_at) AS v
       WHERE t.id = v.id`, [ids, runAts]);
        return result.rowCount || 0;
    }
    /**
     * The v1 catalog contains durable, confirmed source/work mappings that may
     * predate Lite's chapter-manifest table.  They are evidence worth checking,
     * but are not evidence that a numeric chapter gap exists.  This bounded
     * admission turns only a few such mappings into P1 catalog passes.  A deep
     * executable reserve keeps those passes deferred, rather than suppressing
     * them entirely: that small producer lane is what lets an older, healthy
     * source eventually materialize a confirmed manifest. Cataloging remains
     * outside the claim SQL; its confirmed manifest is what may subsequently
     * create IMPORT_CHAPTER tasks.
     *
     * A durable source cursor probes only a few providers per turn.  This avoids
     * letting the newest rows of a giant mapping set hide older providers before
     * source fairness can run, without making the claim path scan the catalog.
     */
    async refillHistoricalP1Catalog(readyFloor = 64, batchLimit = 16) {
        const floor = Math.max(1, Math.min(512, readyFloor));
        const maximum = Math.max(1, Math.min(32, batchLimit));
        // Four normal reserve windows is enough to keep workers supplied while
        // making catalog production strictly yield to a genuinely large,
        // executable P1 backlog. A raw READY row behind a cooled-down or disabled
        // route is not executable demand: counting it here would suppress the
        // bounded, source-diverse catalog producer precisely when healthy sources
        // need to materialize their alternatives. Bound the eligibility check as
        // well; it remains outside the claim path.
        const abundantFloor = Math.min(512, Math.max(64, floor * 4));
        this.historicalP1ReserveAbundant = false;
        // The old abundance fast-path returned here.  That saved one transaction
        // but also made a deep hot backlog permanently invisible to the bounded
        // source producer.  The single transaction below is now the only reserve
        // probe and also serializes the small durable source cursor.
        const metaDb = this.sourceMetadataDb || this.db;
        const client = await metaDb.connect();
        try {
            await client.query('BEGIN');
            const reserve = await client.query(`SELECT count(*)::int AS count FROM (
        SELECT 1
        FROM importer_lite_ready_works rw
        WHERE rw.priority = 2 AND rw.operation_rank = 1 AND rw.next_run_at <= now()
        LIMIT $1
      ) AS bounded_ready`, [abundantFloor]);
            const current = Number(reserve.rows[0]?.count || 0);
            this.historicalP1ReserveAbundant = current >= abundantFloor;
            // A large hot P1 import reserve is productive work.  Keep only a small,
            // source-diverse historical producer reserve while it is healthy, but
            // mark that reserve as deferred: it must not take a chapter worker or a
            // YSQL connection ahead of already-materialised imports.  Once the
            // import reserve is low, newly admitted checks keep their normal fair
            // P1 rank so mappings cannot remain hidden behind one giant work.
            const maintainingHealthyReserve = current >= floor;
            const target = maintainingHealthyReserve ? Math.min(4, maximum) : maximum;
            const perSource = maintainingHealthyReserve ? 1 : 2;
            // The refill batch itself is durable work.  Count it separately from
            // imports: otherwise a steadily draining import reserve can trigger a
            // fresh 16-row catalog batch every recovery turn and grow a hidden P1
            // producer queue.  Existing work is never discarded; this merely caps
            // new pending historical checks at one small batch.
            const pendingBackfill = await client.query(`WITH active_backfill AS (
        SELECT t.state, t.next_run_at, t.created_at, t.payload->>'source' AS source
        FROM importer_lite_tasks t
        WHERE t.state IN ('READY', 'RUNNING', 'RETRY_WAIT')
          AND t.monitoring = false
          AND t.priority = 2
          AND t.task_type = 'CATALOG'
          AND t.payload ? 'historicalP1Backfill'
      ), bounded_backfill AS (
        SELECT state, next_run_at, source
        FROM active_backfill
        ORDER BY CASE WHEN state = 'RUNNING' OR (state = 'READY' AND next_run_at <= now()) THEN 0 ELSE 1 END,
          next_run_at, created_at
        LIMIT $1
      ) SELECT
        (SELECT count(*)::int FROM bounded_backfill) AS total,
        (SELECT count(DISTINCT source) FILTER (
          WHERE state = 'RUNNING' OR (state = 'READY' AND next_run_at <= now())
        )::int FROM bounded_backfill) AS active_sources,
        (SELECT array_agg(DISTINCT source) FILTER (WHERE source IS NOT NULL) FROM active_backfill) AS occupied_sources`, [maximum]);
            const pending = Number(pendingBackfill.rows[0]?.total || 0);
            const activeSources = Number(pendingBackfill.rows[0]?.active_sources || 0);
            const occupiedSources = new Set((pendingBackfill.rows[0]?.occupied_sources || []).map((s) => String(s)));
            // A provider in cooldown remains durable, but it must not occupy every
            // healthy-source check in this tiny producer lane. Total pending work is
            // still capped, while only runnable/running checks consume the target.
            if (pending >= maximum || activeSources >= target) {
                await client.query('COMMIT');
                return 0;
            }
            // This is an explicitly bounded source-diverse producer reserve, not a
            // second scheduler and not a scan in the claim path. It gives genuine
            // alternatives a chance to materialize while a hot source is healthy.
            const limit = Math.min(maximum - pending, target - activeSources);
            // Walk sources durably instead of taking a global newest-mapping window.
            // The latter let a recent MangaFlix/Manhastro burst fill all 128 rows
            // before the per-source rank even ran. Up to six indexed source probes
            // fill at most four producer seats per recovery turn; they never run in
            // claim SQL.
            await client.query(`INSERT INTO importer_lite_metadata_source_cursor(cursor_name,last_source)
        VALUES('historical_p1_catalog',NULL) ON CONFLICT(cursor_name) DO NOTHING`);
            const cursor = await client.query(`
        SELECT last_source FROM importer_lite_metadata_source_cursor
        WHERE cursor_name='historical_p1_catalog' FOR UPDATE`);
            const lastSource = cursor.rows[0]?.last_source || null;
            const eligibleSourceSelect = (cursorPredicate) => `
        SELECT s.id AS source
        FROM importer_sources s
        LEFT JOIN importer_lite_source_health h ON h.source=s.id
        WHERE s.enabled=true AND ${cursorPredicate}
          AND ${sourceExecutionEligibilitySql('s')}
          AND COALESCE(s.chapter_ingestion_enabled,true)
          AND COALESCE(h.health,'HEALTHY') NOT IN ('BROKEN','DISABLED')
          AND (h.cooldown_until IS NULL OR h.cooldown_until<=now())
        ORDER BY s.id LIMIT $2`;
            // Probe a tiny surplus of sources: a durable delayed retry can occupy a
            // source's producer seat, but must not make the next healthy source wait
            // for another recovery interval. Only successfully inspected sources
            // advance the cursor below, so this never skips the remainder.
            const sourceProbeLimit = Math.min(16, Math.max(limit, target) + 2);
            const sourceProbe = await client.query(`
        WITH after_cursor AS (
          ${eligibleSourceSelect(lastSource ? 's.id>$1::text' : '$1::text IS NULL')}
        ), wrapped AS (
          ${eligibleSourceSelect(lastSource ? 's.id<=$1::text' : '$1::text IS NOT NULL')}
        )
        SELECT source FROM after_cursor
        UNION ALL SELECT source FROM wrapped
        LIMIT $2`, [lastSource, sourceProbeLimit]);
            if (!sourceProbe.rowCount) {
                await client.query('COMMIT');
                return 0;
            }
            const candidates = [];
            let lastProbedSource = null;
            for (const source of sourceProbe.rows.map((row) => String(row.source))) {
                if (candidates.length >= limit)
                    break;
                lastProbedSource = source;
                if (occupiedSources.has(source))
                    continue;
                await client.query('SAVEPOINT historical_p1_source_candidate');
                try {
                    await client.query(`SET LOCAL statement_timeout = '2500ms'`);
                    const candidate = await client.query(`
            SELECT m.source,m.source_work_id,m.work_id
            FROM importer_work_mappings m
            JOIN importer_sources s ON s.id=m.source
            LEFT JOIN importer_lite_source_health h ON h.source=m.source
            WHERE m.source=$1 AND m.work_id IS NOT NULL
              AND m.sync_status IN ('WAITING_ADMISSION','SYNCED','ACTIVE')
              AND s.enabled=true
              AND ${sourceExecutionEligibilitySql('s')}
              AND COALESCE(s.chapter_ingestion_enabled,true)
              AND COALESCE(h.health,'HEALTHY') NOT IN ('BROKEN','DISABLED')
              AND (h.cooldown_until IS NULL OR h.cooldown_until<=now())
              AND NOT EXISTS (
                SELECT 1 FROM importer_lite_tasks t
                WHERE t.idempotency_key='lite:catalog:' || m.source || ':' || m.source_work_id
              )
            ORDER BY m.source_work_id
            LIMIT $2`, [source, perSource]);
                    candidates.push(...candidate.rows);
                    await client.query('RELEASE SAVEPOINT historical_p1_source_candidate');
                }
                catch {
                    await client.query('ROLLBACK TO SAVEPOINT historical_p1_source_candidate');
                }
                finally {
                    await client.query(`SET LOCAL statement_timeout = '12000ms'`);
                }
            }
            await client.query(`UPDATE importer_lite_metadata_source_cursor
        SET last_source=$2,updated_at=now() WHERE cursor_name=$1`, ['historical_p1_catalog', lastProbedSource || sourceProbe.rows.at(-1).source]);
            if (!candidates.length) {
                await client.query('COMMIT');
                return 0;
            }
            const values = [];
            const tuples = candidates.map((row, index) => {
                const start = index * 12;
                const source = String(row.source);
                const sourceWorkId = String(row.source_work_id);
                values.push('CATALOG', String(row.work_id), `work:${row.work_id}`, null, LITE_PRIORITY_RANK.P1, LITE_PRIORITY_RANK.P1, `lite:catalog:${source}:${sourceWorkId}`, new Date(), 8, JSON.stringify({ source, sourceWorkId, historicalP1Backfill: true,
                    deferBehindReadyImport: maintainingHealthyReserve }), JSON.stringify({}), false);
                return `($${start + 1},$${start + 2}::uuid,$${start + 3},$${start + 4},$${start + 5},$${start + 6},$${start + 7},$${start + 8},$${start + 9},$${start + 10}::jsonb,$${start + 11}::jsonb,$${start + 12})`;
            });
            const inserted = await client.query(`INSERT INTO importer_lite_tasks
        (task_type,work_id,work_key,chapter_key,priority,base_priority,idempotency_key,next_run_at,max_attempts,payload,checkpoint,monitoring)
        VALUES ${tuples.join(',')}
        ON CONFLICT(idempotency_key) DO NOTHING
        RETURNING work_key`, values);
            await this.refreshReadyWorks(client, inserted.rows.map((row) => String(row.work_key)));
            await client.query('COMMIT');
            return inserted.rowCount || 0;
        }
        catch (error) {
            await client.query('ROLLBACK').catch(() => { });
            throw error;
        }
        finally {
            client.release();
        }
    }
    /**
     * A healthy P1 reserve keeps historical catalog checks deferred so they do
     * not continuously consume chapter workers. Promote one existing check on
     * the already-bounded recovery turn: this is a producer quota, not a
     * second scheduler. It keeps alternative mapped sources progressing while
     * leaving the materialised import reserve dominant.
     */
    async promoteDeferredHistoricalP1Catalog(activeImportSources = []) {
        const metaDb = this.sourceMetadataDb || this.db;
        const client = await metaDb.connect();
        try {
            await client.query('BEGIN');
            const busySources = [...new Set([...activeImportSources].filter(Boolean))];
            const sourcePenalty = busySources.length
                ? `CASE WHEN t.payload->>'source'=ANY($1::text[]) THEN 1 ELSE 0 END,`
                : '';
            const candidate = await client.query(`SELECT t.id,t.work_key
        FROM importer_lite_tasks t
        WHERE t.state IN ('READY', 'RUNNING', 'RETRY_WAIT')
          AND t.state='READY' AND t.monitoring=false AND t.priority=2 AND t.task_type='CATALOG'
          AND t.next_run_at<=now() AND t.payload ? 'historicalP1Backfill'
          AND COALESCE(t.payload->>'deferBehindReadyImport','false')='true'
        ORDER BY ${sourcePenalty} t.next_run_at,t.created_at,t.id
        LIMIT 1 FOR UPDATE OF t SKIP LOCKED`, busySources.length ? [busySources] : []);
            const row = candidate.rows[0];
            if (!row) {
                await client.query('COMMIT');
                return false;
            }
            await client.query(`UPDATE importer_lite_tasks
        SET payload=payload-'deferBehindReadyImport',updated_at=now()
        WHERE id=$1::uuid`, [row.id]);
            await this.refreshReadyWork(client, row.work_key);
            await client.query('COMMIT');
            return true;
        }
        catch (error) {
            await client.query('ROLLBACK').catch(() => { });
            throw error;
        }
        finally {
            client.release();
        }
    }
    /**
     * Recover only catalog tasks blocked by the pre-deduplication batch bug.
     * This is deliberately not a generic BLOCKED retry: normal permanent
     * failures retain their diagnosis and need their own safe repair path.
     */
    async recoverDuplicateCanonicalCatalogFailures(limit = 32) {
        const maximum = Math.max(1, Math.min(64, limit));
        const metaDb = this.db;
        const client = await metaDb.connect();
        try {
            await client.query('BEGIN');
            const recovered = await client.query(`WITH picked AS (
           SELECT id,work_key FROM importer_lite_tasks
           WHERE state='BLOCKED' AND task_type='CATALOG' AND reason=$1
           ORDER BY updated_at,id LIMIT $2 FOR UPDATE SKIP LOCKED
         ) UPDATE importer_lite_tasks task SET
           state='READY',attempts=0,reason=NULL,next_run_at=now(),
           lease_owner=NULL,lease_until=NULL,updated_at=now()
         FROM picked WHERE task.id=picked.id
         RETURNING picked.work_key`, [DUPLICATE_CANONICAL_BATCH_REASON, maximum]);
            await this.refreshReadyWorks(client, recovered.rows.map((row) => String(row.work_key)));
            await client.query('COMMIT');
            return recovered.rowCount || 0;
        }
        catch (error) {
            await client.query('ROLLBACK').catch(() => { });
            throw error;
        }
        finally {
            client.release();
        }
    }
    /**
     * Before the P0/P2 policy boundary was corrected, an operational source
     * with `catalog_discovery_enabled=false` could park its existing-work P0
     * monitor as SOURCE_NOT_READY:ACTIVE. That switch is for source-wide P2
     * DISCOVER only; reopen just those legacy monitor rows in small, idempotent
     * batches. Normal source health/cooldown eligibility remains authoritative.
     */
    async reopenP0MonitorsBlockedByDiscoveryPolicy(limit = 64) {
        const maximum = Math.max(1, Math.min(64, limit));
        const metaDb = this.db;
        const client = await metaDb.connect();
        try {
            await client.query('BEGIN');
            const reopened = await client.query(`
        WITH picked AS (
          SELECT t.id
          FROM importer_lite_tasks t
          JOIN importer_sources s ON s.id=t.payload->>'source'
          LEFT JOIN importer_lite_source_health h ON h.source=s.id
          WHERE t.task_type='CATALOG'
            AND t.monitoring=true
            AND t.state IN ('BLOCKED','RETRY_WAIT')
            AND t.reason='SOURCE_NOT_READY:ACTIVE'
            AND s.enabled=true
            AND COALESCE(s.catalog_discovery_enabled,true)=false
            AND ${sourceExecutionEligibilitySql('s')}
            AND COALESCE(h.health,'HEALTHY') NOT IN ('BROKEN','DISABLED')
            AND (h.cooldown_until IS NULL OR h.cooldown_until<=now())
          ORDER BY t.next_run_at,t.created_at,t.id
          LIMIT $1 FOR UPDATE OF t SKIP LOCKED
        )
        UPDATE importer_lite_tasks task SET
          state='READY',attempts=0,reason=NULL,next_run_at=now(),
          lease_owner=NULL,lease_until=NULL,updated_at=now()
        FROM picked WHERE task.id=picked.id
        RETURNING task.*`, [maximum]);
            await client.query('COMMIT');
            return reopened.rowCount || 0;
        }
        catch (error) {
            await client.query('ROLLBACK').catch(() => { });
            throw error;
        }
        finally {
            client.release();
        }
    }
    /**
     * Source administration and transient Lite health remain durable authority.
     * This indexed one-source read happens before source networking, never in a
     * normal task claim, and prevents a paused/cooling provider from consuming
     * a catalog or discovery slot.
     */
    async sourcePolicy(source) {
        const map = await this.sourcePolicies([source]);
        return map.get(source) || null;
    }
    /**
     * Batch read operational policies for multiple sources in a single query.
     * Replaces N+1 sourcePolicy roundtrips during whole-catalog cross-source discovery.
     */
    async sourcePolicies(sources) {
        const normalized = [...new Set(sources.map((s) => String(s || '').trim()).filter(Boolean))];
        if (!normalized.length)
            return new Map();
        const cacheTtlMs = (process.env.NODE_ENV === 'test' || process.env.VITEST) ? 0 : 15_000;
        const nowMs = Date.now();
        const now = new Date(nowMs);
        const map = new Map();
        const missing = [];
        if (cacheTtlMs > 0) {
            for (const id of normalized) {
                const cached = this.sourcePolicyCache.get(id);
                if (cached && nowMs - cached.at <= cacheTtlMs) {
                    const p = cached.policy;
                    const expiredAdminCooldown = p.status === 'COOLDOWN' && Boolean(p.cooldownUntil && p.cooldownUntil <= now);
                    const adminActive = p.enabled && (['ACTIVE', 'DEGRADED', 'RECOVERING'].includes(p.status) || expiredAdminCooldown);
                    const healthAllowed = !['BROKEN', 'DISABLED'].includes(p.health);
                    const runnable = adminActive && healthAllowed && (!p.cooldownUntil || p.cooldownUntil <= now);
                    map.set(id, { ...p, runnable });
                }
                else {
                    missing.push(id);
                }
            }
        }
        else {
            missing.push(...normalized);
        }
        if (!missing.length)
            return map;
        const policyDb = this.sourceMetadataDb || this.db;
        const result = await policyDb.query(`SELECT s.id,s.enabled,s.status,s.sync_interval_minutes,
        COALESCE(s.catalog_discovery_enabled,true) AS catalog_discovery_enabled,
        COALESCE(s.chapter_ingestion_enabled,true) AS chapter_ingestion_enabled,
        s.cooldown_until AS source_cooldown,h.health,h.cooldown_until AS health_cooldown
      FROM importer_sources s
      LEFT JOIN importer_lite_source_health h ON h.source=s.id
      WHERE s.id = ANY($1::text[])`, [missing]);
        for (const row of result.rows) {
            const sourceCooldown = row.source_cooldown ? new Date(row.source_cooldown) : null;
            const healthCooldown = row.health_cooldown ? new Date(row.health_cooldown) : null;
            const cooldownUntil = [sourceCooldown, healthCooldown]
                .filter((value) => Boolean(value && Number.isFinite(value.getTime())))
                .sort((a, b) => b.getTime() - a.getTime())[0] || null;
            const status = String(row.status || 'ACTIVE').toUpperCase();
            const health = String(row.health || 'HEALTHY').toUpperCase();
            const expiredAdministrativeCooldown = status === 'COOLDOWN' && Boolean(sourceCooldown && sourceCooldown <= now);
            const administrativelyActive = row.enabled === true &&
                (['ACTIVE', 'DEGRADED', 'RECOVERING'].includes(status) || expiredAdministrativeCooldown);
            const healthAllowed = !['BROKEN', 'DISABLED'].includes(health);
            const runnable = administrativelyActive && healthAllowed && (!cooldownUntil || cooldownUntil <= now);
            const minutes = Number(row.sync_interval_minutes);
            const policy = {
                enabled: row.enabled === true,
                status, health, cooldownUntil,
                syncIntervalMs: Math.max(5, Math.min(24 * 60, Number.isFinite(minutes) ? minutes : 30)) * 60_000,
                catalogDiscoveryEnabled: row.catalog_discovery_enabled !== false,
                chapterIngestionEnabled: row.chapter_ingestion_enabled !== false,
                runnable,
            };
            const id = String(row.id);
            map.set(id, policy);
            if (cacheTtlMs > 0)
                this.sourcePolicyCache.set(id, { at: nowMs, policy });
        }
        return map;
    }
    /**
     * Sources that can safely be queried while a newly-created work is still in
     * its initial catalog pass.  This is intentionally one indexed set query:
     * onboarding must not turn a complementary-alias lookup into an N+1 policy
     * read, and mappings already attached to the work are not searched again.
     */
    async complementaryCatalogSources(workId, originSource) {
        const metaDb = this.sourceMetadataDb || this.db;
        const result = await metaDb.query(`
      SELECT s.id AS source
      FROM importer_sources s
      LEFT JOIN importer_lite_source_health health ON health.source=s.id
      WHERE s.id<>$2
        AND s.enabled=true
        AND ${sourceExecutionEligibilitySql('s')}
        AND COALESCE(health.health,'HEALTHY') NOT IN ('BROKEN','DISABLED')
        AND (health.cooldown_until IS NULL OR health.cooldown_until<=now())
        AND NOT EXISTS (
          SELECT 1 FROM importer_work_mappings mapping
          WHERE mapping.work_id=$1::uuid AND mapping.source=s.id
            AND (
              (jsonb_typeof(mapping.metadata->'alternativeTitles') = 'array' AND jsonb_array_length(mapping.metadata->'alternativeTitles') > 0)
              OR (jsonb_typeof(mapping.metadata->'titulo_alternative') = 'array' AND jsonb_array_length(mapping.metadata->'titulo_alternative') > 0)
            )
        )
      ORDER BY s.id`, [workId, originSource]);
        return result.rows.map((row) => String(row.source));
    }
    /**
     * A coverage pass must not assume that the mappings already attached to a
     * canonical work are exhaustive.  Return every still-operational provider
     * that has no mapping for this work, including a temporarily cooled one so
     * its outcome can be recorded as deferred rather than silently skipped.
     */
    async crossSourceDiscoverySources(workId) {
        const dbToUse = this.sourceMetadataDb || this.db;
        const result = await dbToUse.query(`
      SELECT s.id AS source
      FROM importer_sources s
      LEFT JOIN importer_lite_source_health health ON health.source=s.id
      WHERE s.enabled=true
        AND COALESCE(s.status,'ACTIVE') IN ('ACTIVE','DEGRADED','RECOVERING','COOLDOWN')
        AND COALESCE(health.health,'HEALTHY') NOT IN ('BROKEN','DISABLED')
        AND NOT EXISTS (
          SELECT 1 FROM importer_work_mappings mapping
          JOIN importer_lite_metadata_enrichment status
            ON status.source=mapping.source AND status.source_work_id=mapping.source_work_id
          WHERE mapping.work_id=$1::uuid AND mapping.source=s.id
            AND status.version >= 7
            AND (
              (jsonb_typeof(mapping.metadata->'alternativeTitles') = 'array' AND jsonb_array_length(mapping.metadata->'alternativeTitles') > 0)
              OR (jsonb_typeof(mapping.metadata->'titulo_alternative') = 'array' AND jsonb_array_length(mapping.metadata->'titulo_alternative') > 0)
            )
        )
      ORDER BY s.id`, [workId]);
        return result.rows.map((row) => String(row.source));
    }
    /**
     * Admit a tiny, durable whole-catalog cross-source discovery lane.
     * It is deliberately separate from V5's known-mapping refresh: each task
     * records every operational provider considered for its canonical work and
     * only creates a mapping after runtime identity verification.
     */
    async refillCrossSourceMetadataDiscovery(maxPending = 2) {
        // Global coverage is an optional metadata lane. Its refiller writes only
        // its own durable P2 tasks, so it must use the same isolated connection
        // as its claimant instead of waiting behind chapter claims on the main
        // pool and leaving both discovery seats empty.
        const client = await this.sourceMetadataDb.connect();
        try {
            await client.query('BEGIN');
            const v4 = await client.query(`SELECT value FROM settings WHERE key=$1 LIMIT 1`, [METADATA_REPLAY_V4_ENABLED_KEY]);
            if (v4.rows[0]?.value !== 'complete') {
                await client.query('COMMIT');
                return 0;
            }
            const pending = await client.query(`
        SELECT task.work_id::text AS work_id, task.payload->>'discoveryLane' AS lane
        FROM importer_lite_tasks task
        WHERE task.task_type='CATALOG'
          AND task.idempotency_key >= $1 AND task.idempotency_key < $2
          AND task.state IN ('READY','RUNNING','RETRY_WAIT')
        LIMIT 33
      `, [METADATA_CROSS_SOURCE_DISCOVERY_V7_PREFIX, `${METADATA_CROSS_SOURCE_DISCOVERY_V7_PREFIX}~`]);
            const capacity = Math.max(1, Math.min(32, Math.trunc(maxPending)));
            const pendingCount = pending.rows.length;
            if (pendingCount >= capacity) {
                await client.query('COMMIT');
                return 0;
            }
            const hasActiveUnlinked = pending.rows.some((r) => r.lane === 'unlinked' || !r.lane);
            const available = capacity - pendingCount;
            let admittedDeferred = 0;
            // Re-admit due deferred tasks in batch up to available capacity (capped at 16 per turn).
            const deferredLimit = Math.min(available, 16);
            if (deferredLimit > 0) {
                const deferred = await client.query(`
          /*+ IndexScan(importer_lite_tasks importer_lite_tasks_cross_source_deferred_open_idx) */
          SELECT id::text,work_key,checkpoint
          FROM importer_lite_tasks
          WHERE task_type='CATALOG' AND state='DONE'
            AND idempotency_key >= $2 AND idempotency_key < $3
            AND payload ? 'metadataCrossSourceDiscovery'
            AND checkpoint->>'coverage'='CROSS_SOURCE_PARTIAL_EXTERNAL_DEFERRED'
            AND next_run_at<=now()
            -- Literal (not a bind parameter) so the planner can prove the
            -- importer_lite_tasks_cross_source_deferred_open_idx predicate.
            -- Exhausted rows (thousands) otherwise stay in the older index
            -- and every refill walked them all (~7.7 s per call).
            AND COALESCE(NULLIF(checkpoint->>'deferredRounds','')::int,0)<${METADATA_CROSS_SOURCE_DEFERRED_RECHECK_LIMIT}
          ORDER BY next_run_at,id
          LIMIT $1
          FOR UPDATE SKIP LOCKED`, [
                    deferredLimit,
                    METADATA_CROSS_SOURCE_DISCOVERY_V7_PREFIX,
                    `${METADATA_CROSS_SOURCE_DISCOVERY_V7_PREFIX}~`
                ]);
                for (const deferredTask of deferred.rows) {
                    const checkpoint = deferredTask.checkpoint || {};
                    const deferredSources = Array.isArray(checkpoint.deferredSources)
                        ? checkpoint.deferredSources.map((value) => String(value || '').trim()).filter(Boolean) : [];
                    const deferredSet = new Set(deferredSources);
                    if (deferredSet.size) {
                        const checkedSources = Array.isArray(checkpoint.checkedSources)
                            ? checkpoint.checkedSources.map((value) => String(value || '').trim()).filter((value) => value && !deferredSet.has(value)) : [];
                        const nextCheckpoint = {
                            ...checkpoint,
                            coverage: 'CROSS_SOURCE_DEFERRED_RECHECK',
                            checkedSources,
                            deferredSources: [],
                            deferredRounds: Number(checkpoint.deferredRounds || 0) + 1,
                            resumedAt: new Date().toISOString(),
                        };
                        await client.query(`UPDATE importer_lite_tasks
              SET state='READY',attempts=0,reason=NULL,next_run_at=now(),lease_owner=NULL,lease_until=NULL,
                  checkpoint=$2::jsonb,updated_at=now()
              WHERE id=$1::uuid`, [deferredTask.id, JSON.stringify(nextCheckpoint)]);
                        await this.refreshReadyWork(client, deferredTask.work_key);
                        admittedDeferred += 1;
                    }
                }
            }
            const remainingAvailable = available - admittedDeferred;
            if (remainingAvailable <= 0) {
                await client.query('COMMIT');
                return admittedDeferred;
            }
            // Fast indexed window scan via idx_works_published_updated (published ASC, updated_at DESC)
            // plus any newly mapped onboarding works whose chapters are still importing (published = false).
            // This avoids scanning all 9,300 historical V7 task rows on every producer tick.
            const candidateBatch = await client.query(`
        (
          SELECT w.id::text AS work_id,
                 (COALESCE(cardinality(w.aliases), 0) = 0) AS no_aliases,
                 COALESCE(w.views_total, 0)::int AS views_total
          FROM works w
          WHERE w.published = true
          ORDER BY w.updated_at DESC
          LIMIT 256
        )
        UNION ALL
        (
          SELECT w.id::text AS work_id,
                 (COALESCE(cardinality(w.aliases), 0) = 0) AS no_aliases,
                 COALESCE(w.views_total, 0)::int AS views_total
          FROM works w
          WHERE w.published = false
            AND EXISTS (SELECT 1 FROM importer_work_mappings m WHERE m.work_id = w.id)
          ORDER BY w.updated_at DESC
          LIMIT 32
        )
      `);
            const selected = [];
            if (candidateBatch.rows.length > 0) {
                const candidateKeys = candidateBatch.rows.map((r) => `${METADATA_CROSS_SOURCE_DISCOVERY_V7_PREFIX}${r.work_id}`);
                const existingDiscoveryTasks = await client.query(`
          SELECT idempotency_key
          FROM importer_lite_tasks
          WHERE idempotency_key = ANY($1::text[])
        `, [candidateKeys]);
                const existingKeySet = new Set(existingDiscoveryTasks.rows.map((r) => r.idempotency_key));
                const missingCandidates = candidateBatch.rows.filter((r) => !existingKeySet.has(`${METADATA_CROSS_SOURCE_DISCOVERY_V7_PREFIX}${r.work_id}`));
                if (missingCandidates.length > 0) {
                    const candidateIds = [...new Set(missingCandidates.map((r) => r.work_id))];
                    const activeMappings = await client.query(`
            SELECT DISTINCT m.work_id::text
            FROM importer_work_mappings m
            JOIN importer_sources s ON s.id=m.source
            LEFT JOIN importer_lite_source_health h ON h.source=s.id
            WHERE m.work_id = ANY($1::uuid[])
              AND m.sync_status IN ('WAITING_ADMISSION','SYNCED','ACTIVE')
              AND s.enabled=true AND COALESCE(s.status,'ACTIVE') IN ('ACTIVE','DEGRADED','RECOVERING')
              AND COALESCE(h.health,'HEALTHY') NOT IN ('BROKEN','DISABLED')
          `, [candidateIds]);
                    const activeMappingSet = new Set(activeMappings.rows.map((r) => r.work_id));
                    const sortedCandidates = [...missingCandidates].sort((a, b) => {
                        if (a.no_aliases !== b.no_aliases)
                            return a.no_aliases ? -1 : 1;
                        const aLinked = activeMappingSet.has(a.work_id);
                        const bLinked = activeMappingSet.has(b.work_id);
                        if (aLinked !== bLinked) {
                            return hasActiveUnlinked ? (aLinked ? -1 : 1) : (aLinked ? 1 : -1);
                        }
                        if (b.views_total !== a.views_total)
                            return b.views_total - a.views_total;
                        return a.work_id.localeCompare(b.work_id);
                    });
                    const seenSelected = new Set();
                    for (const cand of sortedCandidates) {
                        if (selected.length >= remainingAvailable)
                            break;
                        if (seenSelected.has(cand.work_id))
                            continue;
                        seenSelected.add(cand.work_id);
                        const lane = activeMappingSet.has(cand.work_id) ? 'popular' : 'unlinked';
                        selected.push({ work_id: cand.work_id, lane });
                    }
                }
            }
            if (!selected.length) {
                await client.query('COMMIT');
                return admittedDeferred;
            }
            let admitted = 0;
            for (const row of selected) {
                const workId = String(row.work_id);
                const inserted = await client.query(`INSERT INTO importer_lite_tasks
          (task_type,work_id,work_key,priority,base_priority,idempotency_key,next_run_at,max_attempts,payload,checkpoint,monitoring)
          VALUES('CATALOG',$1::uuid,$2,3,3,$3,now(),16,$4::jsonb,'{}'::jsonb,false)
          ON CONFLICT(idempotency_key) DO NOTHING RETURNING work_key`, [
                    workId, `work:${workId}`, `${METADATA_CROSS_SOURCE_DISCOVERY_V7_PREFIX}${workId}`,
                    JSON.stringify({ metadataCrossSourceDiscovery: true, metadataEnrichmentVersion: 7,
                        discoveryLane: row.lane }),
                ]);
                if (inserted.rowCount) {
                    admitted += 1;
                    await this.refreshReadyWork(client, inserted.rows[0].work_key);
                }
            }
            await client.query('COMMIT');
            return admittedDeferred + admitted;
        }
        catch (error) {
            await client.query('ROLLBACK').catch(() => { });
            throw error;
        }
        finally {
            client.release();
        }
    }
    /**
     * Persist the durable cross-source V7 coverage matrix produced during NEW_WORK
     * onboarding so every new work has an auditable 43-source verification record,
     * and any deferred or gate-busy sources are automatically re-probed.
     */
    async recordOnboardingCrossSourceDiscovery(input) {
        const checkedSources = [...new Set([input.originSource, ...input.attemptedSources, ...input.deferredSources].filter(Boolean))];
        const deferredSources = [...new Set(input.deferredSources.filter(Boolean))];
        const aliasesAdded = input.attached.reduce((sum, item) => sum + Math.max(0, item.aliasesAdded || 0), 0);
        const tagsAdded = input.attached.reduce((sum, item) => sum + Math.max(0, item.tagsAdded || 0), 0);
        const state = input.busy || input.incomplete ? 'RETRY_WAIT' : 'DONE';
        const nextRunOffsetSeconds = input.busy ? 5 : input.incomplete ? 30 : (deferredSources.length > 0 ? 900 : 0);
        const checkpoint = {
            metadataCrossSourceDiscovery: true,
            coverage: input.busy
                ? 'CROSS_SOURCE_ONBOARDING_BUSY_RETRY'
                : input.incomplete ? 'CROSS_SOURCE_IN_PROGRESS'
                    : deferredSources.length > 0
                        ? 'CROSS_SOURCE_PARTIAL_EXTERNAL_DEFERRED'
                        : 'CROSS_SOURCE_PROBES_COMPLETE',
            checkedSources,
            deferredSources,
            matchedSources: input.attached.slice(0, 64),
            ...(input.sourceProbes ? { sourceProbes: input.sourceProbes } : {}),
            aliasesAdded,
            tagsAdded,
            deferredRounds: 0,
            ...(input.busy || input.incomplete ? {} : { completedAt: new Date().toISOString() }),
        };
        const payload = {
            metadataCrossSourceDiscovery: true,
            metadataEnrichmentVersion: 7,
            discoveryLane: 'popular',
        };
        await this.sourceMetadataDb.query(`INSERT INTO importer_lite_tasks
        (task_type,work_id,work_key,priority,base_priority,idempotency_key,state,next_run_at,max_attempts,payload,checkpoint,monitoring)
       VALUES('CATALOG',$1::uuid,$2,3,3,$3,$4,now()+($5::text || ' seconds')::interval,16,$6::jsonb,$7::jsonb,false)
       ON CONFLICT(idempotency_key) DO UPDATE SET
         checkpoint = EXCLUDED.checkpoint,
         state = CASE WHEN importer_lite_tasks.state = 'DONE' THEN EXCLUDED.state ELSE importer_lite_tasks.state END,
         next_run_at = CASE WHEN importer_lite_tasks.state = 'DONE' THEN EXCLUDED.next_run_at ELSE importer_lite_tasks.next_run_at END,
         updated_at = now()`, [
            input.workId,
            `work:${input.workId}`,
            `${METADATA_CROSS_SOURCE_DISCOVERY_V7_PREFIX}${input.workId}`,
            state,
            String(nextRunOffsetSeconds),
            JSON.stringify(payload),
            JSON.stringify(checkpoint),
        ]);
    }
    /**
     * Chapter media is expensive and a Reader chapter cannot be exposed without
     * a minimally valid work presentation.  Catalog checks this once per work
     * snapshot, never on the page or chapter hot path, so a missing cover does
     * not turn into completed-but-unpublishable Telegram uploads.
     */
    async workPresentationReady(workId) {
        const result = await this.db.query(`SELECT w.title,w.slug,w.cover_id,m.storage_ready,m.bytes
      FROM works w LEFT JOIN media m ON m.id=w.cover_id
      WHERE w.id=$1::uuid`, [workId]);
        const work = result.rows[0];
        return Boolean(work?.title && work?.slug && work?.cover_id &&
            work.storage_ready === true && Number(work.bytes) >= 1500);
    }
    /**
     * A catalog pass can prove that a work is not Reader-presentable before a
     * large historical manifest reaches chapter workers. Park only work that
     * has not started: in-flight lower-priority operations must still drain
     * safely, and a later valid catalog pass reopens these exact durable rows.
     */
    async parkUnpublishableWork(workId, workKey) {
        const client = await this.db.connect();
        try {
            await client.query('BEGIN');
            const parked = await client.query(`UPDATE importer_lite_tasks
        SET state='BLOCKED',reason='WORK_PRESENTATION_INCOMPLETE',
            lease_owner=NULL,lease_until=NULL,
            checkpoint=checkpoint || jsonb_build_object(
              'presentationBlockedAt',clock_timestamp(),
              'presentationPreviousReason',reason
            ),updated_at=now()
        WHERE work_id=$1::uuid
          AND task_type IN ('IMPORT_CHAPTER','PUBLISH')
          AND state IN ('READY','RETRY_WAIT')
        RETURNING id`, [workId]);
            await this.refreshReadyWork(client, workKey);
            await client.query('COMMIT');
            return parked.rowCount || 0;
        }
        catch (error) {
            await client.query('ROLLBACK').catch(() => { });
            throw error;
        }
        finally {
            client.release();
        }
    }
    async checkpoint(task, workerId, patch) {
        const result = await this.db.query(`UPDATE importer_lite_tasks SET checkpoint=checkpoint || $3::jsonb, updated_at=now()
       WHERE id=$1::uuid AND state='RUNNING' AND lease_owner=$2 AND lease_until>now()`, [task.id, workerId, JSON.stringify(patch)]);
        return result.rowCount === 1;
    }
    /**
     * Atomically fence the imported chapter, acknowledge its healthy source and
     * materialize the idempotent publication task.  This runs only after every
     * page has a durable receipt.  Network and Telegram work are intentionally
     * outside this transaction.
     */
    async completeImportAndEnqueuePublish(task, workerId, completion) {
        if (!task.workId || !task.chapterKey)
            return false;
        const publishPriority = task.basePriority === 'STAFF' ? 'P0' : task.basePriority;
        const checkpoint = {
            chapterId: completion.chapterId,
            mediaIds: completion.mediaIds,
            pageCount: completion.pageCount,
            selectedSource: completion.selectedSource,
            mediaStoredAt: completion.mediaStoredAt,
        };
        const client = await this.db.connect();
        try {
            await client.query('BEGIN');
            // Keep the post-media transition short: these writes are all fenced by
            // the exact import lease and do not need three separate trips through
            // the small direct YSQL pool.  The CTE dependency makes a lost lease a
            // true no-op: it cannot acknowledge source health or materialize a
            // PUBLISH task. `refreshReadyWork` remains a separate, ordered step so
            // the durable per-work chapter barrier is refreshed before commit.
            const completed = await client.query(`WITH completed AS (
           UPDATE importer_lite_tasks
              SET state='DONE', checkpoint=checkpoint || $3::jsonb,
                  lease_owner=NULL, lease_until=NULL, updated_at=now()
            WHERE id=$1::uuid AND state='RUNNING' AND lease_owner=$2 AND lease_until>now()
          RETURNING id
         ), source_success AS (
           -- A completed upload which began before a later provider 429 must
           -- not erase that newer cooldown. This is the established
           -- recordSourceSuccess condition, now scoped to the completion CTE.
           INSERT INTO importer_lite_source_health(source,health,cooldown_until,consecutive_failures,last_error)
           SELECT $4,'HEALTHY',NULL,0,NULL FROM completed
           ON CONFLICT(source) DO UPDATE SET health='HEALTHY',cooldown_until=NULL,
             consecutive_failures=0,last_error=NULL,updated_at=now()
           WHERE NOT (importer_lite_source_health.health='RATE_LIMITED'
             AND importer_lite_source_health.cooldown_until>now())
         ), publish AS (
           INSERT INTO importer_lite_tasks
             (task_type,work_id,work_key,chapter_key,priority,base_priority,idempotency_key,payload)
           SELECT 'PUBLISH',$5::uuid,$6,$7,
             CASE WHEN EXISTS (
               SELECT 1 FROM importer_staff_requests s
               WHERE s.work_id=$5::uuid AND s.status IN ('QUEUED','IMPORTING','RETRYING','ACTIVE')
             ) THEN 0 ELSE $8::smallint END,
             $8::smallint,$9,$10::jsonb
           FROM completed
           ON CONFLICT(idempotency_key) DO NOTHING
         )
         SELECT EXISTS(SELECT 1 FROM completed) AS completed`, [task.id, workerId, JSON.stringify(checkpoint), completion.selectedSource,
                task.workId, task.workKey, task.chapterKey, LITE_PRIORITY_RANK[publishPriority],
                `lite:publish:${task.workId}:${task.chapterKey}`,
                // The completion transaction already owns these durable, scalar
                // references. Carry them with the PUBLISH task so its normal path
                // does not spend an extra pool round trip rereading the import
                // checkpoint. Legacy/recovery tasks without them still use the
                // conservative checkpoint lookup in the executor.
                JSON.stringify({ importTaskId: task.id, source: completion.selectedSource,
                    chapterId: completion.chapterId, mediaIds: completion.mediaIds,
                    orderKey: String(task.payload.orderKey || '') || null })]);
            if (!completed.rows[0]?.completed) {
                await client.query('ROLLBACK');
                return false;
            }
            await this.refreshReadyWork(client, task.workKey);
            await client.query('COMMIT');
            this.clearNonRateLimitedCoolingSource(completion.selectedSource);
            this.invalidateP1BacklogCache();
            return true;
        }
        catch (error) {
            await client.query('ROLLBACK').catch(() => { });
            throw error;
        }
        finally {
            client.release();
        }
    }
    /**
     * Inline atomic completion + Reader publication for the production worker hot
     * path.  Eliminates the intermediate PUBLISH enqueue, re-claim and second
     * transaction while preserving every lease fence, work-presentation check,
     * importer_replace_pages integrity check and importer_publication_events row.
     * If work presentation became incomplete while media was uploading, rolls
     * back and falls back to completeImportAndEnqueuePublish so durable page
     * uploads are never lost.
     */
    async completeImportAndPublish(task, workerId, completion, fresh) {
        if (!task.workId || !task.chapterKey)
            return false;
        const checkpoint = {
            chapterId: completion.chapterId,
            mediaIds: completion.mediaIds,
            pageCount: completion.pageCount,
            selectedSource: completion.selectedSource,
            mediaStoredAt: completion.mediaStoredAt,
        };
        const effectivePriorityRank = LITE_PRIORITY_RANK[task.priority];
        const pageRows = completion.mediaIds.map((mediaId, index) => ({ media_id: mediaId, position: index + 1 }));
        try {
            const result = await this.db.query(`SELECT public.importer_lite_complete_import_and_publish(
           $1::uuid, $2, $3::uuid, $4::uuid, $5::jsonb, $6, $7, $8::smallint, $9::jsonb, $10
         ) AS outcome`, [
                task.id,
                workerId,
                task.workId,
                completion.chapterId,
                JSON.stringify(pageRows),
                fresh,
                completion.selectedSource,
                effectivePriorityRank,
                JSON.stringify(checkpoint),
                task.workKey,
            ]);
            const outcome = result.rows[0]?.outcome;
            if (outcome === "SUCCESS") {
                this.clearNonRateLimitedCoolingSource(completion.selectedSource);
                this.invalidateP1BacklogCache();
                return true;
            }
            if (outcome === "FALLBACK_TWO_STEP") {
                return this.completeImportAndEnqueuePublish(task, workerId, completion);
            }
            return false;
        }
        catch {
            return this.completeImportAndEnqueuePublish(task, workerId, completion);
        }
    }
    /** Persist successful unchanged observations in one owner-fenced SQL write. */
    async finishUnchangedMonitors(batch) {
        if (!batch.length)
            return new Set();
        const result = await this.sourceMetadataDb.query(`
      WITH input AS (
        SELECT * FROM unnest($1::uuid[], $2::text[], $3::timestamptz[], $4::jsonb[], $5::uuid[])
          AS v(id, owner, next_run_at, checkpoint, work_id)
      )
      UPDATE importer_lite_tasks t SET
        state='RETRY_WAIT', reason=NULL, next_run_at=i.next_run_at, base_priority=3,
        priority=CASE WHEN EXISTS (SELECT 1 FROM importer_staff_requests s
          WHERE s.work_id=COALESCE(i.work_id,t.work_id) AND s.status IN ('QUEUED','IMPORTING','RETRYING','ACTIVE'))
          THEN 0 ELSE 3 END,
        checkpoint=t.checkpoint || i.checkpoint, monitoring=true, work_id=COALESCE(i.work_id,t.work_id),
        attempts=0, lease_owner=NULL, lease_until=NULL, updated_at=now()
      FROM input i WHERE t.id=i.id AND t.state='RUNNING' AND t.lease_owner=i.owner AND t.lease_until>now()
      RETURNING t.id`, [batch.map((e) => e.task.id), batch.map((e) => e.workerId),
            batch.map((e) => e.nextRunAt), batch.map((e) => JSON.stringify(e.checkpoint)),
            batch.map((e) => e.task.workId)]);
        return new Set(result.rows.map((row) => String(row.id)));
    }
    async finish(task, workerId, outcome) {
        if (!task)
            return false;
        const isMetadataTask = task.payload?.metadataEnrichmentVersion === 4
            || task.payload?.metadataEnrichmentVersion === 7
            || Boolean(task.payload?.metadataCrossSourceDiscovery)
            || Boolean(task.payload?.metadataSourceRefresh);
        const isRecurringMonitor = task.monitoring === true && outcome.monitoring !== false;
        const useIsolatedFastFinish = isMetadataTask || isRecurringMonitor;
        const dbToUse = useIsolatedFastFinish ? (this.sourceMetadataDb || this.db) : this.db;
        const params = [
            task.id,
            workerId,
            outcome.state,
            outcome.reason || null,
            outcome.nextRunAt || null,
            JSON.stringify(outcome.checkpoint || {}),
            outcome.priority === undefined ? null : LITE_PRIORITY_RANK[outcome.priority],
            outcome.monitoring === undefined ? null : outcome.monitoring,
            outcome.workId || null,
            Boolean(outcome.resetAttempts),
        ];
        if (useIsolatedFastFinish) {
            const targetDb = this.sourceMetadataDb || this.db;
            const result = await targetDb.query(`UPDATE importer_lite_tasks
            SET state=$3, reason=$4, next_run_at=COALESCE($5::timestamptz,next_run_at),
                base_priority=COALESCE($7::smallint,base_priority),
                priority=CASE WHEN EXISTS (
                  SELECT 1 FROM importer_staff_requests s
                  WHERE s.work_id=COALESCE($9::uuid,importer_lite_tasks.work_id)
                    AND s.status IN ('QUEUED','IMPORTING','RETRYING','ACTIVE')
                ) THEN 0 ELSE COALESCE($7::smallint,base_priority) END,
                checkpoint=checkpoint || $6::jsonb,
                monitoring=COALESCE($8::boolean,monitoring),
                work_id=COALESCE($9::uuid,work_id),
                attempts=CASE WHEN $10::boolean THEN 0 ELSE attempts END,
                lease_owner=NULL, lease_until=NULL, updated_at=now()
          WHERE id=$1::uuid AND state='RUNNING' AND lease_owner=$2 AND lease_until>now()`, params);
            if (result.rowCount === 1 && !useIsolatedFastFinish)
                await this.refreshReadyWork(this.db, task.workKey);
            return result.rowCount === 1;
        }
        const result = await dbToUse.query('SELECT public.importer_lite_finish_task($1::uuid, $2, $3, $4, $5::timestamptz, $6::jsonb, $7::smallint, $8::boolean, $9::uuid, $10::boolean) AS finished', params);
        const finished = result.rows[0]?.finished === true;
        if (finished && !task.monitoring)
            this.invalidateP1BacklogCache();
        return finished;
    }
    /**
     * Catalog evidence is the only event that reopens a source-blocked chapter.
     * It is scoped by the indexed canonical work/chapter keys, so a new mapping
     * never causes a global BLOCKED sweep. Terminal integrity failures stay
     * blocked until an operator changes the underlying condition.
     */
    async wakeCatalogBlocked(workId, workKey) {
        const result = await this.db.query(`
      WITH woken AS (
        UPDATE importer_lite_tasks
        SET state='READY',reason=NULL,next_run_at=now(),updated_at=now()
        WHERE work_id=$1::uuid AND state='BLOCKED' AND (
          (task_type='IMPORT_CHAPTER' AND COALESCE(reason,'') NOT LIKE 'UPLOAD_OUTCOME_UNKNOWN%'
             AND COALESCE(reason,'') NOT LIKE 'CHAPTER_DISPLAY_NUMBER%'
             AND COALESCE(reason,'') NOT LIKE 'CHAPTER_NUMBER_COLLISION%'
             AND COALESCE(reason,'') NOT LIKE 'SOURCE_PAGE_MANIFEST_CHANGED_AFTER_PARTIAL_STORAGE%'
             AND COALESCE(reason,'') NOT LIKE 'PARTIAL_UPLOAD_SOURCE_ROUTE_UNAVAILABLE%') OR
          (task_type='PUBLISH' AND reason='WORK_PRESENTATION_INCOMPLETE')
        )
        RETURNING id
      )
      SELECT count(*)::int AS woken_count FROM woken
    `, [workId]);
        const count = Number(result.rows[0]?.woken_count || 0);
        if (count > 0) {
            await this.db.query('SELECT public.importer_lite_refresh_ready_work($1)', [workKey]);
            this.invalidateP1BacklogCache();
        }
        return count;
    }
    /**
     * Source health is durable operational evidence, shared by discovery,
     * cataloguing and chapter execution.  It is deliberately separate from
     * task priority: a cooled-down preferred source must not suppress a
     * confirmed healthy alternative.
     */
    async recordSourceFailure(source, error) {
        this.invalidateSourcePolicyCache(source);
        const detail = error;
        const retryAfter = Number(detail?.retryAfter || detail?.parameters?.retry_after || 0);
        const cooldownSeconds = retryAfter > 0 ? Math.min(3600, retryAfter + 2) : 60;
        const health = retryAfter > 0 ? 'RATE_LIMITED' : 'DEGRADED';
        const message = String(detail?.message || 'SOURCE_FAILURE').slice(0, 500);
        if (retryAfter > 0) {
            const existingCooling = this.coolingSources.get(source);
            const nextCoolingUntil = Date.now() + cooldownSeconds * 1_000;
            this.coolingSources.set(source, {
                until: existingCooling ? Math.max(existingCooling.until, nextCoolingUntil) : nextCoolingUntil,
                rateLimited: true,
            });
        }
        if (!(retryAfter > 0)) {
            const isChapterContentDefect = /^(?:INVALID_PAGE_MANIFEST|PAGE_HTTP_404|CHAPTER_CONTENT_INVALID_PLACEHOLDER|CATALOG_SOURCE_TIMEOUT|METADATA_SOURCE_TIMEOUT)/.test(message);
            const healthDb = this.sourceMetadataDb || this.db;
            await healthDb.query(`INSERT INTO importer_lite_source_health(source,health,cooldown_until,consecutive_failures,last_error)
        VALUES($1,$2,NULL,1,$3)
        ON CONFLICT(source) DO UPDATE SET health=excluded.health,
          cooldown_until=CASE
            WHEN $5::boolean AND importer_lite_source_health.consecutive_failures + 1 < 6 THEN importer_lite_source_health.cooldown_until
            WHEN importer_lite_source_health.consecutive_failures + 1 >= 500 THEN now() + interval '3600 seconds'
            WHEN importer_lite_source_health.consecutive_failures + 1 >= 100 THEN now() + interval '1800 seconds'
            WHEN importer_lite_source_health.consecutive_failures + 1 >= 50 THEN now() + interval '900 seconds'
            WHEN importer_lite_source_health.consecutive_failures + 1 >= 10 THEN now() + interval '300 seconds'
            WHEN importer_lite_source_health.consecutive_failures + 1 >= 3 THEN now()+($4 || ' seconds')::interval
            ELSE importer_lite_source_health.cooldown_until
          END,
          consecutive_failures=importer_lite_source_health.consecutive_failures+1,last_error=excluded.last_error,updated_at=now()`, [source, health, message, String(cooldownSeconds), isChapterContentDefect]).catch(() => { });
            return;
        }
        const client = await this.db.connect().catch(() => null);
        if (!client)
            return;
        try {
            await client.query('BEGIN');
            const healthRow = await client.query(`INSERT INTO importer_lite_source_health
          (source,health,cooldown_until,consecutive_failures,last_error)
        VALUES($1,'RATE_LIMITED',now()+($2 || ' seconds')::interval,1,$3)
        ON CONFLICT(source) DO UPDATE SET health='RATE_LIMITED',
          cooldown_until=GREATEST(importer_lite_source_health.cooldown_until,excluded.cooldown_until),
          consecutive_failures=importer_lite_source_health.consecutive_failures+1,
          last_error=excluded.last_error,updated_at=now()
        RETURNING cooldown_until`, [source, String(cooldownSeconds), message]);
            const cooldownUntil = healthRow.rows[0]?.cooldown_until;
            if (cooldownUntil) {
                const deferred = await client.query(`WITH raw_tasks AS MATERIALIZED (
            SELECT t.id, t.work_id, t.chapter_key, t.payload
            FROM (
              SELECT work_key FROM importer_lite_ready_works
              WHERE operation_rank <= 1 AND operation_rank = 1
              ORDER BY priority ASC, last_claimed_at ASC NULLS FIRST, next_run_at ASC
              LIMIT 24
            ) rw
            JOIN importer_lite_tasks t
              ON t.work_key = rw.work_key
             AND t.state='READY' AND t.monitoring=false AND t.task_type='IMPORT_CHAPTER'
            LIMIT 128
          ), candidates AS (
            SELECT t.id
            FROM raw_tasks r
            JOIN importer_lite_tasks t ON t.id = r.id
            WHERE t.state='READY'
              AND EXISTS (
                SELECT 1 FROM importer_lite_chapter_sources blocked
                WHERE blocked.work_id=r.work_id AND blocked.canonical_chapter_key=r.chapter_key
                  AND blocked.source=$1 AND blocked.confirmed=true AND blocked.has_chapter=true
                  AND (blocked.language=COALESCE(NULLIF(r.payload->>'language',''),'und'))
                  AND (blocked.edition=COALESCE(NULLIF(r.payload->>'edition',''),'default'))
              )
              AND NOT EXISTS (
                SELECT 1 FROM importer_lite_chapter_sources alternative
                JOIN importer_sources source_policy ON source_policy.id=alternative.source
                LEFT JOIN importer_lite_source_health alternative_health ON alternative_health.source=alternative.source
                WHERE alternative.work_id=r.work_id AND alternative.canonical_chapter_key=r.chapter_key
                  AND alternative.source<>$1 AND alternative.confirmed=true AND alternative.has_chapter=true
                  AND alternative.language=COALESCE(NULLIF(r.payload->>'language',''),'und')
                  AND alternative.edition=COALESCE(NULLIF(r.payload->>'edition',''),'default')
                  AND source_policy.enabled=true
                  AND COALESCE(source_policy.chapter_ingestion_enabled,true)
                  AND ${sourceExecutionEligibilitySql('source_policy')}
                  AND COALESCE(alternative_health.health,'HEALTHY') NOT IN ('BROKEN','DISABLED')
                  AND (alternative_health.cooldown_until IS NULL OR alternative_health.cooldown_until<=now())
              )
            LIMIT 96
            FOR UPDATE OF t SKIP LOCKED
          ), updated AS (
            UPDATE importer_lite_tasks t
            SET state='RETRY_WAIT',lease_owner=NULL,lease_until=NULL,
                next_run_at=GREATEST(t.next_run_at,$2::timestamptz),
                reason='SOURCE_RATE_LIMITED',updated_at=now()
            FROM candidates c
            WHERE t.id=c.id
            RETURNING t.work_key
          ) SELECT DISTINCT work_key FROM updated`, [source, cooldownUntil]);
                if (deferred.rows.length) {
                    await client.query(`SELECT public.importer_lite_refresh_ready_work(wk) FROM unnest($1::text[]) AS wk`, [deferred.rows.map((r) => r.work_key)]);
                }
            }
            await client.query('COMMIT');
        }
        catch {
            await client.query('ROLLBACK').catch(() => { });
            // Source health must never turn a provider failure into a fatal worker
            // failure. The current task still follows its ordinary retry path.
        }
        finally {
            client.release();
        }
    }
    async recordSourceSuccess(source) {
        this.clearNonRateLimitedCoolingSource(source);
        const throttleMs = (process.env.NODE_ENV === 'test' || process.env.VITEST) ? 0 : 60_000;
        const nowMs = Date.now();
        if (throttleMs > 0) {
            const lastWrite = this.lastHealthyRecordAt.get(source) || 0;
            const cached = this.sourcePolicyCache.get(source);
            if (nowMs - lastWrite < throttleMs
                && (!cached || (cached.policy.health === 'HEALTHY' && !cached.policy.cooldownUntil))) {
                return;
            }
            this.lastHealthyRecordAt.set(source, nowMs);
        }
        const healthDb = this.sourceMetadataDb || this.db;
        await healthDb.query(`INSERT INTO importer_lite_source_health(source,health,cooldown_until,consecutive_failures,last_error)
      VALUES($1,'HEALTHY',NULL,0,NULL)
      ON CONFLICT(source) DO UPDATE SET health='HEALTHY',cooldown_until=NULL,
        consecutive_failures=0,last_error=NULL,updated_at=now()
      -- A completion which started before another worker received a provider
      -- 429 must not erase that newer, durable retry boundary. Once the
      -- bounded rate-limit cooldown expires, a later success resumes normal
      -- health clearing as before.
      WHERE NOT (importer_lite_source_health.health='RATE_LIMITED'
        AND importer_lite_source_health.cooldown_until>now())`, [source]).catch(() => { });
    }
    /**
     * The UNKNOWN receipt proof is deliberately expensive: it must join the
     * durable upload intent to a Telegram media row before any task can reopen.
     * Use this index-only watermark to avoid repeating that historical proof
     * when no ambiguous receipt has appeared since the last empty pass.
     */
    async newestUnknownUploadAt() {
        const result = await this.db.query(`SELECT updated_at
      FROM importer_lite_uploads
      WHERE state='UNKNOWN'
      ORDER BY updated_at DESC
      LIMIT 1`);
        return result.rows[0]?.updated_at ? new Date(result.rows[0].updated_at) : null;
    }
    async hasDueSourceCooldowns() {
        const checkDb = this.sourceMetadataDb || this.db;
        const result = await checkDb.query(`SELECT /*+ IndexScan(task importer_lite_tasks_source_cooldown_due_idx) */ EXISTS (
         SELECT 1
         FROM importer_lite_tasks task
         WHERE task.state = 'RETRY_WAIT'
           AND task.monitoring = false
           AND task.task_type = 'IMPORT_CHAPTER'
           AND task.reason = 'SOURCE_RATE_LIMITED'
           AND task.next_run_at <= now()
         LIMIT 1
       ) AS due`);
        return result.rows[0]?.due === true;
    }
    /** One bounded recovery turn; it cannot consume the claim path or scan works. */
    async reconcile(limit = 50, options = {}) {
        const recoverUploadOutcomes = options.recoverUploadOutcomes !== false;
        const recoverSourceCooldowns = options.recoverSourceCooldowns !== false;
        const recoverLegacyMaintenance = options.recoverLegacyMaintenance !== false;
        const client = await this.db.connect();
        try {
            await client.query('BEGIN');
            const leases = await client.query(`WITH picked AS (
           SELECT id,work_key,attempts,max_attempts,task_type,monitoring,payload FROM importer_lite_tasks WHERE state='RUNNING' AND lease_until<now()
           ORDER BY lease_until LIMIT $1 FOR UPDATE SKIP LOCKED
         ) UPDATE importer_lite_tasks t SET
           state=CASE WHEN picked.attempts >= picked.max_attempts
             AND NOT (picked.task_type='DISCOVER' OR (picked.task_type='CATALOG' AND (picked.monitoring OR picked.payload ? 'metadataCrossSourceDiscovery')))
             THEN 'BLOCKED' ELSE 'RETRY_WAIT' END,
           lease_owner=NULL, lease_until=NULL,
           next_run_at=CASE WHEN picked.attempts >= picked.max_attempts
             AND NOT (picked.task_type='DISCOVER' OR (picked.task_type='CATALOG' AND (picked.monitoring OR picked.payload ? 'metadataCrossSourceDiscovery')))
             THEN t.next_run_at ELSE now() + interval '30 seconds' END,
           reason=CASE WHEN picked.attempts >= picked.max_attempts
             AND NOT (picked.task_type='DISCOVER' OR (picked.task_type='CATALOG' AND (picked.monitoring OR picked.payload ? 'metadataCrossSourceDiscovery')))
             THEN 'LEASE_EXPIRED_MAX_ATTEMPTS' ELSE 'LEASE_EXPIRED' END,
           updated_at=now()
         FROM picked WHERE t.id=picked.id RETURNING picked.work_key,t.state,picked.monitoring`, [limit]);
            const retries = await client.query(`WITH picked AS (
           SELECT id,work_key,attempts,max_attempts,task_type,monitoring,payload FROM importer_lite_tasks
           WHERE state='RETRY_WAIT' AND monitoring=false AND next_run_at<=now()
             -- Explicit provider cooldown work is admitted only by the
             -- per-source pass below. A later 429 may extend health cooldown
             -- after this task received its own retry timestamp.
             AND COALESCE(reason,'')<>'SOURCE_RATE_LIMITED'
           ORDER BY next_run_at LIMIT $1 FOR UPDATE SKIP LOCKED
         ) UPDATE importer_lite_tasks t SET
           state=CASE WHEN picked.attempts >= picked.max_attempts
             AND NOT (picked.task_type='DISCOVER' OR (picked.task_type='CATALOG' AND (picked.monitoring OR picked.payload ? 'metadataCrossSourceDiscovery')))
             THEN 'BLOCKED' ELSE 'READY' END,
           reason=CASE WHEN picked.attempts >= picked.max_attempts
             AND NOT (picked.task_type='DISCOVER' OR (picked.task_type='CATALOG' AND (picked.monitoring OR picked.payload ? 'metadataCrossSourceDiscovery')))
             THEN 'MAX_ATTEMPTS_EXHAUSTED' ELSE t.reason END,
           updated_at=now()
         FROM picked WHERE t.id=picked.id RETURNING picked.work_key,t.state,picked.monitoring`, [limit]);
            const dueMonitorLimit = limit >= 20 ? Math.max(limit, 512) : limit;
            const dueMonitors = await client.query(`WITH picked AS (
           SELECT t.id
           FROM importer_lite_tasks t
           LEFT JOIN importer_sources s ON s.id = t.payload->>'source'
           LEFT JOIN importer_lite_source_health h ON h.source = t.payload->>'source'
           WHERE t.state='RETRY_WAIT'
             AND t.monitoring=true
             AND t.task_type='CATALOG'
             AND t.next_run_at<=now()
             AND COALESCE(t.reason,'')<>'SOURCE_RATE_LIMITED'
             AND (
               s.id IS NULL
               OR (
                 s.enabled = true
                 AND s.id NOT IN ('mangaball','mangadex','mangafire')
                 AND ${sourceExecutionEligibilitySql('s')}
                 AND COALESCE(s.chapter_ingestion_enabled, true) = true
                 AND COALESCE(h.health, 'HEALTHY') NOT IN ('BROKEN', 'DISABLED')
                 AND (h.cooldown_until IS NULL OR h.cooldown_until <= now())
               )
             )
           ORDER BY t.next_run_at LIMIT $1 FOR UPDATE OF t SKIP LOCKED
         ) UPDATE importer_lite_tasks t
            SET state='READY', updated_at=now()
           FROM picked
          WHERE t.id=picked.id
          RETURNING t.id`, [dueMonitorLimit]);
            // `recordSourceFailure` moves only sole-route work to a durable
            // SOURCE_RATE_LIMITED cooldown. The normal retry pass above is kept
            // deliberately small for crash recovery, so it must not be the only
            // mechanism that reopens a large healthy source backlog. It selects a
            // bounded, work-fair retry head in one indexed pass (least recently
            // refreshed work first, earliest canonical chapter within each work),
            // then a few routes per source. This runs only in the 30-second
            // reconciler and refreshes the normal ready-work projection.
            const sourceCooldownRecovery = recoverSourceCooldowns ? await client.query(`/*+ Set(enable_nestloop off) IndexScan(task importer_lite_tasks_source_cooldown_due_idx) */ WITH due_window AS MATERIALIZED (
           SELECT task.id,task.work_key,task.work_id,task.chapter_key,task.payload,task.next_run_at,
             task.created_at,task.updated_at
           FROM importer_lite_tasks task
           WHERE task.state='RETRY_WAIT' AND task.monitoring=false AND task.task_type='IMPORT_CHAPTER'
             AND task.reason='SOURCE_RATE_LIMITED' AND task.next_run_at<=now()
           ORDER BY task.next_run_at ASC, task.work_key ASC, task.created_at ASC, task.id ASC
           LIMIT 512
         ), window_routes AS MATERIALIZED (
           SELECT dw.id AS task_id, route.source, route.source_rank,
             (s.enabled=true
               AND COALESCE(s.chapter_ingestion_enabled, true)
               AND ${sourceExecutionEligibilitySql('s')}
               AND COALESCE(h.health,'HEALTHY') IN ('HEALTHY','DEGRADED','RATE_LIMITED')
               AND (h.cooldown_until IS NULL OR h.cooldown_until<=now())
             ) AS is_runnable,
             GREATEST(
               COALESCE(s.cooldown_until, '-infinity'::timestamptz),
               COALESCE(h.cooldown_until, '-infinity'::timestamptz)
             ) AS active_cooldown_until
           FROM due_window dw
           JOIN importer_lite_chapter_sources route
             ON route.work_id=dw.work_id AND route.canonical_chapter_key=dw.chapter_key
           JOIN importer_sources s ON s.id=route.source
           LEFT JOIN importer_lite_source_health h ON h.source=route.source
           WHERE route.confirmed=true AND route.has_chapter=true
             AND route.language=COALESCE(NULLIF(dw.payload->>'language',''),'und')
             AND route.edition=COALESCE(NULLIF(dw.payload->>'edition',''),'default')
         ), cooldown_aligned_candidates AS (
           SELECT wr.task_id AS id, min(wr.active_cooldown_until) AS next_due
           FROM window_routes wr
           GROUP BY wr.task_id
           HAVING bool_or(wr.is_runnable)=false AND min(wr.active_cooldown_until)>now()
         ), locked_aligned AS (
           SELECT t.id, c.next_due
           FROM importer_lite_tasks t
           JOIN cooldown_aligned_candidates c ON c.id=t.id
           WHERE t.state='RETRY_WAIT' AND t.reason='SOURCE_RATE_LIMITED' AND t.next_run_at<c.next_due
           FOR UPDATE OF t SKIP LOCKED
         ), cooldown_aligned AS (
           UPDATE importer_lite_tasks t
           SET next_run_at=la.next_due, updated_at=now()
           FROM locked_aligned la
           WHERE t.id=la.id
           RETURNING t.id
         ), runnable_due AS MATERIALIZED (
           SELECT dw.id,dw.work_key,dw.work_id,dw.chapter_key,dw.payload,dw.next_run_at,
             dw.created_at,dw.updated_at
           FROM due_window dw
           WHERE EXISTS (
             SELECT 1 FROM window_routes wr WHERE wr.task_id=dw.id AND wr.is_runnable=true
           )
           ORDER BY dw.next_run_at ASC, dw.work_key ASC, dw.created_at ASC, dw.id ASC
           LIMIT 512
         ), due_candidates AS MATERIALIZED (
           SELECT task.id,task.work_key,task.work_id,task.chapter_key,task.payload,task.next_run_at,
             task.created_at,
             row_number() OVER (PARTITION BY task.work_key
               ORDER BY NULLIF(task.payload->>'orderKey','') NULLS LAST,
                 task.next_run_at,task.created_at,task.id) AS work_position,
             COALESCE(ready.updated_at,max(task.updated_at) OVER (PARTITION BY task.work_key)) AS work_turn_at,
             min(task.next_run_at) OVER (PARTITION BY task.work_key) AS oldest_due
           FROM runnable_due task
           LEFT JOIN importer_lite_ready_works ready ON ready.work_key=task.work_key
         ), candidate_works AS (
           SELECT DISTINCT candidate.work_key,candidate.work_turn_at,candidate.oldest_due
           FROM due_candidates candidate
           ORDER BY candidate.work_turn_at NULLS FIRST,candidate.oldest_due,candidate.work_key
           LIMIT $1
         ), due AS (
           SELECT task.id,task.work_key,task.work_id,task.chapter_key,task.payload,
             task.next_run_at,task.created_at,candidate.work_position
           FROM due_candidates candidate
           JOIN candidate_works work USING(work_key)
           JOIN importer_lite_tasks task ON task.id=candidate.id
           WHERE candidate.work_position<=$4 AND task.state='RETRY_WAIT'
             AND task.monitoring=false AND task.task_type='IMPORT_CHAPTER'
             AND task.reason='SOURCE_RATE_LIMITED' AND task.next_run_at<=now()
           ORDER BY candidate.work_turn_at NULLS FIRST,candidate.oldest_due,
             candidate.work_key,candidate.work_position
           FOR UPDATE OF task SKIP LOCKED
         ), routes AS (
           SELECT DISTINCT ON (due.id) due.id,due.work_key,due.next_run_at,due.created_at,
             due.work_position,wr.source
           FROM due
           JOIN window_routes wr ON wr.task_id=due.id AND wr.is_runnable=true
           ORDER BY due.id,wr.source_rank,wr.source
         ), source_lanes AS (
           SELECT source,min(next_run_at) AS oldest_due
           FROM routes GROUP BY source
           ORDER BY oldest_due,source LIMIT $2
         ), ranked AS (
           SELECT routes.id,routes.work_key,
             row_number() OVER (PARTITION BY routes.source
               ORDER BY routes.work_position,routes.work_key,routes.next_run_at,routes.created_at) AS source_position
           FROM routes JOIN source_lanes USING(source)
         ), picked AS (
           SELECT id,work_key FROM ranked
           WHERE source_position<=$3
         ), reopened AS (
           UPDATE importer_lite_tasks t
           SET state='READY',reason=NULL,updated_at=now()
           FROM picked WHERE t.id=picked.id
           RETURNING picked.work_key
         )
         SELECT work_key FROM reopened`, [SOURCE_COOLDOWN_RECOVERY_CANDIDATE_WORKS, SOURCE_COOLDOWN_RECOVERY_SOURCES_PER_TURN,
                SOURCE_COOLDOWN_RECOVERY_TASKS_PER_SOURCE, SOURCE_COOLDOWN_RECOVERY_TASKS_PER_WORK]) : { rows: [], rowCount: 0 };
            // Recover only a receipt that is independently proven by a durable
            // Telegram media row with the same reserved id and content hash. Since
            // the receipt/media write is normally atomic, this is rare, but it makes
            // a manually restored or legacy receipt safe to reuse after a crash.
            // These proofs are exceptional crash recovery. The caller schedules
            // them separately so ordinary lease/retry recovery stays hot-path cheap.
            const noUploadOutcomeRecovery = { rows: [], rowCount: 0 };
            const provenUploadReceipts = recoverUploadOutcomes ? await client.query(`WITH picked AS (
          SELECT u.task_id,u.page_position,u.media_id,m.provider_key,m.bot_reference,
            m.storage_shard_id,m.bytes,m.mime,m.sha256
          FROM importer_lite_uploads u JOIN media m ON m.id=u.media_id
          WHERE u.state='UNKNOWN' AND m.provider='telegram' AND m.storage_ready=true
            AND m.sha256=u.sha256 AND m.bytes>0 AND m.mime LIKE 'image/%'
            AND NULLIF(m.provider_key,'') IS NOT NULL
          ORDER BY u.updated_at LIMIT $1 FOR UPDATE OF u SKIP LOCKED
        ) UPDATE importer_lite_uploads u SET state='PERSISTED',provider_key=picked.provider_key,
          bot_reference=COALESCE(u.bot_reference,picked.bot_reference),
          storage_shard_id=COALESCE(u.storage_shard_id,picked.storage_shard_id::text),
          bytes=picked.bytes,mime=picked.mime,updated_at=now()
        FROM picked WHERE u.task_id=picked.task_id AND u.page_position=picked.page_position
          AND u.media_id=picked.media_id RETURNING u.task_id`, [limit]) : noUploadOutcomeRecovery;
            const reopenedUnknownTasks = recoverUploadOutcomes ? await client.query(`WITH picked AS (
          SELECT task.id,task.work_key FROM importer_lite_tasks task
          WHERE task.task_type='IMPORT_CHAPTER' AND task.state='BLOCKED'
            AND task.reason LIKE 'UPLOAD_OUTCOME_UNKNOWN%'
            AND NOT EXISTS (SELECT 1 FROM importer_lite_uploads u WHERE u.task_id=task.id
              AND u.state IN ('UNKNOWN','INTENT','UPLOADED'))
          ORDER BY task.updated_at LIMIT $1 FOR UPDATE SKIP LOCKED
        ) UPDATE importer_lite_tasks task SET state='READY',attempts=0,lease_owner=NULL,lease_until=NULL,
          reason=NULL,next_run_at=now(),updated_at=now()
        FROM picked WHERE task.id=picked.id RETURNING picked.work_key`, [limit]) : noUploadOutcomeRecovery;
            // A Telegram request with an unknown outcome must never be replayed.
            // Likewise, an old intent left by a crashed process has no proof that
            // Telegram did not accept it.  Mark only the owning task BLOCKED;
            // this bounded, indexed pass never holds up unrelated work.
            const uploads = recoverUploadOutcomes ? await client.query(`WITH picked AS (
           SELECT u.task_id,t.work_key,u.page_position
           FROM importer_lite_uploads u
           JOIN importer_lite_tasks t ON t.id=u.task_id
           WHERE t.state NOT IN ('BLOCKED','DONE') AND (
             u.state='UNKNOWN' OR
             (u.state IN ('INTENT','UPLOADED') AND u.updated_at < now() - interval '15 minutes')
           )
           ORDER BY u.updated_at LIMIT $1 FOR UPDATE OF u,t SKIP LOCKED
         ) UPDATE importer_lite_tasks t
           SET state='BLOCKED',lease_owner=NULL,lease_until=NULL,
               reason='UPLOAD_OUTCOME_UNKNOWN:' || picked.page_position::text,updated_at=now()
         FROM picked WHERE t.id=picked.task_id RETURNING picked.work_key`, [limit]) : noUploadOutcomeRecovery;
            // A completed media checkpoint never waits for a global sweep: it gains
            // one durable PUBLISH task. The unique key makes this idempotent.
            const ready = await client.query(`SELECT i.id,i.work_id,i.work_key,i.chapter_key,i.priority,i.base_priority,i.payload->>'orderKey' AS order_key FROM importer_lite_tasks i
          WHERE i.state='DONE' AND i.task_type='IMPORT_CHAPTER'
            AND i.checkpoint ? 'mediaIds' AND NOT i.checkpoint ? 'publishedAt'
            -- An existing PUBLISH task is the durable recovery authority.
            -- Re-inserting its idempotency key every recovery turn is a
            -- no-op that still consumes the shared two-connection pool.
            -- In particular, a presentation-blocked task is reopened by
            -- wakeCatalogBlocked when catalog evidence repairs the work.
            AND NOT EXISTS (
              SELECT 1 FROM importer_lite_tasks p
              WHERE p.idempotency_key='lite:publish:' || i.work_id::text || ':' || i.chapter_key
            )
          ORDER BY i.updated_at LIMIT $1`, [limit]);
            for (const row of ready.rows) {
                await client.query(`INSERT INTO importer_lite_tasks(task_type,work_id,work_key,chapter_key,priority,base_priority,idempotency_key,payload,checkpoint)
           VALUES ('PUBLISH',$1::uuid,$2,$3,$4,$5,'lite:publish:'||$1::text||':'||$3,$6::jsonb,$7::jsonb)
           ON CONFLICT(idempotency_key) DO NOTHING`, [row.work_id, row.work_key, row.chapter_key, row.priority, row.base_priority,
                    JSON.stringify({ importTaskId: row.id, orderKey: row.order_key || null }), JSON.stringify({})]);
            }
            // Before Lite accepted repeated page media IDs, a chapter with visually
            // identical pages could have every reference persisted but fail the old
            // unique-media integrity count.  Re-open that exact exhausted publish
            // once after verifying presentation and every *positional* media
            // reference. This is bounded and fenced in the checkpoint so another
            // genuine publish error never becomes a retry loop.
            const repairedPublishes = recoverLegacyMaintenance ? await client.query(`WITH completed_imports AS (
          SELECT i.id, i.work_id, i.work_key, i.chapter_key, i.checkpoint
          FROM importer_lite_tasks i
          WHERE i.state='DONE' AND i.task_type='IMPORT_CHAPTER'
            AND i.checkpoint ? 'mediaIds' AND NOT i.checkpoint ? 'publishedAt'
          ORDER BY i.updated_at
          LIMIT $1
        ), picked AS (
          SELECT p.id, p.work_key
          FROM completed_imports i
          JOIN importer_lite_tasks p
            ON p.idempotency_key = 'lite:publish:' || i.work_id::text || ':' || i.chapter_key
          JOIN works w ON w.id=i.work_id
          LEFT JOIN media cover ON cover.id=w.cover_id
          WHERE p.task_type='PUBLISH' AND p.state='BLOCKED' AND p.reason='MAX_ATTEMPTS_EXHAUSTED'
            AND NOT p.checkpoint ? 'publishIntegrityRepairV1'
            AND w.title IS NOT NULL AND w.slug IS NOT NULL
            AND cover.storage_ready=true AND cover.bytes>=1500
            AND COALESCE(jsonb_array_length(i.checkpoint->'mediaIds'),0)>0
            AND (SELECT count(*) FROM jsonb_array_elements_text(i.checkpoint->'mediaIds') AS expected(media_id)
                 JOIN media page ON page.id=expected.media_id::uuid
                 WHERE page.storage_ready=true AND page.bytes>0 AND page.mime LIKE 'image/%')
                = jsonb_array_length(i.checkpoint->'mediaIds')
          ORDER BY p.updated_at LIMIT $1 FOR UPDATE OF p SKIP LOCKED
        ) UPDATE importer_lite_tasks p
          SET state='READY',attempts=0,lease_owner=NULL,lease_until=NULL,next_run_at=now(),
              reason='PUBLISH_INTEGRITY_REPAIRED',
              checkpoint=p.checkpoint || '{"publishIntegrityRepairV1":true}'::jsonb,updated_at=now()
          FROM picked WHERE p.id=picked.id RETURNING picked.work_key`, [limit]) : noUploadOutcomeRecovery;
            const disabledBackfillCatalogs = (recoverSourceCooldowns && recoverLegacyMaintenance) ? await client.query(`WITH picked AS (
           SELECT t.id, t.work_key
           FROM importer_lite_tasks t
           JOIN importer_sources s ON s.id = t.payload->>'source'
           WHERE t.state IN ('READY', 'RUNNING', 'RETRY_WAIT')
             AND t.state = 'READY'
             AND t.monitoring = false
             AND t.priority = 2
             AND t.task_type = 'CATALOG'
             AND t.payload ? 'historicalP1Backfill'
             AND (
               s.enabled = false
               OR COALESCE(s.status, 'ACTIVE') = 'DISABLED'
               OR COALESCE(s.chapter_ingestion_enabled, true) = false
               OR s.id IN ('mangaball', 'mangadex', 'mangafire')
             )
           LIMIT $1
           FOR UPDATE OF t SKIP LOCKED
         )
         UPDATE importer_lite_tasks t
            SET state = 'BLOCKED', reason = 'SOURCE_DISABLED', lease_owner = NULL, lease_until = NULL, updated_at = now()
           FROM picked
          WHERE t.id = picked.id
         RETURNING picked.work_key`, [Math.min(64, Math.max(16, limit))]) : noUploadOutcomeRecovery;
            const demotedOutOfSequenceP0 = (recoverSourceCooldowns && recoverLegacyMaintenance) ? await client.query(`WITH candidate_p0 AS (
           SELECT t.id, t.work_id, t.work_key, t.payload
           FROM (
             SELECT work_key FROM importer_lite_ready_works
             WHERE priority <= 1 AND operation_rank <= 1 AND operation_rank = 1
             ORDER BY priority ASC, last_claimed_at ASC NULLS FIRST, next_run_at ASC
             LIMIT 32
           ) rw
           JOIN importer_lite_tasks t
             ON t.work_key = rw.work_key
            AND t.state = 'READY'
            AND t.priority = 1
            AND t.base_priority = 1
            AND t.monitoring = false
            AND t.task_type = 'IMPORT_CHAPTER'
            AND t.work_id IS NOT NULL
            AND NULLIF(t.payload->>'orderKey', '') IS NOT NULL
           UNION ALL
           SELECT r.id, r.work_id, r.work_key, r.payload
           FROM (
             SELECT t.id, t.work_id, t.work_key, t.payload
             FROM importer_lite_tasks t
             WHERE t.state IN ('RUNNING', 'RETRY_WAIT')
               AND t.state = 'RETRY_WAIT'
               AND t.monitoring = false
               AND t.priority = 1
               AND t.base_priority = 1
               AND t.task_type = 'IMPORT_CHAPTER'
               AND t.work_id IS NOT NULL
               AND NULLIF(t.payload->>'orderKey', '') IS NOT NULL
             LIMIT 32
           ) r
         ), picked AS (
           SELECT c.id, c.work_key
           FROM candidate_p0 c
           JOIN importer_lite_tasks t ON t.id = c.id
           WHERE NOT EXISTS (
               SELECT 1 FROM importer_staff_requests sr
               WHERE sr.work_id = c.work_id
                 AND sr.status IN ('QUEUED', 'IMPORTING', 'RETRYING', 'ACTIVE')
             )
             AND EXISTS (
               SELECT 1
               FROM importer_lite_tasks prev
               WHERE prev.work_key = c.work_key
                 AND prev.task_type IN ('IMPORT_CHAPTER', 'PUBLISH')
                 AND prev.state IN ('RUNNING', 'READY', 'RETRY_WAIT')
                 AND prev.monitoring = false
                 AND NULLIF(prev.payload->>'orderKey', '') < NULLIF(c.payload->>'orderKey', '')
                 AND (prev.state <> 'RETRY_WAIT' OR prev.attempts < prev.max_attempts)
                 AND NOT (
                   prev.task_type = 'IMPORT_CHAPTER'
                   AND prev.chapter_key IS NOT NULL
                   AND public.importer_lite_manifest_p1_route_deferred(prev.work_id, prev.chapter_key)
                 )
             )
           LIMIT $1
           FOR UPDATE OF t SKIP LOCKED
         )
         UPDATE importer_lite_tasks t
            SET priority = 2, base_priority = 2, updated_at = now()
           FROM picked
          WHERE t.id = picked.id
         RETURNING picked.work_key`, [Math.min(64, Math.max(16, limit))]) : noUploadOutcomeRecovery;
            await this.refreshReadyWorks(client, [
                ...leases.rows.filter((row) => !row.monitoring),
                ...retries.rows.filter((row) => !row.monitoring),
                ...sourceCooldownRecovery.rows,
                ...uploads.rows,
                ...reopenedUnknownTasks.rows,
                ...ready.rows,
                ...repairedPublishes.rows,
                ...disabledBackfillCatalogs.rows,
                ...demotedOutOfSequenceP0.rows,
            ].map((row) => row.work_key));
            await client.query('COMMIT');
            this.invalidateP1BacklogCache();
            const leaseRetries = leases.rows.filter((row) => row.state === 'RETRY_WAIT').length;
            const readyRetries = retries.rows.filter((row) => row.state === 'READY').length + (dueMonitors.rowCount || 0);
            const exhausted = [...leases.rows, ...retries.rows].filter((row) => row.state === 'BLOCKED').length;
            return { leases: leaseRetries, retries: readyRetries, exhausted, uploads: uploads.rowCount || 0,
                uploadReceiptRecoveries: provenUploadReceipts.rowCount || 0,
                publishable: ready.rowCount || 0, publishRepairs: repairedPublishes.rowCount || 0,
                sourceCooldownRecoveries: sourceCooldownRecovery.rowCount || 0 };
        }
        catch (error) {
            await client.query('ROLLBACK').catch(() => { });
            throw error;
        }
        finally {
            client.release();
        }
    }
    async operationalSnapshot() {
        const result = await this.db.query(`SELECT priority,state,monitoring,count(*)::int AS count,
      min(created_at) FILTER (WHERE state='READY' AND monitoring=false) AS oldest_ready_at
      FROM importer_lite_tasks GROUP BY priority,state,monitoring ORDER BY priority,state,monitoring`);
        return result.rows.map((row) => ({ priority: priorityName(row.priority), state: row.state,
            monitoring: Boolean(row.monitoring), count: Number(row.count), oldestReadyAt: row.oldest_ready_at ? new Date(row.oldest_ready_at) : null }));
    }
    /** Small, indexed progress probe used for a human-operable Lite heartbeat. */
    async progressSnapshot() {
        const result = await this.db.query(`SELECT
      (SELECT count(*)::int FROM importer_publication_events
        WHERE transition_at >= now() - interval '1 minute') AS published_last_minute,
      (SELECT min(created_at) FROM importer_lite_tasks WHERE state='READY' AND monitoring=false) AS oldest_ready_at,
      (SELECT min(next_run_at) FROM importer_lite_tasks
        WHERE state IN ('READY','RETRY_WAIT') AND task_type='CATALOG' AND monitoring=true) AS next_monitor_at,
      (SELECT min(updated_at) FROM importer_lite_tasks
        WHERE state='DONE' AND task_type='IMPORT_CHAPTER'
          AND checkpoint ? 'mediaIds' AND NOT checkpoint ? 'publishedAt') AS oldest_stored_unpublished_at`);
        const row = result.rows[0] || {};
        return {
            publishedLastMinute: Number(row.published_last_minute || 0),
            oldestReadyAt: row.oldest_ready_at ? new Date(row.oldest_ready_at) : null,
            oldestStoredUnpublishedAt: row.oldest_stored_unpublished_at ? new Date(row.oldest_stored_unpublished_at) : null,
            nextMonitorAt: row.next_monitor_at ? new Date(row.next_monitor_at) : null,
        };
    }
    async blockedReasonSnapshot(limit = 8) {
        const result = await this.db.query(`SELECT COALESCE(reason,'UNSPECIFIED') AS reason,count(*)::int AS count
      FROM importer_lite_tasks WHERE state='BLOCKED'
      GROUP BY reason ORDER BY count DESC,reason LIMIT $1`, [Math.max(1, Math.min(limit, 20))]);
        return result.rows.map((row) => ({ reason: String(row.reason), count: Number(row.count) }));
    }
    async sourceHealthSnapshot() {
        const result = await this.db.query(`SELECT health,count(*)::int AS count
      FROM importer_lite_source_health GROUP BY health ORDER BY health`);
        return result.rows.map((row) => ({ health: String(row.health), count: Number(row.count) }));
    }
    /**
     * Load a chapter's durable page receipts once before its bounded page fanout.
     * A fresh chapter normally has no rows, so probing one position at a time
     * needlessly turns every page into an extra YSQL round trip.  The task lease
     * fences concurrent writers; the receipt remains authoritative after a
     * restart and reserveUpload still rechecks the uncommon repair/race path.
     */
    async uploadReceipts(taskId) {
        const result = await this.db.query(`SELECT u.page_position,u.media_id,u.state,u.sha256,
        m.storage_ready,m.bytes,m.mime,m.sha256 AS media_sha256
      FROM importer_lite_uploads u LEFT JOIN media m ON m.id=u.media_id
      WHERE u.task_id=$1::uuid`, [taskId]);
        return new Map(result.rows.map((row) => [Number(row.page_position), {
                mediaId: String(row.media_id), state: String(row.state), sha256: String(row.sha256),
                storageReady: row.storage_ready === null || row.storage_ready === undefined ? null : row.storage_ready === true,
                bytes: row.bytes === null || row.bytes === undefined ? null : Number(row.bytes),
                mime: row.mime === null || row.mime === undefined ? null : String(row.mime),
                mediaSha256: row.media_sha256 === null || row.media_sha256 === undefined ? null : String(row.media_sha256),
            }]));
    }
    /**
     * Reserve a bounded wave of pre-upload intents in one statement.  This is
     * equivalent to `reserveUpload` for a fresh page, but lets concurrently
     * completed downloads share the small direct YSQL pool.  Existing rows use
     * the single-page path below, which retains its stricter stale-intent and
     * invalid-media repair semantics.
     */
    async reserveUploads(inputs) {
        if (!inputs.length)
            return [];
        const output = [];
        for (let offset = 0; offset < inputs.length; offset += 48) {
            const batch = inputs.slice(offset, offset + 48);
            const freshIds = batch.map(() => randomUUID());
            const result = await this.db.query(`WITH input AS (
           SELECT * FROM unnest($1::uuid[],$2::int[],$3::text[],$4::uuid[])
             AS v(task_id,page_position,sha256,fresh_id)
         ), input_hashes AS (
           SELECT DISTINCT sha256 FROM input
         ), reusable_media AS (
           SELECT DISTINCT ON (m.sha256) m.sha256,m.id
           FROM media m JOIN input_hashes h ON h.sha256=m.sha256
           WHERE m.storage_ready=true AND m.bytes>0 AND m.mime LIKE 'image/%'
           ORDER BY m.sha256,m.created_at
         ), reusable AS (
           SELECT i.*,m.id AS reusable_media_id
           FROM input i LEFT JOIN reusable_media m ON m.sha256=i.sha256
         ), inserted AS (
           INSERT INTO importer_lite_uploads(task_id,page_position,media_id,sha256,state)
           SELECT task_id,page_position,COALESCE(reusable_media_id,fresh_id),sha256,
             CASE WHEN reusable_media_id IS NULL THEN 'INTENT' ELSE 'PERSISTED' END
           FROM reusable
           ON CONFLICT(task_id,page_position) DO NOTHING
           RETURNING task_id,page_position,media_id,state
         )
         SELECT i.task_id,i.page_position,
           COALESCE(ins.media_id,prior.media_id) AS media_id,
           COALESCE(ins.state,prior.state) AS state,
           (ins.task_id IS NULL) AS existing
         FROM input i
         LEFT JOIN inserted ins ON ins.task_id=i.task_id AND ins.page_position=i.page_position
         LEFT JOIN importer_lite_uploads prior ON prior.task_id=i.task_id AND prior.page_position=i.page_position`, [batch.map((input) => input.taskId), batch.map((input) => input.pagePosition),
                batch.map((input) => input.sha256), freshIds]);
            const rows = new Map(result.rows.map((row) => [`${row.task_id}:${row.page_position}`, row]));
            for (const input of batch) {
                const row = rows.get(`${input.taskId}:${input.pagePosition}`);
                // A conflict can be a recovery race or a historical invalid media
                // reference.  Defer it to the proven single-row repair path; this is
                // uncommon and never lets an ambiguous intent proceed to Telegram.
                // When the existing row is explicitly RETRYABLE (after a prior HTTP 429
                // or transient upload error), the batch lookup already returned its
                // valid reserved media_id and RETRYABLE state without needing a second
                // per-page round trip.
                if (!row?.media_id || (row.existing === true && row.state !== 'RETRYABLE')) {
                    output.push(await this.reserveUpload(input.taskId, input.pagePosition, input.sha256));
                }
                else {
                    output.push({ mediaId: String(row.media_id), state: String(row.state) });
                }
            }
        }
        return output;
    }
    /** Stable ID is reserved before the non-transactional Telegram call. */
    async reserveUpload(taskId, pagePosition, sha256) {
        // The normal new-page path used to make three sequential round trips:
        // read receipt, look for a reusable media row, then insert the intent.
        // Under a two-connection YSQL pool that turns a 30-page chapter into a
        // large local queue.  Optimistically reserve in one statement instead;
        // only the uncommon already-present/race path reads the receipt back.
        const freshId = randomUUID();
        const inserted = await this.db.query(`WITH reusable AS (
        SELECT id FROM media WHERE sha256=$3 AND storage_ready=true
          AND bytes>0 AND mime LIKE 'image/%' ORDER BY created_at LIMIT 1
      ) INSERT INTO importer_lite_uploads(task_id,page_position,media_id,sha256,state)
      SELECT $1::uuid,$2,COALESCE((SELECT id FROM reusable),$4::uuid),$3,
        CASE WHEN EXISTS(SELECT 1 FROM reusable) THEN 'PERSISTED' ELSE 'INTENT' END
      ON CONFLICT(task_id,page_position) DO NOTHING
      RETURNING media_id,state`, [taskId, pagePosition, sha256, freshId]);
        if (inserted.rows[0])
            return { mediaId: inserted.rows[0].media_id, state: inserted.rows[0].state };
        const existing = await this.db.query(`SELECT u.media_id,u.state,u.sha256,m.storage_ready,m.bytes,m.mime,m.sha256 AS media_sha256
      FROM importer_lite_uploads u LEFT JOIN media m ON m.id=u.media_id
      WHERE u.task_id=$1::uuid AND u.page_position=$2`, [taskId, pagePosition]);
        if (existing.rows[0]) {
            const row = existing.rows[0];
            // An INTENT surviving a crash has no durable confirmation that the
            // request did not reach Telegram. Convert it to UNKNOWN rather than
            // initiating an indistinguishable duplicate upload.
            if (row.state === 'INTENT') {
                await this.db.query(`UPDATE importer_lite_uploads SET state='UNKNOWN',updated_at=now()
          WHERE task_id=$1::uuid AND page_position=$2 AND state='INTENT'`, [taskId, pagePosition]);
                return { mediaId: row.media_id, state: 'UNKNOWN' };
            }
            if (row.state === 'PERSISTED') {
                const valid = row.storage_ready === true && Number(row.bytes) > 0 &&
                    String(row.mime || '').startsWith('image/') && row.media_sha256 === row.sha256;
                if (valid)
                    return { mediaId: row.media_id, state: row.state };
                // Keep the historical media row for audit/recovery, but detach this
                // unpublishable page reference and retry only this page with a fresh
                // id. Valid sibling page receipts remain untouched.
                const replacement = randomUUID();
                const repaired = await this.db.query(`UPDATE importer_lite_uploads
          SET media_id=$3::uuid,sha256=$4,state='RETRYABLE',bytes=NULL,mime=NULL,bot_reference=NULL,
              storage_shard_id=NULL,channel_id=NULL,telegram_message_id=NULL,provider_key=NULL,updated_at=now()
          WHERE task_id=$1::uuid AND page_position=$2 AND state='PERSISTED' AND media_id=$5::uuid
          RETURNING media_id,state`, [taskId, pagePosition, replacement, sha256, row.media_id]);
                if (repaired.rows[0])
                    return { mediaId: repaired.rows[0].media_id, state: repaired.rows[0].state };
                // A valid task lease gives one executor ownership, but retain a safe
                // fallback if an operator/reconciler changed the receipt concurrently.
                return this.reserveUpload(taskId, pagePosition, sha256);
            }
            return { mediaId: row.media_id, state: row.state };
        }
        // Another claimant won between the read and INSERT. Treat it exactly as
        // an interrupted attempt; a lease fencing failure must not duplicate it.
        const raced = await this.db.query(`UPDATE importer_lite_uploads SET state='UNKNOWN',updated_at=now()
      WHERE task_id=$1::uuid AND page_position=$2 AND state='INTENT' RETURNING media_id,state`, [taskId, pagePosition]);
        if (raced.rows[0])
            return { mediaId: raced.rows[0].media_id, state: 'UNKNOWN' };
        const durable = await this.db.query(`SELECT media_id,state FROM importer_lite_uploads
      WHERE task_id=$1::uuid AND page_position=$2`, [taskId, pagePosition]);
        if (!durable.rows[0])
            throw new Error('UPLOAD_INTENT_RACE_NOT_FOUND');
        return { mediaId: durable.rows[0].media_id, state: durable.rows[0].state };
    }
    async recordUploadReceipt(input) {
        await this.recordUploadReceipts([input]);
    }
    /**
     * Persist confirmed Telegram outcomes in one statement.  A page intent is
     * still durable before its upload; batching only removes independent YSQL
     * round trips after Telegram has replied.  On a batch error every caller
     * retains the exact same UNKNOWN-outcome path as the single-page method.
     */
    async recordUploadReceipts(inputs) {
        if (!inputs.length)
            return;
        // The executor flushes at most one bounded page-buffer window.  Keep the
        // repository defensive so another caller cannot build a giant statement.
        for (let offset = 0; offset < inputs.length; offset += 48) {
            const batch = inputs.slice(offset, offset + 48);
            await this.db.query(`WITH input AS (
          SELECT * FROM unnest(
            $1::uuid[],$2::int[],$3::uuid[],$4::text[],$5::int[],$6::text[],$7::int[],$8::int[],
            $9::text[],$10::text[],$11::text[],$12::text[],$13::bigint[],$14::uuid[]
          ) AS v(task_id,page_position,media_id,sha256,bytes,mime,width,height,provider_key,bot_reference,storage_shard_id,channel_id,telegram_message_id,created_by)
        ), media_insert AS (
          INSERT INTO media(id,provider,provider_key,bot_reference,storage_shard_id,mime,width,height,bytes,sha256,created_by,storage_ready,purpose)
          -- importer_lite_uploads preserves the Telegram shard reference as
          -- text, while the legacy media contract stores only a UUID shard.
          -- Do not let a non-UUID adapter label invalidate an already
          -- confirmed Telegram receipt; retain it in the Lite receipt and
          -- omit it from media when it cannot be represented there.
          SELECT media_id,'telegram',provider_key,bot_reference,
            CASE WHEN storage_shard_id ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'
              THEN storage_shard_id::uuid ELSE NULL END,
            mime,width,height,bytes,sha256,created_by,true,'chapter'
          FROM input ON CONFLICT(id) DO NOTHING
        ) UPDATE importer_lite_uploads u SET state='PERSISTED',provider_key=i.provider_key,bot_reference=i.bot_reference,
            storage_shard_id=i.storage_shard_id,channel_id=i.channel_id,telegram_message_id=i.telegram_message_id,
            bytes=i.bytes,mime=i.mime,updated_at=now()
          FROM input i
          WHERE u.task_id=i.task_id AND u.page_position=i.page_position AND u.media_id=i.media_id`, [
                batch.map((input) => input.taskId), batch.map((input) => input.pagePosition),
                batch.map((input) => input.mediaId), batch.map((input) => input.sha256),
                batch.map((input) => input.bytes), batch.map((input) => input.mime),
                batch.map((input) => input.width), batch.map((input) => input.height),
                batch.map((input) => input.providerKey), batch.map((input) => input.botReference),
                batch.map((input) => input.shardId), batch.map((input) => input.channelId),
                batch.map((input) => input.messageId), batch.map((input) => input.createdBy),
            ]);
        }
    }
    async markUploadUnknown(taskId, position) {
        await this.db.query(`UPDATE importer_lite_uploads SET state='UNKNOWN',updated_at=now()
      WHERE task_id=$1::uuid AND page_position=$2 AND state IN ('INTENT','UPLOADED')`, [taskId, position]).catch(() => { });
    }
    /** The provider explicitly rejected the attempt, so the same reserved ID may retry safely. */
    async markUploadRetryable(taskId, position) {
        await this.db.query(`UPDATE importer_lite_uploads SET state='RETRYABLE',updated_at=now()
      WHERE task_id=$1::uuid AND page_position=$2 AND state='INTENT'`, [taskId, position]).catch(() => { });
    }
    async publish(input) {
        const client = await this.db.connect();
        try {
            await client.query('BEGIN');
            // A lease may expire while a previous runtime is still doing network
            // work. Fence the irreversible Reader-visible transition before any
            // page replacement or publication mutation; the reconciler will give
            // the current owner a clean retry instead of allowing two publishers.
            // The task fence and the small work-presentation read are always
            // adjacent and both hold short row locks. Read them together instead
            // of consuming two scarce YSQL round trips per published chapter.
            const presentation = await client.query(`SELECT w.cover_id,w.title,w.slug,m.storage_ready,m.bytes
        FROM importer_lite_tasks t
        JOIN works w ON w.id=$3::uuid
        LEFT JOIN media m ON m.id=w.cover_id
        WHERE t.id=$1::uuid AND t.state='RUNNING' AND t.lease_owner=$2 AND t.lease_until>now()
        FOR UPDATE OF t,w`, [input.task.id, input.workerId, input.workId]);
            const work = presentation.rows[0];
            if (!work)
                throw new Error('LEASE_LOST_BEFORE_PUBLISH');
            if (!work?.title || !work?.slug || !work?.cover_id || work.storage_ready !== true || Number(work.bytes) < 1500) {
                throw new Error('WORK_PRESENTATION_INCOMPLETE');
            }
            const pageRows = input.mediaIds.map((mediaId, index) => ({ media_id: mediaId, position: index + 1 }));
            // importer_replace_pages validates every requested positional media row
            // (including storage readiness, dimensions and byte count) while it
            // holds the importer chapter lock.  A preliminary count duplicated that
            // same read and still could not protect the gap before replacement.
            await client.query('SELECT public.importer_replace_pages($1::uuid,$2::jsonb)', [input.chapterId, JSON.stringify(pageRows)]);
            const importTaskId = typeof input.task.payload.importTaskId === 'string' ? input.task.payload.importTaskId : null;
            // Keep the Reader-visible transition, event and task fence in one
            // statement. The existing transaction remains the rollback boundary:
            // if the final lease fence loses, none of these public writes commit.
            // Updating the chapter with COALESCE also preserves an already-real
            // timestamp on a recovery retry without an extra read round-trip.
            const result = await client.query(`WITH publication AS (
          UPDATE chapters
          SET published_at=COALESCE(
                published_at,
                GREATEST(
                  clock_timestamp(),
                  COALESCE(
                    (SELECT max(ch_prev.published_at) + interval '1 millisecond'
                     FROM chapters ch_prev
                     WHERE ch_prev.work_id = chapters.work_id
                       AND ch_prev.chapter_order_key < chapters.chapter_order_key
                       AND ch_prev.published_at IS NOT NULL),
                    clock_timestamp()
                  )
                )
              ),
              is_fresh_release=CASE WHEN published_at IS NULL THEN $2 ELSE is_fresh_release END
          WHERE id=$1::uuid
          RETURNING published_at
        ), event_row AS (
          INSERT INTO importer_publication_events
            (chapter_id,transition_at,recorded_at,bucket_minute,is_fresh_release,source,work_id,priority)
          SELECT $1::uuid,p.published_at,clock_timestamp(),date_trunc('minute',p.published_at),$2,$3,$4::uuid,$5
          FROM publication p ON CONFLICT(chapter_id) DO NOTHING
        ), work_row AS (
          UPDATE works SET published=true,
            latest_chapter_published_at=GREATEST(COALESCE(latest_chapter_published_at,p.published_at),p.published_at),
            updated_at=now()
          FROM publication p WHERE works.id=$4::uuid
        ), origin_row AS (
          UPDATE importer_lite_tasks SET checkpoint=checkpoint || jsonb_build_object('publishedAt',p.published_at::text),updated_at=now()
          FROM publication p WHERE importer_lite_tasks.id=$6::uuid AND importer_lite_tasks.task_type='IMPORT_CHAPTER'
        ), done AS (
          UPDATE importer_lite_tasks t SET state='DONE',lease_owner=NULL,lease_until=NULL,
            checkpoint=t.checkpoint || jsonb_build_object('publishedAt',p.published_at::text),updated_at=now()
          FROM publication p
          WHERE t.id=$7::uuid AND t.state='RUNNING' AND t.lease_owner=$8 AND t.lease_until>now()
          RETURNING t.work_key
        ) SELECT count(*)::int AS done, (SELECT work_key FROM done LIMIT 1) AS work_key FROM done`, [input.chapterId, input.fresh,
                typeof input.task.payload.source === 'string' ? input.task.payload.source : null,
                input.workId, LITE_PRIORITY_RANK[input.task.priority], importTaskId, input.task.id, input.workerId]);
            // The CTE retains the final lease fence. Throwing rolls back the page
            // replacement too, so a stale owner can never leave a visible chapter
            // with an unfinished durable PUBLISH task.
            if (Number(result.rows[0]?.done || 0) !== 1)
                throw new Error('LEASE_LOST_DURING_PUBLISH');
            await client.query('SELECT public.importer_lite_refresh_ready_work($1)', [input.task.workKey]);
            await client.query('COMMIT');
            return true;
        }
        catch (error) {
            await client.query('ROLLBACK').catch(() => { });
            throw error;
        }
        finally {
            client.release();
        }
    }
}
