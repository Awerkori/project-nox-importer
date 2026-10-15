import { randomUUID } from 'node:crypto';
import { performance } from 'node:perf_hooks';
import { DeduplicationEngine } from '../core/deduplication.js';
import { canonicalChapterKey, chapterKind, chapterOrderKey } from './chapter-identity.js';
import { LiteExecutor } from './executor.js';
import { LiteRepository } from './repository.js';
import { Logger } from '../core/logger.js';
import { readImageBody } from '../core/bounded-body.js';
import { processAndStoreMedia } from '../storage/media.js';
import { AsyncSemaphore } from '../core/concurrency.js';
import { HostRateLimiter } from '../core/rate-limiter.js';
import { telemetryCollector } from '../core/telemetry-collector.js';
import { SourceAdmissionGate } from '../core/source-admission-gate.js';
import { sanitizeSynopsis } from './metadata.js';
import { extractStoredMappingMetadata } from './mapping-metadata.js';
import { PageByteBudget, LITE_PAGE_BUFFER_BUDGET_BYTES } from './page-byte-budget.js';
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const withTimeout = async (operation, timeoutMs, code) => {
    let timer;
    try {
        return await Promise.race([
            operation,
            new Promise((_, reject) => {
                timer = setTimeout(() => reject(new Error(code)), timeoutMs);
                timer.unref();
            }),
        ]);
    }
    finally {
        if (timer)
            clearTimeout(timer);
    }
};
const rankForSource = (source) => source === 'mangaflix' ? 10 : source === 'manhastro' ? 20 : 100;
// Deliberately shared with the legacy runtime.  This fences a rolling
// cutover: Lite must stand by while v1 owns the producer lease (and vice
// versa), rather than merely avoiding duplicate Lite claims.
const RUNTIME_LEADER_KEY = 'importer_runtime_leader';
const RUNTIME_LEASE_SECONDS = 45;
const RUNTIME_LEASE_RENEW_MS = 15_000;
// Five chapter workers can otherwise retain up to thirty decoded/downloaded
// pages while Telegram receipts drain.  Production repeatedly reached the
// 512 MiB container boundary before control-plane timers could run.  This
// bounds only *new page buffers*; uploads, receipts, publication and lease
// renewal remain unblocked so the in-memory wave can drain.
const SAFE_BUFFERED_PAGE_CAP = 24;
// Stored metadata replay is local and no-network. Keep replay admission
// responsive, but poll the source-mapping producer less often: it uses the
// same small YSQL pool as chapter work and only needs to refill a two-task
// buffer, not rescan mappings every timer tick. A completed source refresh
// explicitly asks for its replacement, so this cadence is an idle probe, not
// an artificial 15-second gap between productive refetches.
// The metadata replay remains a single, worker-independent lane, but each
// no-network merge still performs YSQL reads/writes. Production query stats
// showed a costly mapping scan; pace its refill to once per 15 seconds while
// retaining a two-task reserve and leaving chapter workers unchanged.
const STORED_METADATA_REPLAY_INTERVAL_MS = 2_000;
const STORED_METADATA_ENRICHMENT_REFILL_INTERVAL_MS = 15_000;
const SOURCE_METADATA_REFILL_INTERVAL_MS = 15_000;
// Stage 2 metadata lane experiment: two dedicated slots, polled every five
// seconds. Source adapter host limits and source cooldowns remain authoritative.
const SOURCE_METADATA_LANE_INTERVAL_MS = 5_000;
const SOURCE_RECOVERY_PROBE_INTERVAL_MS = 30_000;
/** One process, one executor pool, one bounded reconciler. */
export class ImporterLiteRuntime {
    pool;
    catalogClient;
    registry;
    storage;
    config;
    sourceRateLimiter;
    stopped = false;
    timers = [];
    queue;
    executor;
    dedupe;
    logger = new Logger('ImporterLite');
    downloads;
    bufferedPages;
    bufferedPageBytes;
    claimGate = new AsyncSemaphore(1, 'lite_claim_gate');
    historicalP1BackfillGate = new AsyncSemaphore(1, 'lite_historical_p1_backfill');
    metadataReplayGate = new AsyncSemaphore(1, 'lite_metadata_replay');
    sourceMetadataRefillGate = new AsyncSemaphore(1, 'lite_source_metadata_refill');
    // Two external requests have been production-tested.  Their database merge
    // is deliberately narrower: source network latency must not reserve a YSQL
    // turn, and two slow providers must not commit metadata concurrently.
    sourceMetadataGate = new AsyncSemaphore(2, 'lite_source_metadata_refetch');
    sourceMetadataLaneGate = new AsyncSemaphore(2, 'lite_source_metadata_lane');
    sourceMetadataPersistenceGate = new AsyncSemaphore(1, 'lite_source_metadata_persistence');
    sourceMetadataPoolWaitStartedAt = 0;
    sourceAdmissionGate = new SourceAdmissionGate(10_000);
    lastMetadataEnrichmentAt = 0;
    lastMetadataReplayAt = 0;
    lastSourceMetadataRefillAt = 0;
    lastSourceRecoveryProbeAt = 0;
    lastManifestP1RepairAt = 0;
    // Observation is intentionally a one-slot lane. It may create P0 chapter
    // work, but an empty observation must never compete with chapter workers.
    monitorGate = new AsyncSemaphore(1, 'lite_p0_monitor');
    runtimeInstanceId;
    runtimeBootedAt = new Date();
    chapterWorkerCapacity;
    isRuntimeLeader = false;
    runtimeLeadershipTimer = null;
    runtimeLeadershipRenewing = false;
    leaseRenewStartedAt = null;
    leaseRenewCompletedAt = null;
    leaseRenewError = null;
    heartbeatStartedAt = null;
    heartbeatCompletedAt = null;
    // A full operational snapshot includes optional aggregate diagnostics. It
    // must never overlap itself when the shared two-connection pool is slow:
    // concurrent timer turns otherwise amplify the very YSQL wait they report.
    operationalSnapshotRunning = false;
    workerLiveness = new Map();
    sitePausedUntil = 0;
    idleClaimUntil = 0;
    idleMonitorUntil = 0;
    lastSiteProbe = {
        state: 'UNKNOWN', checkedAt: new Date(0).toISOString(), latencyMs: null,
    };
    /**
     * A queued checkout from the importer's two-connection pool is a local,
     * transient condition, not proof that the Reader is unhealthy.  Claims are
     * already serialized by `claimGate`; freezing every chapter slot whenever
     * one write is queued turns ordinary receipt persistence into avoidable
     * idle time.  Only the independent site-health gate pauses new work.
     */
    shouldPauseChapterAdmission() {
        return Date.now() < this.sitePausedUntil;
    }
    constructor(pool, catalogClient, registry, storage, config, sourceRateLimiter = new HostRateLimiter(2)) {
        this.pool = pool;
        this.catalogClient = catalogClient;
        this.registry = registry;
        this.storage = storage;
        this.config = config;
        this.sourceRateLimiter = sourceRateLimiter;
        if (!config.IMPORTER_USER_ID)
            throw new Error('IMPORTER_USER_ID is required when IMPORTER_RUNTIME_MODE=lite');
        this.runtimeInstanceId = `${config.WORKER_ID}@${process.pid}-${randomUUID().slice(0, 8)}`;
        this.queue = new LiteRepository(pool);
        this.downloads = new AsyncSemaphore(config.DOWNLOAD_INFLIGHT_CONCURRENCY, 'lite_global_page_downloads');
        this.bufferedPages = new AsyncSemaphore(Math.min(config.BUFFERED_PAGE_CONCURRENCY, SAFE_BUFFERED_PAGE_CAP), 'lite_global_page_buffers');
        this.bufferedPageBytes = new PageByteBudget(LITE_PAGE_BUFFER_BUDGET_BYTES);
        this.chapterWorkerCapacity = Math.max(1, Math.min(config.LITE_CHAPTER_WORKERS ?? 1, config.MAX_CONCURRENT_CHAPTERS, config.TESTED_CONCURRENCY_CEILING));
        // Five independent MangaFlix chapter waves can fill the measured 24-page
        // fence while each remains capped at six pages. The regressed 32/8 ramp
        // widened the global fence itself; this controlled ramp does not.
        this.executor = new LiteExecutor(pool, this.queue, registry, storage, config.IMPORTER_USER_ID, this.downloads, Math.max(1, Math.min(6, config.BATCH_PAGE_DOWNLOAD_CONCURRENCY)), this.bufferedPages, sourceRateLimiter, this.chapterWorkerCapacity, (taskId, source) => this.setWorkerSource(taskId, source), this.bufferedPageBytes);
        this.dedupe = new DeduplicationEngine(catalogClient);
    }
    async start() {
        // Do this before taking the shared producer lease or creating a task. A
        // Lite process must fail closed against an old Reader schema, rather than
        // discover a numeric collision after it has downloaded/uploaded media.
        await this.assertReaderCanonicalIdentitySchema();
        if (!(await this.waitForRuntimeLeadership()))
            return;
        await this.seedDiscovery();
        // A former runtime may have left every source observation due at once.
        // Stagger only discovery rows; executable catalog/chapter work remains
        // immediately eligible under the normal strict priority contract.
        await this.queue.staggerReadyDiscoveries(60_000, 64);
        await this.queue.recoverDuplicateCanonicalCatalogFailures();
        await this.refreshSourceFairness();
        await this.refillHistoricalP1Catalog();
        await this.refillManifestProvenP1Imports();
        await this.refillStoredMetadataEnrichment();
        await this.refillSourceMetadataEnrichment();
        // Fill both already-approved external seats at boot.  This is not a
        // third worker: each call is fenced by the two-slot lane below.
        await Promise.all([this.runSourceMetadataRefresh(), this.runSourceMetadataRefresh()]);
        await this.runStoredMetadataReplay();
        // Do not inherit an unmeasured legacy MAX_CONCURRENT_CHAPTERS during a
        // Lite cutover.  The Lite-specific cap begins safely and can only rise
        // through an explicit, benchmarked staging configuration.
        const workers = this.chapterWorkerCapacity;
        telemetryCollector.configureChapterSlots(workers, () => workers);
        // Queue lease owners include the per-boot leader identity. A process that
        // lost leadership can never checkpoint or finish a lease reclaimed by a
        // successor which happens to use the same configured WORKER_ID.
        for (let i = 0; i < workers; i++)
            void this.worker(`${this.runtimeInstanceId}:lite:${i + 1}`, i);
        const timer = setInterval(() => {
            if (this.isRuntimeLeader)
                void this.runRecoveryTurn();
        }, 30_000);
        timer.unref();
        this.timers.push(timer);
        const monitorTimer = setInterval(() => void this.runMonitorTurn().catch(() => { }), 1_000);
        monitorTimer.unref();
        this.timers.push(monitorTimer);
        const metadataTimer = setInterval(() => {
            if (!this.isRuntimeLeader)
                return;
            void this.refillStoredMetadataEnrichment();
            void this.runStoredMetadataReplay();
            void this.refillSourceMetadataEnrichment();
        }, STORED_METADATA_REPLAY_INTERVAL_MS);
        metadataTimer.unref();
        this.timers.push(metadataTimer);
        const sourceMetadataTimer = setInterval(() => {
            if (!this.isRuntimeLeader)
                return;
            void this.runSourceMetadataRefresh();
        }, SOURCE_METADATA_LANE_INTERVAL_MS);
        sourceMetadataTimer.unref();
        this.timers.push(sourceMetadataTimer);
        const healthTimer = setInterval(() => void this.probeSharedResources(), 30_000);
        healthTimer.unref();
        this.timers.push(healthTimer);
        const metricsTimer = setInterval(() => void this.emitOperationalSnapshot().catch(() => { }), 60_000);
        metricsTimer.unref();
        this.timers.push(metricsTimer);
        await this.probeSharedResources();
        await this.emitOperationalSnapshot();
    }
    async assertReaderCanonicalIdentitySchema() {
        const required = ['canonical_chapter_key', 'display_number', 'chapter_order_key', 'chapter_kind', 'edition', 'language'];
        const columns = await this.pool.query(`SELECT column_name,is_nullable
       FROM information_schema.columns
       WHERE table_schema='public' AND table_name='chapters'
         AND column_name IN ('canonical_chapter_key','display_number','chapter_order_key','chapter_kind','edition','language','number')`);
        const present = new Map(columns.rows.map((row) => [row.column_name, row.is_nullable]));
        const missing = required.filter((column) => !present.has(column));
        const numberNullable = present.get('number') === 'YES';
        const canonicalIndex = await this.pool.query(`SELECT EXISTS(
         SELECT 1 FROM pg_indexes
         WHERE schemaname='public' AND tablename='chapters'
           AND indexname='chapters_work_canonical_chapter_key_uq'
       ) AS exists`);
        const numericConstraint = await this.pool.query(`SELECT EXISTS(
         SELECT 1 FROM pg_indexes
         WHERE schemaname='public' AND tablename='chapters'
           AND indexname='chapters_work_id_number_key'
       ) AS exists`);
        if (missing.length || !numberNullable || canonicalIndex.rows[0]?.exists !== true || numericConstraint.rows[0]?.exists === true) {
            const detail = [
                missing.length ? `missing=${missing.join(',')}` : null,
                !numberNullable ? 'number_is_still_required' : null,
                canonicalIndex.rows[0]?.exists !== true ? 'canonical_unique_index_missing' : null,
                numericConstraint.rows[0]?.exists === true ? 'legacy_numeric_uniqueness_active' : null,
            ].filter(Boolean).join(';');
            throw new Error(`READER_CANONICAL_IDENTITY_MIGRATION_REQUIRED:${detail}`);
        }
    }
    stop() {
        this.stopped = true;
        for (const timer of this.timers)
            clearInterval(timer);
        this.timers = [];
        void this.releaseRuntimeLeadership();
    }
    /**
     * The shared settings lease is the only cross-runtime coordination point.
     * It gates admission only: workers which already hold a task lease may
     * finish their durable write while a new STAFF request or a successor
     * runtime waits to claim new work.
     */
    async tryAcquireRuntimeLeadership() {
        try {
            const result = await this.pool.query(`INSERT INTO settings AS leader (key,value)
         VALUES($1,json_build_object('owner',$2::text,'started_at',$4::timestamptz,
           'expires_at',now()+($3::int * interval '1 second'))::text)
         ON CONFLICT(key) DO UPDATE SET value=json_build_object('owner',$2::text,
           'started_at',$4::timestamptz,'expires_at',now()+($3::int * interval '1 second'))::text
         WHERE (leader.value::jsonb->>'owner')=$2::text
            OR COALESCE((leader.value::jsonb->>'expires_at')::timestamptz,'epoch'::timestamptz)<=now()
         RETURNING value`, [RUNTIME_LEADER_KEY, this.runtimeInstanceId, RUNTIME_LEASE_SECONDS, this.runtimeBootedAt.toISOString()]);
            const acquired = result.rows.length > 0;
            if (acquired && !this.isRuntimeLeader) {
                this.isRuntimeLeader = true;
                this.logger.info('Acquired Lite runtime leadership', { instanceId: this.runtimeInstanceId, leaseSeconds: RUNTIME_LEASE_SECONDS });
            }
            return acquired;
        }
        catch (error) {
            this.logger.warn('Failed to acquire Lite runtime leadership', { error: error?.message });
            return false;
        }
    }
    startRuntimeLeadershipRenewal() {
        if (this.runtimeLeadershipTimer)
            return;
        this.runtimeLeadershipTimer = setInterval(async () => {
            if (!this.isRuntimeLeader || this.runtimeLeadershipRenewing || this.stopped)
                return;
            this.runtimeLeadershipRenewing = true;
            this.leaseRenewStartedAt = new Date().toISOString();
            try {
                const result = await this.pool.query(`UPDATE settings SET value=json_build_object('owner',$2::text,'started_at',$4::timestamptz,
             'expires_at',now()+($3::int * interval '1 second'))::text
           WHERE key=$1 AND (value::jsonb->>'owner')=$2::text RETURNING value`, [RUNTIME_LEADER_KEY, this.runtimeInstanceId, RUNTIME_LEASE_SECONDS, this.runtimeBootedAt.toISOString()]);
                if (result.rows.length === 0) {
                    this.isRuntimeLeader = false;
                    this.logger.error('Lost Lite runtime leadership; new claims are gated.', { instanceId: this.runtimeInstanceId });
                }
                this.leaseRenewCompletedAt = new Date().toISOString();
                this.leaseRenewError = null;
            }
            catch (error) {
                // The durable lease remains valid for its remaining window. Retrying
                // renewal is safer than treating a transient YSQL fault as permission
                // for another runtime to publish immediately.
                this.logger.warn('Lite runtime leadership renewal failed', { error: error?.message });
                this.leaseRenewCompletedAt = new Date().toISOString();
                this.leaseRenewError = String(error?.message || 'UNKNOWN').slice(0, 160);
            }
            finally {
                this.runtimeLeadershipRenewing = false;
            }
        }, RUNTIME_LEASE_RENEW_MS);
        this.runtimeLeadershipTimer.unref();
    }
    async waitForRuntimeLeadership() {
        let lastStandbyLog = 0;
        while (!this.stopped) {
            if (await this.tryAcquireRuntimeLeadership()) {
                this.startRuntimeLeadershipRenewal();
                return true;
            }
            if (Date.now() - lastStandbyLog >= 30_000) {
                this.logger.warn('Lite is on standby; another importer owns the runtime lease.');
                lastStandbyLog = Date.now();
            }
            await sleep(5_000);
        }
        return false;
    }
    async releaseRuntimeLeadership() {
        if (this.runtimeLeadershipTimer) {
            clearInterval(this.runtimeLeadershipTimer);
            this.runtimeLeadershipTimer = null;
        }
        if (!this.isRuntimeLeader)
            return;
        this.isRuntimeLeader = false;
        try {
            await this.pool.query(`UPDATE settings SET value=json_build_object('owner',$2::text,'expires_at',now())::text
         WHERE key=$1 AND (value::jsonb->>'owner')=$2::text`, [RUNTIME_LEADER_KEY, this.runtimeInstanceId]);
        }
        catch (error) {
            this.logger.warn('Failed to release Lite runtime leadership', { error: error?.message });
        }
    }
    async seedDiscovery() {
        const sources = await this.pool.query(`SELECT id,sync_interval_minutes FROM importer_sources
      WHERE enabled=true AND (
        COALESCE(status,'ACTIVE') IN ('ACTIVE','DEGRADED','RECOVERING') OR
        (status='COOLDOWN' AND cooldown_until IS NOT NULL AND cooldown_until<=now())
      )
        AND COALESCE((to_jsonb(importer_sources)->>'catalog_discovery_enabled')::boolean,true)
      ORDER BY id`);
        const firstRun = Date.now();
        for (const [index, { id, sync_interval_minutes }] of sources.rows.entries()) {
            await this.queue.enqueue({ taskType: 'DISCOVER', workKey: `source:${id}`, priority: 'P2',
                idempotencyKey: `lite:discover:${id}`, payload: { source: id, syncIntervalMinutes: sync_interval_minutes },
                checkpoint: { cursor: null }, nextRunAt: new Date(firstRun + index * 60_000) });
        }
    }
    /**
     * One periodic, bounded P1 reserve refill.  It runs after reconciliation
     * and never in the claim path, so legacy mappings cannot turn into a new
     * global scheduler scan.  A healthy reserve keeps only a tiny, deferred
     * source-diverse producer set; low reserve checks retain fair P1 admission
     * so a giant work cannot hide missing manifests indefinitely.
     */
    async refillHistoricalP1Catalog() {
        if (!this.isRuntimeLeader || !this.historicalP1BackfillGate.tryAcquire())
            return;
        try {
            const admitted = await this.queue.refillHistoricalP1Catalog();
            // One turn per already-bounded recovery interval lets an alternative
            // healthy source materialize without returning to a continuously
            // competing catalog lane. An abundant materialised P1 reserve is
            // already executable work, so catalog must yield without consuming a
            // second pool turn. It does not affect STAFF/P0/P1/P2 ordering.
            const promoted = this.queue.hasAbundantHistoricalP1Reserve()
                ? false
                : await this.queue.promoteDeferredHistoricalP1Catalog(this.executor.getFairSourceDemand());
            if (admitted || promoted)
                this.logger.info('Advanced bounded historical P1 catalog producer', { admitted, promoted });
        }
        catch (error) {
            this.logger.warn('Historical P1 catalog refill failed', { error: error?.message });
        }
        finally {
            this.historicalP1BackfillGate.release();
        }
    }
    async runRecoveryTurn() {
        try {
            await this.queue.reconcile(50);
        }
        catch { /* bounded recovery retries next turn */ }
        try {
            await this.probeDueSourceRecovery();
        }
        catch (error) {
            this.logger.warn('Source cooldown recovery probe failed', { error: error?.message });
        }
        try {
            await this.queue.recoverDuplicateCanonicalCatalogFailures();
        }
        catch { /* bounded recovery retries next turn */ }
        await this.refreshSourceFairness();
        await this.refillHistoricalP1Catalog();
        await this.refillManifestProvenP1Imports();
        await this.refillStoredMetadataEnrichment();
        // Metadata replay has its own bounded, local lane.  Feeding its durable
        // task is insufficient while P1 remains busy: execute one saved-mapping
        // merge on each eligible recovery turn as well.
        await this.runStoredMetadataReplay();
    }
    /**
     * Probe one expired source cooldown per recovery turn using the same
     * six-stage production admission gate as the legacy runtime. It never
     * bypasses a challenge or rate limit: failures remain parked with a new
     * durable cooldown, while a full PASS explicitly releases the source.
     */
    async probeDueSourceRecovery() {
        if (!this.isRuntimeLeader || Date.now() - this.lastSourceRecoveryProbeAt < SOURCE_RECOVERY_PROBE_INTERVAL_MS)
            return;
        this.lastSourceRecoveryProbeAt = Date.now();
        const selected = await this.pool.query(`SELECT s.id,s.name,s.status,s.base_url
       FROM importer_sources s
       LEFT JOIN importer_lite_source_health health ON health.source=s.id
       WHERE s.enabled=true AND s.status IN ('ACTIVE','COOLDOWN','DEGRADED','PROBING')
         AND ((s.status='ACTIVE' AND s.blocked_reason IS NOT NULL)
           OR (s.status IN ('COOLDOWN','DEGRADED','PROBING')
             AND (s.cooldown_until IS NULL OR s.cooldown_until<=now())))
         AND COALESCE(health.health,'HEALTHY') NOT IN ('BROKEN','DISABLED')
         AND (health.cooldown_until IS NULL OR health.cooldown_until<=now())
         AND (s.last_health_check_at IS NULL OR s.last_health_check_at<=now()-interval '5 minutes')
       ORDER BY s.last_health_check_at NULLS FIRST,s.id LIMIT 1`);
        const source = selected.rows[0];
        if (!source)
            return;
        const adapter = this.registry.get(source.id);
        if (!adapter) {
            await this.pool.query(`UPDATE importer_sources SET status='COOLDOWN',cooldown_until=now()+interval '6 hours',
        blocked_reason='SOURCE_ADAPTER_MISSING',last_health_check_at=now(),updated_at=now() WHERE id=$1`, [source.id]);
            return;
        }
        const claimed = await this.pool.query(`UPDATE importer_sources
      SET status='PROBING',last_health_check_at=now(),updated_at=now()
      WHERE id=$1 AND enabled=true AND status IN ('ACTIVE','COOLDOWN','DEGRADED','PROBING')
        AND (cooldown_until IS NULL OR cooldown_until<=now())
        AND (last_health_check_at IS NULL OR last_health_check_at<=now()-interval '5 minutes')
      RETURNING id`, [source.id]);
        if (claimed.rowCount !== 1)
            return;
        try {
            const report = await this.sourceAdmissionGate.executeProdProbe(adapter);
            const checkedAt = new Date().toISOString();
            const stages = report.stages.map((stage) => ({ stage: stage.stage, status: stage.status,
                httpStatus: stage.httpStatus ?? null, durationMs: stage.durationMs,
                classification: stage.classification ?? null }));
            if (report.overallStatus === 'PASS') {
                await this.pool.query(`UPDATE importer_sources SET status='ACTIVE',cooldown_until=NULL,
          blocked_reason=NULL,blocked_details=$2::jsonb,last_health_check_at=$3,updated_at=now() WHERE id=$1`, [source.id, JSON.stringify({ recovered_at: checkedAt, probe_success: true, stages }), checkedAt]);
                await this.queue.recordSourceSuccess(source.id);
                this.logger.info('Source passed bounded recovery admission probe', { source: source.id });
                return;
            }
            const classification = report.classification || 'UNKNOWN_CLOUDFLARE';
            const longBlock = ['TURNSTILE', 'JS_CHALLENGE', 'DATACENTER_ASN_BLOCK', 'CLOUDFLARE_DATACENTER_BLOCK'].includes(classification);
            const limited = ['RATE_LIMITED', 'BURST_LIMITED'].includes(classification);
            const cooldownMs = longBlock ? 6 * 60 * 60_000 : limited ? 60 * 60_000 : 30 * 60_000;
            const nextProbe = new Date(Date.now() + cooldownMs).toISOString();
            const health = limited ? 'RATE_LIMITED' : 'DEGRADED';
            await this.pool.query(`UPDATE importer_sources SET status='COOLDOWN',cooldown_until=$2,
        blocked_reason=$3,blocked_details=$4::jsonb,last_health_check_at=$5,updated_at=now() WHERE id=$1`, [source.id, nextProbe, classification, JSON.stringify({ stages, cf_ray: report.cfRay, last_checked_at: checkedAt }), checkedAt]);
            await this.pool.query(`INSERT INTO importer_lite_source_health(source,health,cooldown_until,consecutive_failures,last_error)
        VALUES($1,$2,$3,1,$4) ON CONFLICT(source) DO UPDATE SET health=excluded.health,
        cooldown_until=excluded.cooldown_until,consecutive_failures=importer_lite_source_health.consecutive_failures+1,
        last_error=excluded.last_error,updated_at=now()`, [source.id, health, nextProbe, classification]);
            this.logger.info('Source remains parked after bounded recovery probe', { source: source.id, classification, cooldownMs });
        }
        catch (error) {
            const checkedAt = new Date().toISOString();
            const nextProbe = new Date(Date.now() + 30 * 60_000).toISOString();
            await this.pool.query(`UPDATE importer_sources SET status='COOLDOWN',cooldown_until=$2,
        blocked_reason='SOURCE_RECOVERY_PROBE_ERROR',blocked_details=$3::jsonb,last_health_check_at=$4,updated_at=now() WHERE id=$1`, [source.id, nextProbe, JSON.stringify({ last_checked_at: checkedAt }), checkedAt]);
            await this.pool.query(`INSERT INTO importer_lite_source_health(source,health,cooldown_until,consecutive_failures,last_error)
        VALUES($1,'DEGRADED',$2,1,$3) ON CONFLICT(source) DO UPDATE SET health='DEGRADED',
        cooldown_until=excluded.cooldown_until,consecutive_failures=importer_lite_source_health.consecutive_failures+1,
        last_error=excluded.last_error,updated_at=now()`, [source.id, nextProbe, String(error?.message || 'SOURCE_RECOVERY_PROBE_ERROR').slice(0, 300)]);
        }
    }
    /** Existing positive manifests may outlive an older completed catalog task. */
    async refillManifestProvenP1Imports() {
        if (!this.isRuntimeLeader || Date.now() - this.lastManifestP1RepairAt < 5 * 60_000)
            return;
        this.lastManifestP1RepairAt = Date.now();
        try {
            const admitted = await this.queue.refillManifestProvenP1Imports();
            if (admitted)
                this.logger.info('Materialized manifest-proven historical P1 repairs', { admitted });
        }
        catch (error) {
            this.logger.warn('Manifest-proven P1 refill failed', { error: error?.message });
        }
    }
    /**
     * Historical aliases/tags already present in a source mapping are cheap,
     * useful evidence. Feed at most one P2 catalog task per second into the
     * bounded replay lane; normal chapter claims retain strict priority.
     */
    async refillStoredMetadataEnrichment() {
        if (!this.isRuntimeLeader || Date.now() - this.lastMetadataEnrichmentAt < STORED_METADATA_ENRICHMENT_REFILL_INTERVAL_MS)
            return;
        this.lastMetadataEnrichmentAt = Date.now();
        try {
            const admitted = await this.queue.refillStoredMetadataEnrichment();
            if (admitted)
                this.logger.info('Admitted bounded stored metadata enrichment', { admitted });
        }
        catch (error) {
            this.logger.warn('Stored metadata enrichment refill failed', { error: error?.message });
        }
    }
    /** Source-backed historical metadata remains a P2 catalog task and shares
     * normal priority claims; it never steals a chapter worker or bypasses a
     * STAFF/P0/P1 reserve. At most two tasks are durably admitted across sources.
     */
    async refillSourceMetadataEnrichment(replaceCompletedTask = false) {
        if (!this.isRuntimeLeader || (!replaceCompletedTask && Date.now() - this.lastSourceMetadataRefillAt < SOURCE_METADATA_REFILL_INTERVAL_MS))
            return;
        if (this.hasSustainedSourceMetadataPoolPressure() || Date.now() < this.sitePausedUntil || this.lastSiteProbe.state === 'PAUSED'
            || process.memoryUsage().rss >= 384 * 1024 * 1024)
            return;
        if (!this.sourceMetadataRefillGate.tryAcquire())
            return;
        this.lastSourceMetadataRefillAt = Date.now();
        try {
            // A single local pool waiter is normal while chapter receipts drain. It
            // is not evidence that the public site is under pressure, and holding a
            // source fetch idle for it collapses the tested two-fetch lane to one.
            // The persistence gate below still permits just one YSQL merge/commit.
            const admitted = await this.queue.refillSourceMetadataEnrichment(2);
            if (admitted) {
                this.logger.info('Admitted source-backed historical metadata refresh', { admitted });
                // A producer tick that creates executable work must wake the dedicated
                // lane itself. Waiting for its 5 s safety timer needlessly leaves a
                // durable P2 task READY while no source request is in flight. The
                // runner still applies the same site/pool checks and two-seat cap.
                void this.runSourceMetadataRefresh();
            }
        }
        catch (error) {
            this.logger.warn('Source metadata refill failed', { error: error?.message });
        }
        finally {
            this.sourceMetadataRefillGate.release();
        }
    }
    /** Dedicated bounded workers give source refetch continuous progress without
     * consuming chapter workers or competing with P0/P1 task selection. */
    async runSourceMetadataRefresh() {
        // Let in-flight requests finish, but do not start more background source
        // fetches under a sustained YSQL queue, while the site is paused, or when
        // RSS nears the container's 512 MiB boundary. A single importer-pool
        // waiter is normal under chapter load; the one-seat persistence gate
        // below, rather than an idle network slot, protects YSQL in that case.
        if (this.hasSustainedSourceMetadataPoolPressure()
            || Date.now() < this.sitePausedUntil || this.lastSiteProbe.state === 'PAUSED'
            || process.memoryUsage().rss >= 384 * 1024 * 1024)
            return;
        if (!this.isRuntimeLeader || !this.sourceMetadataLaneGate.tryAcquire())
            return;
        let claimed = false;
        try {
            const workerId = `${this.runtimeInstanceId}:source-metadata`;
            const claimStartedAt = performance.now();
            const task = await this.queue.claimSourceMetadataRefresh(workerId, 90);
            if (task) {
                claimed = true;
                // Do not wait for this request's HTTP phase before filling the other
                // approved seat. The lane and fetch gates make this a hard maximum of
                // two; it only removes timer-created idle time.
                void this.runSourceMetadataRefresh();
                await this.catalog(task, workerId, {
                    claimSqlMs: Math.round(performance.now() - claimStartedAt),
                    queueDwellMs: task.createdAt ? Math.max(0, Date.now() - task.createdAt.getTime()) : null,
                });
            }
        }
        catch (error) {
            this.logger.warn('Source metadata lane failed', { error: error?.message });
        }
        finally {
            this.sourceMetadataLaneGate.release();
            // Refill only after actual progress. This keeps an empty catalogue from
            // polling its full candidate set, while a completed task immediately
            // frees its durable replacement instead of waiting for the 15 s timer.
            if (claimed && this.isRuntimeLeader) {
                void this.refillSourceMetadataEnrichment(true)
                    .then(() => this.runSourceMetadataRefresh())
                    .catch((error) => this.logger.warn('Source metadata replacement failed', { error: error?.message }));
            }
        }
    }
    hasSustainedSourceMetadataPoolPressure() {
        const waiters = this.pool.waitingCount;
        if (waiters <= 0) {
            this.sourceMetadataPoolWaitStartedAt = 0;
            return false;
        }
        if (waiters >= 2) {
            this.sourceMetadataPoolWaitStartedAt ||= Date.now();
            return true;
        }
        // A lone direct-pool waiter is expected with five active chapter slots.
        // It is neither a site signal nor enough reason to discard half of the
        // already-tested metadata fetch capacity.
        this.sourceMetadataPoolWaitStartedAt = 0;
        return false;
    }
    /**
     * One local metadata merge at a time prevents P1's legitimate chapter
     * reserve from starving aliases/tags. It reads only a saved mapping and
     * runs through the ordinary durable task/lease/finalize path; no network,
     * source slot, page buffer or chapter worker is admitted here.
     */
    async runStoredMetadataReplay() {
        if (!this.isRuntimeLeader || Date.now() - this.lastMetadataReplayAt < STORED_METADATA_REPLAY_INTERVAL_MS)
            return;
        if (!this.metadataReplayGate.tryAcquire())
            return;
        try {
            this.lastMetadataReplayAt = Date.now();
            const workerId = `${this.runtimeInstanceId}:metadata-replay`;
            const task = await this.queue.claimStoredMetadataReplay(workerId, 45);
            if (task)
                await this.catalog(task, workerId);
        }
        catch (error) {
            this.logger.warn('Stored metadata replay failed', { error: error?.message });
        }
        finally {
            this.metadataReplayGate.release();
        }
    }
    /** A bounded source view refreshes only outside claims and only once per recovery turn. */
    async refreshSourceFairness() {
        try {
            this.executor.setFairSourceDemand(await this.queue.readyImportSources());
        }
        catch (error) {
            this.logger.warn('Source fairness sample failed', { error: error?.message });
        }
    }
    async upsertChapterSources(rows) {
        for (let offset = 0; offset < rows.length; offset += 500) {
            const batch = rows.slice(offset, offset + 500);
            const values = [];
            const tuples = batch.map((row, index) => {
                const start = index * 9;
                values.push(row.workId, row.chapterKey, row.source, row.sourceWorkId, row.sourceChapterId, row.language, row.edition, row.orderKey, row.sourceRank);
                return `($${start + 1}::uuid,$${start + 2},$${start + 3},$${start + 4},$${start + 5},$${start + 6},$${start + 7},$${start + 8},$${start + 9},true,true,now())`;
            });
            await this.pool.query(`INSERT INTO importer_lite_chapter_sources
        (work_id,canonical_chapter_key,source,source_work_id,source_chapter_id,language,edition,order_key,source_rank,confirmed,has_chapter,last_verified_at)
        VALUES ${tuples.join(',')}
        ON CONFLICT(source,source_chapter_id) DO UPDATE SET canonical_chapter_key=excluded.canonical_chapter_key,
          source_work_id=excluded.source_work_id,language=excluded.language,edition=excluded.edition,order_key=excluded.order_key,source_rank=excluded.source_rank,
          confirmed=true,has_chapter=true,last_verified_at=now(),updated_at=now()`, values);
        }
    }
    /**
     * Catalog passes can rediscover a whole work that the legacy importer has
     * already published.  A source/id match plus the canonical work id is
     * sufficient evidence to reuse it; similarity or a numeric display value
     * is deliberately never used here.  Returning this map before admission
     * keeps hundreds of no-op IMPORT_CHAPTER claims out of the P1 lane.
     */
    async legacyPublishedChapterMap(source, workId, sourceChapterIds) {
        const mapped = new Map();
        for (let offset = 0; offset < sourceChapterIds.length; offset += 500) {
            const result = await this.pool.query(`SELECT m.source_chapter_id,m.chapter_id
         FROM importer_chapter_mappings m
         JOIN chapters c ON c.id=m.chapter_id
         WHERE m.source=$1 AND m.source_chapter_id=ANY($2::text[])
           AND c.work_id=$3::uuid AND c.published_at IS NOT NULL`, [source, sourceChapterIds.slice(offset, offset + 500), workId]);
            for (const row of result.rows)
                mapped.set(String(row.source_chapter_id), String(row.chapter_id));
        }
        return mapped;
    }
    /** Persist the exact legacy identity without rewriting any Reader chapter. */
    async upsertLegacyChapterMappings(rows) {
        for (let offset = 0; offset < rows.length; offset += 500) {
            const batch = rows.slice(offset, offset + 500);
            const values = [];
            const tuples = batch.map((row, index) => {
                const start = index * 7;
                values.push(row.workId, row.chapterKey, row.chapterId, row.displayNumber, row.orderKey, row.edition, row.language);
                return `($${start + 1}::uuid,$${start + 2},$${start + 3}::uuid,$${start + 4},$${start + 5},$${start + 6},$${start + 7})`;
            });
            await this.pool.query(`INSERT INTO importer_lite_chapters
        (work_id,canonical_chapter_key,chapter_id,display_number,order_key,edition,language)
        VALUES ${tuples.join(',')}
        ON CONFLICT(work_id,canonical_chapter_key) DO UPDATE
          SET chapter_id=importer_lite_chapters.chapter_id`, values);
        }
    }
    async worker(workerId, slotIndex) {
        telemetryCollector.registerSlot(slotIndex);
        while (!this.stopped) {
            try {
                this.setWorkerLiveness(workerId, 'CLAIM');
                if (!this.isRuntimeLeader) {
                    telemetryCollector.setSlotState(slotIndex, 'WAITING_BARRIER', 'runtime-standby');
                    await sleep(250);
                    continue;
                }
                if (this.shouldPauseChapterAdmission()) {
                    telemetryCollector.setSlotState(slotIndex, 'WAITING_BARRIER', 'site-protection');
                    await sleep(250);
                    continue;
                }
                telemetryCollector.setSlotState(slotIndex, 'WAITING_CLAIM_DB');
                const task = await this.claimNext(workerId);
                if (!task) {
                    this.setWorkerLiveness(workerId, 'IDLE');
                    telemetryCollector.setSlotState(slotIndex, 'IDLE');
                    await sleep(750);
                    continue;
                }
                telemetryCollector.setSlotState(slotIndex, task.taskType === 'IMPORT_CHAPTER' ? 'ACTIVE_DOWNLOAD' : task.taskType === 'PUBLISH' ? 'ACTIVE_DB' : 'ACTIVE_SOURCE', task.taskType);
                this.setWorkerLiveness(workerId, task.taskType === 'PUBLISH' ? 'PUBLISH' : task.taskType === 'IMPORT_CHAPTER' ? 'DOWNLOAD' : 'FETCH', task);
                await this.handle(task, workerId);
                this.setWorkerLiveness(workerId, 'FINALIZE', task);
                telemetryCollector.setSlotState(slotIndex, 'IDLE');
                this.setWorkerLiveness(workerId, 'IDLE');
            }
            catch {
                this.setWorkerLiveness(workerId, 'IDLE');
                telemetryCollector.setSlotState(slotIndex, 'IDLE');
                await sleep(1_000);
            }
        }
    }
    setWorkerLiveness(workerId, stage, task) {
        this.workerLiveness.set(workerId, {
            taskId: task?.id || null,
            source: task ? String(task.payload.source || '') || null : null,
            stage,
            since: new Date().toISOString(),
        });
    }
    setWorkerSource(taskId, source) {
        for (const value of this.workerLiveness.values()) {
            if (value.taskId === taskId)
                value.source = source;
        }
    }
    livenessSnapshot() {
        const memory = process.memoryUsage();
        return {
            at: new Date().toISOString(),
            memoryMiB: { rss: Math.round(memory.rss / 1024 / 1024), heapUsed: Math.round(memory.heapUsed / 1024 / 1024),
                heapTotal: Math.round(memory.heapTotal / 1024 / 1024), external: Math.round(memory.external / 1024 / 1024),
                arrayBuffers: Math.round(memory.arrayBuffers / 1024 / 1024) },
            ysqlPool: { total: this.pool.totalCount, idle: this.pool.idleCount, waiting: this.pool.waitingCount },
            sourceMetadataLane: {
                active: this.sourceMetadataLaneGate.capacity - this.sourceMetadataLaneGate.available,
                capacity: this.sourceMetadataLaneGate.capacity,
                waiters: this.sourceMetadataLaneGate.queued,
            },
            sourceMetadataPersistence: {
                active: this.sourceMetadataPersistenceGate.active,
                capacity: this.sourceMetadataPersistenceGate.capacity,
                waiters: this.sourceMetadataPersistenceGate.queued,
            },
            pageByteBudget: this.bufferedPageBytes.snapshot(),
            heartbeat: { startedAt: this.heartbeatStartedAt, completedAt: this.heartbeatCompletedAt },
            lease: { startedAt: this.leaseRenewStartedAt, completedAt: this.leaseRenewCompletedAt, error: this.leaseRenewError },
            workers: [...this.workerLiveness.entries()].map(([workerId, value]) => ({ workerId, ...value })),
        };
    }
    /** One empty claim probe per idle interval, shared by every worker. */
    async claimNext(workerId) {
        return this.claimGate.runExclusive(async () => {
            if (!this.isRuntimeLeader)
                return null;
            if (Date.now() < this.idleClaimUntil)
                return null;
            const task = await this.queue.claim(workerId, this.config.QUEUE_LEASE_DURATION_SECONDS);
            this.idleClaimUntil = task ? 0 : Date.now() + 750;
            return task;
        });
    }
    /**
     * Claim at most one due P0 observation. A failed/empty probe backs off for
     * fifteen seconds, so thousands of watched works do not turn into empty DB
     * claims. This is a resource budget, not a second priority scheduler.
     */
    async runMonitorTurn() {
        if (this.stopped || !this.isRuntimeLeader || Date.now() < this.sitePausedUntil || this.pool.waitingCount > 0)
            return;
        if (!this.monitorGate.tryAcquire())
            return;
        try {
            if (Date.now() < this.idleMonitorUntil)
                return;
            const task = await this.queue.claimMonitor(`${this.runtimeInstanceId}:lite-monitor`, this.config.QUEUE_LEASE_DURATION_SECONDS);
            this.idleMonitorUntil = task ? 0 : Date.now() + 15_000;
            if (task)
                await this.handle(task, `${this.runtimeInstanceId}:lite-monitor`);
        }
        finally {
            this.monitorGate.release();
        }
    }
    /** A small protective gate, deliberately not another scheduler. */
    async probeSiteHealth() {
        const started = Date.now();
        try {
            const response = await fetch(`${this.config.NOX_MANGA_URL.replace(/\/$/, '')}/api/health`, { signal: AbortSignal.timeout(3_000) });
            await response.body?.cancel().catch(() => { });
            const latencyMs = Date.now() - started;
            return { healthy: response.ok && latencyMs <= 1_500, latencyMs, status: response.status };
        }
        catch {
            return { healthy: false, latencyMs: null, status: null };
        }
    }
    async probeSharedResources() {
        const first = await this.probeSiteHealth();
        if (first.healthy) {
            this.lastSiteProbe = { state: 'HEALTHY', checkedAt: new Date().toISOString(), latencyMs: first.latencyMs };
            return;
        }
        // A one-off DIScloud egress failure previously halted every worker for
        // thirty seconds even while Reader was healthy.  Confirm once, only on a
        // failed probe; a real outage still pauses claims, while a transient does
        // not masquerade as a site incident or collapse productive slots.
        const confirmation = await this.probeSiteHealth();
        if (confirmation.healthy) {
            this.lastSiteProbe = { state: 'HEALTHY', checkedAt: new Date().toISOString(), latencyMs: confirmation.latencyMs };
            this.logger.warn('Transient site-health probe recovered before pausing Lite claims', {
                firstStatus: first.status, firstLatencyMs: first.latencyMs, confirmationLatencyMs: confirmation.latencyMs,
            });
            return;
        }
        // A confirmed outage reduces pressure without retrying tasks or restarting
        // the process.  The next scheduled probe can release this small gate.
        this.sitePausedUntil = Date.now() + 30_000;
        this.lastSiteProbe = { state: 'PAUSED', checkedAt: new Date().toISOString(), latencyMs: confirmation.latencyMs ?? first.latencyMs };
        this.logger.warn('Pausing new Lite claims to protect shared site resources', {
            firstStatus: first.status, firstLatencyMs: first.latencyMs,
            confirmationStatus: confirmation.status, confirmationLatencyMs: confirmation.latencyMs,
        });
    }
    /** A small diagnostic snapshot; it deliberately skips contention and scans. */
    async emitOperationalSnapshot() {
        if (this.operationalSnapshotRunning)
            return;
        this.operationalSnapshotRunning = true;
        try {
            await this.emitOperationalSnapshotOnce();
        }
        finally {
            this.operationalSnapshotRunning = false;
        }
    }
    async emitOperationalSnapshotOnce() {
        this.heartbeatStartedAt = new Date().toISOString();
        const liveness = this.livenessSnapshot();
        const memory = process.memoryUsage();
        // This must run before any optional diagnostic query. It is a bounded
        // process log, so a future stall leaves the last local state without
        // competing for a YSQL connection or retaining per-page telemetry.
        this.logger.info('Lite liveness', {
            leader: this.isRuntimeLeader, ...liveness,
        });
        if (this.pool.waitingCount > 0) {
            // Do not run four diagnostic scans while the small direct pool is busy.
            // Keep a tiny durable liveness marker instead: an old full snapshot must
            // never be mistaken for a stopped runtime during productive traffic.
            const pipelineTelemetry = telemetryCollector.getSnapshotReport();
            const deferred = {
                runtime: 'lite',
                runtimeInstanceId: this.runtimeInstanceId,
                leader: this.isRuntimeLeader,
                chapterWorkers: this.chapterWorkerCapacity,
                snapshotDeferred: 'POOL_BUSY',
                ysql: { poolTotal: this.pool.totalCount, poolIdle: this.pool.idleCount, poolWaiting: this.pool.waitingCount },
                ysqlTelemetry: pipelineTelemetry.database,
                sourceMetadataLane: {
                    active: this.sourceMetadataLaneGate.capacity - this.sourceMetadataLaneGate.available,
                    capacity: this.sourceMetadataLaneGate.capacity,
                    waiters: this.sourceMetadataLaneGate.queued,
                },
                sourceMetadataPersistence: {
                    active: this.sourceMetadataPersistenceGate.active,
                    capacity: this.sourceMetadataPersistenceGate.capacity,
                    waiters: this.sourceMetadataPersistenceGate.queued,
                },
                // In-memory stage telemetry is safe to sample while the two YSQL
                // connections are busy.  Without it, the exact periods that need
                // diagnosis lose download/upload evidence to the protective snapshot
                // deferral.
                pipeline: {
                    // These are effective parsed runtime limits, not inferred load.
                    // Persisting them makes a low production ceiling distinguishable
                    // from a slow provider without exposing any endpoint or credential.
                    limits: {
                        pageDownloads: this.downloads.capacity,
                        pageBuffers: this.bufferedPages.capacity,
                        pageBufferBytes: this.bufferedPageBytes.capacityBytes,
                        pagesPerChapter: Math.max(1, Math.min(6, this.config.BATCH_PAGE_DOWNLOAD_CONCURRENCY)),
                        telegramUploads: this.config.TELEGRAM_MEDIA_CONCURRENCY,
                        uploadBytesPerSecond: this.config.UPLOAD_RATE_LIMIT_BYTES_PER_SEC,
                    },
                    downloads: pipelineTelemetry.imageDownload,
                    telegram: pipelineTelemetry.telegramStorage,
                    pageDownloadLimiter: pipelineTelemetry.limitersAudit?.lite_global_page_downloads,
                    pageBufferLimiter: pipelineTelemetry.limitersAudit?.lite_global_page_buffers,
                    pageByteBudget: this.bufferedPageBytes.snapshot(),
                    uploadBatches: this.executor.getUploadBatchSnapshot(),
                },
                sourceRate: this.sourceRateLimiter.getSummary(),
                liveness,
                observedAt: new Date().toISOString(),
            };
            this.logger.info('Lite operational heartbeat deferred', deferred);
            await this.pool.query(`INSERT INTO settings(key,value) VALUES('importer_heartbeat',$1)
         ON CONFLICT(key) DO UPDATE SET value=excluded.value`, [JSON.stringify(deferred)]);
            return;
        }
        const [tasks, progress, blocked, sourceHealth, metadataProduct] = await Promise.all([
            this.queue.operationalSnapshot(), this.queue.progressSnapshot(),
            this.queue.blockedReasonSnapshot(), this.queue.sourceHealthSnapshot(), this.queue.metadataProductSnapshot(),
        ]);
        // The importer intentionally keeps a small YSQL pool shared with the
        // site. Optional diversity telemetry must not add another concurrent
        // heartbeat query and manufacture pool waiters by itself.
        const diversity = await this.queue.publicationDiversitySnapshot();
        const fairSources = this.executor.getFairSourceDemand();
        const storageMetrics = this.storage.getMetricsSummary?.();
        // Keep the durable heartbeat compact, but include independently measured
        // download and Telegram occupancy.  Aggregate publication rate cannot
        // distinguish a slow CDN from a slow upload/persistence path.
        const pipelineTelemetry = telemetryCollector.getSnapshotReport();
        const telegram = storageMetrics ? {
            uploads: Number(storageMetrics.totalBotUploads || 0),
            shardUploads: Number(storageMetrics.totalShardUploads || 0),
            active: (storageMetrics.bots || []).reduce((sum, bot) => sum + Number(bot.active || 0), 0),
            rateLimits429: (storageMetrics.bots || []).reduce((sum, bot) => sum + Number(bot.rateLimits429 || 0), 0),
        } : null;
        const snapshot = {
            runtime: 'lite',
            runtimeInstanceId: this.runtimeInstanceId,
            leader: this.isRuntimeLeader,
            chapterWorkers: this.chapterWorkerCapacity,
            publishedLastMinute: progress.publishedLastMinute,
            oldestReadyAt: progress.oldestReadyAt?.toISOString() || null,
            oldestStoredUnpublishedAt: progress.oldestStoredUnpublishedAt?.toISOString() || null,
            nextP0MonitorAt: progress.nextMonitorAt?.toISOString() || null,
            tasks, blocked, sourceHealth, metadataProduct, slots: telemetryCollector.getSlotProductivitySnapshot(),
            memoryMiB: {
                rss: Math.round(memory.rss / 1024 / 1024), heapUsed: Math.round(memory.heapUsed / 1024 / 1024),
                external: Math.round(memory.external / 1024 / 1024), arrayBuffers: Math.round(memory.arrayBuffers / 1024 / 1024),
            },
            ysql: { poolTotal: this.pool.totalCount, poolIdle: this.pool.idleCount, poolWaiting: this.pool.waitingCount },
            ysqlTelemetry: pipelineTelemetry.database,
            sourceMetadataLane: {
                active: this.sourceMetadataLaneGate.capacity - this.sourceMetadataLaneGate.available,
                capacity: this.sourceMetadataLaneGate.capacity,
                waiters: this.sourceMetadataLaneGate.queued,
            },
            sourceMetadataPersistence: {
                active: this.sourceMetadataPersistenceGate.active,
                capacity: this.sourceMetadataPersistenceGate.capacity,
                waiters: this.sourceMetadataPersistenceGate.queued,
            },
            pipeline: {
                limits: {
                    pageDownloads: this.downloads.capacity,
                    pageBuffers: this.bufferedPages.capacity,
                    pageBufferBytes: this.bufferedPageBytes.capacityBytes,
                    pagesPerChapter: Math.max(1, Math.min(6, this.config.BATCH_PAGE_DOWNLOAD_CONCURRENCY)),
                    telegramUploads: this.config.TELEGRAM_MEDIA_CONCURRENCY,
                    uploadBytesPerSecond: this.config.UPLOAD_RATE_LIMIT_BYTES_PER_SEC,
                },
                downloads: pipelineTelemetry.imageDownload,
                telegram: pipelineTelemetry.telegramStorage,
                pageDownloadLimiter: pipelineTelemetry.limitersAudit?.lite_global_page_downloads,
                pageBufferLimiter: pipelineTelemetry.limitersAudit?.lite_global_page_buffers,
                pageByteBudget: this.bufferedPageBytes.snapshot(),
                uploadBatches: this.executor.getUploadBatchSnapshot(),
            },
            telegram, sourceRate: this.sourceRateLimiter.getSummary(), diversity,
            sourceFairness: { readySources: fairSources, count: fairSources.length },
            liveness,
            site: this.lastSiteProbe,
        };
        this.logger.info('Lite operational progress', snapshot);
        // A compact durable marker makes a native deploy auditable even when its
        // dashboard only exposes a rolling log window. It is diagnostic state,
        // never an authority for leadership or task eligibility.
        await this.pool.query(`INSERT INTO settings(key,value) VALUES('importer_heartbeat',$1)
       ON CONFLICT(key) DO UPDATE SET value=excluded.value`, [JSON.stringify({ ...snapshot, observedAt: new Date().toISOString() })]);
        this.heartbeatCompletedAt = new Date().toISOString();
    }
    async handle(task, workerId) {
        if (task.taskType === 'DISCOVER')
            return this.discover(task, workerId);
        if (task.taskType === 'CATALOG')
            return this.catalog(task, workerId);
        if (task.taskType === 'IMPORT_CHAPTER')
            return this.executor.importChapter(task, workerId);
        if (task.taskType === 'PUBLISH')
            return this.executor.publish(task, workerId);
        await this.queue.finish(task, workerId, { state: 'BLOCKED', reason: 'UNKNOWN_TASK_TYPE' });
    }
    async discover(task, workerId) {
        const source = String(task.payload.source || '');
        const adapter = this.registry.get(source);
        if (!adapter)
            return void await this.queue.finish(task, workerId, { state: 'BLOCKED', reason: 'SOURCE_ADAPTER_UNAVAILABLE' });
        const policy = await this.queue.sourcePolicy(source);
        if (!policy?.runnable || !policy.catalogDiscoveryEnabled) {
            const nextRunAt = policy?.cooldownUntil && policy.cooldownUntil > new Date()
                ? policy.cooldownUntil : new Date(Date.now() + (policy?.syncIntervalMs || 60 * 60_000));
            await this.queue.finish(task, workerId, { state: 'RETRY_WAIT',
                reason: !policy?.catalogDiscoveryEnabled ? 'SOURCE_DISCOVERY_DISABLED' : `SOURCE_NOT_READY:${policy?.status || 'MISSING'}`, nextRunAt });
            return;
        }
        try {
            const page = await withTimeout(adapter.fetchUpdatedWorks(String(task.checkpoint.cursor || '') || null, { mode: 'maintenance' }), this.config.SOURCE_OPERATION_TIMEOUT_MS, 'DISCOVERY_SOURCE_TIMEOUT');
            await this.queue.recordSourceSuccess(source);
            for (const work of page.works.slice(0, 100)) {
                await this.queue.enqueue({ taskType: 'CATALOG', workKey: `source:${source}:${work.sourceWorkId}`, priority: 'P2',
                    idempotencyKey: `lite:catalog:${source}:${work.sourceWorkId}`, payload: { source, sourceWorkId: work.sourceWorkId } });
            }
            // The durable task itself holds the cursor; no new cursor rows or sweep.
            // A cursor asks for a later small batch, not an immediate unbounded
            // sweep. This leaves a one-worker cutover free to catalog materialized
            // works and prevents one source from filling every P2 admission slot.
            const nextDelay = page.nextCursor
                ? Math.max(60_000, Math.min(policy.syncIntervalMs, 5 * 60_000))
                : policy.syncIntervalMs;
            await this.queue.finish(task, workerId, { state: 'RETRY_WAIT', priority: 'P2', nextRunAt: new Date(Date.now() + nextDelay),
                checkpoint: { cursor: page.nextCursor }, resetAttempts: true });
        }
        catch (error) {
            await this.queue.recordSourceFailure(source, error);
            // Discovery is one durable recurring observation per source. A long
            // upstream outage must not create an unbounded task set or make that
            // source permanently invisible; its retry cadence is capped instead.
            await this.queue.finish(task, workerId, { state: 'RETRY_WAIT', reason: String(error?.message || 'DISCOVERY_FAILED'),
                nextRunAt: new Date(Date.now() + this.recurringRetryDelayMs(task.attempts)) });
        }
    }
    async catalog(task, workerId, metadataTimings) {
        const source = String(task.payload.source || '');
        const sourceWorkId = String(task.payload.sourceWorkId || '');
        if (task.payload.metadataEnrichment)
            return this.enrichStoredMappingMetadata(task, workerId, source, sourceWorkId);
        if (task.payload.metadataSourceRefresh)
            return this.refreshMappingMetadataFromSource(task, workerId, source, sourceWorkId, metadataTimings);
        const adapter = this.registry.get(source);
        if (!adapter || !sourceWorkId)
            return void await this.queue.finish(task, workerId, { state: 'BLOCKED', reason: 'CATALOG_INPUT_INVALID' });
        const policy = await this.queue.sourcePolicy(source);
        if (!policy?.runnable) {
            const nextRunAt = policy?.cooldownUntil && policy.cooldownUntil > new Date()
                ? policy.cooldownUntil : new Date(Date.now() + (policy?.syncIntervalMs || 60 * 60_000));
            await this.queue.finish(task, workerId, {
                state: task.attempts >= task.maxAttempts ? 'BLOCKED' : 'RETRY_WAIT',
                reason: `SOURCE_NOT_READY:${policy?.status || 'MISSING'}`, nextRunAt,
                priority: task.monitoring ? 'P2' : undefined, monitoring: task.monitoring || undefined,
            });
            return;
        }
        let fetchedSnapshot = false;
        try {
            const snapshot = await withTimeout(adapter.fetchWorkSnapshot
                ? adapter.fetchWorkSnapshot(sourceWorkId)
                : Promise.all([adapter.fetchWorkDetails(sourceWorkId), adapter.fetchChapters(sourceWorkId)])
                    .then(([details, chapters]) => ({ details, chapters })), this.config.SOURCE_OPERATION_TIMEOUT_MS, 'CATALOG_SOURCE_TIMEOUT');
            fetchedSnapshot = true;
            const rawContentRating = String(snapshot.details.raw?.contentRating || snapshot.details.raw?.content_rating || '').toUpperCase();
            const resolved = await this.dedupe.resolveWork({ source, sourceWorkId, title: snapshot.details.title, slug: snapshot.details.slug,
                aliases: snapshot.details.alternativeTitles, genres: snapshot.details.genres, author: snapshot.details.author,
                artist: snapshot.details.artist, synopsis: sanitizeSynopsis(snapshot.details.synopsis) || undefined, kind: snapshot.details.kind,
                status: snapshot.details.status, year: snapshot.details.year, ageRating: snapshot.details.ageRating,
                contentRating: rawContentRating === 'ADULT_18' || Number(snapshot.details.ageRating || 0) >= 18 ? 'ADULT_18' : undefined,
                rawMetadata: snapshot.details.raw,
                loadCoverId: snapshot.details.coverUrl ? () => this.loadCover(adapter, snapshot.details.coverUrl) : undefined });
            if (!resolved.workId || resolved.status === 'AMBIGUOUS') {
                await this.queue.finish(task, workerId, { state: 'BLOCKED', reason: resolved.reason || 'AMBIGUOUS_CATALOG_MATCH' });
                return;
            }
            const language = String(snapshot.details.language || snapshot.details.raw?.language || 'pt-BR');
            const edition = String(snapshot.details.edition || snapshot.details.raw?.edition || 'default');
            // A recurring monitor is intentionally separate from P0 *execution*.
            // P0 only accelerates releases beyond the durable manifest frontier.
            // A source can reveal historical chapters later; those are P1 repairs,
            // not P0, without inferring that every numeric gap is a missing chapter.
            const p0Monitoring = task.monitoring || task.priority === 'P0' || task.priority === 'STAFF';
            const knownFrontier = p0Monitoring
                ? await this.pool.query(`SELECT max(order_key) AS order_key
          FROM importer_lite_chapter_sources WHERE work_id=$1::uuid AND order_key LIKE 'n|%'`, [resolved.workId])
                : { rows: [{ order_key: null }] };
            const knownNormalOrder = knownFrontier.rows[0]?.order_key || null;
            // A manifest can reveal a long historical interval on the first pass
            // or after the source repairs an incomplete listing. Only its newest
            // normal chapter is a P0 release; earlier positively listed chapters
            // enter P1 together so the importer cannot keep moving the gap forward.
            const newestManifestNormalOrder = snapshot.chapters
                .map((chapter) => chapterOrderKey({
                displayNumber: chapter.displayNumber || String(chapter.number),
                title: chapter.title || '',
                kind: chapterKind({ displayNumber: chapter.displayNumber || String(chapter.number), title: chapter.title || '' }),
            }))
                .filter((orderKey) => orderKey.startsWith('n|'))
                .sort()
                .at(-1) || null;
            const manifestSourceIds = [...new Set(snapshot.chapters
                    .map((chapter) => String(chapter.sourceChapterId || ''))
                    .filter(Boolean))];
            const legacyPublished = await this.legacyPublishedChapterMap(source, resolved.workId, manifestSourceIds);
            const sourceRows = [];
            const legacyByChapterKey = new Map();
            const chapterTaskByKey = new Map();
            const sourceChapterIds = new Set();
            for (const chapter of snapshot.chapters) {
                // A duplicate manifest entry cannot be allowed to make one batched
                // UPSERT fail and delay every other chapter in this catalog pass.
                if (sourceChapterIds.has(chapter.sourceChapterId))
                    continue;
                sourceChapterIds.add(chapter.sourceChapterId);
                const displayNumber = chapter.displayNumber || String(chapter.number);
                const chapterKindValue = chapterKind({ displayNumber, title: chapter.title });
                const chapterKey = canonicalChapterKey({ displayNumber, title: chapter.title, kind: chapterKindValue, language, edition });
                const orderKey = chapterOrderKey({ displayNumber, title: chapter.title, kind: chapterKindValue });
                const historicalRepair = p0Monitoring && orderKey.startsWith('n|') && Boolean(knownNormalOrder && orderKey <= knownNormalOrder);
                const isNewestManifestRelease = !orderKey.startsWith('n|') || orderKey === newestManifestNormalOrder;
                const chapterPriority = p0Monitoring && !historicalRepair && isNewestManifestRelease ? 'P0' : 'P1';
                sourceRows.push({ workId: resolved.workId, chapterKey, source, sourceWorkId, sourceChapterId: chapter.sourceChapterId,
                    language, edition, orderKey, sourceRank: rankForSource(source) });
                const legacyChapterId = legacyPublished.get(String(chapter.sourceChapterId));
                if (legacyChapterId) {
                    // The existing Reader publication remains authoritative.  This only
                    // gives Lite its canonical identity, so a future catalog pass cannot
                    // spend a P1 slot rediscovering the same source chapter.
                    // Multiple provider entries can normalize to one canonical chapter.
                    // Keep the first durable legacy binding; alternatives remain in
                    // importer_lite_chapter_sources and must never create a second
                    // Reader chapter or a multi-row UPSERT collision.
                    if (!legacyByChapterKey.has(chapterKey)) {
                        legacyByChapterKey.set(chapterKey, { workId: resolved.workId, chapterKey, chapterId: legacyChapterId, displayNumber, orderKey, language, edition });
                    }
                }
                else if (policy.chapterIngestionEnabled) {
                    const candidate = { taskType: 'IMPORT_CHAPTER', workId: resolved.workId, workKey: `work:${resolved.workId}`,
                        chapterKey, priority: chapterPriority, idempotencyKey: `lite:chapter:${resolved.workId}:${chapterKey}`,
                        payload: { displayNumber, chapterTitle: chapter.title || '', chapterKind: chapterKindValue, orderKey, language, edition,
                            ...(p0Monitoring && orderKey.startsWith('n|') && orderKey < String(newestManifestNormalOrder || '')
                                ? { historicalManifestRepair: true } : {}) } };
                    if (!chapterTaskByKey.has(candidate.idempotencyKey))
                        chapterTaskByKey.set(candidate.idempotencyKey, candidate);
                }
            }
            const legacyRows = [...legacyByChapterKey.values()];
            // A mapped legacy publication is authoritative even if another source
            // entry for the same canonical identity was not mapped yet.
            const chapterTasks = [...chapterTaskByKey.values()].filter((task) => !legacyByChapterKey.has(String(task.chapterKey)));
            await this.upsertChapterSources(sourceRows);
            await this.upsertLegacyChapterMappings(legacyRows);
            // Do not spend download, Telegram and YSQL capacity on a chapter that
            // cannot legally become Reader-visible.  Source mappings remain durable
            // and this catalog task retries on its bounded cadence, so a later valid
            // cover (from this or another confirmed source) admits the same manifest
            // without inventing metadata or losing the historical repair.
            if (chapterTasks.length && !await this.queue.workPresentationReady(resolved.workId)) {
                await this.queue.parkUnpublishableWork(resolved.workId, `work:${resolved.workId}`);
                const status = String(snapshot.details.status || 'UNKNOWN').toUpperCase();
                const monitored = task.monitoring || task.priority === 'P0' || task.priority === 'STAFF';
                await this.queue.finish(task, workerId, monitored ? {
                    state: 'RETRY_WAIT', priority: 'P2', monitoring: true,
                    nextRunAt: new Date(Date.now() + this.monitorIntervalMs(task, status, 0)),
                    workId: resolved.workId,
                    checkpoint: { workId: resolved.workId, presentationIncomplete: true, lastCheckedAt: new Date().toISOString() },
                    resetAttempts: true,
                } : {
                    state: task.attempts >= task.maxAttempts ? 'BLOCKED' : 'RETRY_WAIT',
                    reason: 'WORK_PRESENTATION_INCOMPLETE',
                    nextRunAt: new Date(Date.now() + this.recurringRetryDelayMs(task.attempts)),
                    workId: resolved.workId,
                    checkpoint: { workId: resolved.workId, presentationIncomplete: true, lastCheckedAt: new Date().toISOString() },
                });
                await this.queue.recordSourceSuccess(source);
                return;
            }
            const materialized = await this.queue.enqueueMany(chapterTasks);
            const newReleaseCount = chapterTasks.filter((candidate) => candidate.priority === 'P0' && materialized.insertedKeys.has(candidate.idempotencyKey)).length;
            // A newly confirmed mapping (or repaired presentation metadata) is
            // evidence worth re-evaluating. This indexed, per-work wake-up replaces
            // the legacy global blocked-task sweeps.
            if (policy.chapterIngestionEnabled)
                await this.queue.wakeCatalogBlocked(resolved.workId, `work:${resolved.workId}`);
            // Ongoing/hiatus works retain one cheap persistent monitor. The monitor
            // remains base P2 and outside normal claims; only its newly inserted
            // chapters are P0. Completed works remain available to discovery/repair.
            const status = String(snapshot.details.status || 'UNKNOWN').toUpperCase();
            const monitored = status === 'ONGOING' || status === 'HIATUS';
            await this.queue.finish(task, workerId, monitored ? {
                state: 'RETRY_WAIT', priority: 'P2', monitoring: true,
                nextRunAt: new Date(Date.now() + this.monitorIntervalMs(task, status, newReleaseCount)),
                workId: resolved.workId,
                checkpoint: {
                    workId: resolved.workId, lastCheckedAt: new Date().toISOString(),
                    lastNewReleaseCount: newReleaseCount,
                    emptyChecks: newReleaseCount ? 0 : Number(task.checkpoint.emptyChecks || 0) + 1,
                },
                resetAttempts: true,
            } : { state: 'DONE', workId: resolved.workId, checkpoint: { workId: resolved.workId }, resetAttempts: true });
            await this.queue.recordSourceSuccess(source);
        }
        catch (error) {
            if (!fetchedSnapshot)
                await this.queue.recordSourceFailure(source, error);
            const monitorRetry = task.monitoring;
            const retryDelay = monitorRetry ? this.recurringRetryDelayMs(task.attempts) : 60_000;
            // A monitoring row is an ongoing P0 observation, not a failed chapter.
            // Keep exactly one durable row with a capped cadence; its successful
            // next observation resets the failure window. A manually-awakened
            // STAFF catalog task is no longer marked monitoring and still obtains a
            // visible terminal block after its normal bounded attempts.
            await this.queue.finish(task, workerId, { state: monitorRetry ? 'RETRY_WAIT' : task.attempts >= task.maxAttempts ? 'BLOCKED' : 'RETRY_WAIT',
                reason: String(error?.message || 'CATALOG_FAILED').slice(0, 500), nextRunAt: new Date(Date.now() + retryDelay),
                priority: monitorRetry ? 'P2' : undefined, monitoring: monitorRetry || undefined,
                checkpoint: monitorRetry ? { monitorFailures: Number(task.checkpoint.monitorFailures || 0) + 1 } : undefined });
        }
    }
    /** Re-fetch explicit provider metadata for one existing mapping. The task is
     * P2, single-flight, host rate limited, and only calls fetchWorkDetails (no
     * manifest/page discovery). A retryable provider failure is durably marked
     * EXTERNAL_BLOCKED and retried on a bounded exponential cooldown.
     */
    async refreshMappingMetadataFromSource(task, workerId, source, sourceWorkId, initialTimings = {}) {
        const totalStartedAt = performance.now();
        const timings = { ...initialTimings };
        const timePhase = async (name, operation) => {
            const startedAt = performance.now();
            try {
                return await operation();
            }
            finally {
                timings[name] = Math.round(performance.now() - startedAt);
            }
        };
        const reportTiming = (state, finalizeMs) => {
            const totalMs = Math.round(performance.now() - totalStartedAt);
            this.logger.info('Source metadata task timing', {
                source, state, taskId: task.id, ...timings,
                ...(finalizeMs === undefined ? {} : { finalizeMs }), totalMs,
            });
            return { ...timings, ...(finalizeMs === undefined ? {} : { finalizeMs }), totalMs };
        };
        if (!source || !sourceWorkId) {
            await this.queue.finish(task, workerId, { state: 'BLOCKED', reason: 'METADATA_SOURCE_INPUT_INVALID' });
            return;
        }
        const adapter = this.registry.get(source);
        if (!adapter) {
            const mapping = await this.pool.query(`SELECT work_id::text FROM importer_work_mappings
        WHERE source=$1 AND source_work_id=$2 AND work_id IS NOT NULL LIMIT 1`, [source, sourceWorkId]);
            if (mapping.rows[0])
                await this.queue.recordSourceMetadataEnrichment({
                    source, sourceWorkId, workId: String(mapping.rows[0].work_id), aliasesState: 'EXTERNAL_BLOCKED',
                    tagsState: 'EXTERNAL_BLOCKED', error: 'SOURCE_ADAPTER_MISSING', nextRetryAt: new Date(Date.now() + 6 * 60 * 60_000),
                });
            await this.queue.finish(task, workerId, { state: 'RETRY_WAIT', reason: 'SOURCE_ADAPTER_MISSING',
                nextRunAt: new Date(Date.now() + 6 * 60 * 60_000), resetAttempts: true });
            return;
        }
        if (!this.sourceMetadataGate.tryAcquire()) {
            await this.queue.finish(task, workerId, { state: 'RETRY_WAIT', reason: 'METADATA_LANE_BUSY',
                nextRunAt: new Date(Date.now() + 30_000), resetAttempts: true });
            return;
        }
        let mappingWorkId = '';
        try {
            const mapping = await timePhase('mappingAndPolicyQueryMs', () => this.pool.query(`SELECT m.work_id::text,m.source_title,m.source_slug,w.title,
          source_policy.enabled AS source_enabled,source_policy.status AS source_status,
          source_policy.cooldown_until AS source_cooldown,health.health AS source_health,
          health.cooldown_until AS health_cooldown
         FROM importer_work_mappings m JOIN works w ON w.id=m.work_id
         JOIN importer_sources source_policy ON source_policy.id=m.source
         LEFT JOIN importer_lite_source_health health ON health.source=m.source
         WHERE m.source=$1 AND m.source_work_id=$2 AND m.work_id IS NOT NULL LIMIT 1`, [source, sourceWorkId]));
            const row = mapping.rows[0];
            if (!row) {
                await this.queue.finish(task, workerId, { state: 'BLOCKED', reason: 'METADATA_ENRICHMENT_MAPPING_MISSING' });
                return;
            }
            mappingWorkId = row.work_id;
            // The mapping query already joins the source and health rows. Reuse that
            // snapshot instead of issuing a second YSQL round-trip for the same
            // source policy immediately before the provider request.
            const sourceCooldown = row.source_cooldown ? new Date(row.source_cooldown) : null;
            const healthCooldown = row.health_cooldown ? new Date(row.health_cooldown) : null;
            const cooldownUntil = [sourceCooldown, healthCooldown]
                .filter((value) => Boolean(value && Number.isFinite(value.getTime())))
                .sort((left, right) => right.getTime() - left.getTime())[0] || null;
            const status = String(row.source_status || 'ACTIVE').toUpperCase();
            const health = String(row.source_health || 'HEALTHY').toUpperCase();
            const expiredAdministrativeCooldown = status === 'COOLDOWN' && Boolean(sourceCooldown && sourceCooldown <= new Date());
            const administrativelyActive = row.source_enabled === true &&
                (['ACTIVE', 'DEGRADED', 'RECOVERING'].includes(status) || expiredAdministrativeCooldown);
            const healthAllowed = !['BROKEN', 'DISABLED'].includes(health);
            const runnable = administrativelyActive && healthAllowed && (!cooldownUntil || cooldownUntil <= new Date());
            if (!runnable) {
                const nextRunAt = cooldownUntil && cooldownUntil > new Date()
                    ? cooldownUntil : new Date(Date.now() + 6 * 60 * 60_000);
                await this.queue.recordSourceMetadataEnrichment({ source, sourceWorkId, workId: mappingWorkId,
                    aliasesState: 'EXTERNAL_BLOCKED', tagsState: 'EXTERNAL_BLOCKED',
                    error: `SOURCE_NOT_READY:${status || 'MISSING'}`, nextRetryAt: nextRunAt });
                await this.queue.finish(task, workerId, { state: 'RETRY_WAIT', reason: `SOURCE_NOT_READY:${status || 'MISSING'}`,
                    nextRunAt, resetAttempts: true });
                return;
            }
            const host = new URL(adapter.baseUrl).host;
            await timePhase('rateLimitWaitMs', () => this.sourceRateLimiter.acquire(host, undefined, 60_000));
            const details = await timePhase('httpMs', () => withTimeout(adapter.fetchWorkDetails(sourceWorkId), this.config.SOURCE_OPERATION_TIMEOUT_MS, 'METADATA_SOURCE_TIMEOUT'));
            const raw = details.raw && typeof details.raw === 'object' ? details.raw : {};
            const sourceMetadata = {
                ...raw,
                ...(details.alternativeTitles ? { alternativeTitles: details.alternativeTitles } : {}),
                ...(details.genres ? { genres: details.genres } : {}),
            };
            const extracted = await timePhase('parseNormalizeMs', async () => extractStoredMappingMetadata(sourceMetadata, details.title || row.title || row.source_title || ''));
            // Compare parser evidence before primary-title deduplication. A source
            // may explicitly return only its canonical title in an alias field; that
            // is a valid NO_ALIASES result, not a parser failure.
            const parsedAliasEvidence = extracted.aliases.length > 0 ? extracted.aliases
                : extractStoredMappingMetadata(sourceMetadata, '').aliases;
            const hasUnparsedExplicitValue = (fieldPattern) => Object.entries(raw).some(([key, value]) => {
                if (!fieldPattern.test(key))
                    return false;
                if (Array.isArray(value))
                    return value.length > 0;
                if (typeof value === 'string')
                    return value.trim().length > 0 && !['[]', '{}', 'null'].includes(value.trim().toLowerCase());
                if (value && typeof value === 'object')
                    return Object.keys(value).length > 0;
                return value !== null && value !== undefined;
            });
            const rawAliasEvidence = hasUnparsedExplicitValue(/^(alternative.?titles?|aliases|alternative.?names|other.?names|also.?known.?as|original.?title|native.?(title|name)|english.?title|romanized.?title|romaji.?title)$/i);
            const rawTagEvidence = hasUnparsedExplicitValue(/^(genres?|tags?|categories)$/i);
            if ((rawAliasEvidence && parsedAliasEvidence.length === 0) || (rawTagEvidence && extracted.genres.length === 0)) {
                await timePhase('metadataPersistCommitMs', () => this.queue.recordSourceMetadataEnrichment({ source, sourceWorkId, workId: mappingWorkId,
                    aliasesState: rawAliasEvidence && parsedAliasEvidence.length === 0 ? 'PARSER_ERROR' : (extracted.aliases.length ? 'ENRICHED' : 'NO_ALIASES'),
                    tagsState: rawTagEvidence && extracted.genres.length === 0 ? 'PARSER_ERROR' : (extracted.genres.length ? 'ENRICHED' : 'NO_TAGS'),
                    error: 'EXPLICIT_SOURCE_METADATA_NOT_PARSED', rawMetadata: sourceMetadata }));
                const checkpointTiming = { ...timings, totalBeforeFinalizeMs: Math.round(performance.now() - totalStartedAt) };
                const finalizeStartedAt = performance.now();
                await this.queue.finish(task, workerId, { state: 'BLOCKED', reason: 'METADATA_SOURCE_PARSER_ERROR',
                    checkpoint: { metadataSourceRefresh: true, parserError: true, metadataTiming: checkpointTiming } });
                timings.finishCommitMs = Math.round(performance.now() - finalizeStartedAt);
                reportTiming('PARSER_ERROR');
                return;
            }
            const persistenceQueuedAt = performance.now();
            await this.sourceMetadataPersistenceGate.runExclusive(async () => {
                timings.metadataPersistenceWaitMs = Math.round(performance.now() - persistenceQueuedAt);
                const resolved = await timePhase('mergePersistenceMs', () => this.dedupe.mergeKnownMappedWork({
                    source, sourceWorkId, title: details.title || row.source_title || row.title,
                    slug: details.slug || row.source_slug || row.title,
                    aliases: extracted.aliases, genres: extracted.genres,
                    author: details.author, artist: details.artist, synopsis: details.synopsis,
                    kind: details.kind, status: details.status, year: details.year, ageRating: details.ageRating,
                    rawMetadata: sourceMetadata,
                }, mappingWorkId, source));
                if (resolved.workId !== mappingWorkId || resolved.status === 'AMBIGUOUS' || resolved.status === 'FAILED') {
                    throw new Error(`METADATA_SOURCE_MAPPING_CONFLICT:${resolved.reason || resolved.status}`);
                }
                // The merge already knows the exact newly-added aliases and tag links.
                // Avoid a second post-merge SELECT (including a work_tags count scan)
                // just to reconstruct deltas for the enrichment checkpoint.
                const aliasDelta = Math.max(0, Number(resolved.metadataDelta?.aliasesAdded || 0));
                const tagDelta = Math.max(0, Number(resolved.metadataDelta?.tagsAdded || 0));
                await timePhase('metadataFinalizeCommitMs', () => this.queue.completeSourceMetadataRefresh(task, workerId, {
                    source, sourceWorkId, workId: mappingWorkId,
                    aliasesState: extracted.aliases.length ? 'ENRICHED' : 'NO_ALIASES',
                    tagsState: extracted.genres.length ? 'ENRICHED' : 'NO_TAGS',
                    aliasesAdded: aliasDelta, tagsAdded: tagDelta, rawMetadata: sourceMetadata,
                }, { metadataSourceRefresh: true, version: 2, aliases: extracted.aliases.length, aliasDelta,
                    tags: extracted.genres.length, tagDelta,
                    metadataTiming: { ...timings, totalBeforeFinalizeMs: Math.round(performance.now() - totalStartedAt) } }));
            });
            reportTiming('DONE');
        }
        catch (error) {
            const message = String(error?.message || 'METADATA_SOURCE_FETCH_FAILED').slice(0, 500);
            // Source anti-bot/login HTML can be parsed as a title by an adapter and
            // then rejected by the editorial guard. That is an external block, not
            // a parser defect; retain it with bounded retry instead of terminally
            // classifying the mapping as PARSER_ERROR.
            const externalChallengePage = /INVALID_EDITORIAL_TITLE:.*(?:authentication|login) prompt|TURNSTILE|CLOUDFLARE|HTTP[_ ]?(?:403|429)|RATE[_ ]?LIMIT/i.test(message);
            const persistenceFailure = message.startsWith('WORK_ALIAS_PERSIST_FAILED:');
            const parserFailure = message.startsWith('METADATA_SOURCE_MAPPING_CONFLICT:') && !externalChallengePage;
            // External failures reset the generic task attempt counter so a healthy
            // source observation can start cleanly. Keep a separate durable counter
            // in the task checkpoint; otherwise a 404/challenge retries every two
            // minutes forever and repeatedly occupies one of the tiny metadata
            // admission slots.
            const sourceExternalFailureCount = Math.max(0, Number(task.checkpoint.sourceExternalFailureCount) || 0) + 1;
            const retryDelay = Math.min(6 * 60 * 60_000, 60_000 * 2 ** Math.min(sourceExternalFailureCount, 8));
            if (mappingWorkId && !persistenceFailure)
                await timePhase('metadataPersistCommitMs', () => this.queue.recordSourceMetadataEnrichment({
                    source, sourceWorkId, workId: mappingWorkId,
                    aliasesState: parserFailure ? 'PARSER_ERROR' : 'EXTERNAL_BLOCKED',
                    tagsState: parserFailure ? 'PARSER_ERROR' : 'EXTERNAL_BLOCKED', error: message,
                    nextRetryAt: parserFailure ? null : new Date(Date.now() + retryDelay),
                }));
            if (!parserFailure && !persistenceFailure)
                await timePhase('sourceHealthPersistMs', () => this.queue.recordSourceFailure(source, error));
            const checkpointTiming = { ...timings, totalBeforeFinalizeMs: Math.round(performance.now() - totalStartedAt) };
            const finalizeStartedAt = performance.now();
            await this.queue.finish(task, workerId, parserFailure
                ? { state: 'BLOCKED', reason: message, checkpoint: { metadataTiming: checkpointTiming } }
                : persistenceFailure
                    ? { state: 'RETRY_WAIT', reason: 'METADATA_ALIAS_PERSISTENCE_FAILED',
                        nextRunAt: new Date(Date.now() + Math.min(5 * 60_000, retryDelay)), resetAttempts: true,
                        checkpoint: { metadataTiming: checkpointTiming } }
                    : { state: 'RETRY_WAIT', reason: `METADATA_SOURCE_EXTERNAL:${message}`,
                        nextRunAt: new Date(Date.now() + retryDelay),
                        checkpoint: { metadataSourceRefresh: true, sourceExternalFailureCount, metadataTiming: checkpointTiming }, resetAttempts: true });
            timings.finishCommitMs = Math.round(performance.now() - finalizeStartedAt);
            reportTiming(parserFailure ? 'PARSER_ERROR' : persistenceFailure ? 'PERSISTENCE_FAILURE' : 'EXTERNAL_BLOCKED');
        }
        finally {
            this.sourceMetadataGate.release();
        }
    }
    /** A no-network P2 pass over explicit metadata saved with a known mapping. */
    async enrichStoredMappingMetadata(task, workerId, source, sourceWorkId) {
        if (!source || !sourceWorkId) {
            await this.queue.finish(task, workerId, { state: 'BLOCKED', reason: 'METADATA_ENRICHMENT_INPUT_INVALID' });
            return;
        }
        try {
            const mapping = await this.pool.query(`SELECT m.work_id::text,m.source_title,m.source_slug,w.title AS work_title,m.metadata
         FROM importer_work_mappings m JOIN works w ON w.id=m.work_id
         WHERE m.source=$1 AND m.source_work_id=$2 AND m.work_id IS NOT NULL LIMIT 1`, [source, sourceWorkId]);
            const row = mapping.rows[0];
            if (!row) {
                await this.queue.finish(task, workerId, { state: 'BLOCKED', reason: 'METADATA_ENRICHMENT_MAPPING_MISSING' });
                return;
            }
            const extracted = extractStoredMappingMetadata(row.metadata, row.work_title || row.source_title || '');
            if (!extracted.aliases.length && !extracted.genres.length) {
                await this.queue.finish(task, workerId, { state: 'DONE', checkpoint: { metadataEnrichment: true, empty: true }, resetAttempts: true });
                return;
            }
            const resolved = await this.dedupe.resolveWork({
                source, sourceWorkId,
                title: row.source_title || row.work_title,
                slug: row.source_slug || row.work_title,
                aliases: extracted.aliases,
                genres: extracted.genres,
            });
            if (!resolved.workId || resolved.status === 'AMBIGUOUS') {
                await this.queue.finish(task, workerId, { state: 'BLOCKED', reason: resolved.reason || 'METADATA_ENRICHMENT_AMBIGUOUS' });
                return;
            }
            await this.queue.finish(task, workerId, {
                state: 'DONE', workId: resolved.workId,
                checkpoint: { metadataEnrichment: true, aliases: extracted.aliases.length, tags: extracted.genres.length },
                resetAttempts: true,
            });
        }
        catch (error) {
            await this.queue.finish(task, workerId, {
                state: task.attempts >= task.maxAttempts ? 'BLOCKED' : 'RETRY_WAIT',
                reason: String(error?.message || 'METADATA_ENRICHMENT_FAILED').slice(0, 500),
                nextRunAt: new Date(Date.now() + this.recurringRetryDelayMs(task.attempts)),
            });
        }
    }
    /** Release bursts get a quick recheck; quiet works exponentially back off. */
    monitorIntervalMs(task, status, newReleaseCount) {
        if (status === 'HIATUS')
            return 6 * 60 * 60_000;
        if (newReleaseCount > 0)
            return 5 * 60_000;
        const emptyChecks = Math.max(0, Number(task.checkpoint.emptyChecks || 0));
        return Math.min(6 * 60 * 60_000, 15 * 60_000 * 2 ** Math.min(emptyChecks, 5));
    }
    /** Bounded health recovery for the two recurring observation task types. */
    recurringRetryDelayMs(attempts) {
        return Math.min(6 * 60 * 60_000, 60_000 * 2 ** Math.min(Math.max(0, attempts), 8));
    }
    /** Invalid/blocked covers remain absent; they never overwrite a known cover. */
    async loadCover(adapter, url) {
        try {
            const bytes = await this.downloads.runExclusive(async () => {
                let host = null;
                try {
                    host = new URL(url).host;
                }
                catch { }
                if (host)
                    await this.sourceRateLimiter.acquire(host);
                const response = await fetch(url, { headers: await adapter.getImageHeaders?.(url), signal: AbortSignal.timeout(30_000) });
                if (!response.ok) {
                    if (response.status === 429 && host)
                        this.sourceRateLimiter.handle429(host, response.headers.get('retry-after'));
                    throw new Error(`COVER_HTTP_${response.status}`);
                }
                const bytes = await readImageBody(response, { signal: AbortSignal.timeout(30_000), maxBytes: 8 * 1024 * 1024 });
                if (host)
                    this.sourceRateLimiter.recordSuccess(host);
                return bytes;
            });
            const stored = await processAndStoreMedia(this.catalogClient, this.storage, bytes, this.config.IMPORTER_USER_ID, 'cover');
            return stored.mediaId;
        }
        catch {
            return null;
        }
    }
}
