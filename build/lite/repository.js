import { randomUUID } from 'node:crypto';
import { LITE_PRIORITY_RANK } from './types.js';
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
// bounded per source and per recovery turn so a recovered large source cannot
// crowd out other healthy work or immediately recreate the rate limit.
const SOURCE_COOLDOWN_RECOVERY_SOURCES_PER_TURN = 4;
const SOURCE_COOLDOWN_RECOVERY_TASKS_PER_SOURCE = 8;
const SOURCE_COOLDOWN_RECOVERY_CANDIDATE_TASKS = 48;
const priorityName = (value) => ['STAFF', 'P0', 'P1', 'P2'][value] || 'P2';
const asTask = (row) => ({
    id: row.id, taskType: row.task_type, workId: row.work_id, workKey: row.work_key,
    chapterKey: row.chapter_key, priority: priorityName(row.priority), basePriority: priorityName(row.base_priority),
    state: row.state, idempotencyKey: row.idempotency_key, nextRunAt: new Date(row.next_run_at),
    attempts: row.attempts, maxAttempts: row.max_attempts, leaseOwner: row.lease_owner,
    leaseUntil: row.lease_until ? new Date(row.lease_until) : null, reason: row.reason,
    monitoring: Boolean(row.monitoring),
    payload: row.payload || {}, checkpoint: row.checkpoint || {},
});
/** Direct YSQL persistence. Network, adapters and Telegram are never invoked here. */
export class LiteRepository {
    db;
    // This is only a local maintenance hint. Durable task state remains the
    // authority; the runtime uses the hint to avoid a second catalog turn when
    // the materialised P1 import reserve is already abundant.
    historicalP1ReserveAbundant = false;
    hasAbundantHistoricalP1Reserve() { return this.historicalP1ReserveAbundant; }
    constructor(db) {
        this.db = db;
    }
    async refreshReadyWork(client, workKey) {
        await client.query('SELECT public.importer_lite_refresh_ready_work($1)', [workKey]);
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
            await this.refreshReadyWork(client, input.workKey);
            await client.query('COMMIT');
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
          ON CONFLICT (idempotency_key) DO NOTHING RETURNING idempotency_key`, values);
                inserted += result.rowCount || 0;
                for (const row of result.rows)
                    insertedKeys.add(String(row.idempotency_key));
            }
            for (const workKey of new Set(uniqueInputs.map((input) => input.workKey)))
                await this.refreshReadyWork(client, workKey);
            await client.query('COMMIT');
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
    async claim(workerId, leaseSeconds) {
        const result = await this.db.query('SELECT * FROM public.importer_lite_claim_task($1,$2::interval)', [workerId, `${Math.max(30, leaseSeconds)} seconds`]);
        return result.rows[0] ? asTask(result.rows[0]) : null;
    }
    /**
     * P0 observation has one durable, explicitly bounded lane.  It is never
     * selected by normal claims, so unchanged watches cannot starve P1/P2.
     */
    async claimMonitor(workerId, leaseSeconds) {
        const result = await this.db.query('SELECT * FROM public.importer_lite_claim_monitor_task($1,$2::interval)', [workerId, `${Math.max(30, leaseSeconds)} seconds`]);
        return result.rows[0] ? asTask(result.rows[0]) : null;
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
    async readyImportSources(limit = 64) {
        const maximum = Math.max(1, Math.min(128, limit));
        const result = await this.db.query(`WITH lane AS (
        SELECT min(priority) AS priority
        FROM importer_lite_tasks
        WHERE state='READY' AND monitoring=false AND task_type='IMPORT_CHAPTER' AND next_run_at<=now()
      ), candidate_works AS (
        SELECT rw.work_key
        FROM importer_lite_ready_works rw JOIN lane ON lane.priority=rw.priority
        ORDER BY rw.last_claimed_at NULLS FIRST,rw.next_run_at,rw.work_key
        LIMIT $1
      ), candidates AS (
        SELECT t.id,t.work_id,t.chapter_key
        FROM candidate_works rw
        JOIN LATERAL (
          SELECT id,work_id,chapter_key
          FROM importer_lite_tasks t JOIN lane ON lane.priority=t.priority
          WHERE t.work_key=rw.work_key AND t.state='READY' AND t.monitoring=false
            AND t.task_type='IMPORT_CHAPTER' AND t.next_run_at<=now()
          ORDER BY t.next_run_at,t.created_at
          LIMIT 1
        ) t ON true
      ), import_routes AS (
        SELECT DISTINCT ON (c.id) c.id,cs.source
        FROM candidates c
        JOIN importer_lite_chapter_sources cs
          ON cs.work_id=c.work_id AND cs.canonical_chapter_key=c.chapter_key
        JOIN importer_sources s ON s.id=cs.source
        LEFT JOIN importer_lite_source_health h ON h.source=cs.source
        WHERE cs.confirmed=true AND cs.has_chapter=true
          AND s.enabled=true
          AND COALESCE((to_jsonb(s)->>'chapter_ingestion_enabled')::boolean,true)
          AND (COALESCE(s.status,'ACTIVE') IN ('ACTIVE','DEGRADED','RECOVERING') OR
               (s.status='COOLDOWN' AND s.cooldown_until IS NOT NULL AND s.cooldown_until<=now()))
          AND COALESCE(h.health,'HEALTHY') NOT IN ('BROKEN','DISABLED')
          AND (h.cooldown_until IS NULL OR h.cooldown_until<=now())
      )
      SELECT DISTINCT source FROM import_routes ORDER BY source`, [maximum]);
        return result.rows.map((row) => String(row.source));
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
        const client = await this.db.connect();
        try {
            await client.query('BEGIN');
            const result = await client.query(`UPDATE importer_lite_tasks
        SET state='READY',attempts=GREATEST(attempts-1,0),lease_owner=NULL,lease_until=NULL,
            next_run_at=GREATEST(next_run_at,now()+($3::int * interval '1 millisecond')),updated_at=now()
        WHERE id=$1::uuid AND state='RUNNING' AND lease_owner=$2`, [task.id, workerId, Math.max(50, Math.min(5_000, delayMs))]);
            if (result.rowCount === 1)
                await this.refreshReadyWork(client, task.workKey);
            await client.query('COMMIT');
            return result.rowCount === 1;
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
     * Discovery is a low-cost producer, never a reason to monopolize P2.  This
     * bounded startup repair only defers due DISCOVER rows; catalog and chapter
     * tasks are untouched.  Each ready-work projection is refreshed in the
     * same short transaction so a stale projection cannot create empty claims.
     */
    async staggerReadyDiscoveries(spacingMs, limit = 64) {
        const client = await this.db.connect();
        try {
            await client.query('BEGIN');
            const candidates = await client.query(`SELECT id,work_key FROM importer_lite_tasks
         WHERE task_type='DISCOVER' AND state='READY' AND next_run_at<=now()
         ORDER BY created_at,id LIMIT $1 FOR UPDATE SKIP LOCKED`, [Math.max(1, Math.min(limit, 256))]);
            const base = Date.now();
            for (const [index, row] of candidates.rows.entries()) {
                await client.query(`UPDATE importer_lite_tasks SET next_run_at=$2,updated_at=now() WHERE id=$1::uuid`, [row.id, new Date(base + index * Math.max(1_000, spacingMs))]);
            }
            for (const workKey of new Set(candidates.rows.map((row) => row.work_key)))
                await this.refreshReadyWork(client, workKey);
            await client.query('COMMIT');
            return candidates.rowCount || 0;
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
     * The v1 catalog contains durable, confirmed source/work mappings that may
     * predate Lite's chapter-manifest table.  They are evidence worth checking,
     * but are not evidence that a numeric chapter gap exists.  This bounded
     * admission turns only a few such mappings into P1 catalog passes when the
     * real P1 reserve becomes low.  Cataloging remains outside the claim SQL;
     * its confirmed manifest is what may subsequently create IMPORT_CHAPTER
     * tasks.
     *
     * The inner window is deliberately small and indexed by the legacy mapping
     * status.  Limiting each source to two candidates prevents a large legacy
     * mapping set from monopolising the next P1 refill.
     */
    async refillHistoricalP1Catalog(readyFloor = 64, batchLimit = 16) {
        const floor = Math.max(1, Math.min(512, readyFloor));
        const maximum = Math.max(1, Math.min(32, batchLimit));
        // Four normal reserve windows is enough to keep workers supplied while
        // making catalog production strictly yield to a genuinely large, already
        // executable P1 backlog. Bound the count query itself as well.
        const abundantFloor = Math.min(512, Math.max(64, floor * 4));
        this.historicalP1ReserveAbundant = false;
        const client = await this.db.connect();
        try {
            await client.query('BEGIN');
            const reserve = await client.query(`SELECT count(*)::int AS count FROM (
        SELECT 1 FROM importer_lite_tasks
        WHERE state='READY' AND monitoring=false AND priority=2 AND task_type='IMPORT_CHAPTER'
        LIMIT $1
      ) AS bounded_ready`, [abundantFloor]);
            const current = Number(reserve.rows[0]?.count || 0);
            if (current >= abundantFloor) {
                // Do not spend the shared pool on catalog selection or promotion
                // while hundreds of P1 chapters are already executable. When this
                // reserve drains, the next recovery turn automatically resumes the
                // bounded, source-diverse producer.
                this.historicalP1ReserveAbundant = true;
                await client.query('COMMIT');
                return 0;
            }
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
            const pendingBackfill = await client.query(`WITH bounded_backfill AS (
        SELECT state,next_run_at,payload FROM importer_lite_tasks
        WHERE state IN ('READY','RUNNING','RETRY_WAIT') AND priority=2 AND task_type='CATALOG'
          AND payload ? 'historicalP1Backfill'
        ORDER BY CASE WHEN state='RUNNING' OR (state='READY' AND next_run_at<=now()) THEN 0 ELSE 1 END,
          next_run_at,created_at
        LIMIT $1
      ) SELECT count(*)::int AS total,
        count(DISTINCT payload->>'source') FILTER (
          WHERE state='RUNNING' OR (state='READY' AND next_run_at<=now())
        )::int AS active_sources
        FROM bounded_backfill`, [maximum]);
            const pending = Number(pendingBackfill.rows[0]?.total || 0);
            const activeSources = Number(pendingBackfill.rows[0]?.active_sources || 0);
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
            const candidates = await client.query(`WITH bounded AS (
          SELECT m.source,m.source_work_id,m.work_id,m.updated_at,m.id
          FROM importer_work_mappings m
          JOIN importer_sources s ON s.id=m.source
          LEFT JOIN importer_lite_source_health h ON h.source=m.source
          WHERE m.work_id IS NOT NULL
            AND m.sync_status IN ('WAITING_ADMISSION','SYNCED','ACTIVE')
            AND s.enabled=true
            AND (COALESCE(s.status,'ACTIVE') IN ('ACTIVE','DEGRADED','RECOVERING') OR
                 (s.status='COOLDOWN' AND s.cooldown_until IS NOT NULL AND s.cooldown_until<=now()))
            -- This is P1 repair for an existing canonical mapping, not P2
            -- discovery.  An operator may disable new-work discovery while
            -- still allowing an already-mapped work to fill its chapters.
            AND COALESCE((to_jsonb(s)->>'chapter_ingestion_enabled')::boolean,true)
            AND COALESCE(h.health,'HEALTHY') NOT IN ('BROKEN','DISABLED')
            AND (h.cooldown_until IS NULL OR h.cooldown_until<=now())
            AND NOT EXISTS (
              SELECT 1 FROM importer_lite_tasks t
              WHERE t.idempotency_key='lite:catalog:' || m.source || ':' || m.source_work_id
            )
            -- An old failed check stays durable for diagnosis and bounded
            -- retry, but cannot consume every source seat in the producer
            -- lane.  Never queue another mapping from a source that already
            -- has a pending historical check: use this small opportunity to
            -- ask a different healthy source for a manifest instead.
            AND NOT EXISTS (
              SELECT 1 FROM importer_lite_tasks t
              WHERE t.state IN ('READY','RUNNING','RETRY_WAIT')
                AND t.priority=2 AND t.task_type='CATALOG'
                AND t.payload ? 'historicalP1Backfill'
                AND t.payload->>'source'=m.source
            )
          ORDER BY m.updated_at DESC,m.id DESC
          LIMIT $1
        ), fair AS (
          SELECT *,row_number() OVER (PARTITION BY source ORDER BY updated_at DESC,id DESC) AS source_position
          FROM bounded
        )
        SELECT source,source_work_id,work_id FROM fair
        WHERE source_position<=$3
        ORDER BY updated_at DESC,id DESC
        LIMIT $2`, [Math.max(128, maximum * 16), limit, perSource]);
            if (!candidates.rowCount) {
                await client.query('COMMIT');
                return 0;
            }
            const values = [];
            const tuples = candidates.rows.map((row, index) => {
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
            for (const workKey of new Set(inserted.rows.map((row) => String(row.work_key))))
                await this.refreshReadyWork(client, workKey);
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
        const client = await this.db.connect();
        try {
            await client.query('BEGIN');
            const busySources = [...new Set([...activeImportSources].filter(Boolean))];
            const sourcePenalty = busySources.length
                ? `CASE WHEN payload->>'source'=ANY($1::text[]) THEN 1 ELSE 0 END,`
                : '';
            const candidate = await client.query(`SELECT id,work_key
        FROM importer_lite_tasks
        WHERE state='READY' AND monitoring=false AND priority=2 AND task_type='CATALOG'
          AND next_run_at<=now() AND payload ? 'historicalP1Backfill'
          AND COALESCE(payload->>'deferBehindReadyImport','false')='true'
        ORDER BY ${sourcePenalty} next_run_at,created_at,id
        LIMIT 1 FOR UPDATE SKIP LOCKED`, busySources.length ? [busySources] : []);
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
        const client = await this.db.connect();
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
            for (const row of recovered.rows)
                await this.refreshReadyWork(client, String(row.work_key));
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
     * Source administration and transient Lite health remain durable authority.
     * This indexed one-source read happens before source networking, never in a
     * normal task claim, and prevents a paused/cooling provider from consuming
     * a catalog or discovery slot.
     */
    async sourcePolicy(source) {
        const result = await this.db.query(`SELECT s.enabled,s.status,s.sync_interval_minutes,
        COALESCE((to_jsonb(s)->>'catalog_discovery_enabled')::boolean,true) AS catalog_discovery_enabled,
        COALESCE((to_jsonb(s)->>'chapter_ingestion_enabled')::boolean,true) AS chapter_ingestion_enabled,
        s.cooldown_until AS source_cooldown,h.health,h.cooldown_until AS health_cooldown
      FROM importer_sources s
      LEFT JOIN importer_lite_source_health h ON h.source=s.id
      WHERE s.id=$1`, [source]);
        const row = result.rows[0];
        if (!row)
            return null;
        const sourceCooldown = row.source_cooldown ? new Date(row.source_cooldown) : null;
        const healthCooldown = row.health_cooldown ? new Date(row.health_cooldown) : null;
        const cooldownUntil = [sourceCooldown, healthCooldown]
            .filter((value) => Boolean(value && Number.isFinite(value.getTime())))
            .sort((a, b) => b.getTime() - a.getTime())[0] || null;
        const status = String(row.status || 'ACTIVE').toUpperCase();
        const health = String(row.health || 'HEALTHY').toUpperCase();
        // COOLDOWN is temporary only when it has a durable deadline.  Once that
        // deadline passes it must resume; otherwise a source can remain invisible
        // to P2 forever after a restart.  A cooldown without a deadline, and
        // PAUSED/DISABLED, remain explicit operator actions.
        const expiredAdministrativeCooldown = status === 'COOLDOWN' && Boolean(sourceCooldown && sourceCooldown <= new Date());
        const administrativelyActive = row.enabled === true &&
            (['ACTIVE', 'DEGRADED', 'RECOVERING'].includes(status) || expiredAdministrativeCooldown);
        const healthAllowed = !['BROKEN', 'DISABLED'].includes(health);
        const runnable = administrativelyActive && healthAllowed && (!cooldownUntil || cooldownUntil <= new Date());
        const minutes = Number(row.sync_interval_minutes);
        return {
            enabled: row.enabled === true,
            status, health, cooldownUntil,
            syncIntervalMs: Math.max(5, Math.min(24 * 60, Number.isFinite(minutes) ? minutes : 30)) * 60_000,
            catalogDiscoveryEnabled: row.catalog_discovery_enabled !== false,
            chapterIngestionEnabled: row.chapter_ingestion_enabled !== false,
            runnable,
        };
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
            const completed = await client.query(`UPDATE importer_lite_tasks
           SET state='DONE', checkpoint=checkpoint || $3::jsonb,
               lease_owner=NULL, lease_until=NULL, updated_at=now()
         WHERE id=$1::uuid AND state='RUNNING' AND lease_owner=$2 AND lease_until>now()`, [task.id, workerId, JSON.stringify(checkpoint)]);
            if (completed.rowCount !== 1) {
                await client.query('ROLLBACK');
                return false;
            }
            // A completed upload which began before a later provider 429 must not
            // erase that newer cooldown. This mirrors recordSourceSuccess exactly,
            // but keeps the successful transition in one brief transaction.
            await client.query(`INSERT INTO importer_lite_source_health(source,health,cooldown_until,consecutive_failures,last_error)
        VALUES($1,'HEALTHY',NULL,0,NULL)
        ON CONFLICT(source) DO UPDATE SET health='HEALTHY',cooldown_until=NULL,
          consecutive_failures=0,last_error=NULL,updated_at=now()
        WHERE NOT (importer_lite_source_health.health='RATE_LIMITED'
          AND importer_lite_source_health.cooldown_until>now())`, [completion.selectedSource]);
            await client.query(`INSERT INTO importer_lite_tasks
          (task_type,work_id,work_key,chapter_key,priority,base_priority,idempotency_key,payload)
        SELECT 'PUBLISH',$1::uuid,$2,$3,
          CASE WHEN EXISTS (
            SELECT 1 FROM importer_staff_requests s
            WHERE s.work_id=$1::uuid AND s.status IN ('QUEUED','IMPORTING','RETRYING','ACTIVE')
          ) THEN 0 ELSE $4::smallint END,
          $4::smallint,$5,$6::jsonb
        ON CONFLICT(idempotency_key) DO NOTHING`, [task.workId, task.workKey, task.chapterKey, LITE_PRIORITY_RANK[publishPriority],
                `lite:publish:${task.workId}:${task.chapterKey}`,
                // The completion transaction already owns these durable, scalar
                // references. Carry them with the PUBLISH task so its normal path
                // does not spend an extra pool round trip rereading the import
                // checkpoint. Legacy/recovery tasks without them still use the
                // conservative checkpoint lookup in the executor.
                JSON.stringify({ importTaskId: task.id, source: completion.selectedSource,
                    chapterId: completion.chapterId, mediaIds: completion.mediaIds })]);
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
    async finish(task, workerId, outcome) {
        const client = await this.db.connect();
        try {
            await client.query('BEGIN');
            const result = await client.query(`UPDATE importer_lite_tasks
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
         WHERE id=$1::uuid AND state='RUNNING' AND lease_owner=$2 AND lease_until>now()`, [task.id, workerId, outcome.state, outcome.reason || null, outcome.nextRunAt || null, JSON.stringify(outcome.checkpoint || {}), outcome.priority === undefined ? null : LITE_PRIORITY_RANK[outcome.priority], outcome.monitoring === undefined ? null : outcome.monitoring, outcome.workId || null, Boolean(outcome.resetAttempts)]);
            if (result.rowCount === 1)
                await this.refreshReadyWork(client, task.workKey);
            await client.query('COMMIT');
            return result.rowCount === 1;
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
     * Catalog evidence is the only event that reopens a source-blocked chapter.
     * It is scoped by the indexed canonical work/chapter keys, so a new mapping
     * never causes a global BLOCKED sweep. Terminal integrity failures stay
     * blocked until an operator changes the underlying condition.
     */
    async wakeCatalogBlocked(workId, workKey) {
        const client = await this.db.connect();
        try {
            await client.query('BEGIN');
            const result = await client.query(`UPDATE importer_lite_tasks
        SET state='READY',reason=NULL,next_run_at=now(),updated_at=now()
        WHERE work_id=$1::uuid AND state='BLOCKED' AND (
          (task_type='IMPORT_CHAPTER' AND COALESCE(reason,'') NOT LIKE 'UPLOAD_OUTCOME_UNKNOWN%'
             AND COALESCE(reason,'') NOT LIKE 'CHAPTER_DISPLAY_NUMBER%'
             AND COALESCE(reason,'') NOT LIKE 'CHAPTER_NUMBER_COLLISION%'
             AND COALESCE(reason,'') NOT LIKE 'SOURCE_PAGE_MANIFEST_CHANGED_AFTER_PARTIAL_STORAGE%'
             AND COALESCE(reason,'') NOT LIKE 'PARTIAL_UPLOAD_SOURCE_ROUTE_UNAVAILABLE%') OR
          (task_type='PUBLISH' AND reason='WORK_PRESENTATION_INCOMPLETE')
        )`, [workId]);
            await this.refreshReadyWork(client, workKey);
            await client.query('COMMIT');
            return result.rowCount || 0;
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
     * Source health is durable operational evidence, shared by discovery,
     * cataloguing and chapter execution.  It is deliberately separate from
     * task priority: a cooled-down preferred source must not suppress a
     * confirmed healthy alternative.
     */
    async recordSourceFailure(source, error) {
        const detail = error;
        const retryAfter = Number(detail?.retryAfter || detail?.parameters?.retry_after || 0);
        const cooldownSeconds = retryAfter > 0 ? Math.min(3600, retryAfter + 2) : 60;
        const health = retryAfter > 0 ? 'RATE_LIMITED' : 'DEGRADED';
        const message = String(detail?.message || 'SOURCE_FAILURE').slice(0, 500);
        // Ordinary transient failures retain the one-statement health update: a
        // failure must never make the error path compete with normal imports for
        // extra pool time. A provider-specified retry boundary is different. If
        // a source has explicitly rate-limited us, leaving thousands of its
        // single-route tasks READY makes workers repeatedly claim, inspect and
        // immediately defer them while lower executable work waits. Move only
        // those tasks to the same durable cooldown in one bounded event. Tasks
        // with a healthy confirmed alternative stay READY for executor fallback.
        if (!(retryAfter > 0)) {
            await this.db.query(`INSERT INTO importer_lite_source_health(source,health,cooldown_until,consecutive_failures,last_error)
        VALUES($1,$2,now()+($3 || ' seconds')::interval,1,$4)
        ON CONFLICT(source) DO UPDATE SET health=excluded.health,cooldown_until=excluded.cooldown_until,
          consecutive_failures=importer_lite_source_health.consecutive_failures+1,last_error=excluded.last_error,updated_at=now()`, [source, health, String(cooldownSeconds), message]).catch(() => { });
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
                await client.query(`WITH deferred AS (
            UPDATE importer_lite_tasks t
            SET state='RETRY_WAIT',lease_owner=NULL,lease_until=NULL,
                next_run_at=GREATEST(t.next_run_at,$2::timestamptz),
                reason='SOURCE_RATE_LIMITED',updated_at=now()
            WHERE t.state='READY' AND t.monitoring=false AND t.task_type='IMPORT_CHAPTER'
              AND EXISTS (
                SELECT 1 FROM importer_lite_chapter_sources blocked
                WHERE blocked.work_id=t.work_id AND blocked.canonical_chapter_key=t.chapter_key
                  AND blocked.source=$1 AND blocked.confirmed=true AND blocked.has_chapter=true
                  AND (blocked.language=COALESCE(NULLIF(t.payload->>'language',''),'und'))
                  AND (blocked.edition=COALESCE(NULLIF(t.payload->>'edition',''),'default'))
              )
              AND NOT EXISTS (
                SELECT 1 FROM importer_lite_chapter_sources alternative
                JOIN importer_sources source_policy ON source_policy.id=alternative.source
                LEFT JOIN importer_lite_source_health alternative_health ON alternative_health.source=alternative.source
                WHERE alternative.work_id=t.work_id AND alternative.canonical_chapter_key=t.chapter_key
                  AND alternative.source<>$1 AND alternative.confirmed=true AND alternative.has_chapter=true
                  AND alternative.language=COALESCE(NULLIF(t.payload->>'language',''),'und')
                  AND alternative.edition=COALESCE(NULLIF(t.payload->>'edition',''),'default')
                  AND source_policy.enabled=true
                  AND COALESCE((to_jsonb(source_policy)->>'chapter_ingestion_enabled')::boolean,true)
                  AND (COALESCE(source_policy.status,'ACTIVE') IN ('ACTIVE','DEGRADED','RECOVERING') OR
                       (source_policy.status='COOLDOWN' AND source_policy.cooldown_until IS NOT NULL
                         AND source_policy.cooldown_until<=now()))
                  AND COALESCE(alternative_health.health,'HEALTHY') NOT IN ('BROKEN','DISABLED')
                  AND (alternative_health.cooldown_until IS NULL OR alternative_health.cooldown_until<=now())
              )
            RETURNING t.work_key
          ), refreshed AS (
            SELECT public.importer_lite_refresh_ready_work(work_key)
            FROM (SELECT DISTINCT work_key FROM deferred) affected
          ) SELECT count(*)::int AS refreshed FROM refreshed`, [source, cooldownUntil]);
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
        await this.db.query(`INSERT INTO importer_lite_source_health(source,health,cooldown_until,consecutive_failures,last_error)
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
    /** One bounded recovery turn; it cannot consume the claim path or scan works. */
    async reconcile(limit = 50) {
        const client = await this.db.connect();
        try {
            await client.query('BEGIN');
            const leases = await client.query(`WITH picked AS (
           SELECT id,work_key,attempts,max_attempts,task_type,monitoring FROM importer_lite_tasks WHERE state='RUNNING' AND lease_until<now()
           ORDER BY lease_until LIMIT $1 FOR UPDATE SKIP LOCKED
         ) UPDATE importer_lite_tasks t SET
           state=CASE WHEN picked.attempts >= picked.max_attempts
             AND NOT (picked.task_type='DISCOVER' OR (picked.task_type='CATALOG' AND picked.monitoring))
             THEN 'BLOCKED' ELSE 'RETRY_WAIT' END,
           lease_owner=NULL, lease_until=NULL,
           next_run_at=CASE WHEN picked.attempts >= picked.max_attempts
             AND NOT (picked.task_type='DISCOVER' OR (picked.task_type='CATALOG' AND picked.monitoring))
             THEN t.next_run_at ELSE now() + interval '30 seconds' END,
           reason=CASE WHEN picked.attempts >= picked.max_attempts
             AND NOT (picked.task_type='DISCOVER' OR (picked.task_type='CATALOG' AND picked.monitoring))
             THEN 'LEASE_EXPIRED_MAX_ATTEMPTS' ELSE 'LEASE_EXPIRED' END,
           updated_at=now()
         FROM picked WHERE t.id=picked.id RETURNING picked.work_key,t.state`, [limit]);
            const retries = await client.query(`WITH picked AS (
           SELECT id,work_key,attempts,max_attempts,task_type,monitoring FROM importer_lite_tasks
           WHERE state='RETRY_WAIT' AND next_run_at<=now()
             -- Explicit provider cooldown work is admitted only by the
             -- per-source pass below. A later 429 may extend health cooldown
             -- after this task received its own retry timestamp.
             AND COALESCE(reason,'')<>'SOURCE_RATE_LIMITED'
           ORDER BY next_run_at LIMIT $1 FOR UPDATE SKIP LOCKED
         ) UPDATE importer_lite_tasks t SET
           state=CASE WHEN picked.attempts >= picked.max_attempts
             AND NOT (picked.task_type='DISCOVER' OR (picked.task_type='CATALOG' AND picked.monitoring))
             THEN 'BLOCKED' ELSE 'READY' END,
           reason=CASE WHEN picked.attempts >= picked.max_attempts
             AND NOT (picked.task_type='DISCOVER' OR (picked.task_type='CATALOG' AND picked.monitoring))
             THEN 'MAX_ATTEMPTS_EXHAUSTED' ELSE t.reason END,
           updated_at=now()
         FROM picked WHERE t.id=picked.id RETURNING picked.work_key,t.state`, [limit]);
            // `recordSourceFailure` moves only sole-route work to a durable
            // SOURCE_RATE_LIMITED cooldown. The normal retry pass above is kept
            // deliberately small for crash recovery, so it must not be the only
            // mechanism that reopens a large healthy source backlog. Start from a
            // small indexed retry head, then select a few routes per source after
            // its explicit cooldown has elapsed. This runs only in the 30-second
            // reconciler and refreshes the normal ready-work projection.
            const sourceCooldownRecovery = await client.query(`WITH due AS (
           SELECT id,work_key,work_id,chapter_key,payload,next_run_at,created_at
           FROM importer_lite_tasks
           WHERE state='RETRY_WAIT' AND monitoring=false AND task_type='IMPORT_CHAPTER'
             AND reason='SOURCE_RATE_LIMITED' AND next_run_at<=now()
           ORDER BY next_run_at,created_at
           LIMIT $1 FOR UPDATE SKIP LOCKED
         ), routes AS (
           SELECT DISTINCT ON (due.id) due.id,due.work_key,due.next_run_at,due.created_at,route.source
           FROM due
           JOIN importer_lite_chapter_sources route
             ON route.work_id=due.work_id AND route.canonical_chapter_key=due.chapter_key
           JOIN importer_sources s ON s.id=route.source
           LEFT JOIN importer_lite_source_health h ON h.source=route.source
           WHERE route.confirmed=true AND route.has_chapter=true
             AND route.language=COALESCE(NULLIF(due.payload->>'language',''),'und')
             AND route.edition=COALESCE(NULLIF(due.payload->>'edition',''),'default')
             AND s.enabled=true
             AND COALESCE((to_jsonb(s)->>'chapter_ingestion_enabled')::boolean,true)
             AND (COALESCE(s.status,'ACTIVE') IN ('ACTIVE','DEGRADED','RECOVERING') OR
                  (s.status='COOLDOWN' AND s.cooldown_until IS NOT NULL AND s.cooldown_until<=now()))
             AND COALESCE(h.health,'HEALTHY') IN ('HEALTHY','DEGRADED','RATE_LIMITED')
             AND (h.cooldown_until IS NULL OR h.cooldown_until<=now())
           ORDER BY due.id,route.source_rank,route.source
         ), source_lanes AS (
           SELECT source,min(next_run_at) AS oldest_due
           FROM routes GROUP BY source
           ORDER BY oldest_due,source LIMIT $2
         ), ranked AS (
           SELECT routes.id,routes.work_key,
             row_number() OVER (PARTITION BY routes.source ORDER BY routes.next_run_at,routes.created_at) AS source_position
           FROM routes JOIN source_lanes USING(source)
         ), picked AS (
           SELECT id,work_key FROM ranked
           WHERE source_position<=$3
         ), reopened AS (
           UPDATE importer_lite_tasks t
           SET state='READY',reason=NULL,updated_at=now()
           FROM picked WHERE t.id=picked.id
           RETURNING picked.work_key
         ), refreshed AS (
           SELECT public.importer_lite_refresh_ready_work(work_key)
           FROM (SELECT DISTINCT work_key FROM reopened) affected
         )
         SELECT (SELECT count(*)::int FROM reopened) AS count, count(*)::int AS refreshed
         FROM refreshed`, [SOURCE_COOLDOWN_RECOVERY_CANDIDATE_TASKS, SOURCE_COOLDOWN_RECOVERY_SOURCES_PER_TURN,
                SOURCE_COOLDOWN_RECOVERY_TASKS_PER_SOURCE]);
            // A Telegram request with an unknown outcome must never be replayed.
            // Likewise, an old intent left by a crashed process has no proof that
            // Telegram did not accept it.  Mark only the owning task BLOCKED;
            // this bounded, indexed pass never holds up unrelated work.
            const uploads = await client.query(`WITH picked AS (
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
         FROM picked WHERE t.id=picked.task_id RETURNING picked.work_key`, [limit]);
            // A completed media checkpoint never waits for a global sweep: it gains
            // one durable PUBLISH task. The unique key makes this idempotent.
            const ready = await client.query(`SELECT i.id,i.work_id,i.work_key,i.chapter_key,i.priority,i.base_priority FROM importer_lite_tasks i
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
                    JSON.stringify({ importTaskId: row.id }), JSON.stringify({})]);
            }
            // Before Lite accepted repeated page media IDs, a chapter with visually
            // identical pages could have every reference persisted but fail the old
            // unique-media integrity count.  Re-open that exact exhausted publish
            // once after verifying presentation and every *positional* media
            // reference. This is bounded and fenced in the checkpoint so another
            // genuine publish error never becomes a retry loop.
            const repairedPublishes = await client.query(`WITH picked AS (
          SELECT p.id,p.work_key
          FROM importer_lite_tasks p
          JOIN importer_lite_tasks i ON i.id=(p.payload->>'importTaskId')::uuid
          JOIN works w ON w.id=i.work_id
          LEFT JOIN media cover ON cover.id=w.cover_id
          WHERE p.task_type='PUBLISH' AND p.state='BLOCKED' AND p.reason='MAX_ATTEMPTS_EXHAUSTED'
            AND NOT p.checkpoint ? 'publishIntegrityRepairV1'
            AND i.task_type='IMPORT_CHAPTER' AND i.state='DONE'
            AND i.checkpoint ? 'mediaIds' AND NOT i.checkpoint ? 'publishedAt'
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
          FROM picked WHERE p.id=picked.id RETURNING picked.work_key`, [limit]);
            for (const key of new Set([...leases.rows, ...retries.rows, ...uploads.rows, ...ready.rows, ...repairedPublishes.rows].map((row) => row.work_key))) {
                await this.refreshReadyWork(client, key);
            }
            await client.query('COMMIT');
            const leaseRetries = leases.rows.filter((row) => row.state === 'RETRY_WAIT').length;
            const readyRetries = retries.rows.filter((row) => row.state === 'READY').length;
            const exhausted = [...leases.rows, ...retries.rows].filter((row) => row.state === 'BLOCKED').length;
            return { leases: leaseRetries, retries: readyRetries, exhausted, uploads: uploads.rowCount || 0,
                publishable: ready.rowCount || 0, publishRepairs: repairedPublishes.rowCount || 0,
                sourceCooldownRecoveries: Number(sourceCooldownRecovery.rows[0]?.count || 0) };
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
                if (!row?.media_id || row.existing === true) {
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
          SET published_at=COALESCE(published_at,clock_timestamp()),
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
        ), refreshed AS (
          SELECT public.importer_lite_refresh_ready_work(work_key) FROM done
        ) SELECT count(*)::int AS done,(SELECT count(*)::int FROM refreshed) AS refreshed FROM done`, [input.chapterId, input.fresh,
                typeof input.task.payload.source === 'string' ? input.task.payload.source : null,
                input.workId, LITE_PRIORITY_RANK[input.task.priority], importTaskId, input.task.id, input.workerId]);
            // The CTE retains the final lease fence. Throwing rolls back the page
            // replacement too, so a stale owner can never leave a visible chapter
            // with an unfinished durable PUBLISH task.
            if (Number(result.rows[0]?.done || 0) !== 1)
                throw new Error('LEASE_LOST_DURING_PUBLISH');
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
