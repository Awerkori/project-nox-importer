import { randomUUID } from 'node:crypto';
import { LITE_PRIORITY_RANK } from './types.js';
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
        if (inputs.length === 0)
            return { inserted: 0, insertedKeys: new Set() };
        const client = await this.db.connect();
        try {
            await client.query('BEGIN');
            const workIds = [...new Set(inputs.map((input) => input.workId).filter((id) => Boolean(id)))];
            const staff = workIds.length ? await client.query(`SELECT DISTINCT work_id::text AS work_id
        FROM importer_staff_requests WHERE work_id=ANY($1::uuid[])
          AND status IN ('QUEUED','IMPORTING','RETRYING','ACTIVE')`, [workIds]) : { rows: [] };
            const staffWorkIds = new Set(staff.rows.map((row) => String(row.work_id)));
            let inserted = 0;
            const insertedKeys = new Set();
            // Keep far below PostgreSQL's parameter limit even if a source returns
            // an unusually large manifest.
            for (let offset = 0; offset < inputs.length; offset += 500) {
                const batch = inputs.slice(offset, offset + 500);
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
            for (const workKey of new Set(inputs.map((input) => input.workKey)))
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
        const administrativelyActive = row.enabled === true && ['ACTIVE', 'DEGRADED', 'RECOVERING'].includes(status);
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
        await this.db.query(`INSERT INTO importer_lite_source_health(source,health,cooldown_until,consecutive_failures,last_error)
      VALUES($1,$2,now()+($3 || ' seconds')::interval,1,$4)
      ON CONFLICT(source) DO UPDATE SET health=excluded.health,cooldown_until=excluded.cooldown_until,
        consecutive_failures=importer_lite_source_health.consecutive_failures+1,last_error=excluded.last_error,updated_at=now()`, [source, health, String(cooldownSeconds), String(detail?.message || 'SOURCE_FAILURE').slice(0, 500)]).catch(() => { });
    }
    async recordSourceSuccess(source) {
        await this.db.query(`INSERT INTO importer_lite_source_health(source,health,cooldown_until,consecutive_failures,last_error)
      VALUES($1,'HEALTHY',NULL,0,NULL)
      ON CONFLICT(source) DO UPDATE SET health='HEALTHY',cooldown_until=NULL,
        consecutive_failures=0,last_error=NULL,updated_at=now()`, [source]).catch(() => { });
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
           SELECT id,work_key,attempts,max_attempts,task_type,monitoring FROM importer_lite_tasks WHERE state='RETRY_WAIT' AND next_run_at<=now()
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
            const ready = await client.query(`SELECT id,work_id,work_key,chapter_key,priority,base_priority FROM importer_lite_tasks
          WHERE state='DONE' AND task_type='IMPORT_CHAPTER'
            AND checkpoint ? 'mediaIds' AND NOT checkpoint ? 'publishedAt'
          ORDER BY updated_at LIMIT $1`, [limit]);
            for (const row of ready.rows) {
                await client.query(`INSERT INTO importer_lite_tasks(task_type,work_id,work_key,chapter_key,priority,base_priority,idempotency_key,payload,checkpoint)
           VALUES ('PUBLISH',$1::uuid,$2,$3,$4,$5,'lite:publish:'||$1::text||':'||$3,$6::jsonb,$7::jsonb)
           ON CONFLICT(idempotency_key) DO NOTHING`, [row.work_id, row.work_key, row.chapter_key, row.priority, row.base_priority,
                    JSON.stringify({ importTaskId: row.id }), JSON.stringify({})]);
            }
            for (const key of new Set([...leases.rows, ...retries.rows, ...uploads.rows, ...ready.rows].map((row) => row.work_key))) {
                await this.refreshReadyWork(client, key);
            }
            await client.query('COMMIT');
            const leaseRetries = leases.rows.filter((row) => row.state === 'RETRY_WAIT').length;
            const readyRetries = retries.rows.filter((row) => row.state === 'READY').length;
            const exhausted = [...leases.rows, ...retries.rows].filter((row) => row.state === 'BLOCKED').length;
            return { leases: leaseRetries, retries: readyRetries, exhausted, uploads: uploads.rowCount || 0, publishable: ready.rowCount || 0 };
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
    /** Stable ID is reserved before the non-transactional Telegram call. */
    async reserveUpload(taskId, pagePosition, sha256) {
        const existing = await this.db.query(`SELECT media_id,state FROM importer_lite_uploads
      WHERE task_id=$1::uuid AND page_position=$2`, [taskId, pagePosition]);
        if (existing.rows[0]) {
            // An INTENT surviving a crash has no durable confirmation that the
            // request did not reach Telegram. Convert it to UNKNOWN rather than
            // initiating an indistinguishable duplicate upload.
            if (existing.rows[0].state === 'INTENT') {
                await this.db.query(`UPDATE importer_lite_uploads SET state='UNKNOWN',updated_at=now()
          WHERE task_id=$1::uuid AND page_position=$2 AND state='INTENT'`, [taskId, pagePosition]);
                return { mediaId: existing.rows[0].media_id, state: 'UNKNOWN' };
            }
            return { mediaId: existing.rows[0].media_id, state: existing.rows[0].state };
        }
        const reusable = await this.db.query(`SELECT id FROM media WHERE sha256=$1 AND storage_ready=true
      AND bytes>0 AND mime LIKE 'image/%' ORDER BY created_at LIMIT 1`, [sha256]);
        if (reusable.rows[0]?.id) {
            const reused = await this.db.query(`INSERT INTO importer_lite_uploads(task_id,page_position,media_id,sha256,state)
         VALUES($1::uuid,$2,$3::uuid,$4,'PERSISTED')
         ON CONFLICT(task_id,page_position) DO UPDATE SET updated_at=now()
         RETURNING media_id,state`, [taskId, pagePosition, reusable.rows[0].id, sha256]);
            return { mediaId: reused.rows[0].media_id, state: reused.rows[0].state };
        }
        const id = randomUUID();
        const result = await this.db.query(`INSERT INTO importer_lite_uploads(task_id,page_position,media_id,sha256)
       VALUES($1::uuid,$2,$3::uuid,$4)
       ON CONFLICT(task_id,page_position) DO NOTHING
       RETURNING media_id,state`, [taskId, pagePosition, id, sha256]);
        if (result.rows[0])
            return { mediaId: result.rows[0].media_id, state: result.rows[0].state };
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
        const client = await this.db.connect();
        try {
            await client.query('BEGIN');
            await client.query(`INSERT INTO media(id,provider,provider_key,bot_reference,storage_shard_id,mime,width,height,bytes,sha256,created_by,storage_ready,purpose)
         SELECT $1::uuid,'telegram',$2,$3,$4,$5,$6,$7,$8,$9,$10::uuid,true,'chapter'
         WHERE NOT EXISTS (SELECT 1 FROM media WHERE id=$1::uuid)`, [input.mediaId, input.providerKey, input.botReference, input.shardId, input.mime, input.width, input.height, input.bytes, input.sha256, input.createdBy]);
            await client.query(`UPDATE importer_lite_uploads SET state='PERSISTED',provider_key=$4,bot_reference=$5,storage_shard_id=$6,
           channel_id=$7,telegram_message_id=$8,bytes=$9,mime=$10,updated_at=now()
         WHERE task_id=$1::uuid AND page_position=$2 AND media_id=$3::uuid`, [input.taskId, input.pagePosition, input.mediaId, input.providerKey, input.botReference, input.shardId, input.channelId, input.messageId, input.bytes, input.mime]);
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
            const lease = await client.query(`SELECT 1 FROM importer_lite_tasks
        WHERE id=$1::uuid AND state='RUNNING' AND lease_owner=$2 AND lease_until>now() FOR UPDATE`, [input.task.id, input.workerId]);
            if (lease.rowCount !== 1)
                throw new Error('LEASE_LOST_BEFORE_PUBLISH');
            const presentation = await client.query(`SELECT w.cover_id,w.title,w.slug,m.storage_ready,m.bytes
        FROM works w LEFT JOIN media m ON m.id=w.cover_id WHERE w.id=$1::uuid FOR UPDATE OF w`, [input.workId]);
            const work = presentation.rows[0];
            if (!work?.title || !work?.slug || !work?.cover_id || work.storage_ready !== true || Number(work.bytes) < 1500) {
                throw new Error('WORK_PRESENTATION_INCOMPLETE');
            }
            const integrity = await client.query(`SELECT count(*) FROM media WHERE id=ANY($1::uuid[]) AND storage_ready=true AND bytes>0 AND mime LIKE 'image/%'`, [input.mediaIds]);
            if (Number(integrity.rows[0]?.count) !== input.mediaIds.length || input.mediaIds.length === 0) {
                throw new Error('PUBLISH_MEDIA_INCOMPLETE');
            }
            const pageRows = input.mediaIds.map((mediaId, index) => ({ media_id: mediaId, position: index + 1 }));
            await client.query('SELECT public.importer_replace_pages($1::uuid,$2::jsonb)', [input.chapterId, JSON.stringify(pageRows)]);
            const visible = await client.query(`UPDATE chapters SET published_at=clock_timestamp(),is_fresh_release=$2
          WHERE id=$1::uuid AND published_at IS NULL RETURNING published_at`, [input.chapterId, input.fresh]);
            // A retry after a commit can see an already-visible chapter. Preserve
            // that real timestamp and settle the originating import task too, so
            // the reconciler never keeps re-materialising the same publish task.
            let publishedAt = visible.rows[0]?.published_at || null;
            if (!publishedAt) {
                const existing = await client.query('SELECT published_at FROM chapters WHERE id=$1::uuid', [input.chapterId]);
                publishedAt = existing.rows[0]?.published_at || null;
            }
            if (publishedAt) {
                await client.query(`INSERT INTO importer_publication_events(chapter_id,transition_at,recorded_at,bucket_minute,is_fresh_release)
          VALUES($1::uuid,$2::timestamptz,clock_timestamp(),date_trunc('minute',$2::timestamptz),$3) ON CONFLICT(chapter_id) DO NOTHING`, [input.chapterId, publishedAt, input.fresh]);
                await client.query(`UPDATE works SET published=true,latest_chapter_published_at=GREATEST(COALESCE(latest_chapter_published_at,$2),$2),updated_at=now() WHERE id=$1::uuid`, [input.workId, publishedAt]);
                const importTaskId = typeof input.task.payload.importTaskId === 'string' ? input.task.payload.importTaskId : null;
                if (importTaskId) {
                    await client.query(`UPDATE importer_lite_tasks
            SET checkpoint=checkpoint || jsonb_build_object('publishedAt',$2::text),updated_at=now()
            WHERE id=$1::uuid AND task_type='IMPORT_CHAPTER'`, [importTaskId, publishedAt]);
                }
            }
            const done = await client.query(`UPDATE importer_lite_tasks SET state='DONE',lease_owner=NULL,lease_until=NULL,
        checkpoint=checkpoint || jsonb_build_object('publishedAt',COALESCE($3::text,checkpoint->>'publishedAt')),updated_at=now()
        WHERE id=$1::uuid AND state='RUNNING' AND lease_owner=$2 AND lease_until>now()`, [input.task.id, input.workerId, publishedAt]);
            if (done.rowCount === 1)
                await this.refreshReadyWork(client, input.task.workKey);
            await client.query('COMMIT');
            return done.rowCount === 1;
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
