import { Logger } from './logger.js';
export class ImporterQueue {
    supabase;
    workerId;
    logger = new Logger('Queue');
    constructor(supabase, workerId) {
        this.supabase = supabase;
        this.workerId = workerId;
    }
    /**
     * Enqueue a new task safely with deduplication key and optional deterministic sort key
     */
    async enqueue(taskType, source, dedupeKey, payload = {}, priority = 10, chapterSortKey) {
        const insertRow = {
            task_type: taskType,
            source,
            dedupe_key: dedupeKey,
            payload,
            priority,
            status: 'QUEUED',
        };
        if (chapterSortKey !== undefined && chapterSortKey !== null) {
            insertRow.chapter_sort_key = chapterSortKey;
        }
        const { error } = await this.supabase.from('importer_queue').insert(insertRow);
        if (error) {
            // Conflict on dedupe_key: check if job failed/cancelled and needs revival
            if (error.code === '23505') {
                try {
                    const { data: existing } = await this.supabase
                        .from('importer_queue')
                        .select('id, status, priority, source')
                        .eq('dedupe_key', dedupeKey)
                        .maybeSingle();
                    if (existing &&
                        (existing.status === 'FAILED' ||
                            existing.status === 'CANCELLED' ||
                            // A staff cancellation aborts the attempt but the engine restores
                            // its chapter mapping to QUEUED. Allow the next reconciliation to
                            // revive that dedupe row; otherwise the mapping remains queued
                            // forever with no executable queue job.
                            existing.status === 'CANCELLED_BY_STAFF' ||
                            (existing.status === 'RETRY' && existing.source !== source))) {
                        let revPrio = Math.max(existing.priority || 10, priority);
                        const wId = payload?.workId;
                        if (wId) {
                            try {
                                const { data: staffReq } = await this.supabase
                                    .from('importer_staff_requests')
                                    .select('id')
                                    .eq('work_id', wId)
                                    .eq('status', 'ACTIVE')
                                    .maybeSingle();
                                if (staffReq) {
                                    if (revPrio < 1000 && payload.originalPriority === undefined) {
                                        payload.originalPriority = revPrio;
                                    }
                                    revPrio = 1000;
                                    payload.staffForced = true;
                                }
                            }
                            catch { }
                        }
                        const updateData = {
                            source,
                            status: 'QUEUED',
                            attempts: 0,
                            last_error: null,
                            locked_by: null,
                            locked_at: null,
                            lease_expires_at: null,
                            priority: revPrio,
                            payload,
                            next_run_at: new Date().toISOString(),
                            updated_at: new Date().toISOString(),
                        };
                        if (chapterSortKey !== undefined && chapterSortKey !== null) {
                            updateData.chapter_sort_key = chapterSortKey;
                        }
                        const { error: revErr } = await this.supabase
                            .from('importer_queue')
                            .update(updateData)
                            .eq('id', existing.id);
                        if (!revErr) {
                            this.logger.info('Revived failed/cancelled job back to QUEUED', {
                                dedupeKey,
                                jobId: existing.id,
                                priority: updateData.priority,
                            });
                            return true;
                        }
                        else {
                            this.logger.warn('Failed to update revived job in queue', {
                                dedupeKey,
                                error: revErr.message,
                            });
                        }
                    }
                }
                catch (revErr) {
                    this.logger.warn('Failed to check/revive existing queue item on dedupe hit', {
                        dedupeKey,
                        error: revErr?.message,
                    });
                }
                this.logger.debug('Job already queued (dedupe hit)', { dedupeKey });
                return false;
            }
            this.logger.error('Failed to enqueue job', { error: error.message, dedupeKey });
            throw error;
        }
        this.logger.info('Enqueued job', { taskType, source, dedupeKey, priority, chapterSortKey });
        return true;
    }
    /**
     * Batch enqueue multiple tasks safely with deduplication
     */
    async enqueueBatch(jobs) {
        if (jobs.length === 0)
            return 0;
        const rows = jobs.map((j) => {
            const row = {
                task_type: j.taskType,
                source: j.source,
                dedupe_key: j.dedupeKey,
                payload: j.payload || {},
                priority: j.priority ?? 10,
                status: j.status || 'QUEUED',
            };
            if (j.chapterSortKey !== undefined && j.chapterSortKey !== null) {
                row.chapter_sort_key = j.chapterSortKey;
            }
            return row;
        });
        const CHUNK_SIZE = 50;
        let enqueuedCount = 0;
        for (let i = 0; i < rows.length; i += CHUNK_SIZE) {
            const chunk = rows.slice(i, i + CHUNK_SIZE);
            const { error } = await this.supabase
                .from('importer_queue')
                .upsert(chunk, { onConflict: 'dedupe_key', ignoreDuplicates: true });
            if (error) {
                this.logger.warn('Batch insert encountered error, falling back to individual inserts', {
                    error: error.message,
                });
                for (const job of chunk) {
                    try {
                        const ok = await this.enqueue(job.task_type, job.source, job.dedupe_key, job.payload, job.priority, job.chapter_sort_key);
                        if (ok)
                            enqueuedCount++;
                    }
                    catch {
                        // dedupe or transient ignore
                    }
                }
            }
            else {
                enqueuedCount += chunk.length;
            }
        }
        this.logger.info(`Batch enqueued ${enqueuedCount}/${jobs.length} jobs`);
        return enqueuedCount;
    }
    /**
     * Acquire the next job atomically using SKIP LOCKED stored procedure,
     * optionally filtered by source and/or task type for dedicated runner lanes.
     */
    async acquireNextJob(leaseDurationMinutes = 5, source, taskType) {
        const params = {
            p_worker_id: this.workerId,
            p_lease_duration: `${leaseDurationMinutes} minutes`,
        };
        if (Array.isArray(source)) {
            params.p_allowed_sources = source;
        }
        else if (source) {
            params.p_source = source;
        }
        if (taskType) {
            params.p_task_type = taskType;
        }
        const { data, error } = await this.supabase.rpc('importer_acquire_job', params);
        if (error) {
            this.logger.error('Error acquiring queue job', { error: error.message, source });
            throw error;
        }
        if (!data || data.length === 0) {
            return null;
        }
        const job = data[0];
        this.logger.info('Acquired job with atomic lease lock', {
            jobId: job.id,
            taskType: job.task_type,
            source: job.source,
            attempts: job.attempts,
            chapterSortKey: job.chapter_sort_key,
        });
        return job;
    }
    /**
     * Heartbeat renewal of an active lease
     */
    async renewLease(jobId, leaseDurationMinutes = 5) {
        const { data, error } = await this.supabase.rpc('importer_renew_lease', {
            p_job_id: jobId,
            p_worker_id: this.workerId,
            p_lease_duration: `${leaseDurationMinutes} minutes`,
        });
        if (error) {
            this.logger.warn('Failed to renew lease', { jobId, error: error.message });
            return false;
        }
        return data === true;
    }
    /**
     * Release an acquired job as completed, failed, or queued for retry with second-level precision
     */
    async releaseJob(jobId, status, lastError, retryDelaySeconds, retryClass) {
        const retryDelay = retryDelaySeconds !== undefined && retryDelaySeconds !== null
            ? `${Math.max(1, Math.round(retryDelaySeconds))} seconds`
            : null;
        const { data, error } = await this.supabase.rpc('importer_release_job', {
            p_job_id: jobId,
            p_worker_id: this.workerId,
            p_status: status,
            p_error: lastError ?? null,
            p_retry_delay: retryDelay,
            p_retry_delay_seconds: retryDelaySeconds,
            p_retry_class: retryClass,
        });
        if (error) {
            this.logger.error('Failed to release job', { jobId, status, error: error.message });
            return false;
        }
        this.logger.info('Released job', { jobId, status, retryClass, retryDelaySeconds, hasError: !!lastError });
        return data === true;
    }
    /**
     * Create a lease heartbeat handle that periodically renews the lease
     * until stopped. Uses .unref() to avoid blocking graceful shutdown.
     * Also aborts on lost lease ownership and polls for staff cancellation
     * requests (cancel_requested = true).
     */
    startHeartbeat(jobId, intervalSeconds = 60, onCancelRequested) {
        let stopped = false;
        let timer = null;
        const scheduleNext = () => {
            if (stopped)
                return;
            // Add jitter between 0 and 3000ms to avoid synchronized renew requests across workers
            const jitterMs = Math.floor(Math.random() * 3000);
            const delayMs = Math.max(1000, (intervalSeconds * 1000) + jitterMs);
            timer = setTimeout(async () => {
                if (stopped)
                    return;
                try {
                    const renewed = await this.renewLease(jobId);
                    if (!renewed && !stopped) {
                        this.logger.warn('Heartbeat lease renewal failed or lost ownership', { jobId });
                        onCancelRequested?.('LEASE_LOST');
                    }
                    if (!stopped && onCancelRequested) {
                        const isCancelled = await this.isCancelRequested(jobId);
                        if (isCancelled && !stopped) {
                            this.logger.warn('Staff requested cancellation detected during heartbeat', { jobId });
                            onCancelRequested('STAFF_REQUESTED');
                        }
                    }
                }
                catch (err) {
                    this.logger.error('Heartbeat interval error', { jobId, message: err?.message });
                }
                finally {
                    if (!stopped) {
                        scheduleNext();
                    }
                }
            }, delayMs);
            timer.unref();
        };
        scheduleNext();
        return {
            stop: () => {
                stopped = true;
                if (timer)
                    clearTimeout(timer);
            },
        };
    }
    /**
     * Checks if staff requested cancellation for this job in real-time
     */
    async isCancelRequested(jobId) {
        try {
            const { data, error } = await this.supabase
                .from('importer_queue')
                .select('cancel_requested, status')
                .eq('id', jobId)
                .maybeSingle();
            if (error || !data)
                return false;
            return Boolean(data.cancel_requested || data.status === 'CANCELLED_BY_STAFF');
        }
        catch {
            return false;
        }
    }
    /**
     * Generic crash-safe lease recovery for any stalled job across the entire system.
     * Scans for jobs stuck in 'IMPORTING' with expired lease (lease_expires_at < now()).
     * - REGRA: Erros técnicos / lease expirado NUNCA marcam jobs como 'FAILED'.
     * - Jobs são re-enfileirados em 'RETRY' ou 'QUEUED' com backoff progressivo proporcional às tentativas.
     * - Clears locked_by, locked_at, and lease_expires_at, while strictly preserving attempts.
     */
    async recoverExpiredLeases() {
        const nowIso = new Date().toISOString();
        // 1. First attempt atomic RPC if available in database
        try {
            const { data: rpcRes, error: rpcErr } = await this.supabase.rpc('importer_recover_stalled_leases');
            if (!rpcErr && rpcRes && rpcRes.length > 0) {
                const rec = Number(rpcRes[0].recovered_count ?? 0);
                const fld = Number(rpcRes[0].failed_count ?? 0);
                if (rec > 0 || fld > 0) {
                    this.logger.info(`Atomic lease recovery via RPC: ${rec} requeued to RETRY, ${fld} failed`, {
                        recovered: rec,
                        failed: fld,
                    });
                }
                return { recovered: rec, failed: fld };
            }
        }
        catch {
            // RPC may not exist yet; proceed with atomic query fallback below
        }
        // 2. Direct atomic query fallback
        try {
            const { data: stalled, error: fetchErr } = await this.supabase
                .from('importer_queue')
                .select('id, attempts, max_attempts, last_error')
                .eq('status', 'IMPORTING')
                .lt('lease_expires_at', nowIso);
            if (fetchErr) {
                this.logger.warn('Failed to query stalled jobs during lease recovery', { error: fetchErr.message });
                return { recovered: 0, failed: 0 };
            }
            if (!stalled || stalled.length === 0) {
                return { recovered: 0, failed: 0 };
            }
            let recoveredCount = 0;
            let failedCount = 0;
            for (const job of stalled) {
                const attempts = job.attempts || 1;
                const maxAttempts = job.max_attempts || 7;
                if (attempts >= maxAttempts) {
                    const { error: failErr } = await this.supabase
                        .from('importer_queue')
                        .update({
                        status: 'FAILED',
                        locked_by: null,
                        locked_at: null,
                        lease_expires_at: null,
                        next_run_at: nowIso,
                        last_error: `[LEASE_EXPIRED_EXHAUSTED] Lease expired and retry budget exhausted (${attempts}/${maxAttempts} attempts): ${job.last_error || 'Worker unresponsive'}`,
                        last_error_at: nowIso,
                        retry_reason: 'LEASE_EXPIRED',
                        updated_at: nowIso,
                    })
                        .eq('id', job.id)
                        .eq('status', 'IMPORTING');
                    if (!failErr) {
                        failedCount++;
                    }
                }
                else {
                    const { error: requeueErr } = await this.supabase
                        .from('importer_queue')
                        .update({
                        status: 'QUEUED',
                        locked_by: null,
                        locked_at: null,
                        lease_expires_at: null,
                        next_run_at: new Date(Date.now() + 10_000).toISOString(),
                        last_recovered_error: job.last_error || `Lease expirado (recuperado automaticamente na tentativa ${attempts})`,
                        recovered_at: nowIso,
                        retry_reason: 'LEASE_EXPIRED',
                        last_error_at: nowIso,
                        updated_at: nowIso,
                    })
                        .eq('id', job.id)
                        .eq('status', 'IMPORTING');
                    if (!requeueErr) {
                        recoveredCount++;
                    }
                    else {
                        this.logger.error('Failed to requeue expired job to QUEUED', { jobId: job.id, error: requeueErr.message });
                    }
                }
            }
            if (recoveredCount > 0 || failedCount > 0) {
                this.logger.info(`Lease recovery completed: ${recoveredCount} requeued to QUEUED, ${failedCount} permanently failed`, {
                    recovered: recoveredCount,
                    failed: failedCount,
                });
            }
            return { recovered: recoveredCount, failed: 0 };
        }
        catch (err) {
            this.logger.error('Unexpected error during generic lease recovery', { error: err?.message });
            return { recovered: 0, failed: 0 };
        }
    }
    /**
     * Reopens chapter jobs that were permanently failed by an older build while
     * the publication safety barrier was CLOSED/RECOVERING.  That condition is
     * transient: the current engine parks the job in QUEUED instead.  Only a
     * matching non-gap PENDING mapping is revived, so already completed/gap
     * mappings and unrelated failures remain untouched.  Source health is still
     * enforced by processJob, which parks blocked sources safely.
     */
    async recoverPublicationBarrierFailures(limit = 100) {
        try {
            const { data: failed, error } = await this.supabase
                .from('importer_queue')
                .select('id, source, priority, attempts, payload, chapter_sort_key, last_error')
                .eq('task_type', 'IMPORT_CHAPTER')
                .eq('status', 'FAILED')
                .ilike('last_error', '%PublicationSafetyBarrier%')
                .order('updated_at', { ascending: true })
                .limit(limit);
            if (error || !failed?.length)
                return 0;
            let recovered = 0;
            for (const job of failed) {
                const workId = job.payload?.workId;
                if (!workId || job.chapter_sort_key === null || job.chapter_sort_key === undefined)
                    continue;
                const { data: mapping, error: mappingError } = await this.supabase
                    .from('importer_chapter_mappings')
                    .select('id')
                    .eq('work_id', workId)
                    .eq('source', job.source)
                    .eq('chapter_sort_key', job.chapter_sort_key)
                    .eq('status', 'PENDING')
                    .eq('is_gap', false)
                    .maybeSingle();
                if (mappingError || !mapping)
                    continue;
                const { error: updateError } = await this.supabase
                    .from('importer_queue')
                    .update({
                    status: 'QUEUED',
                    attempts: 0,
                    last_error: null,
                    last_error_at: null,
                    retry_reason: 'PUBLICATION_BARRIER_RECOVERY',
                    locked_by: null,
                    locked_at: null,
                    lease_expires_at: null,
                    next_run_at: new Date().toISOString(),
                    updated_at: new Date().toISOString(),
                })
                    .eq('id', job.id)
                    .eq('status', 'FAILED');
                if (!updateError)
                    recovered++;
            }
            if (recovered > 0) {
                this.logger.info('Reopened chapter jobs failed only by the transient publication barrier', { recovered });
            }
            return recovered;
        }
        catch (err) {
            this.logger.warn('Publication barrier failure recovery skipped', { error: err?.message });
            return 0;
        }
    }
    /** Reopen old reservation-limit failures when their canonical mapping is still pending. */
    async recoverReservationLimitFailures(limit = 100) {
        try {
            // Direct YSQL is the production path. Keep recovery bounded and avoid a
            // planner-wide queue/mapping join: the queue has thousands of terminal
            // failures, while only a small subset are reservation races. Select a
            // bounded failure window, resolve its mappings with a parameterized VALUES
            // list, then update only still-FAILED rows. Every write remains guarded by
            // the pending mapping check, so concurrent recovery callers are idempotent.
            const directSql = this.supabase?.sql;
            if (typeof directSql === 'function') {
                // Start from the status/updated_at index, then inspect the bounded
                // recent failure window for the legacy reservation signature.  A
                // direct `last_error LIKE` predicate over every historical FAILED row
                // repeatedly timed out on YSQL and held a pool client that claims
                // need. Reservation failures are transient and are created by the
                // current runtime, so the recent window is the only population that
                // needs prompt recovery; historical rows remain intact and never get
                // rewritten by this maintenance path.
                const scanLimit = Math.max(limit * 4, 100);
                const failedResult = await directSql.call(this.supabase, `
          WITH recent_failed AS MATERIALIZED (
            SELECT id, source, payload, chapter_sort_key, last_error, task_type
            FROM importer_queue
            WHERE status = 'FAILED'
              AND updated_at >= NOW() - INTERVAL '7 days'
            ORDER BY updated_at DESC, id DESC
            LIMIT $1
          )
          SELECT id, source, payload, chapter_sort_key, last_error
          FROM recent_failed
          WHERE task_type = 'IMPORT_CHAPTER'
            AND last_error LIKE 'Concurrent reservation limit:%'
          LIMIT $1
        `, [scanLimit]);
                const failed = (failedResult?.rows || []).filter((job) => typeof job?.payload?.workId === 'string'
                    && job.chapter_sort_key !== null
                    && job.chapter_sort_key !== undefined);
                if (!failed.length)
                    return 0;
                const values = [];
                const mappingParams = [];
                for (const [index, job] of failed.entries()) {
                    const offset = index * 4;
                    values.push(`($${offset + 1}::uuid, $${offset + 2}::text, $${offset + 3}::text, $${offset + 4}::numeric)`);
                    mappingParams.push(job.id, job.payload.workId, job.source, job.chapter_sort_key);
                }
                const mappingResult = await directSql.call(this.supabase, `
          SELECT requested.queue_id
          FROM importer_chapter_mappings m
          JOIN (VALUES ${values.join(', ')}) AS requested(queue_id, work_id, source, chapter_sort_key)
            ON requested.work_id = m.work_id::text
           AND requested.source = m.source
           AND requested.chapter_sort_key::numeric = m.chapter_sort_key
          WHERE m.status = 'PENDING'
            AND m.is_gap IS FALSE
        `, mappingParams);
                const eligible = new Set((mappingResult?.rows || []).map((row) => String(row.queue_id)));
                const ids = failed.slice(0, scanLimit)
                    .filter((job) => eligible.has(String(job.id)))
                    .slice(0, limit)
                    .map((job) => job.id);
                if (!ids.length)
                    return 0;
                const revived = await directSql.call(this.supabase, `
          UPDATE importer_queue q
          SET status = 'QUEUED',
              attempts = 0,
              last_error = NULL,
              last_error_at = NULL,
              retry_reason = 'RESERVATION_LIMIT_RECOVERY',
              locked_by = NULL,
              locked_at = NULL,
              lease_expires_at = NULL,
              next_run_at = NOW(),
              updated_at = NOW()
          WHERE q.id = ANY($1::uuid[])
            AND q.status = 'FAILED'
            AND q.last_error LIKE 'Concurrent reservation limit:%'
            AND EXISTS (
              SELECT 1
              FROM importer_chapter_mappings m
              WHERE m.work_id::text = q.payload->>'workId'
                AND m.source = q.source
                AND m.chapter_sort_key = q.chapter_sort_key
                AND m.status = 'PENDING'
                AND m.is_gap IS FALSE
            )
          RETURNING q.id
        `, [ids]);
                const recovered = Number(revived?.rowCount ?? revived?.rows?.length ?? 0);
                if (recovered > 0)
                    this.logger.info('Reopened reservation-limit chapter failures', { recovered });
                return recovered;
            }
            const { data: failed, error } = await this.supabase
                .from('importer_queue')
                .select('id, source, payload, chapter_sort_key')
                .eq('task_type', 'IMPORT_CHAPTER')
                .eq('status', 'FAILED')
                .ilike('last_error', 'Concurrent reservation limit:%')
                .order('updated_at', { ascending: true })
                .limit(limit);
            if (error || !failed?.length)
                return 0;
            let recovered = 0;
            for (const job of failed) {
                const workId = job.payload?.workId;
                if (!workId || job.chapter_sort_key === null || job.chapter_sort_key === undefined)
                    continue;
                const { data: mappings, error: mappingError } = await this.supabase
                    .from('importer_chapter_mappings')
                    .select('id')
                    .eq('work_id', workId)
                    .eq('source', job.source)
                    .eq('chapter_sort_key', job.chapter_sort_key)
                    .eq('status', 'PENDING')
                    .eq('is_gap', false)
                    .limit(1);
                if (mappingError || !mappings?.length)
                    continue;
                const { error: updateError } = await this.supabase
                    .from('importer_queue')
                    .update({
                    status: 'QUEUED', attempts: 0, last_error: null, last_error_at: null,
                    retry_reason: 'RESERVATION_LIMIT_RECOVERY', locked_by: null,
                    locked_at: null, lease_expires_at: null, next_run_at: new Date().toISOString(),
                    updated_at: new Date().toISOString(),
                })
                    .eq('id', job.id)
                    .eq('status', 'FAILED');
                if (!updateError)
                    recovered++;
            }
            if (recovered > 0)
                this.logger.info('Reopened reservation-limit chapter failures', { recovered });
            return recovered;
        }
        catch (err) {
            this.logger.warn('Reservation-limit failure recovery skipped', { error: err?.message });
            return 0;
        }
    }
}
