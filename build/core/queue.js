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
            // Conflict on dedupe_key is normal and ignored
            if (error.code === '23505') {
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
     * Acquire the next job atomically using SKIP LOCKED stored procedure,
     * optionally filtered by source for concurrent source runners.
     */
    async acquireNextJob(leaseDurationMinutes = 5, source) {
        const params = {
            p_worker_id: this.workerId,
            p_lease_duration: `${leaseDurationMinutes} minutes`,
        };
        if (source) {
            params.p_source = source;
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
            p_retry_delay_minutes: retryDelaySeconds ? Math.ceil(retryDelaySeconds / 60) : null,
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
     */
    startHeartbeat(jobId, intervalSeconds = 60) {
        let stopped = false;
        const timer = setInterval(async () => {
            if (stopped)
                return;
            try {
                const renewed = await this.renewLease(jobId);
                if (!renewed && !stopped) {
                    this.logger.warn('Heartbeat lease renewal failed or lost ownership', { jobId });
                }
            }
            catch (err) {
                this.logger.error('Heartbeat interval error', { jobId, message: err?.message });
            }
        }, intervalSeconds * 1000);
        timer.unref();
        return {
            stop: () => {
                stopped = true;
                clearInterval(timer);
            },
        };
    }
    /**
     * Generic crash-safe lease recovery for any stalled job across the entire system.
     * Scans for jobs stuck in 'IMPORTING' with expired lease (lease_expires_at < now()).
     * - Jobs reaching or exceeding max_attempts are marked as 'FAILED'.
     * - Jobs with remaining attempts are atomically reset back to 'QUEUED' with next_run_at = now().
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
                    this.logger.info(`Atomic lease recovery via RPC: ${rec} requeued to QUEUED, ${fld} marked as FAILED`, {
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
                .select('id, attempts, max_attempts')
                .eq('status', 'IMPORTING')
                .lt('lease_expires_at', nowIso);
            if (fetchErr) {
                this.logger.warn('Failed to query stalled jobs during lease recovery', { error: fetchErr.message });
                return { recovered: 0, failed: 0 };
            }
            if (!stalled || stalled.length === 0) {
                return { recovered: 0, failed: 0 };
            }
            const toFailIds = [];
            const toRequeueIds = [];
            for (const job of stalled) {
                if (job.attempts >= job.max_attempts) {
                    toFailIds.push(job.id);
                }
                else {
                    toRequeueIds.push(job.id);
                }
            }
            let failedCount = 0;
            let recoveredCount = 0;
            if (toFailIds.length > 0) {
                const { error: failErr } = await this.supabase
                    .from('importer_queue')
                    .update({
                    status: 'FAILED',
                    locked_by: null,
                    locked_at: null,
                    lease_expires_at: null,
                    last_error: 'Lease expired after max attempts',
                    updated_at: nowIso,
                })
                    .in('id', toFailIds)
                    .eq('status', 'IMPORTING');
                if (!failErr) {
                    failedCount = toFailIds.length;
                }
                else {
                    this.logger.error('Failed to mark expired jobs as FAILED', { error: failErr.message });
                }
            }
            if (toRequeueIds.length > 0) {
                const { error: requeueErr } = await this.supabase
                    .from('importer_queue')
                    .update({
                    status: 'QUEUED',
                    locked_by: null,
                    locked_at: null,
                    lease_expires_at: null,
                    next_run_at: nowIso,
                    updated_at: nowIso,
                })
                    .in('id', toRequeueIds)
                    .eq('status', 'IMPORTING');
                if (!requeueErr) {
                    recoveredCount = toRequeueIds.length;
                }
                else {
                    this.logger.error('Failed to requeue expired jobs to QUEUED', { error: requeueErr.message });
                }
            }
            if (recoveredCount > 0 || failedCount > 0) {
                this.logger.info(`Lease recovery completed: ${recoveredCount} requeued to QUEUED, ${failedCount} marked as FAILED`, {
                    recovered: recoveredCount,
                    failed: failedCount,
                });
            }
            return { recovered: recoveredCount, failed: failedCount };
        }
        catch (err) {
            this.logger.error('Unexpected error during generic lease recovery', { error: err?.message });
            return { recovered: 0, failed: 0 };
        }
    }
}
