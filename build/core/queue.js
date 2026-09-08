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
     * Enqueue a new task safely with deduplication key
     */
    async enqueue(taskType, source, dedupeKey, payload = {}, priority = 10) {
        const { error } = await this.supabase.from('importer_queue').insert({
            task_type: taskType,
            source,
            dedupe_key: dedupeKey,
            payload,
            priority,
            status: 'QUEUED',
        });
        if (error) {
            // Conflict on dedupe_key is normal and ignored
            if (error.code === '23505') {
                this.logger.debug('Job already queued (dedupe hit)', { dedupeKey });
                return false;
            }
            this.logger.error('Failed to enqueue job', { error: error.message, dedupeKey });
            throw error;
        }
        this.logger.info('Enqueued job', { taskType, source, dedupeKey, priority });
        return true;
    }
    /**
     * Acquire the next job atomically using SKIP LOCKED stored procedure
     */
    async acquireNextJob(leaseDurationMinutes = 5) {
        const { data, error } = await this.supabase.rpc('importer_acquire_job', {
            p_worker_id: this.workerId,
            p_lease_duration: `${leaseDurationMinutes} minutes`,
        });
        if (error) {
            this.logger.error('Error acquiring queue job', { error: error.message });
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
     * Release an acquired job as completed, failed, or queued for retry
     */
    async releaseJob(jobId, status, lastError, retryDelayMinutes) {
        const retryDelay = retryDelayMinutes ? `${retryDelayMinutes} minutes` : null;
        const { data, error } = await this.supabase.rpc('importer_release_job', {
            p_job_id: jobId,
            p_worker_id: this.workerId,
            p_status: status,
            p_error: lastError ?? null,
            p_retry_delay: retryDelay,
        });
        if (error) {
            this.logger.error('Failed to release job', { jobId, status, error: error.message });
            return false;
        }
        this.logger.info('Released job', { jobId, status, hasError: !!lastError });
        return data === true;
    }
    /**
     * Create a lease heartbeat handle that periodically renews the lease
     * until stopped.
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
        return {
            stop: () => {
                stopped = true;
                clearInterval(timer);
            },
        };
    }
}
