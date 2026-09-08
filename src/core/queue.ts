import type { SupabaseClient } from '@supabase/supabase-js';
import { Logger } from './logger.js';

export type TaskType = 'DISCOVER_WORKS' | 'SYNC_WORK' | 'IMPORT_CHAPTER';
export type JobStatus = 'QUEUED' | 'IMPORTING' | 'COMPLETED' | 'FAILED' | 'RETRY';

export interface QueueJob {
  id: string;
  task_type: TaskType;
  source: string;
  priority: number;
  payload: Record<string, any>;
  dedupe_key: string;
  status: JobStatus;
  attempts: number;
  max_attempts: number;
  locked_by: string | null;
  locked_at: string | null;
  lease_expires_at: string | null;
  next_run_at: string;
  last_error: string | null;
  chapter_sort_key?: number | null;
}

export class ImporterQueue {
  private logger = new Logger('Queue');

  constructor(private supabase: SupabaseClient, private workerId: string) {}

  /**
   * Enqueue a new task safely with deduplication key and optional deterministic sort key
   */
  async enqueue(
    taskType: TaskType,
    source: string,
    dedupeKey: string,
    payload: Record<string, any> = {},
    priority: number = 10,
    chapterSortKey?: number | null
  ): Promise<boolean> {
    const insertRow: Record<string, any> = {
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
  async acquireNextJob(leaseDurationMinutes: number = 5, source?: string): Promise<QueueJob | null> {
    const params: Record<string, any> = {
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

    const job = data[0] as QueueJob;
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
  async renewLease(jobId: string, leaseDurationMinutes: number = 5): Promise<boolean> {
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
  async releaseJob(
    jobId: string,
    status: JobStatus,
    lastError?: string,
    retryDelayMinutes?: number
  ): Promise<boolean> {
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
   * until stopped. Uses .unref() to avoid blocking graceful shutdown.
   */
  startHeartbeat(jobId: string, intervalSeconds: number = 60): { stop: () => void } {
    let stopped = false;
    const timer = setInterval(async () => {
      if (stopped) return;
      try {
        const renewed = await this.renewLease(jobId);
        if (!renewed && !stopped) {
          this.logger.warn('Heartbeat lease renewal failed or lost ownership', { jobId });
        }
      } catch (err: any) {
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
}
