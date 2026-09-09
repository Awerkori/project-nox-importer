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
  last_recovered_error?: string | null;
  recovered_at?: string | null;
  last_error_at?: string | null;
  retry_reason?: string | null;
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
      // Conflict on dedupe_key: check if job failed/cancelled and needs revival
      if (error.code === '23505') {
        try {
          const { data: existing } = await this.supabase
            .from('importer_queue')
            .select('id, status, priority, source')
            .eq('dedupe_key', dedupeKey)
            .maybeSingle();

          if (
            existing &&
            (existing.status === 'FAILED' ||
              existing.status === 'CANCELLED' ||
              (existing.status === 'RETRY' && existing.source !== source))
          ) {
            const updateData: Record<string, any> = {
              source,
              status: 'QUEUED',
              attempts: 0,
              last_error: null,
              locked_by: null,
              locked_at: null,
              lease_expires_at: null,
              priority: Math.max(existing.priority || 10, priority),
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
            } else {
              this.logger.warn('Failed to update revived job in queue', {
                dedupeKey,
                error: revErr.message,
              });
            }
          }
        } catch (revErr: any) {
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
  async enqueueBatch(
    jobs: Array<{
      taskType: TaskType;
      source: string;
      dedupeKey: string;
      payload?: Record<string, any>;
      priority?: number;
      chapterSortKey?: number | null;
    }>
  ): Promise<number> {
    if (jobs.length === 0) return 0;

    const rows = jobs.map((j) => {
      const row: Record<string, any> = {
        task_type: j.taskType,
        source: j.source,
        dedupe_key: j.dedupeKey,
        payload: j.payload || {},
        priority: j.priority ?? 10,
        status: 'QUEUED',
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
            const ok = await this.enqueue(
              job.task_type,
              job.source,
              job.dedupe_key,
              job.payload,
              job.priority,
              job.chapter_sort_key
            );
            if (ok) enqueuedCount++;
          } catch {
            // dedupe or transient ignore
          }
        }
      } else {
        enqueuedCount += chunk.length;
      }
    }

    this.logger.info(`Batch enqueued ${enqueuedCount}/${jobs.length} jobs`);
    return enqueuedCount;
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
   * Release an acquired job as completed, failed, or queued for retry with second-level precision
   */
  async releaseJob(
    jobId: string,
    status: JobStatus,
    lastError?: string,
    retryDelaySeconds?: number,
    retryClass?: string
  ): Promise<boolean> {
    const retryDelay = retryDelaySeconds !== undefined && retryDelaySeconds !== null
      ? `${Math.max(1, Math.round(retryDelaySeconds))} seconds`
      : null;

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
    this.logger.info('Released job', { jobId, status, retryClass, retryDelaySeconds, hasError: !!lastError });
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

  /**
   * Generic crash-safe lease recovery for any stalled job across the entire system.
   * Scans for jobs stuck in 'IMPORTING' with expired lease (lease_expires_at < now()).
   * - REGRA: Erros técnicos / lease expirado NUNCA marcam jobs como 'FAILED'.
   * - Jobs são re-enfileirados em 'RETRY' ou 'QUEUED' com backoff progressivo proporcional às tentativas.
   * - Clears locked_by, locked_at, and lease_expires_at, while strictly preserving attempts.
   */
  async recoverExpiredLeases(): Promise<{ recovered: number; failed: number }> {
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
    } catch {
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

      let recoveredCount = 0;

      for (const job of stalled) {
        const attempts = job.attempts || 1;

        const { error: requeueErr } = await this.supabase
          .from('importer_queue')
          .update({
            status: 'QUEUED',
            locked_by: null,
            locked_at: null,
            lease_expires_at: null,
            next_run_at: nowIso,
            last_error: `Lease expirado (tentativa ${attempts})`,
            updated_at: nowIso,
          })
          .eq('id', job.id)
          .eq('status', 'IMPORTING');

        if (!requeueErr) {
          recoveredCount++;
        } else {
          this.logger.error('Failed to requeue expired job to QUEUED', { jobId: job.id, error: requeueErr.message });
        }
      }

      if (recoveredCount > 0) {
        this.logger.info(`Lease recovery completed: ${recoveredCount} requeued to RETRY with backoff (0 permanently failed)`, {
          recovered: recoveredCount,
          failed: 0,
        });
      }

      return { recovered: recoveredCount, failed: 0 };
    } catch (err: any) {
      this.logger.error('Unexpected error during generic lease recovery', { error: err?.message });
      return { recovered: 0, failed: 0 };
    }
  }
}
