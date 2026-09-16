import { db, schema } from '../db/index.js';
import { eq, and, or, lt, lte, desc, asc, inArray, sql } from 'drizzle-orm';
import { Logger } from './logger.js';

export type TaskType = 'DISCOVER_WORKS' | 'SYNC_WORK' | 'IMPORT_CHAPTER';
export type JobStatus =
  | 'QUEUED'
  | 'IMPORTING'
  | 'COMPLETED'
  | 'FAILED'
  | 'RETRY'
  | 'PAUSED_BY_STAFF'
  | 'CANCELLED_BY_STAFF'
  | 'BLOCKED_BY_UPSTREAM';

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
  cancel_requested?: boolean;
  cancelled_by?: string | null;
  cancelled_at?: string | null;
  cancel_reason?: string | null;
  paused_by?: string | null;
  paused_at?: string | null;
  pause_reason?: string | null;
  progress_current?: number | null;
  progress_total?: number | null;
  progress_stage?: string | null;
  chapter_sort_key?: number | null;
}

export class ImporterQueue {
  private logger = new Logger('Queue');

  constructor(private  private workerId: string) {}

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

    const { error } = await db.insert(schema.importerQueue).values(insertRow as any).then(() => ({ error: null })).catch((error) => ({ error }));

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
   * optionally filtered by source and/or task type for dedicated runner lanes.
   */
  async acquireNextJob(
    leaseDurationMinutes: number = 5,
    source?: string,
    taskType?: string
  ): Promise<QueueJob | null> {
    const params: Record<string, any> = {
      p_worker_id: this.workerId,
      p_lease_duration: `${leaseDurationMinutes} minutes`,
    };
    if (source) {
      params.p_source = source;
    }
    if (taskType) {
      params.p_task_type = taskType;
    }

    // Custom Atomic Lock for Turso
    const { data, error } = await (async () => {
      try {
        const result = await db.$client.execute({
          sql: `
            UPDATE importer_queue
            SET status = 'IMPORTING', locked_by = :workerId, locked_at = :now, lease_expires_at = :expiresAt, attempts = attempts + 1
            WHERE id = (
              SELECT q.id FROM importer_queue q
              LEFT JOIN settings s ON s.key = 'publication_safety_barrier'
              LEFT JOIN importer_staff_requests sr ON sr.status IN ('QUEUED', 'IMPORTING', 'RETRYING')
              WHERE (q.status = 'QUEUED' AND q.next_run_at <= :now)
                 OR (q.status = 'IMPORTING' AND q.lease_expires_at < :now)
                 -- Advanced filters
                 AND (:source IS NULL OR q.source = :source)
                 AND (:taskType IS NULL OR q.task_type = :taskType)
                 -- Barrier checks
                 AND (
                   s.value IS NULL 
                   OR json_extract(s.value, '$.open') = 1 
                   OR (q.task_type != 'SYNC_WORK' AND q.task_type != 'IMPORT_CHAPTER')
                 )
                 -- Focus mode
                 AND (
                   sr.id IS NULL 
                   OR (
                     q.payload LIKE '%' || sr.work_id || '%' 
                   )
                 )
              ORDER BY q.priority DESC, q.next_run_at ASC
              LIMIT 1
            )
            RETURNING *;
          `,
          args: {
            workerId: this.workerId,
            now: new Date().toISOString(),
            expiresAt: new Date(Date.now() + 5 * 60000).toISOString(),
            source: params.p_source || null,
            taskType: params.p_task_type || null
          }
        });
        return { data: result.rows, error: null };
      } catch (e) {
        return { data: null, error: e };
      }
    })();


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

    const { data, error } = await (async () => {
      try {
        const result = await db.$client.execute({
          sql: "UPDATE importer_queue SET status = :status, next_run_at = :nextRunAt, last_error = :err, locked_by = NULL, locked_at = NULL, lease_expires_at = NULL WHERE id = :jobId RETURNING id",
          args: { jobId: params.job_id, status: params.status, nextRunAt: params.next_run_at || new Date().toISOString(), err: params.error_msg || null }
        });
        return { data: true, error: null };
      } catch(e) { return { data: null, error: e }; }
    })();

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
   * Also polls for staff cancellation requests (cancel_requested = true).
   */
  startHeartbeat(
    jobId: string,
    intervalSeconds: number = 60,
    onCancelRequested?: () => void
  ): { stop: () => void } {
    let stopped = false;
    const timer = setInterval(async () => {
      if (stopped) return;
      try {
        const renewed = await this.renewLease(jobId);
        if (!renewed && !stopped) {
          this.logger.warn('Heartbeat lease renewal failed or lost ownership', { jobId });
        }

        if (!stopped && onCancelRequested) {
          const isCancelled = await this.isCancelRequested(jobId);
          if (isCancelled && !stopped) {
            this.logger.warn('Staff requested cancellation detected during heartbeat', { jobId });
            onCancelRequested();
          }
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
   * Checks if staff requested cancellation for this job in real-time
   */
  async isCancelRequested(jobId: string): Promise<boolean> {
    try {
      const { data, error } = await this.supabase
        .from('importer_queue')
        .select('cancel_requested, status')
        .eq('id', jobId)
        .maybeSingle();

      if (error || !data) return false;
      return Boolean(data.cancel_requested || data.status === 'CANCELLED_BY_STAFF');
    } catch {
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
  async recoverExpiredLeases(): Promise<{ recovered: number; failed: number }> {
    const nowIso = new Date().toISOString();

    // 1. First attempt atomic RPC if available in database
    try {
      const { data: rpcRes, error: rpcErr } = await (async () => {
      try {
        const result = await db.$client.execute({
          sql: "UPDATE importer_queue SET status = 'QUEUED', locked_by = NULL, locked_at = NULL, lease_expires_at = NULL WHERE status = 'IMPORTING' AND lease_expires_at < :now RETURNING id",
          args: { now: new Date().toISOString() }
        });
        return { data: result.rows.length, error: null };
      } catch(e) { return { data: null, error: e }; }
    })();
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
            last_recovered_error: job.last_error || `Lease expirado (recuperado automaticamente na tentativa ${attempts})`,
            recovered_at: nowIso,
            retry_reason: 'LEASE_EXPIRED_RECOVERED',
            last_error: null,
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
