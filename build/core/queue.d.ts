import type { SupabaseClient } from '@supabase/supabase-js';
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
export declare class ImporterQueue {
    private supabase;
    private workerId;
    private logger;
    constructor(supabase: SupabaseClient, workerId: string);
    /**
     * Enqueue a new task safely with deduplication key and optional deterministic sort key
     */
    enqueue(taskType: TaskType, source: string, dedupeKey: string, payload?: Record<string, any>, priority?: number, chapterSortKey?: number | null): Promise<boolean>;
    /**
     * Batch enqueue multiple tasks safely with deduplication
     */
    enqueueBatch(jobs: Array<{
        taskType: TaskType;
        source: string;
        dedupeKey: string;
        payload?: Record<string, any>;
        priority?: number;
        chapterSortKey?: number | null;
    }>): Promise<number>;
    /**
     * Acquire the next job atomically using SKIP LOCKED stored procedure,
     * optionally filtered by source for concurrent source runners.
     */
    acquireNextJob(leaseDurationMinutes?: number, source?: string): Promise<QueueJob | null>;
    /**
     * Heartbeat renewal of an active lease
     */
    renewLease(jobId: string, leaseDurationMinutes?: number): Promise<boolean>;
    /**
     * Release an acquired job as completed, failed, or queued for retry with second-level precision
     */
    releaseJob(jobId: string, status: JobStatus, lastError?: string, retryDelaySeconds?: number, retryClass?: string): Promise<boolean>;
    /**
     * Create a lease heartbeat handle that periodically renews the lease
     * until stopped. Uses .unref() to avoid blocking graceful shutdown.
     */
    startHeartbeat(jobId: string, intervalSeconds?: number): {
        stop: () => void;
    };
    /**
     * Generic crash-safe lease recovery for any stalled job across the entire system.
     * Scans for jobs stuck in 'IMPORTING' with expired lease (lease_expires_at < now()).
     * - REGRA: Erros técnicos / lease expirado NUNCA marcam jobs como 'FAILED'.
     * - Jobs são re-enfileirados em 'RETRY' ou 'QUEUED' com backoff progressivo proporcional às tentativas.
     * - Clears locked_by, locked_at, and lease_expires_at, while strictly preserving attempts.
     */
    recoverExpiredLeases(): Promise<{
        recovered: number;
        failed: number;
    }>;
}
