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
}
export declare class ImporterQueue {
    private supabase;
    private workerId;
    private logger;
    constructor(supabase: SupabaseClient, workerId: string);
    /**
     * Enqueue a new task safely with deduplication key
     */
    enqueue(taskType: TaskType, source: string, dedupeKey: string, payload?: Record<string, any>, priority?: number): Promise<boolean>;
    /**
     * Acquire the next job atomically using SKIP LOCKED stored procedure
     */
    acquireNextJob(leaseDurationMinutes?: number): Promise<QueueJob | null>;
    /**
     * Heartbeat renewal of an active lease
     */
    renewLease(jobId: string, leaseDurationMinutes?: number): Promise<boolean>;
    /**
     * Release an acquired job as completed, failed, or queued for retry
     */
    releaseJob(jobId: string, status: JobStatus, lastError?: string, retryDelayMinutes?: number): Promise<boolean>;
    /**
     * Create a lease heartbeat handle that periodically renews the lease
     * until stopped.
     */
    startHeartbeat(jobId: string, intervalSeconds?: number): {
        stop: () => void;
    };
}
