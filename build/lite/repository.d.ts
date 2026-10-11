import type { Pool } from 'pg';
import { type LitePriority, type LiteTask, type LiteTaskState, type LiteTaskType } from './types.js';
type Db = Pick<Pool, 'query' | 'connect'>;
export interface EnqueueLiteTask {
    taskType: LiteTaskType;
    workId?: string | null;
    workKey: string;
    chapterKey?: string | null;
    priority: Exclude<LitePriority, 'STAFF'>;
    idempotencyKey: string;
    nextRunAt?: Date;
    maxAttempts?: number;
    payload?: Record<string, unknown>;
    checkpoint?: Record<string, unknown>;
}
/** Direct YSQL persistence. Network, adapters and Telegram are never invoked here. */
export declare class LiteRepository {
    private readonly db;
    constructor(db: Db);
    private refreshReadyWork;
    enqueue(input: EnqueueLiteTask): Promise<{
        inserted: boolean;
        task: LiteTask | null;
    }>;
    claim(workerId: string, leaseSeconds: number): Promise<LiteTask | null>;
    checkpoint(task: LiteTask, workerId: string, patch: Record<string, unknown>): Promise<boolean>;
    finish(task: LiteTask, workerId: string, outcome: {
        state: Extract<LiteTaskState, 'DONE' | 'RETRY_WAIT' | 'BLOCKED'>;
        reason?: string | null;
        nextRunAt?: Date;
        checkpoint?: Record<string, unknown>;
        priority?: Exclude<LitePriority, 'STAFF'>;
    }): Promise<boolean>;
    /**
     * Catalog evidence is the only event that reopens a source-blocked chapter.
     * It is scoped by the indexed canonical work/chapter keys, so a new mapping
     * never causes a global BLOCKED sweep. Terminal integrity failures stay
     * blocked until an operator changes the underlying condition.
     */
    wakeCatalogBlocked(workId: string, workKey: string): Promise<number>;
    /** One bounded recovery turn; it cannot consume the claim path or scan works. */
    reconcile(limit?: number): Promise<{
        leases: number;
        retries: number;
        publishable: number;
    }>;
    operationalSnapshot(): Promise<Array<{
        priority: LitePriority;
        state: LiteTaskState;
        count: number;
        oldestReadyAt: Date | null;
    }>>;
    /** Stable ID is reserved before the non-transactional Telegram call. */
    reserveUpload(taskId: string, pagePosition: number, sha256: string): Promise<{
        mediaId: string;
        state: string;
    }>;
    recordUploadReceipt(input: {
        taskId: string;
        pagePosition: number;
        mediaId: string;
        sha256: string;
        bytes: number;
        mime: string;
        width: number;
        height: number;
        providerKey: string;
        botReference: string | null;
        shardId: string | null;
        channelId: string | null;
        createdBy: string;
    }): Promise<void>;
    markUploadUnknown(taskId: string, position: number): Promise<void>;
    /** The provider explicitly rejected the attempt, so the same reserved ID may retry safely. */
    markUploadRetryable(taskId: string, position: number): Promise<void>;
    publish(input: {
        task: LiteTask;
        workerId: string;
        chapterId: string;
        workId: string;
        mediaIds: string[];
        fresh: boolean;
    }): Promise<boolean>;
}
export {};
