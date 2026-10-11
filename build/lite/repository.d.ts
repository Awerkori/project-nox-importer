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
    /** Reserved for the one-resource P0 observation lane, never normal work. */
    monitoring?: boolean;
}
export interface LiteSourcePolicy {
    enabled: boolean;
    status: string;
    health: string;
    cooldownUntil: Date | null;
    syncIntervalMs: number;
    runnable: boolean;
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
    /**
     * Cataloguing can reveal hundreds of chapters at once.  Insert their durable
     * work in one short transaction, then refresh each affected ready-work row
     * once.  It intentionally does no network work and never claims anything.
     */
    enqueueMany(inputs: EnqueueLiteTask[]): Promise<{
        inserted: number;
        insertedKeys: Set<string>;
    }>;
    claim(workerId: string, leaseSeconds: number): Promise<LiteTask | null>;
    /**
     * P0 observation has one durable, explicitly bounded lane.  It is never
     * selected by normal claims, so unchanged watches cannot starve P1/P2.
     */
    claimMonitor(workerId: string, leaseSeconds: number): Promise<LiteTask | null>;
    /**
     * Source administration and transient Lite health remain durable authority.
     * This indexed one-source read happens before source networking, never in a
     * normal task claim, and prevents a paused/cooling provider from consuming
     * a catalog or discovery slot.
     */
    sourcePolicy(source: string): Promise<LiteSourcePolicy | null>;
    checkpoint(task: LiteTask, workerId: string, patch: Record<string, unknown>): Promise<boolean>;
    finish(task: LiteTask, workerId: string, outcome: {
        state: Extract<LiteTaskState, 'DONE' | 'RETRY_WAIT' | 'BLOCKED'>;
        reason?: string | null;
        nextRunAt?: Date;
        checkpoint?: Record<string, unknown>;
        priority?: Exclude<LitePriority, 'STAFF'>;
        monitoring?: boolean;
        /** Catalog attaches the resolved canonical work without changing source identity. */
        workId?: string;
    }): Promise<boolean>;
    /**
     * Catalog evidence is the only event that reopens a source-blocked chapter.
     * It is scoped by the indexed canonical work/chapter keys, so a new mapping
     * never causes a global BLOCKED sweep. Terminal integrity failures stay
     * blocked until an operator changes the underlying condition.
     */
    wakeCatalogBlocked(workId: string, workKey: string): Promise<number>;
    /**
     * Source health is durable operational evidence, shared by discovery,
     * cataloguing and chapter execution.  It is deliberately separate from
     * task priority: a cooled-down preferred source must not suppress a
     * confirmed healthy alternative.
     */
    recordSourceFailure(source: string, error: unknown): Promise<void>;
    recordSourceSuccess(source: string): Promise<void>;
    /** One bounded recovery turn; it cannot consume the claim path or scan works. */
    reconcile(limit?: number): Promise<{
        leases: number;
        retries: number;
        exhausted: number;
        uploads: number;
        publishable: number;
    }>;
    operationalSnapshot(): Promise<Array<{
        priority: LitePriority;
        state: LiteTaskState;
        monitoring: boolean;
        count: number;
        oldestReadyAt: Date | null;
    }>>;
    /** Small, indexed progress probe used for a human-operable Lite heartbeat. */
    progressSnapshot(): Promise<{
        publishedLastMinute: number;
        oldestReadyAt: Date | null;
        oldestStoredUnpublishedAt: Date | null;
        nextMonitorAt: Date | null;
    }>;
    blockedReasonSnapshot(limit?: number): Promise<Array<{
        reason: string;
        count: number;
    }>>;
    sourceHealthSnapshot(): Promise<Array<{
        health: string;
        count: number;
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
        messageId: number | null;
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
