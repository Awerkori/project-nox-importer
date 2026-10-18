import pg from 'pg';
import type { GatewayJob, PublishBatchParams } from '../core/gateway-client.js';
/**
 * A bounded, parameter-free SQL class for runtime telemetry. Do not return
 * raw SQL: values and unbounded query text do not belong in diagnostics.
 */
export declare function getSqlFingerprint(queryArg: unknown): string;
/**
 * Protected connections for low-priority historical metadata bookkeeping.
 * Normal operation admits one task; the explicit V4-exclusive campaign may
 * use two distinct-work merges without borrowing the public/chapter pool.
 * A third connection is reserved for the serialized durable producer: with
 * only two connections, refilling a successor waits behind both active
 * merges and leaves the exclusive lane intermittently empty.
 */
export declare function createYugabyteMetadataPool(): pg.Pool;
export declare function getYugabytePool(): pg.Pool;
export declare function closeYugabytePool(): Promise<void>;
/** Test-only injection for disposable SQL contract tests. */
export declare function setYugabytePoolForTest(testPool: pg.Pool | null): void;
export declare function testConnection(): Promise<{
    connected: boolean;
    database: string;
    version: string;
    totalConnections: number;
    activeConnections: number;
    idleConnections: number;
    idleInTransaction: number;
}>;
export declare function testRollback(): Promise<{
    rollbackOk: boolean;
    openTransactions: number;
    idleInTransaction: number;
}>;
export declare function acquireJobsDirect(options: {
    workerId: string;
    leaseDurationMinutes?: number;
    source?: string;
    allowedSources?: string[];
    /**
     * Catalog maintenance supplies a short, already-rotated source window.
     * Preserve that source order for this one bounded claim so an older queue
     * head from a neighbouring source cannot win every visit to the window.
     */
    preferAllowedSourceOrder?: boolean;
    /** Claim only the bounded legacy same-source ambiguity recovery jobs. */
    onlyLegacySameSourceClaimRecovery?: boolean;
    /** Claim only SYNC_WORK rows mapped to an existing published work (P1). */
    onlyExistingPublishedWorkSync?: boolean;
    /** Claim only P1 sync rows for works with due P1 chapter-frontier work. */
    onlyQueuedP1FrontierWorkSync?: boolean;
    taskType?: string;
    batchSize?: number;
}): Promise<GatewayJob[]>;
export declare function heartbeatDirect(workerId: string, jobs: Array<{
    jobId: string;
    progressCurrent?: number;
    progressTotal?: number;
    progressStage?: string;
    leaseDurationMinutes?: number;
}>): Promise<Array<{
    jobId: string;
    status: string;
    cancelRequested: boolean;
    renewed: boolean;
}>>;
export declare function failBatchDirect(jobs: Array<{
    jobId: string;
    error?: string;
    status?: 'RETRY' | 'FAILED' | 'PAUSED_BY_STAFF' | 'CANCELLED_BY_STAFF' | 'BLOCKED_BY_UPSTREAM' | 'COMPLETED';
    retryDelaySeconds?: number;
    retryReason?: string;
    workerId?: string;
    recoveredReason?: string;
}>): Promise<number>;
export declare function recoverStalledLeasesDirect(staleGraceSeconds?: number): Promise<{
    recoveredCount: number;
    failedCount: number;
}>;
export interface PublishBatchDirectMetrics {
    beginMs: number;
    validationMs: number;
    masterCteMs: number;
    mediaPagesCteMs: number;
    commitMs: number;
    sqlExecutionMs: number;
    queryCount: number;
    roundTrips: number;
    rowsWritten: number;
    workMappingMs?: number;
    chapterMs?: number;
    mediaMs?: number;
    pagesDeleteMs?: number;
    pagesInsertMs?: number;
    chapterMappingMs?: number;
    queueUpdateMs?: number;
    workUpdateMs?: number;
}
export interface PublishBatchDirectResult {
    success: boolean;
    workId: string;
    chapterId: string;
    pageCount: number;
    publishedAt: string;
    dbDurationMs: number;
    metrics: PublishBatchDirectMetrics;
}
/**
 * The v1 publisher only receives a JavaScript number, so it cannot recover a
 * textual scale that was already lost (for example 12.10 vs 12.1).  During a
 * rollback after the Reader canonical-identity cutover it must therefore use
 * the stable source chapter id as its identity, rather than recreating the
 * removed `(work_id, number)` collision.  Lite itself supplies the richer
 * source-independent identity and never uses this compatibility path.
 */
export declare function legacyRollbackChapterIdentity(source: string, sourceChapterId: string, number: number): {
    canonicalKey: string;
    displayNumber: string;
    orderKey: string;
};
export declare function publishBatchDirect(payload: PublishBatchParams): Promise<PublishBatchDirectResult>;
