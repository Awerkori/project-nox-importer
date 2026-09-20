import pg from 'pg';
import type { GatewayJob, PublishBatchParams } from '../core/gateway-client.js';
export declare function getYugabytePool(): pg.Pool;
export declare function closeYugabytePool(): Promise<void>;
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
export declare function publishBatchDirect(payload: PublishBatchParams): Promise<PublishBatchDirectResult>;
