import type pg from 'pg';
export interface CoverCandidate {
    source: string;
    coverUrl: string;
}
export interface CoverVisionWorkTask {
    workId: string;
    candidateCovers: CoverCandidate[];
    status: 'PENDING' | 'RUNNING' | 'COMPLETED' | 'NO_PTBR_FOUND' | 'FAILED';
    attempts: number;
    maxAttempts: number;
    inspectedUrls: string[];
    detectedTitle: string | null;
    attributingSource: string | null;
    confidence: number | null;
    evidence: any | null;
    lastError: string | null;
    createdAt: string;
    updatedAt: string;
    nextRetryAt: string | null;
}
export declare class CoverVisionQueue {
    private readonly pool;
    private logger;
    private schemaReady;
    constructor(pool: pg.Pool);
    ensureSchema(): Promise<void>;
    enqueueTask(workId: string, candidateCovers: CoverCandidate[], maxAttempts?: number): Promise<void>;
    findDueTasks(limit?: number): Promise<CoverVisionWorkTask[]>;
    markRunning(workId: string): Promise<void>;
    markCompleted(workId: string, result: {
        detectedTitle: string;
        attributingSource: string;
        confidence: number;
        evidence: any;
        inspectedUrls: string[];
    }): Promise<void>;
    markNoPtBrFound(workId: string, inspectedUrls: string[]): Promise<void>;
    markFailed(workId: string, error: string, inspectedUrls: string[], maxAttempts?: number, retryDelayMinutes?: number): Promise<void>;
    getTask(workId: string): Promise<CoverVisionWorkTask | null>;
}
