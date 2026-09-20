export interface GatewayJob {
    id: string;
    task_type: string;
    source: string;
    priority: number;
    payload: Record<string, any>;
    dedupe_key: string;
    status: string;
    attempts: number;
    max_attempts: number;
    locked_by: string | null;
    locked_at: string | null;
    lease_expires_at: string | null;
    next_run_at: string;
    last_error: string | null;
    chapter_sort_key: number | null;
}
export interface PublishBatchParams {
    jobId?: string;
    work: {
        id?: string;
        slug: string;
        title: string;
        synopsis?: string;
        description?: string;
        author?: string;
        artist?: string;
        kind?: string;
        status?: string;
        ageRating?: number;
        aliases?: string[];
    };
    workMapping?: {
        source: string;
        sourceWorkId: string;
        sourceSlug: string;
        sourceTitle: string;
        metadata?: any;
        confidenceScore?: number;
        isPrimary?: boolean;
    };
    chapter: {
        number: number;
        title?: string;
        chapterSortKey?: number;
        sourceChapterId: string;
        source: string;
    };
    pages: Array<{
        position: number;
        mediaId?: string;
        providerKey: string;
        botReference?: string;
        storageShardId?: string;
        mime: string;
        width: number;
        height: number;
        bytes: number;
        sha256: string;
    }>;
    isPageProvider?: boolean;
}
export declare class ImporterGatewayClient {
    private logger;
    private baseUrl;
    private bridgeToken;
    private gatewayLimiter;
    constructor(mangaUrl: string, bridgeToken: string);
    private request;
    acquireJobs(options: {
        workerId: string;
        leaseDurationMinutes?: number;
        source?: string;
        taskType?: string;
        batchSize?: number;
    }): Promise<GatewayJob[]>;
    heartbeat(workerId: string, jobs: Array<{
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
    failBatch(workerId: string, jobs: Array<{
        jobId: string;
        status?: 'RETRY' | 'FAILED' | 'PAUSED_BY_STAFF' | 'BLOCKED_BY_UPSTREAM';
        error?: string;
        retryDelaySeconds?: number;
        retryReason?: string;
    }>): Promise<number>;
    publishBatch(payload: PublishBatchParams): Promise<{
        success: boolean;
        workId: string;
        chapterId: string;
        pageCount: number;
        publishedAt: string;
    }>;
    enqueueJobs(jobs: Array<{
        taskType: string;
        source: string;
        dedupeKey: string;
        payload?: Record<string, any>;
        priority?: number;
        chapterSortKey?: number | null;
    }>): Promise<number>;
    recoverStalled(): Promise<number>;
    getSources(enabledOnly?: boolean): Promise<any[]>;
    updateSource(sourceIdOrName: string, update: {
        lastSyncAt?: string;
        enabled?: boolean;
        config?: Record<string, any>;
    }): Promise<boolean>;
    getCheckpoint(source: string): Promise<any | null>;
    saveCheckpoint(source: string, cursorValue: string | null, metadata?: Record<string, any>): Promise<void>;
    resolveWork(candidate: {
        source: string;
        sourceWorkId: string;
        sourceSlug?: string;
        title: string;
        slug?: string;
        aliases?: string[];
    }): Promise<{
        matched: boolean;
        matchMethod?: string;
        workId?: string;
        mappingId?: string | null;
        work?: any;
    }>;
    getSafetyBarrier(): Promise<string>;
    setSafetyBarrier(state: string): Promise<void>;
    getReconcileWork(workId: string): Promise<any>;
    saveWorkHealth(healthData: {
        workId: string;
        status?: string;
        totalKnownChapters?: number;
        totalImportedChapters?: number;
        missingStart?: boolean;
        firstChapterNumber?: number | null;
        latestChapterNumber?: number | null;
        gaps?: any;
        unresolvedGaps?: any;
        providersSummary?: any;
    }): Promise<void>;
    getStats(): Promise<any>;
    sql<T = any>(query: string, params?: any[]): Promise<{
        rows: T[];
        rowCount: number;
    }>;
    batchSql(queries: Array<{
        text: string;
        params?: any[];
    }>): Promise<Array<{
        rows: any[];
        rowCount: number;
    }>>;
}
