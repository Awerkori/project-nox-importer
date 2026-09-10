import type { SupabaseClient } from '@supabase/supabase-js';
import { SourceRegistry } from '../sources/registry.js';
import { StorageProvider } from '../storage/provider.js';
import { computeCanonicalChapterKey } from './deduplication.js';
import { HostRateLimiter } from './rate-limiter.js';
import { Config } from '../config.js';
import { AdaptiveAutotuner } from './concurrency.js';
export { computeCanonicalChapterKey };
export declare class JobCancelledByStaffError extends Error {
    readonly jobId: string;
    constructor(jobId: string, message?: string);
}
export type PageSemanticType = 'CONTENT_PAGE' | 'CREDIT_PAGE' | 'PROMO_PAGE' | 'RECRUITMENT_PAGE' | 'WARNING_PAGE';
export declare function classifyPageUrl(url: string, index: number, total: number): PageSemanticType;
export declare class NarrativePageUnavailableError extends Error {
    source: string;
    pageIndex: number;
    totalPages: number;
    originalError: string;
    constructor(source: string, pageIndex: number, totalPages: number, originalError: string);
}
export declare class ImporterEngine {
    private supabase;
    private storage;
    private registry;
    private rateLimiter;
    private config;
    private logger;
    private queue;
    private deduplication;
    private checkpoints;
    private autotuner;
    private publicationBarrier;
    private reconciler;
    private isRunning;
    private stopSignal;
    private abortController;
    static activeBufferedBytes: number;
    static readonly MAX_BUFFERED_BYTES: number;
    constructor(supabase: SupabaseClient, storage: StorageProvider, registry: SourceRegistry, rateLimiter: HostRateLimiter, config: Config);
    getAutotuner(): AdaptiveAutotuner;
    start(): Promise<void>;
    runStartupRecovery(): Promise<void>;
    /**
     * Recalculates next_run_at for legacy retry jobs that were given long exponential backoffs (16-32 min)
     * due to transient 502/503 errors, rescheduling them for quick execution (10-35s).
     */
    recoverStalled502Retries(): Promise<number>;
    stop(): void;
    /**
     * Periodic discovery scheduler running in the background
     */
    private runDiscoveryLoop;
    /**
     * Periodic publication sweep loop (every 10s) to unblock STAGED chapters
     */
    private runPublicationSweepLoop;
    /**
     * Periodic lease recovery loop (every 60s) to rescue stalled jobs from crashed instances
     */
    private runLeaseRecoveryLoop;
    /**
     * Periodic existing works reconciliation loop
     * Handles high-priority staff requests, on-demand admin reconciliations, and periodic catalog health batches.
     */
    private runReconciliationLoop;
    /**
     * Periodic upstream provider health check loop (every 5 min)
     * Evaluates UPSTREAM_BLOCKED, RECOVERING, and DEGRADED sources.
     * If Cloudflare lifts 403 on datacenter egress, stages safe recovery:
     * UPSTREAM_BLOCKED -> RECOVERING -> ACTIVE (only after validating Search, Chapters, Pages, and Download)
     */
    private runUpstreamHealthLoop;
    checkBlockedSourcesHealth(): Promise<void>;
    probeSourceHealth(src: {
        id: string;
        name: string;
        status: string;
        base_url?: string;
        blocked_reason?: string | null;
        blocked_details?: any;
    }): Promise<void>;
    private autotunerCycleCount;
    /**
     * Periodic autotuner telemetry & evaluation loop (every 30s)
     */
    private runAutotunerLoop;
    /**
     * Dedicated worker loop for a specific source
     */
    private runSourceWorker;
    /**
     * General worker loop to process jobs with no source filter
     */
    private runGeneralWorker;
    /**
     * Executes a job respecting global and per-source concurrency semaphores.
     * Starts atomic lease heartbeat immediately upon acquisition so that the lease
     * is continuously renewed even while waiting for concurrency semaphore permits.
     */
    private executeJobWithLimits;
    /**
     * Discrete step method preserved for unit tests & single iterations
     */
    step(source?: string): Promise<boolean>;
    private scheduleSources;
    private processJob;
    private handleDiscoverWorks;
    private handleSyncWork;
    computeCanonicalChapterKey(chapterNumber: number | string, chapterTitle?: string): {
        normalizedNumber: number;
        sortKey: number;
        isSpecial: boolean;
        specialCategory?: "prologue" | "extra" | "special" | "side";
    };
    private computeChapterSortKey;
    private handleImportChapter;
    private recordJobMetric;
    private recordTelemetrySnapshot;
    private pruneTelemetry;
    private sanitizeErrorMessage;
    private downloadAndRegisterImage;
    private fetchImageBytes;
    private resolveDynamicCandidateFallbacks;
    private cachedBotUserId;
    private resolveBotUserId;
    /**
     * Verifies if all chapters in the canonical manifest for a prioritized work are accounted for
     * (either PUBLISHED or marked as UNRESOLVED_GAP). If no chapters remain in QUEUED or STAGED,
     * marks the staff request as COMPLETED.
     */
    checkStaffRequestCompletion(workId: string): Promise<void>;
    private sleep;
}
