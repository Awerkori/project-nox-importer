import type { SupabaseClient } from '@supabase/supabase-js';
import { SourceRegistry } from '../sources/registry.js';
import { StorageProvider } from '../storage/provider.js';
import { computeCanonicalChapterKey } from './deduplication.js';
import { HostRateLimiter } from './rate-limiter.js';
import { Config } from '../config.js';
import { AdaptiveAutotuner } from './concurrency.js';
import { PublicationSafetyBarrier } from './publication-safety-barrier.js';
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
    private safetyBarrier;
    private reconciler;
    private circuitBreaker;
    private sharedNetworkDetector;
    private admissionGate;
    private isRunning;
    private stopSignal;
    private abortController;
    static activeBufferedBytes: number;
    constructor(supabase: SupabaseClient, storage: StorageProvider, registry: SourceRegistry, rateLimiter: HostRateLimiter, config: Config);
    getAutotuner(): AdaptiveAutotuner;
    getSafetyBarrier(): PublicationSafetyBarrier;
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
     * Continuous catalog backfill loop (expands catalog from ~100 to thousands of works).
     * Traverses pages 1..N of active sources using persistent checkpoints.
     */
    private runCatalogBackfillLoop;
    private scheduleCatalogBackfill;
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
    private sourceEmptyCooldown;
    private sourceStatusCache;
    private checkSourceAvailability;
    /**
     * Dedicated multi-slot concurrent runner for a specific source.
     * Runs up to sourceLimits.maxChapters parallel worker slots, acquiring jobs atomically.
     */
    private runSourceWorker;
    private runSourceSlot;
    /**
     * General fallback worker runner running multiple concurrent slots
     */
    private runGeneralWorker;
    private runGeneralSlot;
    /**
     * Dedicated discovery worker loop to guarantee discovery is NEVER starved by chapter backlog.
     * Continuously claims DISCOVER_WORKS and SYNC_WORK jobs from the queue.
     */
    private runDiscoveryWorker;
    /**
     * Executes a job with active lease heartbeat and hard timeout watchdog.
     */
    private executeJobDirectly;
    /**
     * Backward-compatible entrypoint used by step() and test suites.
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
