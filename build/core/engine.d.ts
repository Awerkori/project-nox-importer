import type { SupabaseClient } from '@supabase/supabase-js';
import { SourceRegistry } from '../sources/registry.js';
import { StorageProvider } from '../storage/provider.js';
import { computeCanonicalChapterKey } from './deduplication.js';
import { HostRateLimiter } from './rate-limiter.js';
import { Config } from '../config.js';
import { AdaptiveAutotuner, BufferReservation } from './concurrency.js';
import { PublicationSafetyBarrier } from './publication-safety-barrier.js';
import { WorkAffinityScheduler, SchedulerStateStore, AdmissionController } from './scheduler/index.js';
import { AutoHealWatchdog } from './auto-heal-watchdog.js';
import { RateBucketTracker } from './rate-bucket-tracker.js';
export { computeCanonicalChapterKey };
/**
 * The buffer budget is an operational limit, so it must be resolved where the
 * engine constructs the autotuner rather than being silently replaced by a
 * historical constant.
 */
export declare function resolveBufferBudgetBytes(value?: string | undefined): number;
export type InternalLivenessState = 'HEALTHY_IDLE' | 'HEALTHY_WORKING' | 'BACKPRESSURED' | 'STALLED';
export type ExternalLivenessState = InternalLivenessState | 'DEAD';
export declare function computeInternalLivenessState(params: {
    isStopActive: boolean;
    rssMb: number;
    rssTripwireMb?: number;
    eligibleCount: number;
    importingCount: number;
    activeJobsCount: number;
    minutesSinceProgress: number;
}): InternalLivenessState;
export declare function computeExternalLivenessState(params: {
    lastHeartbeatTimestamp: number;
    now?: number;
    heartbeatTimeoutMs?: number;
    internalState: InternalLivenessState;
}): ExternalLivenessState;
/**
 * A container can restart with the same WORKER_ID before its old 5-minute
 * leases expire.  Those leases belong to a process which cannot still be
 * running, so retaining them would briefly admit a second cohort on top of
 * the configured capacity.  Keep this deliberately scoped to this worker;
 * other workers and genuinely active leases are never touched.
 */
export declare function reclaimStartupOwnedLeases(pool: {
    query: (text: string, params?: unknown[]) => Promise<{
        rowCount?: number | null;
        rows?: unknown[];
    }>;
}, workerId: string): Promise<number>;
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
    private protectiveSentinel;
    private reconciler;
    schedulerStateStore: SchedulerStateStore;
    admissionController: AdmissionController;
    scheduler: WorkAffinityScheduler;
    private circuitBreaker;
    private sharedNetworkDetector;
    private admissionGate;
    private sourceProbesInFlight;
    private isRunning;
    private stopSignal;
    private abortController;
    private chapterClaimMutex;
    private catalogMaintenanceLane;
    private activeSourcesCache;
    private sourceScheduleSnapshot;
    private sourceScheduleSnapshotFlight;
    private catalogBackfillCursor;
    private knownCoveredWorks;
    private lastProgressTimestamp;
    private lastAutoRecoveryTimestamp;
    private lastNewWorkAutoRecoveryTimestamp;
    autoHealWatchdog: AutoHealWatchdog;
    rateBucketTracker: RateBucketTracker;
    private isRestarting;
    private dbPool;
    static activeBufferedBytes: number;
    constructor(supabase: SupabaseClient, storage: StorageProvider, registry: SourceRegistry, rateLimiter: HostRateLimiter, config: Config);
    private isExplicitExitHandlerSet;
    private exitHandler;
    setExitHandlerForTest(handler: (code: number) => void): void;
    /**
     * Initiates in-process soft restart when an unresolvable critical stall occurs:
     * 1. Pauses acceptance of new claims (isRestarting = true).
     * 2. Bounded drain of in-flight jobs (up to 6s).
     * 3. Clears orphaned leases and in-flight jobs in database.
     * 4. Validates Yugabyte database connectivity.
     * 5. Re-initializes scheduler and admission controller state.
     * 6. Resets AdaptiveAutotuner to capacity 1 in RECOVERING mode.
     * 7. Resumes worker loops smoothly without process exit (protects Discloud uptime).
     */
    initiateControlledSelfRestart(reason: string, metrics?: any): Promise<void>;
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
    private discoveryAllowedCache;
    private discoveryAllowedCachedAt;
    /**
     * Checks whether catalog discovery and backfill are globally enabled in system settings.
     * Cached for 5s to eliminate unnecessary database calls on tight loops.
     */
    isDiscoveryAllowed(): Promise<boolean>;
    /**
     * Scheduling uses only this projection, so a short cache is safe and avoids a
     * full importer_sources read for each independent maintenance loop.  Runtime
     * source admission continues to use its own much shorter status cache.
     */
    private getSourceScheduleSnapshot;
    private getRecentDiscoveryJobsBySource;
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
     * Periodic publication sweep loop: 10s when active progress, backed off to 30s when no chapters are published
     */
    private runPublicationSweepLoop;
    /**
     * Periodic lease recovery loop (every 60s) to rescue stalled jobs from crashed instances
     */
    private runLeaseRecoveryLoop;
    /**
     * Periodic bounded hygiene sweep (~5 min):
     * 1. Reclaims expired job leases
     * 2. Cleans stale source cooldowns in importer_sources
     * 3. Prunes rate buckets older than 48h
     * 4. Prunes old autotuner telemetry
     */
    private runHygieneSweepLoop;
    /**
     * Periodic auto-probe and auto-healing loop for sources in COOLDOWN / DEGRADED (runs every 30s).
     * Restores expired cooldowns immediately through production admission probe.
     */
    private runSourceCooldownProbeLoop;
    /**
     * Periodic atomic background cleanup of redundant queue jobs (runs every 30s).
     * Safely marks queued/retrying jobs as COMPLETED with CANONICAL_ALREADY_SATISFIED
     * if their canonical chapter has already been published in chapters table.
     * Full worker slots wasted = 0.
     */
    private runRedundantJobCleanupLoop;
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
    /**
     * Periodic Liveness Watchdog and Auto-Recovery Loop (Section 7, 8 & 16)
     * Evaluates pipeline liveness against 5 distinct operational states:
     * HEALTHY_IDLE, HEALTHY_WORKING, BACKPRESSURED, STALLED, DEAD.
     * Emits operational WARNING (>=15m) and CRITICAL (>=30m) alerts and runs auto-recovery.
     */
    private runLivenessWatchdogLoop;
    checkBlockedSourcesHealth(): Promise<void>;
    probeSourceHealth(src: {
        id: string;
        name: string;
        status: string;
        base_url?: string;
        blocked_reason?: string | null;
        blocked_details?: any;
        cooldown_until?: string | null;
        last_health_check_at?: string | null;
    }): Promise<void>;
    private autotunerCycleCount;
    private consecutiveUnderutilizedCycles;
    /**
     * Periodic autotuner telemetry & evaluation loop (every 30s)
     */
    private runAutotunerLoop;
    private sourceEmptyCooldown;
    private sourceStatusCache;
    private activeStaffFocusCache;
    private activeStaffFocusFlight;
    private getActiveStaffFocusWorkId;
    private getEligibleChapterSources;
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
     * Dedicated discovery worker loop to guarantee catalog scanning is NEVER starved by chapter backlog.
     * Continuously claims DISCOVER_WORKS jobs from the queue.
     */
    private runDiscoveryWorker;
    /**
     * Dedicated catalog sync worker loop to guarantee work metadata / chapter discovery runs steadily.
     * Continuously claims SYNC_WORK jobs from the queue.
     */
    private runCatalogSyncWorker;
    /**
     * Executes a job with an active lease heartbeat and a bounded soft deadline.
     *
     * A Promise.race cannot cancel network I/O. Releasing its permits when the
     * deadline wins creates a zombie upload that continues consuming Telegram,
     * CPU and DB while another slot claims more work. Keep the lease and permits
     * until the real operation settles; a slow dependency then reduces only its
     * own effective capacity instead of exceeding global concurrency.
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
    downloadAndRegisterImage(url: string, userId: string, purpose?: string, source?: string): Promise<string>;
    fetchImageBytes(url: string, source?: string, options?: {
        timeoutMs?: number;
        freshConnection?: boolean;
        refererOverride?: string;
        reservation?: BufferReservation;
    }): Promise<Uint8Array>;
    tryRawMetadataCoverFallback(raw: Record<string, any>, botUserId: string, source: string, excludeUrl?: string | null): Promise<string | null>;
    trySiblingMappingCoverFallback(workId: string | undefined, excludeSource: string, slugOrTitle: string, botUserId: string): Promise<string | null>;
    ensureWorkHasCover(workId: string, botUserId: string): Promise<string | null>;
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
