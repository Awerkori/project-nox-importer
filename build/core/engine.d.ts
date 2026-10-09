import type { SupabaseClient } from '@supabase/supabase-js';
import { SourceRegistry } from '../sources/registry.js';
import { StorageProvider } from '../storage/provider.js';
import { computeCanonicalChapterKey } from './deduplication.js';
import { HostRateLimiter } from './rate-limiter.js';
import { Config } from '../config.js';
import { AdaptiveAutotuner, BufferReservation } from './concurrency.js';
import { PermanentDataError } from './retry-policy.js';
import { PublicationSafetyBarrier } from './publication-safety-barrier.js';
import { WorkAffinityScheduler, SchedulerStateStore, AdmissionController } from './scheduler/index.js';
import { AutoHealWatchdog } from './auto-heal-watchdog.js';
import { RateBucketTracker } from './rate-bucket-tracker.js';
export { computeCanonicalChapterKey };
/**
 * A catalog lane has one bounded claimant, so FIFO ordering across all source
 * rows can let one large, old source monopolize it for hours. Rotate a small
 * probe window instead. The caller still checks each source atomically and
 * receives no lease until it is selected, preserving source eligibility and
 * all existing rate limits.
 */
export declare function selectCatalogMaintenanceProbeSources(eligibleSources: string[], cursor: number, maxProbes?: number): {
    sources: string[];
    nextCursor: number;
};
/**
 * Maintenance cursors describe a moving updates feed, while bootstrap cursors
 * describe a stable catalog page or offset. Once a full pass has completed,
 * never feed its maintenance cursor back into a new bootstrap: adapters can
 * legitimately interpret an ISO timestamp as a large numeric page/offset.
 */
export declare function resolveCatalogBackfillCursor(checkpoint: {
    cursor_value?: string | null;
    metadata?: {
        catalog_completed?: boolean | null;
    } | null;
} | null | undefined): string | null;
type DownloadRequestTrace = {
    jobId: string;
    source: string;
    chapterNumber: number;
    pageIndex: number;
    totalPages: number;
    producerAttempt: number;
    emit: (event: string, meta?: Record<string, unknown>) => void;
};
/**
 * The buffer budget is an operational limit, so it must be resolved where the
 * engine constructs the autotuner rather than being silently replaced by a
 * historical constant.
 */
export declare function resolveBufferBudgetBytes(value?: string | undefined): number;
/**
 * Queue selection is DB-bound, while a claimed chapter spends nearly all of
 * its lifetime on upstream/Telegram I/O.  The YSQL pool remains the hard
 * bound for concurrent SQL, but the claim gate must cover all execution
 * slots; otherwise slots beyond the pool size stay idle even while the pool
 * is making progress.  Execution permits are acquired only after claim and
 * validation, so pool wait cannot consume chapter capacity.
 */
export declare function resolveChapterClaimConcurrency(globalConcurrency: number, dbPoolMax: number): number;
/**
 * Claims are validated before an execution permit is taken, so the nominal
 * claim gate can be wider than the YSQL pool when the importer is healthy.
 * When the governor temporarily lowers execution capacity, however, allowing
 * every nominal slot to run the expensive claim query creates a stampede:
 * only one can ultimately take the reduced execution permit.  Keep the
 * DB-backed claim phase no wider than the current effective capacity.
 */
export declare function resolveEffectiveClaimGateCapacity(configuredClaimConcurrency: number, effectiveChapterConcurrency: number, emptySchedulerScanMode?: boolean): number;
/**
 * A null scheduler result means every priority lane and the bounded catalog
 * fallback were just checked without finding executable work. Repeating that
 * DB-heavy scan after the historical 50-150ms sleep crowds out real claims on
 * the two-connection YSQL pool. Back off proportionally to the exhausted scan
 * but cap it at 1.1s including jitter, so a newly discovered P0 is never
 * delayed long and no priority rule is changed.
 */
export declare function resolveEmptySchedulerBackoffMs(schedulerAcquireMs: number, random?: () => number): number;
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
 * A worker identity can survive a container restart.  It is never safe to
 * reclaim an unexpired lease merely because it has the same logical worker
 * name: a rolling deploy may briefly overlap processes.  Recover only leases
 * that have crossed their fencing expiry.
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
/**
 * A chapter payload whose source-work identity cannot be proven is unsafe to
 * retry or publish.  It is a permanent, quarantinable data error: another
 * healthy job/source may still make progress normally.
 */
export declare class WorkIdentityMismatchError extends PermanentDataError {
    constructor(message: string);
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
/**
 * A deadline/lease cancellation is not evidence that a narrative page is
 * absent upstream.  The media pipeline can observe the abort while draining
 * its producer/consumer queues and otherwise wrap it as a narrative-page
 * failure.  Preserve the cancellation reason so the queue takes its bounded
 * retry path instead of permanently recording a false chapter gap.
 */
export declare function getJobAbortError(signal?: AbortSignal): Error | null;
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
    private activeChapterExecutions;
    private chapterClaimMutex;
    private chapterClaimGate;
    private emptySchedulerScanMode;
    private nextEmptySchedulerProbeAt;
    private emptySchedulerScanRevision;
    private chapterClaimPhaseReady;
    private chapterClaimStartupSlots;
    private catalogMaintenanceLane;
    private catalogMaintenanceSourceCursor;
    private activeSourcesCache;
    private sourceScheduleSnapshot;
    private sourceScheduleSnapshotFlight;
    private sourceRecoveryProbeCursor;
    private catalogBackfillCursor;
    private knownCoveredWorks;
    private lastProgressTimestamp;
    private lastAutoRecoveryTimestamp;
    private lastNewWorkAutoRecoveryTimestamp;
    autoHealWatchdog: AutoHealWatchdog;
    rateBucketTracker: RateBucketTracker;
    private isRestarting;
    private dbPool;
    private runtimeInstanceId;
    private runtimeBootedAt;
    private isRuntimeLeader;
    private runtimeLeadershipTimer;
    private runtimeLeadershipRenewing;
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
    initiateControlledSelfRestart(reason: string, metrics?: any): Promise<boolean>;
    getAutotuner(): AdaptiveAutotuner;
    getSafetyBarrier(): PublicationSafetyBarrier;
    /**
     * Acquires the single runtime lease used to fence rolling deploy overlap.
     * The row is intentionally independent from heartbeat/settings hot paths.
     * A contender observes no returned row while another live process owns it.
     */
    private tryAcquireRuntimeLeadership;
    private startRuntimeLeadershipRenewal;
    private waitForRuntimeLeadership;
    private releaseRuntimeLeadership;
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
     * Probe a bounded, rotating subset of ACTIVE-with-block / COOLDOWN / DEGRADED
     * sources.  The old loop awaited every candidate serially.  A single
     * tarpit probe could therefore postpone recovery of all later sources for
     * minutes or hours, leaving recoverable backlogs permanently ineligible.
     */
    private probeDueSourceRecoveries;
    /**
     * Periodic atomic background cleanup of redundant queue jobs (runs every 5 min).
     * Safely marks queued/retrying jobs as COMPLETED with CANONICAL_ALREADY_SATISFIED
     * if their canonical chapter has already been published in chapters table.
     * Full worker slots wasted = 0.
     */
    private runRedundantJobCleanupLoop;
    /**
     * Dedicated bounded repair for chapter frontiers that were parked by a
     * transient SOURCE/WORK reservation race. This intentionally does not
     * revive permanent failures: ImporterQueue checks the still-PENDING,
     * non-gap canonical mapping before requeueing each row.
     */
    private runReservationRecoveryLoop;
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
    private latestCanonicalRate5m;
    /**
     * Periodic autotuner telemetry & evaluation loop (every 30s)
     */
    private runAutotunerLoop;
    private sourceEmptyCooldown;
    private sourceStatusCache;
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
     * Catalog maintenance waits for one fair turn in the bounded claim phase.
     * It acquires no job lease until that turn arrives and releases the permit
     * before network or job execution, so one maintenance lane cannot reduce
     * configured chapter execution concurrency or create a claim stampede.
     */
    private acquireCatalogMaintenanceJob;
    /**
     * Wait until every initial chapter slot has attempted a claim. Afterwards
     * the catalog lane obtains a fair claim-gate turn before its bounded DB
     * acquisition, rather than relying on a transient physical pool-idle
     * snapshot that can starve it indefinitely.
     */
    private shouldDeferCatalogMaintenance;
    private markChapterClaimPhaseAttempt;
    private getEligibleCatalogSources;
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
    /**
     * Last fail-closed boundary before a chapter can consume media or become a
     * canonical chapter.  The work mapping is the durable identity authority;
     * a queue payload is only a cached transport envelope and must agree with
     * it exactly.  For URL-scoped sources (Madara/MangaLivre), the adapter also
     * proves that the chapter URL lives beneath the mapped work path.
     */
    private assertChapterWorkIdentity;
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
        /**
         * Page imports already own a bounded retry ladder (including manifest
         * refresh).  Letting the low-level fetch retry that same request again
         * multiplies a bad CDN response into minutes of occupied chapter-slot
         * time.  Direct callers retain the defensive default of three tries.
         */
        maxAttempts?: number;
        signal?: AbortSignal;
        requestTrace?: DownloadRequestTrace;
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
