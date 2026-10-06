/**
 * Work-Affinity Scheduler for Project Nox Importer.
 *
 * Implements:
 * - P0: Absolute priority preemption for fresh new releases (priority >= 100).
 * - P1 Critical Gap: Prioritizes missing chapters unblocking STAGED barrier cascade (priority 90-95).
 * - P1 Backfill: Fair scheduling across ACTIVE_BACKFILL_WORKS (<= 10 works).
 * - P2 Active New Works: Fair scheduling with work affinity across ACTIVE_NEW_WORKS (<= 8 works).
 * - Max in-flight per work: MAX_INFLIGHT_PER_WORK = 2 (ensures >= 9 concurrent works across 18 workers).
 * - Anti-starvation: P1 and P2 make steady progress even under sustained P0 traffic.
 * - Work-conserving fallback: No worker sits idle if any eligible job exists.
 * - Explainable scheduler: Detailed telemetry on why each job was selected.
 * - Shadow mode & live cutover toggle.
 */
import { ProtectiveSentinel } from '../protective-sentinel.js';
import { AdmissionController } from './admission-controller.js';
import { SchedulerStateStore } from './state-store.js';
import { SchedulerDecision, SchedulerLane, SchedulerMetrics } from './types.js';
export declare function isActiveChapterClaimConflict(error: any): boolean;
/**
 * Staff requests own the scheduling order, not the resource budget. They are
 * absolute while a claimable Staff job exists; the caller still applies the
 * normal source, DB, media and global chapter permits, so this cannot
 * manufacture capacity.
 */
export declare function shouldReserveP0AfterStaffBurst(consecutiveStaffClaims: number, antiStarvationRatio: number, hasP0Candidate: boolean): boolean;
/**
 * When no tracked active work exists, the first catalog fallback already
 * probes the full published catalog.  A second identical probe in the same
 * claim attempt only doubles YSQL pressure when it returns no row.
 */
export declare function shouldRunCatalogFallbackAgain(activeWorkIdsEmpty: boolean, alreadyAttempted: boolean): boolean;
/**
 * Catalog fallback is deliberately serialized because each probe performs
 * distributed Yugabyte reads.  A caller may try again after the interval when
 * the previous probe found no row, but concurrent probes must not pile up on
 * the bounded pool.
 */
export declare function shouldStartCatalogProbe(nowMs: number, lastProbeAtMs: number, inFlight: boolean, minIntervalMs?: number): boolean;
/**
 * Staff and P0 are order-only lanes, but an endless stream of either must not
 * make already-admitted P1/P2 work mathematically impossible to finish.  One
 * normal-lane claim after a bounded high-priority burst preserves the normal
 * resource budget and gives lower lanes forward progress without weakening
 * their usual priority when the burst has not happened.
 */
export declare function shouldReserveLowerPriorityAfterHighBurst(consecutiveHighPriorityClaims: number, antiStarvationRatio: number): boolean;
export interface AcquiredSchedulerJob {
    job: any;
    lane: SchedulerLane;
    decision: SchedulerDecision;
}
export declare class WorkAffinityScheduler {
    private stateStore;
    private admissionController;
    private protectiveSentinel;
    private logger;
    private pool;
    private inFlightByWork;
    private inFlightChapterKeys;
    private pendingClaimReservationsByWork;
    private staffConsecutiveClaims;
    private highPriorityConsecutiveClaims;
    private rrIndexP0;
    private rrIndexP1;
    private rrIndexP2;
    private rrCatalogSourceIndex;
    private catalogProbeInFlight;
    private lastCatalogProbeAt;
    private readonly catalogProbeMinIntervalMs;
    private publicationBarrier?;
    private sourcePermitProvider?;
    private chapterCapacityProvider;
    setPublicationBarrier(barrier: any): void;
    setSourcePermitProvider(provider: (source: string) => number): void;
    setChapterCapacityProvider(provider: () => number): void;
    private lastStaffCheckTime;
    private cachedStaffWorkIds;
    private lastStaffCandidateProbeAt;
    private hasStaffCandidate;
    private staffCandidateProbeFlight;
    private readonly staffCandidateProbeTtlMs;
    private unclaimableWorksCooldown;
    private staleActiveWorkClaimMisses;
    markWorkUnclaimable(workId: string, ttlMs?: number): void;
    isWorkUnclaimable(workId: string): boolean;
    clearWorkUnclaimable(workId: string): void;
    private noteStaleActiveWorkClaimMiss;
    private p0WaitTimes;
    private p0Count1h;
    private p1Count1h;
    private p2Count1h;
    private lastClaimTime;
    private lastCompletionTime;
    private lastAnyPublicationTime;
    private lastFreshReleaseTime;
    private lastBackfillPublicationTime;
    specificClaimAttempts: number;
    specificClaimSuccesses: number;
    genericClaimAttempts: number;
    genericClaimSuccesses: number;
    emptyClaimAttempts: number;
    private lastP0ProbeAt;
    private hasP0InQueue;
    private cachedP0WorkIds;
    private p0CandidateProbeFlight;
    private hasP0Candidate;
    /** A bounded, short-lived P0 work list so concurrent releases take turns. */
    private getP0CandidateWorkIds;
    private hasStaffForcedCandidate;
    getClaimStats(): {
        specificAttempts: number;
        specificSuccesses: number;
        genericAttempts: number;
        genericSuccesses: number;
        emptyAttempts: number;
        specificSuccessRate: number;
        genericSuccessRate: number;
    };
    private stagedBlockedWorks;
    markWorkStagedBlocked(workId: string, ttlMs?: number): void;
    isWorkStagedBlocked(workId: string): boolean;
    clearWorkStagedBlocked(workId: string): void;
    constructor(stateStore: SchedulerStateStore, admissionController: AdmissionController, protectiveSentinel: ProtectiveSentinel, pool?: any);
    private runQuery;
    /** Maintenance probes must yield to chapter claims on the bounded pool. */
    private isPoolUnderClaimPressure;
    /**
     * Initializes state and synchronizes in-flight counts from DB.
     */
    initialize(): Promise<void>;
    recordPublication(isFreshRelease: boolean): void;
    recordJobCompletion(): void;
    /**
     * Hydrates publication and activity heartbeats from database on boot.
     * Prevents watchdog blindness across process restarts.
     */
    private hydrateHeartbeatFromDb;
    /**
     * Authoritative calculation of total workers currently executing chapter jobs.
     * Counts SUM of all in-flight jobs across all works, NOT merely Map keys count.
     */
    getTotalInFlight(): number;
    /**
     * Returns list of work IDs that have reached or exceeded MAX_INFLIGHT_PER_WORK.
     * Used to strictly prevent exceeding 2 concurrent jobs per work across all paths.
     */
    getFullInFlightWorkIds(maxInFlight?: number): string[];
    /**
     * Synchronizes in-flight job counts per work from DB at startup.
     */
    syncInFlightCountsFromDb(): Promise<void>;
    /**
     * Main entry point for worker slots claiming IMPORT_CHAPTER jobs.
     */
    acquireNextChapterJob(options: {
        workerId: string;
        leaseDurationMinutes?: number;
        allowedSources?: string[];
    }): Promise<any | null>;
    private completeStaffClaim;
    /**
     * Core intelligent claim logic implementing P0 -> P1 -> P2 -> Fallback.
     */
    private executeIntelligentClaim;
    /**
     * Helper to atomically claim 1 P1 job for ANY existing catalog work with SKIP LOCKED.
     * Strictly restricts to published works (w.published = true) on active, enabled sources.
     * Enforces that P1 work across the catalog is processed before ANY P2 work!
     */
    private executeClaimCatalogQuery;
    /**
     * Helper to atomically claim 1 P1 job for ANY existing catalog work with SKIP LOCKED.
     * Strictly restricts to published works (w.published = true) on active, enabled sources.
     * Enforces that P1 work across the catalog is processed before ANY P2 work!
     * Distributes concurrent worker claims across multiple available sources to prevent lock-step saturation.
     */
    private claimCatalogP1Job;
    /**
     * Helper to atomically claim 1 STAFF_FORCED job with SKIP LOCKED.
     * Priority >= 1000 or payload.staffForced = true or work with active importer_staff_requests.
     * Staff is absolute against other lanes, but active Staff works take turns
     * inside that lane. The small cached list is rotated after each successful
     * claim; this never changes resource limits.
     */
    private claimStaffForcedJob;
    /**
     * Helper to atomically claim 1 job with SKIP LOCKED.
     * Ensures the source is enabled, active, and not in cooldown.
     */
    private claimSingleJob;
    /**
     * Concurrently validates a claimed job outside the global chapterClaimMutex.
     * Checks for already-published canonical chapters and STAGED barriers.
     * If invalid, sanitizes database records and reverts the job to QUEUED.
     */
    validateClaimedJobPostMutex(job: any): Promise<{
        valid: boolean;
        reason?: string;
    }>;
    /**
     * Publication Watchdog & Auto-Recovery Tree (Sections 6, 7, 8, 16).
     * Monitors elapsed time since last publication and real backlog.
     * If any safe backlog exists and no publication occurs for 5m -> WARNING.
     * If no publication occurs for 10m -> Triggers AUTO-RECOVERY routine!
     */
    private startPublicationWatchdog;
    /**
     * Shadow Mode simulation: calculates what the intelligent scheduler would choose,
     * compares with the legacy choice, and returns the legacy job.
     */
    private executeShadowModeSimulation;
    onJobStarted(workId: string, chapterSortKey?: number | null): void;
    onJobFinished(workId: string, chapterSortKey?: number | null): void;
    getInFlightCount(workId: string): number;
    getMaxInflightPerWork(): number;
    getWatermark(workId: string, source: string): Promise<import("./types.js").WorkWatermark | undefined>;
    setWatermark(watermark: any): Promise<void>;
    private logDecision;
    private startMetricsReporter;
    collectMetrics(): Promise<SchedulerMetrics>;
    /**
     * Controlled atomic background cleanup for redundant queue jobs.
     * Safely marks queued/retrying jobs as COMPLETED with CANONICAL_ALREADY_SATISFIED
     * if their canonical chapter is already published in chapters table.
     * Preserves provider mappings and fallbacks without blind DELETES.
     */
    runControlledRedundantJobCleanup(batchSize?: number): Promise<{
        cleaned: number;
    }>;
    /**
     * Moves only already-exhausted QUEUED/RETRY jobs out of the hot queue.
     * Claim queries correctly exclude them, but leaving them there forever
     * makes every scheduler/admission scan pay for terminal work.  This is a
     * bounded, idempotent state transition: it never deletes mappings and
     * never touches an active lease.
     */
    runControlledExhaustedJobCleanup(batchSize?: number): Promise<{
        failed: number;
    }>;
}
