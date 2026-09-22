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
    private p0ConsecutiveClaims;
    private rrIndexP1;
    private rrIndexP2;
    private p0WaitTimes;
    private p0Count1h;
    private p1Count1h;
    private p2Count1h;
    private lastClaimTime;
    private lastCompletionTime;
    private lastAnyPublicationTime;
    private lastFreshReleaseTime;
    private lastBackfillPublicationTime;
    private watchdogRunning;
    constructor(stateStore: SchedulerStateStore, admissionController: AdmissionController, protectiveSentinel: ProtectiveSentinel, pool?: any);
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
    private syncInFlightCountsFromDb;
    /**
     * Main entry point for worker slots claiming IMPORT_CHAPTER jobs.
     */
    acquireNextChapterJob(options: {
        workerId: string;
        leaseDurationMinutes?: number;
        allowedSources?: string[];
    }): Promise<any | null>;
    /**
     * Core intelligent claim logic implementing P0 -> P1 -> P2 -> Fallback.
     */
    private executeIntelligentClaim;
    /**
     * Helper to atomically claim 1 P1 job for ANY existing catalog work with SKIP LOCKED.
     * Strictly restricts to published works (w.published = true) on active, enabled sources.
     * Enforces that P1 work across the catalog is processed before ANY P2 work!
     */
    private claimCatalogP1Job;
    /**
     * Helper to atomically claim 1 job with SKIP LOCKED.
     * Ensures the source is enabled, active, and not in cooldown.
     */
    private claimSingleJob;
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
}
