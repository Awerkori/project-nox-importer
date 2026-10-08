/**
 * Admission Controller for Project Nox Work-Oriented Scheduler.
 *
 * Responsibilities:
 * 1. Maintains bounded active sets (ACTIVE_BACKFILL_WORKS <= 10, ACTIVE_NEW_WORKS <= 8).
 * 2. Sliding window admission: promotes 5-8 chapters per active work to QUEUED, keeping
 *    the executable queue small (~150 jobs), fast, and contention-free.
 * 3. Work lifecycle transitions (NEW -> FILLING -> CAUGHT_UP / COMPLETE / BLOCKED).
 * 4. Critical Gap & Barrier Frontier detection (BARRIER_UNBLOCK_SCORE).
 * 5. Source diversity (prevents active set concentration on a single source).
 * 6. Adheres strictly to PROTECTIVE_STOP and auto-healing circuit breakers.
 */
import { ProtectiveSentinel } from '../protective-sentinel.js';
import { SchedulerStateStore } from './state-store.js';
import { ActiveWork } from './types.js';
export declare class AdmissionController {
    private stateStore;
    private protectiveSentinel;
    private logger;
    private pool;
    private isRunning;
    private loopTimer;
    private sourcePermitProvider?;
    private chapterCapacityProvider;
    private p0CandidateProvider?;
    private inFlightChapterCountProvider?;
    setChapterCapacityProvider(provider: () => number): void;
    setP0CandidateProvider(provider: () => Promise<boolean> | boolean): void;
    setInFlightChapterCountProvider(provider: () => number): void;
    private admissionInFlight;
    private demandFlights;
    private deadWorksCache;
    private admissionOperationTail;
    private p1BacklogProbeAt;
    private p1BacklogProbeFlight;
    private p1BacklogSnapshot;
    private lastOnDemandP1Source;
    private p1SourceWindowCursor;
    private readonly p1SourceWindowSize;
    private visibleP2LifecycleRepairComplete;
    private orphanCancelledRecoveryAt;
    private legacyTransientFailureRecoveryAt;
    private sourceRecoveredFailureRecoveryAt;
    private claimPressureSince;
    private lastPressureMaintenanceAt;
    private static readonly PRESSURE_MAINTENANCE_AFTER_MS;
    private static readonly PRESSURE_MAINTENANCE_INTERVAL_MS;
    private getP1AdmissionCursors;
    private advanceP1AdmissionCursor;
    setSourcePermitProvider(provider: (source: string) => number): void;
    private getP1SourceWindow;
    constructor(stateStore: SchedulerStateStore, protectiveSentinel: ProtectiveSentinel, pool?: any);
    private runQuery;
    /**
     * Slow admission stages can be either a distributed SQL read or time spent
     * waiting for the intentionally small YSQL pool.  Keep that distinction at
     * the query boundary for the few frontier probes that determine whether an
     * idle chapter slot can receive work.  This is diagnostic only: it executes
     * the same SQL and parameters as runQuery and logs no row data.
     */
    private runTimedAdmissionQuery;
    private getCurrentInFlightChapterCount;
    private getActiveWorksCount;
    /**
     * Admission is control-plane work.  Never start a broad reconciliation or
     * recovery query while the bounded pool is already servicing/waiting for
     * chapter claims.  Claims are the work-conserving data plane; letting a
     * maintenance scan take the last idle connection can leave every chapter
     * slot parked in WAITING_CLAIM_DB.  The next cycle retries automatically
     * once the pool drains, so this is backpressure, not a disabled recovery.
     */
    private isPoolUnderClaimPressure;
    private shouldRunPressureMaintenance;
    /**
     * Keep one executable P1 chapter per active work. Older scheduler versions
     * could leave an entire backfill window (or more) QUEUED, then replenish it
     * before it drained. That made the durable source cursor fair only on
     * paper: a large work could retain its cohort position indefinitely.
     *
     * This is work-scoped, idempotent and touches no P0/Staff row. Remaining
     * chapters remain PAUSED_BY_STAFF and are re-admitted through the ordinary
     * per-source cursor. A retry remains ahead of a new promotion so retry and
     * frontier safety retain their existing semantics.
     */
    private enforceP1FairWindow;
    /**
     * Periodic reconciliation is the fallback; real vacancies trigger an
     * immediate coalesced cycle. Keep this cadence low enough that aggregate
     * queue scans do not compete with claims and publication.
     */
    start(): void;
    stop(): void;
    private immediateReplenishTimer;
    private isReplenishingCycle;
    private scheduleNextCycle;
    /**
     * Triggers immediate admission reconciliation and replenishment.
     * Debounced with 50ms trailing window to collapse concurrent vacate events.
     */
    private lastVacancyReplenishAt;
    triggerImmediateReplenishment(reason: string): void;
    /**
     * Section 5: Admission Gate Obrigatório
     *
     * CAN_ADMIT_NEW_WORK =
     *   NO_P0_WAITING
     *   AND NO_HEALTHY_P1_CLAIMABLE
     *   AND P2_ACTIVE_COHORT_BELOW_LIMIT
     *   AND SYSTEM_HEALTHY
     *
     * Se false: obra permanece WAITING_ADMISSION.
     */
    canAdmitNewWork(options?: {
        allowDuringClaimPressure?: boolean;
    }): Promise<{
        allowed: boolean;
        reason: string;
        metrics: {
            p0Waiting: number;
            p1Claimable: number;
            p1AvailableChapters: number;
            p1WorksWaiting: number;
            p2ActiveCohortSize: number;
            p2UnfinishedCount: number;
            systemHealthy: boolean;
        };
    }>;
    /**
     * A bounded health-aware P1 admission signal. This is an existence check,
     * not a catalog aggregate: admission only needs to know whether P1 must go
     * first, and a full COUNT(DISTINCT ...) scan would compete with imports.
     */
    private getP1BacklogSnapshot;
    /** Promote the remaining non-terminal initial batch of one visible work. */
    private promoteP2WorkToP1;
    /**
     * Reclassify visible works that an older scheduler stranded below P1.
     *
     * The candidate set starts from bounded ready queue rows, so it also covers
     * mappings whose sync state is SYNCED rather than ACTIVE. Each cycle touches
     * at most 12 works and is naturally idempotent because promoted rows no
     * longer match priority <75. The matching initial window is reopened in
     * the same statement so the normal
     * indexed P1 claim path can immediately see the repaired work.
     */
    private repairVisibleP2LifecycleBacklog;
    /**
     * Requeue legacy cancellation rows only when the canonical mapping is still
     * executable and there is no active staff request for the work.  The strict
     * null metadata predicates are intentional: explicit staff cancellations
     * remain untouched.  Keep the batch bounded and rate-limited so recovery
     * cannot turn into a queue-wide scan or compete with claims.
     */
    private recoverOrphanedCancelledChapterJobs;
    /**
     * Reopen failures produced by the old controlled-recovery path.  That path
     * cancelled in-flight work during a process recovery and then exhausted the
     * normal retry budget, even when the source is healthy again.  Only its
     * exact diagnostic marker is eligible here; permanent media/identity/source
     * failures remain terminal.  Queue and mapping are repaired in one bounded
     * statement so the canonical frontier cannot observe a half-recovered pair.
     */
    private recoverLegacyTransientFailures;
    /**
     * Reopen a transient frontier only after its source has demonstrably
     * recovered.  A failed predecessor can otherwise leave every later
     * canonical chapter behind a permanent barrier even though the source is
     * healthy again.  The source-row update is the recovery edge: a retry is
     * eligible once, and a second failure is not reopened until a newer source
     * probe records another recovery.  Permanent failures and failures without
     * a source recovery marker remain terminal.
     */
    private recoverSourceRecoveredTransientFailures;
    /**
     * Executes a single admission reconciliation cycle.
     */
    runAdmissionCycle(): Promise<void>;
    private enqueueAdmissionOperation;
    /**
     * Control-plane admission intentionally serializes its work on the small
     * YSQL pool.  Keep stage timing local to the operation so a slow cycle can
     * be attributed without changing that serialization or retaining state.
     */
    private timeAdmissionStage;
    private logSlowAdmissionStages;
    private executeAdmissionCycle;
    private runPressureMaintenance;
    /**
     * Step 1: Reconciles all currently tracked active works.
     * Updates their progress, checks if they reached CAUGHT_UP, detects barrier gaps.
     */
    private reconcileActiveWorks;
    /**
     * Step 2: Replenishes active sets (P1 Backfill and P2 New Works) if slots are free.
     * Work-conserving: considers actual worker utilization and elastic capacity.
     */
    private replenishActiveSets;
    /**
     * Step 3: Maintains sliding windows for active works.
     * When an active work has fewer than `slidingWindowMin` queued chapters,
     * promotes the next batch (up to `slidingWindowSize`) from PAUSED_BY_STAFF to QUEUED.
     */
    private maintainSlidingWindows;
    /**
     * On-demand admission: admits the highest priority waiting work into the active set
     * when workers are idle and currently active works cannot supply jobs.
     * Work-conserving and strictly controlled: preserves work-affinity, fairness, and sliding window.
     */
    admitNextWorkOnDemand(preferredLane?: 'P1' | 'P2', allowedSources?: string[]): Promise<ActiveWork | null>;
    private executeOnDemandAdmission;
}
