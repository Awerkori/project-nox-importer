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
    constructor(stateStore: SchedulerStateStore, protectiveSentinel: ProtectiveSentinel, pool?: any);
    /**
     * Starts the periodic admission background loop (every 5 seconds).
     */
    start(): void;
    stop(): void;
    private scheduleNextCycle;
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
    canAdmitNewWork(): Promise<{
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
     * Executes a single admission reconciliation cycle.
     */
    runAdmissionCycle(): Promise<void>;
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
}
