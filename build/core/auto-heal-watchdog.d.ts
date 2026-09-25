import type { Pool } from 'pg';
import type { WorkAffinityScheduler } from './scheduler/work-affinity-scheduler.js';
import type { AdmissionController } from './scheduler/admission-controller.js';
import type { ProtectiveSentinel } from './protective-sentinel.js';
import type { PublicationBarrier } from './publication.js';
export type ImporterHealthStatus = 'HEALTHY' | 'DEGRADED' | 'STALLED' | 'CRITICAL_STALL' | 'IDLE' | 'PAUSED_BY_PROTECTION';
export type ProcessingHealth = 'HEALTHY' | 'DEGRADED' | 'STALLED' | 'CRITICAL_STALL';
export type PublicationHealth = 'HEALTHY' | 'DEGRADED' | 'STALLED' | 'CRITICAL_STALL' | 'NO_FRESH_EXPECTED';
export type AutoHealState = 'IDLE' | 'MONITORING' | 'LEVEL_1_LIGHT_RECONCILIATION' | 'LEVEL_2_STUCK_STATE_AUDIT' | 'LEVEL_3_RESTART_PENDING' | 'CIRCUIT_OPEN' | 'RECOVERED';
export interface HealthPanelMetrics {
    status: ImporterHealthStatus;
    autoHealState: AutoHealState;
    processingHealth: ProcessingHealth;
    publicationHealth: PublicationHealth;
    lastStartedAgeSec: number;
    lastCompletedAgeSec: number;
    lastFreshVisibleAgeSec: number;
    startedLast15m: number;
    completedLast15m: number;
    freshLast15m: number;
    eligibleJobs: number;
    claimableWorks: number;
    activeWorksCount: number;
    zombieWorksCount: number;
    importingCount: number;
    retryCount: number;
    stagedUnique: number;
    publishableStaged: number;
    waitingPredecessorStaged: number;
    stuckStaged: number;
    recentCorrelatedBreakdown?: {
        alreadyCanonical: number;
        dedupeSource: number;
        freshPublished: number;
        freshExpected: number;
    };
    lastAutoHealAt: string | null;
    autoRestartCount1h: number;
    circuitBreakerOpen: boolean;
    protectiveStopActive: boolean;
    protectiveStopReason?: string | null;
    rssMb: number;
    pid: number;
    timestamp: string;
}
export interface AutoRestartRecord {
    timestamp: string;
    reason: string;
    progressAgeSec: number;
    eligibleJobs: number;
}
export interface AutoHealWatchdogOptions {
    pool: Pool;
    scheduler?: WorkAffinityScheduler;
    admissionController?: AdmissionController;
    protectiveSentinel?: ProtectiveSentinel;
    publicationBarrier?: PublicationBarrier;
    onControlledRestart?: (reason: string, metrics: HealthPanelMetrics) => Promise<void>;
    intervalMs?: number;
    workerId?: string;
}
/**
 * AutoHealWatchdog
 *
 * Implements permanent autonomous recovery for Project Nox Importer:
 * - Real-progress based health classification (HEALTHY, DEGRADED, STALLED, CRITICAL_STALL, IDLE, PAUSED_BY_PROTECTION)
 * - Silent stall detection (workers alive + eligible > 0 but 0 completions => STALL)
 * - Escalated recovery ladder:
 *     Level 1: Light reconciliation (scheduler state, active works, cooldowns, caches, in-flight, admission, publication sweep)
 *     Level 2: Stuck state reconciliation (expired leases >15m, zombie active works eviction)
 *     Level 3: Controlled graceful self-restart (circuit breaker protected: max 1/15m, max 3/1h)
 * - Circuit breaker protection to prevent restart loops
 * - Complete data safety preservation (canonical ordering, barriers, gap safety)
 */
export declare class AutoHealWatchdog {
    private logger;
    private pool;
    private scheduler?;
    private admissionController?;
    private protectiveSentinel?;
    private publicationBarrier?;
    private onControlledRestart?;
    private intervalMs;
    private workerId;
    private isRunning;
    private stopSignal;
    private timer;
    private autoHealState;
    private lastAutoHealAt;
    private lastLevel1At;
    private lastLevel2At;
    private lastRestartAt;
    private circuitBreakerOpen;
    constructor(options: AutoHealWatchdogOptions);
    /**
     * Starts the background evaluation loop.
     */
    start(): void;
    /**
     * Stops the background loop cleanly.
     */
    stop(): void;
    /**
     * Collects real-time telemetry from database and memory.
     */
    collectTelemetry(): Promise<HealthPanelMetrics>;
    /**
     * Deterministic Multidimensional Health evaluation separating processing and publication health.
     */
    evaluateMultidimensionalHealth(params: {
        eligibleJobs: number;
        importingCount: number;
        lastCompletedAgeSec: number;
        lastFreshVisibleAgeSec: number;
        protectiveStopActive: boolean;
        protectiveStopReason?: string | null;
        protectiveStopTriggeredAt?: string | null;
        recentCompletionsAreDedupeOnly?: boolean;
        hasStagedPublications?: boolean;
        publishableStaged?: number;
        waitingPredecessorStaged?: number;
        stuckStaged?: number;
    }): {
        status: ImporterHealthStatus;
        processingHealth: ProcessingHealth;
        publicationHealth: PublicationHealth;
    };
    /**
     * Deterministic Health Status evaluation based on REAL PROGRESS (backward-compatible).
     */
    determineHealthStatus(params: {
        eligibleJobs: number;
        importingCount: number;
        lastCompletedAgeSec: number;
        lastFreshVisibleAgeSec: number;
        protectiveStopActive: boolean;
        protectiveStopReason?: string | null;
        protectiveStopTriggeredAt?: string | null;
        recentCompletionsAreDedupeOnly?: boolean;
        hasStagedPublications?: boolean;
        publishableStaged?: number;
        waitingPredecessorStaged?: number;
        stuckStaged?: number;
    }): ImporterHealthStatus;
    /**
     * Executes a single evaluation cycle:
     * 1. Collect telemetry & determine status
     * 2. Persist heartbeat / health metrics
     * 3. Trigger Escalated Recovery Ladder if STALLED / CRITICAL_STALL
     */
    evaluateCycle(): Promise<HealthPanelMetrics>;
    /**
     * Escalated Recovery Ladder:
     * Level 1 (STALLED >= 15m): Light reconciliation
     * Level 2 (STALLED >= 20m): Stuck state audit (expired leases, zombie active works)
     * Level 3 (CRITICAL_STALL >= 30m): Controlled graceful self-restart
     */
    executeRecoveryLadder(metrics: HealthPanelMetrics): Promise<void>;
    /**
     * NÍVEL 1 — RECONCILIAÇÃO LEVE
     */
    runLevel1LightReconciliation(metrics: HealthPanelMetrics): Promise<void>;
    /**
     * NÍVEL 2 — ESTADO PRESO
     */
    runLevel2StuckStateAudit(metrics: HealthPanelMetrics): Promise<void>;
    /**
     * Persists health panel to settings table for supervisor, site, and external monitors.
     */
    persistHealthMetrics(metrics: HealthPanelMetrics): Promise<void>;
    /**
     * Records an auto-restart event to settings.importer_auto_restarts.
     */
    recordAutoRestart(record: AutoRestartRecord): Promise<void>;
    /**
     * Reads recent auto-restarters from settings.importer_auto_restarts.
     */
    getRecentAutoRestarts(): Promise<AutoRestartRecord[]>;
}
