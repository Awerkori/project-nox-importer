import type { Pool } from 'pg';
import { type WorkAffinityScheduler } from './scheduler/work-affinity-scheduler.js';
import type { AdmissionController } from './scheduler/admission-controller.js';
import type { ProtectiveSentinel } from './protective-sentinel.js';
import type { PublicationBarrier } from './publication.js';
import type { PublicationSafetyBarrier } from './publication-safety-barrier.js';
import type { AdaptiveAutotuner } from './concurrency.js';
export type ImporterHealthStatus = 'HEALTHY' | 'DEGRADED' | 'STALLED' | 'CRITICAL_STALL' | 'IDLE' | 'PAUSED_BY_PROTECTION';
export type ProcessingHealth = 'HEALTHY' | 'DEGRADED' | 'STALLED' | 'CRITICAL_STALL';
export type PublicationHealth = 'HEALTHY' | 'DEGRADED' | 'STALLED' | 'CRITICAL_STALL' | 'NO_FRESH_EXPECTED';
export type AutoHealState = 'IDLE' | 'MONITORING' | 'LEVEL_1_LIGHT_RECONCILIATION' | 'LEVEL_2_STUCK_STATE_AUDIT' | 'LEVEL_3_RESTART_PENDING' | 'LEVEL_3_RESTART_DEFERRED' | 'CIRCUIT_OPEN' | 'RECOVERED';
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
    stuckStagedAgeSec?: number;
    classifiedStaged?: number;
    unclassifiedStaged?: number;
    classificationCursor?: {
        lastFrontierSortKey: number | null;
        lastWorkId: string | null;
    } | null;
    classifiedThisCycle?: number;
    classificationCoverageEstimate?: number;
    oldestUnclassifiedAge?: number;
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
    noProgressReason?: string | null;
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
export declare function isCountedAutoRestart(record: AutoRestartRecord): boolean;
/**
 * Heavy staged-frontier diagnostics may yield to real chapter claims when a
 * previous snapshot is available.  This is deliberately a pure policy helper
 * so the pressure gate cannot regress silently.
 */
export declare function shouldDeferHeavyStagedClassification(params: {
    cachedTelemetryAvailable: boolean;
    healthyState: boolean;
    claimPressureHigh: boolean;
}): boolean;
export interface AutoHealWatchdogOptions {
    pool: Pool;
    scheduler?: WorkAffinityScheduler;
    admissionController?: AdmissionController;
    protectiveSentinel?: ProtectiveSentinel;
    publicationBarrier?: PublicationBarrier;
    safetyBarrier?: PublicationSafetyBarrier;
    autotuner?: AdaptiveAutotuner;
    /**
     * Returns true while the bounded chapter-claim phase is under pressure.
     * Heavy staged classification must yield to real chapter claims because it
     * shares the same bounded YSQL pool.
     */
    isChapterClaimPressureHigh?: () => boolean;
    /**
     * Returns false when the engine cannot safely quiesce. A deferred recovery
     * is deliberately not counted as a restart: counting it would open the
     * restart circuit even though no restart actually occurred.
     */
    onControlledRestart?: (reason: string, metrics: HealthPanelMetrics) => Promise<boolean | void>;
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
    private safetyBarrier?;
    private autotuner?;
    private isChapterClaimPressureHigh?;
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
    private lastDeferredRestartAt;
    private lastSweepAt;
    private lastCircuitContainmentAt;
    private stuckIdentities;
    private circuitBreakerOpen;
    private cachedTelemetry;
    private telemetryFlight;
    private lastTelemetryAt;
    private telemetryCacheTtlMs;
    private lastDeepStagedAt;
    private lastStaleMappingRecoveryAt;
    private staleMappingRecoveryFlight;
    private classificationCursor;
    private lastCoverageResetAt;
    private classifiedSinceReset;
    constructor(options: AutoHealWatchdogOptions);
    getStuckIdentityAge(key: string): number;
    setStuckIdentity(key: string, detectedAtMs: number): void;
    clearStuckIdentities(): void;
    getClassificationCursor(): {
        lastFrontierSortKey: number | null;
        lastWorkId: string | null;
    } | null;
    setClassificationCursor(cursor: {
        lastFrontierSortKey: number | null;
        lastWorkId: string | null;
    } | null): void;
    removeStuckIdentity(key: string): void;
    getTrackedStuckKeys(): string[];
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
    collectTelemetry(forceFresh?: boolean): Promise<HealthPanelMetrics>;
    private collectTelemetrySnapshot;
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
        unclassifiedStaged?: number;
        stuckStagedAgeSec?: number;
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
        unclassifiedStaged?: number;
        stuckStagedAgeSec?: number;
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
     * Level 1 (STALLED >= 8m): Light reconciliation
     * Level 2 (STALLED >= 10m): Stuck state audit (expired leases, zombie active works)
     * Level 3 (sustained stall >= 12m): Controlled graceful self-restart
     */
    executeRecoveryLadder(metrics: HealthPanelMetrics): Promise<void>;
    /**
     * The old circuit breaker made a stalled importer quieter after three
     * failed restarts: it forced survival capacity and then waited for a human.
     * Keep the loop guard, but continue low-frequency recovery.  This uses the
     * existing controlled drain/cancellation path and never changes resource
     * ceilings or retries at a high cadence.
     */
    private runCircuitContainment;
    private reconcileStaleChapterMappings;
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
