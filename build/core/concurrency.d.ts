import type { PressureSnapshot, SiteHealthState } from './protective-sentinel.js';
export declare class AsyncSemaphore {
    private activePermits;
    private maxPermits;
    private waitQueue;
    name: string;
    constructor(maxPermits: number, name?: string);
    tryAcquire(): boolean;
    acquire(signal?: AbortSignal): Promise<void>;
    release(): void;
    private drain;
    waitSamples: number[];
    holdSamples: number[];
    runExclusive<T>(fn: () => Promise<T>): Promise<T>;
    getMetrics(): {
        waitP50: number;
        waitP95: number;
        holdP50: number;
        holdP95: number;
        samples: number;
    };
    getLastWaitMs(): number;
    /**
     * Updates semaphore capacity.
     * ABSOLUTE INVARIANT: capacity cannot be set lower than 1.
     * In-flight holders drain naturally; never reissue their permits.
     */
    setCapacity(newCapacity: number): void;
    get capacity(): number;
    get available(): number;
    get active(): number;
    get queued(): number;
}
export declare function withSourceChapterPermits<T>(source: AsyncSemaphore, global: AsyncSemaphore, fn: () => Promise<T>, signal?: AbortSignal): Promise<T>;
export interface SourceConcurrencyConfig {
    maxChapters: number;
    maxPagesPerChapter: number;
}
export declare const SOURCE_CONCURRENCY_LIMITS: Record<string, SourceConcurrencyConfig>;
export declare const DEFAULT_SOURCE_LIMIT: SourceConcurrencyConfig;
export declare const TESTED_CONCURRENCY_CEILING = 32;
export interface AutotunerConfig {
    minConcurrency: number;
    maxConcurrency: number;
    initialConcurrency: number;
    requiredStableCycles: number;
    cooldownPeriodMs: number;
    maxRssMb: number;
    maxHeapMb: number;
    maxExternalAndBuffersMb: number;
    maxEventLoopLagMs: number;
    rssSoftLimitMb: number;
    rssHardLimitMb: number;
    rssEmergencyLimitMb: number;
    maxBufferedBytes: number;
    adaptiveEnabled: boolean;
    scaleUpDwellTimeMs: number;
    desiredFloorFreshPerMin?: number;
    optimalFreshPerMinLow?: number;
    optimalFreshPerMinHigh?: number;
    preferredFreshPerMin?: number;
    maxFreshPerMin?: number;
    catastrophicSiteLatencyMs?: number;
}
export type AdaptiveCapacityState = 'RUNNING_ACCELERATING' | 'RUNNING_STABLE' | 'RUNNING_THROTTLED' | 'SURVIVAL' | 'WAITING_DEPENDENCY' | 'WAITING_SOURCES' | 'RECOVERING' | 'MANUAL_STOP' | 'AUTO_EMERGENCY_PAUSE' | 'THROUGHPUT_CONSTRAINED' | 'RUNNING_BELOW_TARGET' | 'RUNNING_OPTIMAL' | 'RUNNING_PREFERRED' | 'CEILING_REACHED';
export type ThroughputStatus = 'STALL' | 'THROUGHPUT_CONSTRAINED' | 'RUNNING_BELOW_TARGET' | 'RUNNING_OPTIMAL' | 'RUNNING_PREFERRED' | 'CEILING_REACHED';
export interface ThroughputTelemetry {
    rate1m: number;
    rate3m: number;
    rate5m: number;
    emaRate: number;
    completedJobs1m: number;
    completedJobs5m: number;
    targetFloor: number;
    optimalLow: number;
    optimalHigh: number;
    preferredHigh: number;
    ceiling: number;
    status: ThroughputStatus;
    limitingFactor: string | null;
}
export interface AutotunerCycleResult {
    concurrency: number;
    targetConcurrency?: number;
    action: 'SCALED_UP' | 'SCALED_DOWN' | 'STABLE' | 'HOLD' | 'COOLDOWN' | 'STRESS_DETECTED' | 'SURVIVAL';
    state: AdaptiveCapacityState;
    reason: string;
    pressureScore: number;
    pressureBreakdown: PressureSnapshot['pressureBreakdown'];
    siteHealth: SiteHealthState;
}
export interface AutotunerEvaluationContext {
    allSourcesBlocked?: boolean;
    dbUnavailable?: boolean;
    manualStopActive?: boolean;
    emergencyPauseActive?: boolean;
    emergencyPauseReason?: string;
    stagedDebt?: number;
    storageUnavailable?: boolean;
    eligibleJobs?: number;
}
export declare class WorkCostEstimator {
    static estimateCost(pageCount?: number | null, historicalBytes?: number | null): number;
}
export declare class BufferReservation {
    private autotuner;
    private _reservedBytes;
    private _released;
    private _committed;
    constructor(autotuner: AdaptiveAutotuner, _reservedBytes: number);
    get reservedBytes(): number;
    get isCommitted(): boolean;
    get isReleased(): boolean;
    upgrade(newBytes: number, signal?: AbortSignal): Promise<void>;
    commit(actualBytes: number): void;
    release(): void;
}
/**
 * AdaptiveAutotuner: The SINGLE Authority for Global Chapter Concurrency.
 * INVARIANT: GLOBAL_CONCURRENCY_WRITERS = 1.
 * Automatic performance stop is strictly prohibited; capacity never drops below 1.
 */
export declare class AdaptiveAutotuner {
    private logger;
    private globalChapterSemaphore;
    private sourceSemaphores;
    private globalMediaSemaphore;
    private globalInflightRequestSemaphore;
    private bufferedPageSemaphore;
    private currentConcurrency;
    private stableCycleCount;
    private cooldownUntil;
    private config;
    private currentState;
    private lastCapacityChangeAt;
    private lastStableConcurrency;
    private lastStableAt;
    private activeBufferedBytes;
    private reservedBufferedBytes;
    private maxCommittedBytesObserved;
    private reservationWaiters;
    private cycleErrors;
    private cycleRateLimits;
    private cycleTimeouts;
    private freshChapterTimestamps;
    private completedJobTimestamps;
    private emaRate;
    private latestResult;
    constructor(config?: Partial<AutotunerConfig>);
    getGlobalChapterSemaphore(): AsyncSemaphore;
    getGlobalMediaSemaphore(): AsyncSemaphore;
    getGlobalInflightRequestSemaphore(): AsyncSemaphore;
    getBufferedPageSemaphore(): AsyncSemaphore;
    recordFreshChapterPublished(count?: number): void;
    recordJobCompleted(): void;
    getRate1m(): number;
    getRate3m(): number;
    getRate5m(): number;
    getCompletedRate1m(): number;
    getCompletedRate5m(): number;
    getEmaRate(): number;
    getThroughputTelemetry(context?: AutotunerEvaluationContext): ThroughputTelemetry;
    canAdmitReservation(requestedBytes: number): boolean;
    reserveBufferBudget(requestedBytes?: number, signal?: AbortSignal): Promise<BufferReservation>;
    upgradeReservation(additionalBytes: number, signal?: AbortSignal): Promise<void>;
    commitReservation(reservedBytes: number, actualBytes: number): void;
    releaseReservation(reservedBytes: number): void;
    releaseActiveBufferedBytes(actualBytes: number): void;
    private drainReservationWaiters;
    trackBufferedBytes(bytes: number): void;
    releaseBufferedBytes(bytes: number): void;
    getBufferedBytes(): number;
    getReservedBytes(): number;
    getCommittedBytes(): number;
    private updateMaxCommittedObserved;
    getMaxCommittedBytesObserved(): number;
    waitForMemoryHeadroom(estimatedBytes?: number, signal?: AbortSignal): Promise<void>;
    getSourceLimits(source: string): SourceConcurrencyConfig;
    getSourcePageConcurrency(source: string): number;
    getSourceSemaphore(source: string, limitPerSource?: number): AsyncSemaphore;
    isSourceCapacityAvailable(source: string): boolean;
    private sourceHealth;
    recordSourceFailure(source: string): {
        throttled: boolean;
        newCapacity: number;
    };
    recordSourceSuccess(source: string): {
        restored: boolean;
        newCapacity: number;
    };
    recordError(type: 'error' | 'ratelimit' | 'timeout'): void;
    /**
     * Evaluates system pressure and adjusts global chapter concurrency.
     * Single authority: FAST DOWN, SLOW UP, HYSTERESIS, DWELL TIME, MIN_CONCURRENCY = 1.
     */
    evaluateCycle(pressureSnapshot?: PressureSnapshot, context?: AutotunerEvaluationContext): AutotunerCycleResult;
    private applyCapacityChange;
    getCurrentConcurrency(): number;
    getAdaptiveState(): AdaptiveCapacityState;
    getState(): AdaptiveCapacityState;
    getMaxConcurrency(): number;
    getLatestResult(): AutotunerCycleResult;
    getLastStableConcurrency(): number;
    setCapacity(newCapacity: number, stateOrReason?: AdaptiveCapacityState | string, optionalReason?: string): void;
}
