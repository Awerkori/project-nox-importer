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
    runExclusive<T>(fn: () => Promise<T>): Promise<T>;
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
    /**
     * Upgrades the reserved byte budget if Content-Length exceeds initial reservation.
     */
    upgrade(newBytes: number, signal?: AbortSignal): Promise<void>;
    /**
     * Commits actual downloaded bytes into activeBufferedBytes and frees the reserved budget.
     * Defensive invariant: actualBytes must NOT exceed reservedBytes.
     */
    commit(actualBytes: number): void;
    /**
     * Releases the reserved budget on failure, cancellation, or skip without committing.
     */
    release(): void;
}
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
    private activeBufferedBytes;
    private reservedBufferedBytes;
    private maxCommittedBytesObserved;
    private reservationWaiters;
    private cycleErrors;
    private cycleRateLimits;
    private cycleTimeouts;
    constructor(config?: Partial<AutotunerConfig>);
    getGlobalChapterSemaphore(): AsyncSemaphore;
    getGlobalMediaSemaphore(): AsyncSemaphore;
    getGlobalInflightRequestSemaphore(): AsyncSemaphore;
    getBufferedPageSemaphore(): AsyncSemaphore;
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
    evaluateCycle(): {
        concurrency: number;
        action: 'SCALED_UP' | 'SCALED_DOWN' | 'STABLE' | 'COOLDOWN' | 'STRESS_DETECTED';
        reason: string;
    };
    getCurrentConcurrency(): number;
}
