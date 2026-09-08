export declare class AsyncSemaphore {
    private currentPermits;
    private maxPermits;
    private waitQueue;
    constructor(maxPermits: number);
    acquire(): Promise<void>;
    release(): void;
    runExclusive<T>(fn: () => Promise<T>): Promise<T>;
    setCapacity(newCapacity: number): void;
    get capacity(): number;
    get available(): number;
    get active(): number;
    get queued(): number;
}
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
}
export declare class AdaptiveAutotuner {
    private logger;
    private globalChapterSemaphore;
    private sourceSemaphores;
    private globalMediaSemaphore;
    private currentConcurrency;
    private stableCycleCount;
    private cooldownUntil;
    private config;
    private cycleErrors;
    private cycleRateLimits;
    private cycleTimeouts;
    constructor(config?: Partial<AutotunerConfig>);
    getGlobalChapterSemaphore(): AsyncSemaphore;
    getGlobalMediaSemaphore(): AsyncSemaphore;
    getSourceSemaphore(source: string, limitPerSource?: number): AsyncSemaphore;
    recordError(type: 'error' | 'ratelimit' | 'timeout'): void;
    evaluateCycle(): {
        concurrency: number;
        action: 'SCALED_UP' | 'SCALED_DOWN' | 'STABLE' | 'COOLDOWN';
        reason: string;
    };
    getCurrentConcurrency(): number;
}
