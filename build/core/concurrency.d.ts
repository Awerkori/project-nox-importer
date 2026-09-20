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
    private cycleErrors;
    private cycleRateLimits;
    private cycleTimeouts;
    constructor(config?: Partial<AutotunerConfig>);
    getGlobalChapterSemaphore(): AsyncSemaphore;
    getGlobalMediaSemaphore(): AsyncSemaphore;
    getGlobalInflightRequestSemaphore(): AsyncSemaphore;
    getBufferedPageSemaphore(): AsyncSemaphore;
    getSourceLimits(source: string): SourceConcurrencyConfig;
    getSourcePageConcurrency(source: string): number;
    getSourceSemaphore(source: string, limitPerSource?: number): AsyncSemaphore;
    isSourceCapacityAvailable(source: string): boolean;
    recordError(type: 'error' | 'ratelimit' | 'timeout'): void;
    evaluateCycle(): {
        concurrency: number;
        action: 'SCALED_UP' | 'SCALED_DOWN' | 'STABLE' | 'COOLDOWN';
        reason: string;
    };
    getCurrentConcurrency(): number;
}
