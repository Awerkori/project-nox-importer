import type { Pool } from 'pg';
export interface RateMetrics {
    rate5m: number;
    rate30m: number;
    fresh5m: number;
    fresh30m: number;
    completedRate5m: number;
    completedRate30m: number;
    completed5m: number;
    completed30m: number;
}
export declare class RateBucketTracker {
    private logger;
    private pool;
    private pendingFresh;
    private pendingCompleted;
    private flushTimer;
    private isFlushing;
    private cachedRates;
    private cachedRatesAt;
    private readonly CACHE_TTL_MS;
    constructor(pool: Pool);
    recordFreshPublication(): void;
    recordJobCompletion(): void;
    startPeriodicFlush(intervalMs?: number): void;
    stop(): void;
    flush(): Promise<void>;
    getRecentRates(forceFresh?: boolean): Promise<RateMetrics>;
    pruneOldBuckets(): Promise<number>;
}
