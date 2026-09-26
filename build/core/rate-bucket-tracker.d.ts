import type { Pool } from 'pg';
export interface RateMetrics {
    rate5m: number;
    rate30m: number;
    visible5m: number;
    visible30m: number;
    freshRate5m: number;
    freshRate30m: number;
    fresh5m: number;
    fresh30m: number;
    backfill5m: number;
    backfill30m: number;
    completedRate5m: number;
    completedRate30m: number;
    completed5m: number;
    completed30m: number;
}
export declare class RateBucketTracker {
    private logger;
    private pool;
    private pendingVisible;
    private pendingFresh;
    private pendingCompleted;
    private flushTimer;
    private isFlushing;
    private cachedRates;
    private cachedRatesAt;
    private readonly CACHE_TTL_MS;
    constructor(pool: Pool);
    /**
     * Records a visible publication on the site.
     * If isFreshRelease is true, it is also credited as a genuine fresh release.
     */
    recordVisiblePublication(isFreshRelease?: boolean): void;
    /**
     * Backward-compatible alias for genuine fresh releases.
     */
    recordFreshPublication(): void;
    recordJobCompletion(): void;
    startPeriodicFlush(intervalMs?: number): void;
    stop(): void;
    flush(): Promise<void>;
    getRecentRates(forceFresh?: boolean): Promise<RateMetrics>;
    pruneOldBuckets(): Promise<number>;
}
