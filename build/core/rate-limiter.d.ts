export declare class HostRateLimiter {
    private defaultRatePerSecond;
    private buckets;
    private logger;
    constructor(defaultRatePerSecond?: number);
    setHostRate(host: string, ratePerSecond: number, capacity?: number): void;
    private getBucket;
    /**
     * Acquire a token for host with jitter and sleep if necessary
     */
    acquire(host: string): Promise<void>;
    /**
     * Handle HTTP 429 response by honoring Retry-After or applying exponential backoff
     */
    handle429(host: string, retryAfterHeader?: string | null, attemptNumber?: number): number;
    private sleep;
}
