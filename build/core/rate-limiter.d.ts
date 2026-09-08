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
export interface StorageRateLimiterConfig {
    maxRequestsPerMinute?: number;
    minIntervalMs?: number;
}
/**
 * Centralized global rate limiter for Storage Bridge uploads across all sources.
 * Enforces a strict Token Bucket + Sliding Window rate limit (default 105 req/min)
 * with dynamic backoff upon receiving HTTP 429 or Retry-After headers.
 */
export declare class GlobalStorageRateLimiter {
    private logger;
    private tokens;
    private lastRefill;
    private capacity;
    private ratePerSecond;
    private blockedUntil;
    private minIntervalMs;
    private lastAcquiredTime;
    private currentRatePerMinute;
    private baseRatePerMinute;
    private recentUploadTimestamps;
    constructor(config?: StorageRateLimiterConfig);
    /**
     * Acquire an upload token before sending an image to the Storage Bridge.
     * Blocks if the rate limit or pacing threshold is reached.
     */
    acquire(): Promise<void>;
    /**
     * Handle rate limits (HTTP 429, FloodWait, Retry-After) reported by the Storage Bridge.
     * Immediately blocks subsequent uploads and backs off by reducing rate by 20%.
     */
    recordRateLimit(retryAfterSeconds?: number): void;
    /**
     * Gradually restore rate back towards baseRatePerMinute when operating stably
     */
    restoreRate(): void;
    getCurrentRatePerMinute(): number;
    getRecentUploadCount(): number;
    isBlocked(): boolean;
    getBlockedRemainingMs(): number;
}
