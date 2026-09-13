export declare class HostRateLimiter {
    private defaultRatePerSecond;
    private buckets;
    private logger;
    private turboMode;
    constructor(defaultRatePerSecond?: number);
    setHostRate(host: string, ratePerSecond: number, capacity?: number, maxRatePerSecond?: number, minRatePerSecond?: number): void;
    private getBucket;
    recordSuccess(host: string): void;
    setTurboMode(enabled: boolean): void;
    isTurboMode(): boolean;
    getHostRate(host: string): number;
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
    minRequestsPerMinute?: number;
    safetyCeilingRate?: number;
    minIntervalMs?: number;
}
export interface StorageMetricsSummary {
    currentRate: number;
    peakTestedRate: number;
    successfulPagesLastMinute: number;
    mbPerMinute: number;
    avgUploadLatencyMs: number;
    recent429Count: number;
    recent502Count: number;
    isBlocked: boolean;
    blockedRemainingSeconds: number;
}
/**
 * Centralized global rate limiter for Storage Bridge uploads across all sources.
 * Enforces an Adaptive AIMD (Additive Increase, Multiplicative Decrease) control loop:
 * - Increases rate gradually (+2 req/min) ONLY when real throughput (pages/min) increases.
 * - Detects throughput plateau with hysteresis to avoid ratcheting into rate limits.
 * - Enforces immediate Multiplicative Decrease (-20%) and cooldown upon receiving HTTP 429.
 * - Configurable operational safety ceiling against runaway telemetry.
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
    private minRatePerMinute;
    private safetyCeilingRate;
    private peakTestedRate;
    private inCooldownUntil;
    private consecutiveSuccessfulUploads;
    private windowStartTime;
    private windowUploadCount;
    private windowBytesSum;
    private windowDurationSumMs;
    private previousWindowRate;
    private recentUploadTimestamps;
    private recentTransientErrors;
    private total429Count;
    private total502Count;
    constructor(config?: StorageRateLimiterConfig);
    /**
     * Acquire an upload token before sending an image to the Storage Bridge.
     * Blocks if the rate limit or pacing threshold is reached.
     */
    acquire(): Promise<void>;
    /**
     * Record a successful upload with page size and latency metrics.
     * Evaluates real throughput (pages/min) over 30s windows with plateau detection:
     * - If real throughput increased: scales up rate (+2 req/min).
     * - If real throughput plateaued: stops increasing to avoid inducing 429.
     * - If safety ceiling reached: emits SAFETY_CEILING_REACHED.
     */
    recordSuccess(bytes?: number, durationMs?: number): void;
    /**
     * Handle rate limits (HTTP 429, FloodWait, Retry-After) reported by the Storage Bridge.
     * Multiplicative decrease (-20%) and cooldown.
     * Pauses ONLY the Storage Bridge; the rest of the Importer remains fully active.
     */
    recordRateLimit(retryAfterSeconds?: number): void;
    /**
     * Track transient upstream errors (502/503/network) from the Storage Bridge.
     * Isolated failures do NOT block or pause the global rate limiter.
     * Transient errors adjust rate target slightly if repeated, but NEVER place
     * the global rate limiter into a hard blockedUntil lock (which is reserved for 429).
     */
    recordTransientError(): void;
    getRecentTransientErrorCount(): number;
    /**
     * Gradually restore rate towards baseRatePerMinute when recovering from throttling
     */
    restoreRate(): void;
    getMetricsSummary(): StorageMetricsSummary;
    getCurrentRatePerMinute(): number;
    getPeakTestedRate(): number;
    getRecentUploadCount(): number;
    isBlocked(): boolean;
    getBlockedRemainingMs(): number;
}
