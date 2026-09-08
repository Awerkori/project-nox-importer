import { Logger } from './logger.js';
export class HostRateLimiter {
    defaultRatePerSecond;
    buckets = new Map();
    logger = new Logger('RateLimiter');
    constructor(defaultRatePerSecond = 2.0) {
        this.defaultRatePerSecond = defaultRatePerSecond;
    }
    setHostRate(host, ratePerSecond, capacity) {
        const cap = capacity ?? Math.max(2, Math.ceil(ratePerSecond * 2));
        this.buckets.set(host, {
            tokens: cap,
            lastRefill: Date.now(),
            capacity: cap,
            ratePerSecond,
            blockedUntil: 0,
        });
    }
    getBucket(host) {
        let bucket = this.buckets.get(host);
        if (!bucket) {
            const cap = Math.max(2, Math.ceil(this.defaultRatePerSecond * 2));
            bucket = {
                tokens: cap,
                lastRefill: Date.now(),
                capacity: cap,
                ratePerSecond: this.defaultRatePerSecond,
                blockedUntil: 0,
            };
            this.buckets.set(host, bucket);
        }
        return bucket;
    }
    /**
     * Acquire a token for host with jitter and sleep if necessary
     */
    async acquire(host) {
        const bucket = this.getBucket(host);
        while (true) {
            const now = Date.now();
            // Check if blocked due to 429 Retry-After
            if (bucket.blockedUntil > now) {
                const waitMs = bucket.blockedUntil - now;
                this.logger.debug(`Host ${host} is rate-blocked, waiting ${waitMs}ms`);
                await this.sleep(waitMs);
                continue;
            }
            // Refill tokens
            const elapsedSeconds = (now - bucket.lastRefill) / 1000;
            bucket.tokens = Math.min(bucket.capacity, bucket.tokens + elapsedSeconds * bucket.ratePerSecond);
            bucket.lastRefill = now;
            if (bucket.tokens >= 1) {
                bucket.tokens -= 1;
                // Apply micro-jitter (15-30ms) to avoid perfectly periodic bursts
                const jitter = Math.floor(Math.random() * 15) + 15;
                await this.sleep(jitter);
                return;
            }
            // Wait until at least 1 token is available + random jitter
            const timeForTokenMs = Math.ceil(((1 - bucket.tokens) / bucket.ratePerSecond) * 1000);
            const jitter = Math.floor(Math.random() * 50) + 10;
            await this.sleep(timeForTokenMs + jitter);
        }
    }
    /**
     * Handle HTTP 429 response by honoring Retry-After or applying exponential backoff
     */
    handle429(host, retryAfterHeader, attemptNumber = 1) {
        const bucket = this.getBucket(host);
        let waitSeconds = 5;
        if (retryAfterHeader) {
            const parsedSeconds = parseInt(retryAfterHeader, 10);
            if (!isNaN(parsedSeconds) && parsedSeconds > 0) {
                waitSeconds = Math.min(300, parsedSeconds);
            }
            else {
                const parsedDate = Date.parse(retryAfterHeader);
                if (!isNaN(parsedDate)) {
                    const delta = Math.ceil((parsedDate - Date.now()) / 1000);
                    waitSeconds = Math.max(1, Math.min(300, delta));
                }
            }
        }
        else {
            // Exponential backoff with jitter
            const base = Math.min(60, Math.pow(2, attemptNumber) * 2);
            const jitterFactor = 0.85 + Math.random() * 0.3; // 85% to 115%
            waitSeconds = Math.round(base * jitterFactor);
        }
        bucket.blockedUntil = Date.now() + waitSeconds * 1000;
        this.logger.warn(`Host ${host} rate limit backoff triggered for ${waitSeconds}s`, {
            attemptNumber,
            retryAfterHeader,
        });
        return waitSeconds;
    }
    sleep(ms) {
        return new Promise((resolve) => setTimeout(resolve, ms));
    }
}
/**
 * Centralized global rate limiter for Storage Bridge uploads across all sources.
 * Enforces a strict Token Bucket + Sliding Window rate limit (default 105 req/min)
 * with dynamic backoff upon receiving HTTP 429 or Retry-After headers.
 */
export class GlobalStorageRateLimiter {
    logger = new Logger('GlobalStorageRateLimiter');
    tokens;
    lastRefill;
    capacity;
    ratePerSecond;
    blockedUntil = 0;
    minIntervalMs;
    lastAcquiredTime = 0;
    currentRatePerMinute;
    baseRatePerMinute;
    recentUploadTimestamps = [];
    recentTransientErrors = [];
    constructor(config = {}) {
        this.baseRatePerMinute = config.maxRequestsPerMinute ?? 105;
        this.currentRatePerMinute = this.baseRatePerMinute;
        this.capacity = this.currentRatePerMinute;
        this.ratePerSecond = this.currentRatePerMinute / 60;
        this.tokens = this.capacity;
        this.lastRefill = Date.now();
        this.minIntervalMs = config.minIntervalMs ?? 350;
    }
    /**
     * Acquire an upload token before sending an image to the Storage Bridge.
     * Blocks if the rate limit or pacing threshold is reached.
     */
    async acquire() {
        while (true) {
            const now = Date.now();
            // 1. Check if blocked due to 429 / cooldown
            if (this.blockedUntil > now) {
                const waitMs = this.blockedUntil - now;
                this.logger.warn(`Storage Bridge is rate-blocked, waiting ${waitMs}ms before retry`);
                await new Promise((r) => setTimeout(r, Math.min(waitMs, 5000)));
                continue;
            }
            // 2. Sliding window check over the last 60 seconds
            this.recentUploadTimestamps = this.recentUploadTimestamps.filter((t) => now - t < 60_000);
            if (this.recentUploadTimestamps.length >= this.currentRatePerMinute) {
                const oldest = this.recentUploadTimestamps[0];
                const waitMs = Math.max(100, 60_000 - (now - oldest) + 50);
                this.logger.debug(`Sliding window limit reached (${this.recentUploadTimestamps.length}/${this.currentRatePerMinute} req/min), pacing for ${waitMs}ms`);
                await new Promise((r) => setTimeout(r, Math.min(waitMs, 2000)));
                continue;
            }
            // 3. Token bucket refill
            const elapsedSec = (now - this.lastRefill) / 1000;
            this.tokens = Math.min(this.capacity, this.tokens + elapsedSec * this.ratePerSecond);
            this.lastRefill = now;
            // 4. Minimum spacing pacing between releases
            const sinceLast = now - this.lastAcquiredTime;
            if (sinceLast < this.minIntervalMs) {
                await new Promise((r) => setTimeout(r, this.minIntervalMs - sinceLast));
                continue;
            }
            if (this.tokens >= 1) {
                this.tokens -= 1;
                this.lastAcquiredTime = Date.now();
                this.recentUploadTimestamps.push(this.lastAcquiredTime);
                return;
            }
            // Wait until next token is generated
            const waitMs = Math.ceil(((1 - this.tokens) / this.ratePerSecond) * 1000);
            await new Promise((r) => setTimeout(r, Math.max(50, Math.min(waitMs, 1000))));
        }
    }
    /**
     * Handle rate limits (HTTP 429, FloodWait, Retry-After) reported by the Storage Bridge.
     * Immediately blocks subsequent uploads and backs off by reducing rate by 20%.
     */
    recordRateLimit(retryAfterSeconds) {
        const cooldownMs = retryAfterSeconds && retryAfterSeconds > 0 ? retryAfterSeconds * 1000 : 60_000;
        this.blockedUntil = Date.now() + cooldownMs;
        // Step down currentRatePerMinute by 20% to avoid immediate re-triggering (floor at 60 req/min)
        this.currentRatePerMinute = Math.max(60, Math.floor(this.currentRatePerMinute * 0.8));
        this.ratePerSecond = this.currentRatePerMinute / 60;
        this.tokens = 0; // Empty bucket during cooldown
        this.logger.warn(`Rate limit recorded from Storage Bridge! Cooldown for ${Math.round(cooldownMs / 1000)}s. Upload rate throttled to ${this.currentRatePerMinute} req/min`);
    }
    /**
     * Gradually restore rate back towards baseRatePerMinute when operating stably
     */
    restoreRate() {
        if (this.currentRatePerMinute < this.baseRatePerMinute) {
            this.currentRatePerMinute = Math.min(this.baseRatePerMinute, this.currentRatePerMinute + 5);
            this.ratePerSecond = this.currentRatePerMinute / 60;
            this.capacity = this.currentRatePerMinute;
            this.logger.info(`Storage upload rate gradually restored to ${this.currentRatePerMinute} req/min`);
        }
    }
    /**
     * Track transient upstream errors (502/503/network) from the Storage Bridge.
     * Isolated failures (1 or 2) do NOT block or pause the global rate limiter.
     * Only repeated transient failures in a concentrated window (>= 3 in 30s) trigger
     * a mild global pacing pause of 15 seconds.
     */
    recordTransientError() {
        const now = Date.now();
        this.recentTransientErrors = this.recentTransientErrors.filter((t) => now - t < 30_000);
        this.recentTransientErrors.push(now);
        if (this.recentTransientErrors.length >= 3) {
            const pacingMs = 15_000;
            if (this.blockedUntil < now + pacingMs) {
                this.blockedUntil = now + pacingMs;
                this.logger.warn(`Repeated transient errors detected (${this.recentTransientErrors.length} in 30s). Applying mild global pacing of 15s.`);
            }
        }
        else {
            this.logger.debug(`Recorded isolated transient error (${this.recentTransientErrors.length}/3 in 30s). No global pacing applied.`);
        }
    }
    getRecentTransientErrorCount() {
        const now = Date.now();
        this.recentTransientErrors = this.recentTransientErrors.filter((t) => now - t < 30_000);
        return this.recentTransientErrors.length;
    }
    getCurrentRatePerMinute() {
        return this.currentRatePerMinute;
    }
    getRecentUploadCount() {
        const now = Date.now();
        this.recentUploadTimestamps = this.recentUploadTimestamps.filter((t) => now - t < 60_000);
        return this.recentUploadTimestamps.length;
    }
    isBlocked() {
        return this.blockedUntil > Date.now();
    }
    getBlockedRemainingMs() {
        return Math.max(0, this.blockedUntil - Date.now());
    }
}
