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
 * Enforces an Adaptive AIMD (Additive Increase, Multiplicative Decrease) control loop:
 * - Increases rate gradually (+2 req/min) ONLY when real throughput (pages/min) increases.
 * - Detects throughput plateau with hysteresis to avoid ratcheting into rate limits.
 * - Enforces immediate Multiplicative Decrease (-20%) and cooldown upon receiving HTTP 429.
 * - Configurable operational safety ceiling against runaway telemetry.
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
    minRatePerMinute;
    safetyCeilingRate;
    peakTestedRate;
    inCooldownUntil = 0;
    // Window metrics for AIMD throughput gain measurement
    consecutiveSuccessfulUploads = 0;
    windowStartTime = Date.now();
    windowUploadCount = 0;
    windowBytesSum = 0;
    windowDurationSumMs = 0;
    previousWindowRate = 0;
    recentUploadTimestamps = [];
    recentTransientErrors = [];
    total429Count = 0;
    total502Count = 0;
    constructor(config = {}) {
        this.baseRatePerMinute = config.maxRequestsPerMinute ?? 105;
        this.currentRatePerMinute = this.baseRatePerMinute;
        this.minRatePerMinute = config.minRequestsPerMinute ?? 60;
        const envCeiling = process.env.STORAGE_SAFETY_CEILING_RATE ? parseInt(process.env.STORAGE_SAFETY_CEILING_RATE, 10) : null;
        this.safetyCeilingRate = config.safetyCeilingRate ?? (envCeiling && !isNaN(envCeiling) ? envCeiling : 160);
        this.peakTestedRate = this.currentRatePerMinute;
        this.capacity = this.currentRatePerMinute;
        this.ratePerSecond = this.currentRatePerMinute / 60;
        this.tokens = this.capacity;
        this.lastRefill = Date.now();
        this.minIntervalMs = config.minIntervalMs ?? 250;
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
     * Record a successful upload with page size and latency metrics.
     * Evaluates real throughput (pages/min) over 30s windows with plateau detection:
     * - If real throughput increased: scales up rate (+2 req/min).
     * - If real throughput plateaued: stops increasing to avoid inducing 429.
     * - If safety ceiling reached: emits SAFETY_CEILING_REACHED.
     */
    recordSuccess(bytes = 0, durationMs = 0) {
        this.consecutiveSuccessfulUploads++;
        this.windowUploadCount++;
        this.windowBytesSum += bytes;
        this.windowDurationSumMs += durationMs;
        const now = Date.now();
        const windowElapsedMs = now - this.windowStartTime;
        if (windowElapsedMs >= 30_000 &&
            this.consecutiveSuccessfulUploads >= 20 &&
            now > this.inCooldownUntil) {
            const currentEffectivePagesPerMin = Math.round((this.windowUploadCount / (windowElapsedMs / 1000)) * 60);
            const avgLatency = Math.round(this.windowDurationSumMs / Math.max(1, this.windowUploadCount));
            if (this.currentRatePerMinute >= this.safetyCeilingRate) {
                this.logger.info(`[Storage AIMD] SAFETY_CEILING_REACHED (${this.currentRatePerMinute} req/min). Stable throughput: ${currentEffectivePagesPerMin} pages/min.`);
            }
            else {
                // Compare with previous window throughput to detect plateau vs real gain
                if (this.previousWindowRate > 0) {
                    const deltaPercent = ((currentEffectivePagesPerMin - this.previousWindowRate) / this.previousWindowRate) * 100;
                    if (deltaPercent >= 2.0 && avgLatency < 5000) {
                        // Real gain verified! Scale up by +2 req/min
                        const prev = this.currentRatePerMinute;
                        this.currentRatePerMinute = Math.min(this.safetyCeilingRate, this.currentRatePerMinute + 2);
                        this.peakTestedRate = Math.max(this.peakTestedRate, this.currentRatePerMinute);
                        this.ratePerSecond = this.currentRatePerMinute / 60;
                        this.capacity = this.currentRatePerMinute;
                        this.logger.info(`[Storage AIMD +] Real throughput gain (+${deltaPercent.toFixed(1)}%). Increased upload rate: ${prev} -> ${this.currentRatePerMinute} req/min. AvgLatency: ${avgLatency}ms`);
                    }
                    else if (deltaPercent < -5.0 && avgLatency > 6000) {
                        // Degradation observed: back off slightly (-2 req/min)
                        this.currentRatePerMinute = Math.max(this.minRatePerMinute, this.currentRatePerMinute - 2);
                        this.ratePerSecond = this.currentRatePerMinute / 60;
                        this.capacity = this.currentRatePerMinute;
                        this.logger.warn(`[Storage AIMD] Latency degradation detected. Settling rate to ${this.currentRatePerMinute} req/min.`);
                    }
                    else {
                        // Plateau: throughput stabilized within noise margin, maintain rate without ratcheting
                        this.logger.debug(`[Storage AIMD PLATEAU] Throughput at sweet spot (${currentEffectivePagesPerMin} pages/min). Maintaining ${this.currentRatePerMinute} req/min.`);
                    }
                }
                else {
                    // First stable window: slight initial probe
                    if (avgLatency < 4500) {
                        this.currentRatePerMinute = Math.min(this.safetyCeilingRate, this.currentRatePerMinute + 2);
                        this.ratePerSecond = this.currentRatePerMinute / 60;
                        this.capacity = this.currentRatePerMinute;
                    }
                }
            }
            // Reset for next 30s window
            this.previousWindowRate = currentEffectivePagesPerMin;
            this.windowStartTime = now;
            this.windowUploadCount = 0;
            this.windowBytesSum = 0;
            this.windowDurationSumMs = 0;
            this.consecutiveSuccessfulUploads = 0;
        }
    }
    /**
     * Handle rate limits (HTTP 429, FloodWait, Retry-After) reported by the Storage Bridge.
     * Multiplicative decrease (-20%) and cooldown.
     * Pauses ONLY the Storage Bridge; the rest of the Importer remains fully active.
     */
    recordRateLimit(retryAfterSeconds) {
        this.total429Count++;
        const cooldownMs = retryAfterSeconds && retryAfterSeconds > 0 ? retryAfterSeconds * 1000 : 60_000;
        const now = Date.now();
        this.blockedUntil = now + cooldownMs;
        this.inCooldownUntil = this.blockedUntil + 30_000; // 30s buffer before attempting upward probe
        const prev = this.currentRatePerMinute;
        // Step down by 20% (Multiplicative Decrease)
        this.currentRatePerMinute = Math.max(this.minRatePerMinute, Math.floor(this.currentRatePerMinute * 0.8));
        this.ratePerSecond = this.currentRatePerMinute / 60;
        this.capacity = this.currentRatePerMinute;
        this.tokens = 0; // Empty bucket during cooldown
        this.consecutiveSuccessfulUploads = 0;
        this.previousWindowRate = 0;
        this.logger.warn(`[Storage AIMD -] Rate limit (429)! Cooldown for ${Math.round(cooldownMs / 1000)}s. Rate stepped down: ${prev} -> ${this.currentRatePerMinute} req/min (-20%)`);
    }
    /**
     * Track transient upstream errors (502/503/network) from the Storage Bridge.
     * Isolated failures (1 or 2) do NOT block or pause the global rate limiter.
     * Repeated transient failures in a concentrated window (>= 3 in 30s) trigger
     * a mild global pacing pause of 15 seconds and slight rate adjustment.
     */
    recordTransientError() {
        this.total502Count++;
        const now = Date.now();
        this.recentTransientErrors = this.recentTransientErrors.filter((t) => now - t < 30_000);
        this.recentTransientErrors.push(now);
        if (this.recentTransientErrors.length >= 3) {
            const pacingMs = 15_000;
            if (this.blockedUntil < now + pacingMs) {
                this.blockedUntil = now + pacingMs;
                this.currentRatePerMinute = Math.max(this.minRatePerMinute, this.currentRatePerMinute - 5);
                this.ratePerSecond = this.currentRatePerMinute / 60;
                this.capacity = this.currentRatePerMinute;
                this.consecutiveSuccessfulUploads = 0;
                this.logger.warn(`[Storage AIMD 502] Concentrated 502/503s detected (${this.recentTransientErrors.length} in 30s). Applying 15s pacing, rate adjusted to ${this.currentRatePerMinute} req/min.`);
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
    /**
     * Gradually restore rate towards baseRatePerMinute when recovering from throttling
     */
    restoreRate() {
        if (this.currentRatePerMinute < this.baseRatePerMinute) {
            this.currentRatePerMinute = Math.min(this.baseRatePerMinute, this.currentRatePerMinute + 5);
            this.ratePerSecond = this.currentRatePerMinute / 60;
            this.capacity = this.currentRatePerMinute;
            this.logger.info(`Storage upload rate restored to ${this.currentRatePerMinute} req/min`);
        }
    }
    getMetricsSummary() {
        const now = Date.now();
        this.recentUploadTimestamps = this.recentUploadTimestamps.filter((t) => now - t < 60_000);
        const windowElapsedSec = Math.max(1, (now - this.windowStartTime) / 1000);
        const mbPerMin = Math.round((this.windowBytesSum / (1024 * 1024) / windowElapsedSec) * 60 * 10) / 10;
        const avgLatency = Math.round(this.windowDurationSumMs / Math.max(1, this.windowUploadCount));
        return {
            currentRate: this.currentRatePerMinute,
            peakTestedRate: this.peakTestedRate,
            successfulPagesLastMinute: this.recentUploadTimestamps.length,
            mbPerMinute: mbPerMin,
            avgUploadLatencyMs: avgLatency,
            recent429Count: this.total429Count,
            recent502Count: this.total502Count,
            isBlocked: this.blockedUntil > now,
            blockedRemainingSeconds: Math.max(0, Math.ceil((this.blockedUntil - now) / 1000)),
        };
    }
    getCurrentRatePerMinute() {
        return this.currentRatePerMinute;
    }
    getPeakTestedRate() {
        return this.peakTestedRate;
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
