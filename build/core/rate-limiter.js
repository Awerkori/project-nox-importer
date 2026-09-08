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
