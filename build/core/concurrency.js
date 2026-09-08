import { Logger } from './logger.js';
import { diagnostics } from './diagnostics.js';
export class AsyncSemaphore {
    currentPermits;
    maxPermits;
    waitQueue = [];
    constructor(maxPermits) {
        this.maxPermits = Math.max(1, maxPermits);
        this.currentPermits = this.maxPermits;
    }
    async acquire() {
        if (this.currentPermits > 0) {
            this.currentPermits--;
            return;
        }
        return new Promise((resolve) => {
            this.waitQueue.push(resolve);
        });
    }
    release() {
        if (this.waitQueue.length > 0) {
            const next = this.waitQueue.shift();
            if (next)
                next();
        }
        else {
            if (this.currentPermits < this.maxPermits) {
                this.currentPermits++;
            }
        }
    }
    async runExclusive(fn) {
        await this.acquire();
        try {
            return await fn();
        }
        finally {
            this.release();
        }
    }
    setCapacity(newCapacity) {
        const target = Math.max(1, newCapacity);
        const diff = target - this.maxPermits;
        this.maxPermits = target;
        if (diff > 0) {
            // Release waiting callers for newly added capacity
            for (let i = 0; i < diff && this.waitQueue.length > 0; i++) {
                const next = this.waitQueue.shift();
                if (next)
                    next();
            }
            this.currentPermits = Math.min(this.maxPermits, this.currentPermits + diff);
        }
        else if (diff < 0) {
            this.currentPermits = Math.max(0, this.currentPermits + diff);
        }
    }
    get capacity() {
        return this.maxPermits;
    }
    get available() {
        return this.currentPermits;
    }
    get active() {
        return this.maxPermits - this.currentPermits;
    }
    get queued() {
        return this.waitQueue.length;
    }
}
const DEFAULT_AUTOTUNER_CONFIG = {
    minConcurrency: 1,
    maxConcurrency: 6,
    initialConcurrency: 3,
    requiredStableCycles: 4, // 4 cycles * 30s = 2 minutes of continuous stability
    cooldownPeriodMs: 5 * 60 * 1000, // 5 minutes cooldown after any stress/scale-down
    maxRssMb: 360, // Container is 512MB: keep RSS comfortably below 360MB
    maxHeapMb: 240, // Heap threshold
    maxExternalAndBuffersMb: 60, // Native buffers + external
    maxEventLoopLagMs: 100, // Maximum tolerated event loop lag
};
export class AdaptiveAutotuner {
    logger = new Logger('Autotuner');
    globalChapterSemaphore;
    sourceSemaphores = new Map();
    globalMediaSemaphore;
    currentConcurrency;
    stableCycleCount = 0;
    cooldownUntil = 0;
    config;
    // Window error counters
    cycleErrors = 0;
    cycleRateLimits = 0;
    cycleTimeouts = 0;
    constructor(config = {}) {
        this.config = { ...DEFAULT_AUTOTUNER_CONFIG, ...config };
        this.currentConcurrency = this.config.initialConcurrency;
        this.globalChapterSemaphore = new AsyncSemaphore(this.currentConcurrency);
        this.globalMediaSemaphore = new AsyncSemaphore(10); // Global cap on concurrent image downloads/uploads
    }
    getGlobalChapterSemaphore() {
        return this.globalChapterSemaphore;
    }
    getGlobalMediaSemaphore() {
        return this.globalMediaSemaphore;
    }
    getSourceSemaphore(source, limitPerSource = 2) {
        let sem = this.sourceSemaphores.get(source);
        if (!sem) {
            sem = new AsyncSemaphore(limitPerSource);
            this.sourceSemaphores.set(source, sem);
        }
        return sem;
    }
    recordError(type) {
        if (type === 'ratelimit')
            this.cycleRateLimits++;
        else if (type === 'timeout')
            this.cycleTimeouts++;
        else
            this.cycleErrors++;
    }
    evaluateCycle() {
        const mem = diagnostics.getMemorySnapshot();
        const lag = diagnostics.lagMonitor.getMetrics();
        const totalExternal = mem.externalMb + mem.arrayBuffersMb;
        const errors = this.cycleErrors;
        const rateLimits = this.cycleRateLimits;
        const timeouts = this.cycleTimeouts;
        // Reset window counters for next cycle
        this.cycleErrors = 0;
        this.cycleRateLimits = 0;
        this.cycleTimeouts = 0;
        const now = Date.now();
        // Check for ANY stress condition
        let stressReason = null;
        if (mem.rssMb >= this.config.maxRssMb) {
            stressReason = `High RSS: ${mem.rssMb}MB >= limit ${this.config.maxRssMb}MB`;
        }
        else if (mem.heapUsedMb >= this.config.maxHeapMb) {
            stressReason = `High Heap: ${mem.heapUsedMb}MB >= limit ${this.config.maxHeapMb}MB`;
        }
        else if (totalExternal >= this.config.maxExternalAndBuffersMb) {
            stressReason = `High External/Buffers: ${totalExternal}MB >= limit ${this.config.maxExternalAndBuffersMb}MB`;
        }
        else if (lag.avgLagMs >= this.config.maxEventLoopLagMs) {
            stressReason = `High Event Loop Lag: ${lag.avgLagMs}ms >= limit ${this.config.maxEventLoopLagMs}ms`;
        }
        else if (rateLimits > 0) {
            stressReason = `Detected ${rateLimits} HTTP 429 Rate Limits in cycle`;
        }
        else if (errors > 0) {
            stressReason = `Detected ${errors} unexpected errors in cycle`;
        }
        else if (timeouts > 0) {
            stressReason = `Detected ${timeouts} network timeouts in cycle`;
        }
        if (stressReason) {
            // Immediate scale-down
            this.stableCycleCount = 0;
            this.cooldownUntil = now + this.config.cooldownPeriodMs;
            const previous = this.currentConcurrency;
            const target = Math.max(this.config.minConcurrency, Math.min(previous - 1, 2));
            this.currentConcurrency = target;
            this.globalChapterSemaphore.setCapacity(target);
            this.logger.warn(`[Autotuner STRESS] Scaled down concurrency: ${previous} -> ${target}. Cause: ${stressReason}`, {
                previous,
                target,
                stressReason,
                cooldownMinutes: Math.round(this.config.cooldownPeriodMs / 60000),
                memory: mem,
                lag,
            });
            return { concurrency: target, action: 'SCALED_DOWN', reason: stressReason };
        }
        // No stress: check if in cooldown
        if (now < this.cooldownUntil) {
            const remainingSeconds = Math.ceil((this.cooldownUntil - now) / 1000);
            return {
                concurrency: this.currentConcurrency,
                action: 'COOLDOWN',
                reason: `In cooldown for ${remainingSeconds}s`,
            };
        }
        // System is healthy: increment stable cycle counter
        this.stableCycleCount++;
        if (this.stableCycleCount >= this.config.requiredStableCycles &&
            this.currentConcurrency < this.config.maxConcurrency) {
            const previous = this.currentConcurrency;
            const target = previous + 1;
            this.currentConcurrency = target;
            this.globalChapterSemaphore.setCapacity(target);
            this.stableCycleCount = 0; // Reset counter for the next tier
            this.logger.info(`[Autotuner SCALE UP] System stable for ${this.config.requiredStableCycles} consecutive cycles. Scaled up: ${previous} -> ${target}`, {
                previous,
                target,
                memory: mem,
                lag,
            });
            return {
                concurrency: target,
                action: 'SCALED_UP',
                reason: `Stable across ${this.config.requiredStableCycles} cycles`,
            };
        }
        return {
            concurrency: this.currentConcurrency,
            action: 'STABLE',
            reason: `Stable (${this.stableCycleCount}/${this.config.requiredStableCycles} cycles towards scale-up)`,
        };
    }
    getCurrentConcurrency() {
        return this.currentConcurrency;
    }
}
