import { Logger } from './logger.js';
import { diagnostics } from './diagnostics.js';
import { telemetryCollector } from './telemetry-collector.js';
import { performance } from 'node:perf_hooks';
export class AsyncSemaphore {
    activePermits = 0;
    maxPermits;
    waitQueue = [];
    name;
    constructor(maxPermits, name = 'unnamed_semaphore') {
        this.maxPermits = Math.max(1, maxPermits);
        this.name = name;
    }
    async acquire(signal) {
        signal?.throwIfAborted();
        telemetryCollector.updateLimiterConcurrency(this.name, this.activePermits, this.maxPermits);
        if (this.activePermits < this.maxPermits) {
            this.activePermits++;
            telemetryCollector.recordLimiterWait(this.name, 0, this.maxPermits);
            telemetryCollector.updateLimiterConcurrency(this.name, this.activePermits, this.maxPermits);
            return;
        }
        const t0 = performance.now();
        return new Promise((resolve, reject) => {
            const granted = () => {
                signal?.removeEventListener('abort', cancelled);
                const waitMs = performance.now() - t0;
                telemetryCollector.recordLimiterWait(this.name, waitMs, this.maxPermits);
                telemetryCollector.updateLimiterConcurrency(this.name, this.activePermits, this.maxPermits);
                resolve();
            };
            const cancelled = () => {
                const index = this.waitQueue.indexOf(granted);
                if (index >= 0)
                    this.waitQueue.splice(index, 1);
                reject(signal?.reason || new Error('Semaphore acquisition aborted'));
            };
            signal?.addEventListener('abort', cancelled, { once: true });
            this.waitQueue.push(granted);
        });
    }
    release() {
        if (this.activePermits === 0)
            throw new Error('Semaphore released without an active permit');
        this.activePermits--;
        telemetryCollector.updateLimiterConcurrency(this.name, this.activePermits, this.maxPermits);
        this.drain();
    }
    drain() {
        while (this.activePermits < this.maxPermits && this.waitQueue.length > 0) {
            this.activePermits++;
            this.waitQueue.shift()();
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
        this.maxPermits = target;
        // Existing holders drain naturally after a downscale; never reissue their permits.
        this.drain();
    }
    get capacity() {
        return this.maxPermits;
    }
    get available() {
        return Math.max(0, this.maxPermits - this.activePermits);
    }
    get active() {
        return this.activePermits;
    }
    get queued() {
        return this.waitQueue.length;
    }
}
// Every runner must acquire source before global capacity to avoid lock inversion.
export async function withSourceChapterPermits(source, global, fn, signal) {
    await source.acquire(signal);
    try {
        await global.acquire(signal);
        try {
            return await fn();
        }
        finally {
            global.release();
        }
    }
    finally {
        source.release();
    }
}
export const SOURCE_CONCURRENCY_LIMITS = {
    mangaflix: { maxChapters: 2, maxPagesPerChapter: 4 },
    manhastro: { maxChapters: 2, maxPagesPerChapter: 4 },
    mangotoons: { maxChapters: 2, maxPagesPerChapter: 4 },
    megahentai: { maxChapters: 2, maxPagesPerChapter: 4 },
    taimumangas: { maxChapters: 2, maxPagesPerChapter: 4 },
    hipercool: { maxChapters: 2, maxPagesPerChapter: 4 },
    nexus: { maxChapters: 2, maxPagesPerChapter: 4 },
    instahentai: { maxChapters: 2, maxPagesPerChapter: 4 },
    euphoriascan: { maxChapters: 2, maxPagesPerChapter: 4 },
    fleurblanche: { maxChapters: 2, maxPagesPerChapter: 4 },
    littletyrant: { maxChapters: 2, maxPagesPerChapter: 4 },
    mangalivreto: { maxChapters: 2, maxPagesPerChapter: 4 },
    montetai: { maxChapters: 2, maxPagesPerChapter: 4 },
    nebulosascan: { maxChapters: 2, maxPagesPerChapter: 4 },
    nocturnesummer: { maxChapters: 2, maxPagesPerChapter: 4 },
    tankouhentai: { maxChapters: 2, maxPagesPerChapter: 4 },
    cafecomyaoi: { maxChapters: 2, maxPagesPerChapter: 4 },
    kuro: { maxChapters: 1, maxPagesPerChapter: 2 },
    hanamiheaven: { maxChapters: 1, maxPagesPerChapter: 2 },
    hotcabaretscan: { maxChapters: 4, maxPagesPerChapter: 6 },
    amuy: { maxChapters: 1, maxPagesPerChapter: 2 },
    arthurscan: { maxChapters: 1, maxPagesPerChapter: 2 },
    borutoexplorer: { maxChapters: 4, maxPagesPerChapter: 6 },
    covenscan: { maxChapters: 4, maxPagesPerChapter: 6 },
    kamisamaexplorer: { maxChapters: 4, maxPagesPerChapter: 6 },
    mrtenzus: { maxChapters: 4, maxPagesPerChapter: 6 },
    ninjascan: { maxChapters: 4, maxPagesPerChapter: 6 },
    yuriverso: { maxChapters: 1, maxPagesPerChapter: 2 },
    tiamanhwa: { maxChapters: 1, maxPagesPerChapter: 2 },
    pointzerotoons: { maxChapters: 4, maxPagesPerChapter: 6 },
    apecomics: { maxChapters: 4, maxPagesPerChapter: 6 },
    pizzariascan: { maxChapters: 4, maxPagesPerChapter: 6 },
    acervohentai: { maxChapters: 1, maxPagesPerChapter: 2 },
    inkapk: { maxChapters: 1, maxPagesPerChapter: 2 },
    yaoifanclub: { maxChapters: 1, maxPagesPerChapter: 2 },
    mangaonlinetv: { maxChapters: 4, maxPagesPerChapter: 6 },
    mangaonline: { maxChapters: 1, maxPagesPerChapter: 2 },
    pinkrosa: { maxChapters: 4, maxPagesPerChapter: 6 },
    galaxscanlator: { maxChapters: 4, maxPagesPerChapter: 6 },
    apenasumafa: { maxChapters: 4, maxPagesPerChapter: 6 },
    ler999: { maxChapters: 4, maxPagesPerChapter: 6 },
    osakascan: { maxChapters: 4, maxPagesPerChapter: 6 },
    maidscan: { maxChapters: 4, maxPagesPerChapter: 6 },
    vegitoons: { maxChapters: 4, maxPagesPerChapter: 6 },
    hentaihome: { maxChapters: 4, maxPagesPerChapter: 6 },
    mundohentai: { maxChapters: 4, maxPagesPerChapter: 6 },
    hentaiseason: { maxChapters: 4, maxPagesPerChapter: 6 },
    hentaitokyo: { maxChapters: 4, maxPagesPerChapter: 6 },
    universohentai: { maxChapters: 4, maxPagesPerChapter: 6 },
    hentaifusion: { maxChapters: 4, maxPagesPerChapter: 6 },
    zettahq: { maxChapters: 4, maxPagesPerChapter: 6 },
    nhentaibr: { maxChapters: 4, maxPagesPerChapter: 6 },
    brasilhentai: { maxChapters: 4, maxPagesPerChapter: 6 },
};
export const DEFAULT_SOURCE_LIMIT = {
    maxChapters: 2,
    maxPagesPerChapter: 4,
};
const DEFAULT_AUTOTUNER_CONFIG = {
    minConcurrency: 1,
    maxConcurrency: 32,
    initialConcurrency: 32,
    requiredStableCycles: 3,
    cooldownPeriodMs: 25 * 1000,
    maxRssMb: 800,
    maxHeapMb: 400,
    maxExternalAndBuffersMb: 300,
    maxEventLoopLagMs: 100,
};
export class AdaptiveAutotuner {
    logger = new Logger('Autotuner');
    globalChapterSemaphore;
    sourceSemaphores = new Map();
    globalMediaSemaphore;
    globalInflightRequestSemaphore;
    bufferedPageSemaphore = new AsyncSemaphore(60, 'buffered_page_semaphore');
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
        this.globalChapterSemaphore = new AsyncSemaphore(this.currentConcurrency, 'global_chapter_semaphore');
        this.globalMediaSemaphore = new AsyncSemaphore(12, 'telegram_media_semaphore');
        this.globalInflightRequestSemaphore = new AsyncSemaphore(32, 'global_download_inflight_semaphore');
    }
    getGlobalChapterSemaphore() {
        return this.globalChapterSemaphore;
    }
    getGlobalMediaSemaphore() {
        return this.globalMediaSemaphore;
    }
    getGlobalInflightRequestSemaphore() {
        return this.globalInflightRequestSemaphore;
    }
    // Hold a slot from before downloading until the page has finished uploading.
    getBufferedPageSemaphore() {
        return this.bufferedPageSemaphore;
    }
    getSourceLimits(source) {
        return SOURCE_CONCURRENCY_LIMITS[source] || DEFAULT_SOURCE_LIMIT;
    }
    getSourcePageConcurrency(source) {
        return this.getSourceLimits(source).maxPagesPerChapter;
    }
    getSourceSemaphore(source, limitPerSource) {
        let sem = this.sourceSemaphores.get(source);
        if (!sem) {
            const configuredLimit = limitPerSource ?? this.getSourceLimits(source).maxChapters;
            sem = new AsyncSemaphore(configuredLimit, `source_semaphore:${source}`);
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
        // Node includes arrayBuffers in external; adding both double-counts image buffers.
        const totalExternal = mem.externalMb;
        const errors = this.cycleErrors;
        const rateLimits = this.cycleRateLimits;
        const timeouts = this.cycleTimeouts;
        // Reset window counters for next cycle
        this.cycleErrors = 0;
        this.cycleRateLimits = 0;
        this.cycleTimeouts = 0;
        const now = Date.now();
        // Check for stress condition (requiring scale-down)
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
        else if (errors >= 2) {
            stressReason = `Detected error pattern: ${errors} errors in cycle`;
        }
        else if (timeouts >= 2) {
            stressReason = `Detected timeout pattern: ${timeouts} network timeouts in cycle`;
        }
        else if (errors + timeouts >= 2) {
            stressReason = `Detected repeated failures: ${errors} errors, ${timeouts} timeouts in cycle`;
        }
        if (stressReason) {
            // Scale-down on real pattern or resource stress
            this.stableCycleCount = 0;
            this.cooldownUntil = now + this.config.cooldownPeriodMs;
            const previous = this.currentConcurrency;
            const target = previous <= 4
                ? Math.max(this.config.minConcurrency, previous - 1)
                : Math.max(this.config.minConcurrency, Math.floor(previous * 0.75));
            this.currentConcurrency = target;
            this.globalChapterSemaphore.setCapacity(target);
            this.logger.warn(`[Autotuner STRESS] Scaled down concurrency: ${previous} -> ${target}. Cause: ${stressReason}`, {
                previous,
                target,
                stressReason,
                cooldownSeconds: Math.round(this.config.cooldownPeriodMs / 1000),
                memory: mem,
                lag,
            });
            return { concurrency: target, action: 'SCALED_DOWN', reason: stressReason };
        }
        // Isolated error handling: cycleErrors === 1 or cycleTimeouts === 1
        // Do NOT scale down; do NOT enter cooldown; maintain concurrency and pause ramp-up
        if (errors === 1 || timeouts === 1) {
            this.stableCycleCount = 0;
            this.logger.info(`[Autotuner ISOLATED] Single error/timeout in cycle (errors: ${errors}, timeouts: ${timeouts}). Maintaining concurrency at ${this.currentConcurrency} without cooldown.`);
            return {
                concurrency: this.currentConcurrency,
                action: 'STABLE',
                reason: `Isolated failure handled: concurrency ${this.currentConcurrency} preserved`,
            };
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
            const target = Math.min(this.config.maxConcurrency, previous + 1);
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
