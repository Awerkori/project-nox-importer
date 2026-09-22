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
    tryAcquire() {
        if (this.activePermits < this.maxPermits && this.waitQueue.length === 0) {
            this.activePermits++;
            telemetryCollector.recordLimiterWait(this.name, 0, this.maxPermits);
            telemetryCollector.updateLimiterConcurrency(this.name, this.activePermits, this.maxPermits);
            return true;
        }
        return false;
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
    mangaflix: { maxChapters: 3, maxPagesPerChapter: 6 },
    manhastro: { maxChapters: 3, maxPagesPerChapter: 6 },
    mangotoons: { maxChapters: 2, maxPagesPerChapter: 4 },
    megahentai: { maxChapters: 2, maxPagesPerChapter: 4 },
    taimumangas: { maxChapters: 3, maxPagesPerChapter: 6 },
    hipercool: { maxChapters: 2, maxPagesPerChapter: 4 },
    nexus: { maxChapters: 2, maxPagesPerChapter: 4 },
    instahentai: { maxChapters: 2, maxPagesPerChapter: 4 },
    euphoriascan: { maxChapters: 2, maxPagesPerChapter: 4 },
    fleurblanche: { maxChapters: 3, maxPagesPerChapter: 6 },
    littletyrant: { maxChapters: 2, maxPagesPerChapter: 4 },
    mangalivreto: { maxChapters: 3, maxPagesPerChapter: 6 },
    montetai: { maxChapters: 3, maxPagesPerChapter: 6 },
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
    maxConcurrency: 18,
    initialConcurrency: 8,
    requiredStableCycles: 3,
    cooldownPeriodMs: 25 * 1000,
    maxRssMb: 350,
    maxHeapMb: 200,
    maxExternalAndBuffersMb: 100,
    maxEventLoopLagMs: 250,
    rssSoftLimitMb: parseInt(process.env.RSS_SOFT_LIMIT_MB || '330', 10),
    rssHardLimitMb: parseInt(process.env.RSS_HARD_LIMIT_MB || '380', 10),
    rssEmergencyLimitMb: parseInt(process.env.RSS_EMERGENCY_LIMIT_MB || '410', 10),
    maxBufferedBytes: parseInt(process.env.MAX_BUFFERED_BYTES || String(64 * 1024 * 1024), 10),
};
// RAII Token representing an atomic slice of the memory buffer budget.
export class BufferReservation {
    autotuner;
    _reservedBytes;
    _released = false;
    _committed = false;
    constructor(autotuner, _reservedBytes) {
        this.autotuner = autotuner;
        this._reservedBytes = _reservedBytes;
    }
    get reservedBytes() {
        return this._reservedBytes;
    }
    get isCommitted() {
        return this._committed;
    }
    get isReleased() {
        return this._released;
    }
    /**
     * Upgrades the reserved byte budget if Content-Length exceeds initial reservation.
     */
    async upgrade(newBytes, signal) {
        if (this._released || this._committed)
            return;
        if (newBytes <= this._reservedBytes)
            return;
        const additional = newBytes - this._reservedBytes;
        await this.autotuner.upgradeReservation(additional, signal);
        this._reservedBytes = newBytes;
    }
    /**
     * Commits actual downloaded bytes into activeBufferedBytes and frees the reserved budget.
     * Defensive invariant: actualBytes must NOT exceed reservedBytes.
     */
    commit(actualBytes) {
        if (this._released || this._committed)
            return;
        if (actualBytes > this._reservedBytes) {
            throw new Error(`BufferReservation invariant violation: cannot commit ${actualBytes} bytes exceeding reserved budget ${this._reservedBytes} bytes without prior upgrade`);
        }
        this._committed = true;
        this.autotuner.commitReservation(this._reservedBytes, actualBytes);
    }
    /**
     * Releases the reserved budget on failure, cancellation, or skip without committing.
     */
    release() {
        if (this._released || this._committed)
            return;
        this._released = true;
        this.autotuner.releaseReservation(this._reservedBytes);
    }
}
export class AdaptiveAutotuner {
    logger = new Logger('Autotuner');
    globalChapterSemaphore;
    sourceSemaphores = new Map();
    globalMediaSemaphore;
    globalInflightRequestSemaphore;
    bufferedPageSemaphore;
    currentConcurrency;
    stableCycleCount = 0;
    cooldownUntil = 0;
    config;
    // Active and reserved buffer tracking & backpressure waiters
    activeBufferedBytes = 0;
    reservedBufferedBytes = 0;
    maxCommittedBytesObserved = 0;
    reservationWaiters = [];
    // Window error counters
    cycleErrors = 0;
    cycleRateLimits = 0;
    cycleTimeouts = 0;
    constructor(config = {}) {
        this.config = { ...DEFAULT_AUTOTUNER_CONFIG, ...config };
        this.currentConcurrency = this.config.initialConcurrency;
        const mediaConcurrency = parseInt(process.env.TELEGRAM_MEDIA_CONCURRENCY || '12', 10);
        const inflightConcurrency = parseInt(process.env.DOWNLOAD_INFLIGHT_CONCURRENCY || '16', 10);
        const bufferedConcurrency = parseInt(process.env.BUFFERED_PAGE_CONCURRENCY || '32', 10);
        this.bufferedPageSemaphore = new AsyncSemaphore(bufferedConcurrency, 'buffered_page_semaphore');
        this.globalChapterSemaphore = new AsyncSemaphore(this.currentConcurrency, 'global_chapter_semaphore');
        this.globalMediaSemaphore = new AsyncSemaphore(mediaConcurrency, 'telegram_media_semaphore');
        this.globalInflightRequestSemaphore = new AsyncSemaphore(inflightConcurrency, 'global_download_inflight_semaphore');
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
    canAdmitReservation(requestedBytes) {
        const mem = diagnostics.getMemorySnapshot();
        const totalCommitted = this.activeBufferedBytes + this.reservedBufferedBytes;
        // Hard ceiling: ACTIVE + RESERVED + REQUESTED <= MAX_BUFFERED_BYTES
        if (totalCommitted + requestedBytes > this.config.maxBufferedBytes) {
            return false;
        }
        // Memory headroom: RSS below soft limit
        if (mem.rssMb >= this.config.rssSoftLimitMb) {
            // Forward progress exception: If ZERO active and ZERO reserved buffers exist,
            // allow single-slot progress to prevent permanent deadlock
            // when baseline Node process RSS is warm without buffers.
            if (totalCommitted === 0) {
                return true;
            }
            return false;
        }
        return true;
    }
    async reserveBufferBudget(requestedBytes = 2 * 1024 * 1024, signal) {
        signal?.throwIfAborted();
        // Fast-path: Synchronously admit if no queue and headroom exists.
        // Node.js single-threaded event loop guarantees check-and-increment is atomic.
        if (this.reservationWaiters.length === 0 && this.canAdmitReservation(requestedBytes)) {
            this.reservedBufferedBytes += requestedBytes;
            this.updateMaxCommittedObserved();
            return new BufferReservation(this, requestedBytes);
        }
        const t0 = performance.now();
        return new Promise((resolve, reject) => {
            let intervalTimer = null;
            const waiter = {
                requestedBytes,
                t0,
                signal,
                resolve: (reservation) => {
                    cleanup();
                    const waitedMs = performance.now() - t0;
                    telemetryCollector.recordLimiterWait('memory_backpressure', waitedMs, this.config.maxBufferedBytes);
                    if (waitedMs > 1000) {
                        this.logger.info(`[Memory Backpressure] Admitted after ${Math.round(waitedMs)}ms wait (Active: ${Math.round(this.activeBufferedBytes / 1024 / 1024)}MB, Reserved: ${Math.round(this.reservedBufferedBytes / 1024 / 1024)}MB, RSS: ${diagnostics.getMemorySnapshot().rssMb}MB)`);
                    }
                    resolve(reservation);
                },
                reject: (err) => {
                    cleanup();
                    reject(err);
                },
            };
            const cleanup = () => {
                if (intervalTimer)
                    clearInterval(intervalTimer);
                signal?.removeEventListener('abort', onAbort);
                const idx = this.reservationWaiters.indexOf(waiter);
                if (idx >= 0)
                    this.reservationWaiters.splice(idx, 1);
            };
            const onAbort = () => {
                cleanup();
                reject(signal?.reason || new Error('Buffer reservation aborted'));
            };
            signal?.addEventListener('abort', onAbort, { once: true });
            this.reservationWaiters.push(waiter);
            // Periodically check if conditions became favorable (e.g. RSS dropped or GC ran)
            intervalTimer = setInterval(() => {
                this.drainReservationWaiters();
            }, 200);
        });
    }
    async upgradeReservation(additionalBytes, signal) {
        if (additionalBytes <= 0)
            return;
        signal?.throwIfAborted();
        if (this.reservationWaiters.length === 0 && this.canAdmitReservation(additionalBytes)) {
            this.reservedBufferedBytes += additionalBytes;
            this.updateMaxCommittedObserved();
            return;
        }
        const t0 = performance.now();
        return new Promise((resolve, reject) => {
            let intervalTimer = null;
            const waiter = {
                requestedBytes: additionalBytes,
                t0,
                signal,
                resolve: () => {
                    cleanup();
                    resolve();
                },
                reject: (err) => {
                    cleanup();
                    reject(err);
                },
            };
            const cleanup = () => {
                if (intervalTimer)
                    clearInterval(intervalTimer);
                signal?.removeEventListener('abort', onAbort);
                const idx = this.reservationWaiters.indexOf(waiter);
                if (idx >= 0)
                    this.reservationWaiters.splice(idx, 1);
            };
            const onAbort = () => {
                cleanup();
                reject(signal?.reason || new Error('Buffer reservation upgrade aborted'));
            };
            signal?.addEventListener('abort', onAbort, { once: true });
            this.reservationWaiters.push(waiter);
            intervalTimer = setInterval(() => {
                this.drainReservationWaiters();
            }, 200);
        });
    }
    commitReservation(reservedBytes, actualBytes) {
        if (actualBytes > reservedBytes) {
            throw new Error(`commitReservation invariant violation: actualBytes (${actualBytes}) exceeds reservedBytes (${reservedBytes})`);
        }
        this.reservedBufferedBytes = Math.max(0, this.reservedBufferedBytes - reservedBytes);
        this.activeBufferedBytes += actualBytes;
        this.updateMaxCommittedObserved();
        this.drainReservationWaiters();
    }
    releaseReservation(reservedBytes) {
        this.reservedBufferedBytes = Math.max(0, this.reservedBufferedBytes - reservedBytes);
        this.drainReservationWaiters();
    }
    releaseActiveBufferedBytes(actualBytes) {
        if (actualBytes <= 0)
            return;
        this.activeBufferedBytes = Math.max(0, this.activeBufferedBytes - actualBytes);
        this.drainReservationWaiters();
    }
    drainReservationWaiters() {
        while (this.reservationWaiters.length > 0) {
            const next = this.reservationWaiters[0];
            if (this.canAdmitReservation(next.requestedBytes)) {
                this.reservationWaiters.shift();
                this.reservedBufferedBytes += next.requestedBytes;
                this.updateMaxCommittedObserved();
                const reservation = new BufferReservation(this, next.requestedBytes);
                next.resolve(reservation);
            }
            else {
                break; // FIFO barrier: if next cannot fit, keep waiting
            }
        }
    }
    trackBufferedBytes(bytes) {
        if (bytes <= 0)
            return;
        this.activeBufferedBytes += bytes;
        this.updateMaxCommittedObserved();
    }
    releaseBufferedBytes(bytes) {
        this.releaseActiveBufferedBytes(bytes);
    }
    getBufferedBytes() {
        return this.activeBufferedBytes;
    }
    getReservedBytes() {
        return this.reservedBufferedBytes;
    }
    getCommittedBytes() {
        return this.activeBufferedBytes + this.reservedBufferedBytes;
    }
    updateMaxCommittedObserved() {
        const total = this.activeBufferedBytes + this.reservedBufferedBytes;
        if (total > this.maxCommittedBytesObserved) {
            this.maxCommittedBytesObserved = total;
        }
    }
    getMaxCommittedBytesObserved() {
        return this.maxCommittedBytesObserved;
    }
    async waitForMemoryHeadroom(estimatedBytes = 1.5 * 1024 * 1024, signal) {
        const reservation = await this.reserveBufferBudget(estimatedBytes, signal);
        reservation.release();
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
    isSourceCapacityAvailable(source) {
        const sem = this.getSourceSemaphore(source);
        return sem.available > 0;
    }
    sourceHealth = new Map();
    recordSourceFailure(source) {
        const limits = this.getSourceLimits(source);
        let health = this.sourceHealth.get(source);
        if (!health) {
            health = { consecutiveFailures: 0, consecutiveSuccesses: 0, currentCapacity: limits.maxChapters };
            this.sourceHealth.set(source, health);
        }
        health.consecutiveFailures++;
        health.consecutiveSuccesses = 0;
        // After 2 consecutive failures on a source, throttle concurrency by 1 (minimum 1)
        if (health.consecutiveFailures >= 2 && health.currentCapacity > 1) {
            health.currentCapacity = Math.max(1, health.currentCapacity - 1);
            const sem = this.getSourceSemaphore(source);
            sem.setCapacity(health.currentCapacity);
            this.logger.warn(`Source ${source} concurrency throttled: ${health.currentCapacity + 1} -> ${health.currentCapacity} due to ${health.consecutiveFailures} consecutive failures`);
            return { throttled: true, newCapacity: health.currentCapacity };
        }
        return { throttled: false, newCapacity: health.currentCapacity };
    }
    recordSourceSuccess(source) {
        const limits = this.getSourceLimits(source);
        let health = this.sourceHealth.get(source);
        if (!health) {
            health = { consecutiveFailures: 0, consecutiveSuccesses: 0, currentCapacity: limits.maxChapters };
            this.sourceHealth.set(source, health);
        }
        health.consecutiveFailures = 0;
        health.consecutiveSuccesses++;
        // After 5 consecutive successes, restore capacity gradually
        if (health.consecutiveSuccesses >= 5 && health.currentCapacity < limits.maxChapters) {
            health.currentCapacity = Math.min(limits.maxChapters, health.currentCapacity + 1);
            health.consecutiveSuccesses = 0;
            const sem = this.getSourceSemaphore(source);
            sem.setCapacity(health.currentCapacity);
            this.logger.info(`Source ${source} concurrency restored: ${health.currentCapacity - 1} -> ${health.currentCapacity} after consecutive successes`);
            return { restored: true, newCapacity: health.currentCapacity };
        }
        return { restored: false, newCapacity: health.currentCapacity };
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
        // Check for stress condition (requiring scale-down or cooldown)
        let stressReason = null;
        let isMemoryStress = false;
        let isEmergency = false;
        let isSevere = false;
        if (mem.rssMb >= this.config.rssEmergencyLimitMb) {
            stressReason = `Emergency RSS: ${mem.rssMb}MB >= limit ${this.config.rssEmergencyLimitMb}MB`;
            isEmergency = true;
            isMemoryStress = true;
        }
        else if (mem.rssMb >= this.config.rssHardLimitMb) {
            stressReason = `Hard RSS: ${mem.rssMb}MB >= limit ${this.config.rssHardLimitMb}MB`;
            isSevere = true;
            isMemoryStress = true;
        }
        else if (mem.rssMb >= this.config.rssSoftLimitMb || mem.rssMb >= this.config.maxRssMb) {
            const limit = Math.min(this.config.rssSoftLimitMb, this.config.maxRssMb);
            stressReason = `High RSS: ${mem.rssMb}MB >= limit ${limit}MB`;
            isMemoryStress = true;
        }
        else if (mem.heapUsedMb >= this.config.maxHeapMb) {
            stressReason = `High Heap: ${mem.heapUsedMb}MB >= limit ${this.config.maxHeapMb}MB`;
            isMemoryStress = true;
        }
        else if (totalExternal >= this.config.maxExternalAndBuffersMb) {
            stressReason = `High External/Buffers: ${totalExternal}MB >= limit ${this.config.maxExternalAndBuffersMb}MB`;
            isMemoryStress = true;
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
            this.stableCycleCount = 0;
            this.cooldownUntil = now + this.config.cooldownPeriodMs;
            const previous = this.currentConcurrency;
            let target = previous;
            let action = 'STRESS_DETECTED';
            if (isMemoryStress) {
                if (isEmergency) {
                    target = this.config.minConcurrency;
                    if (typeof global.gc === 'function') {
                        try {
                            global.gc();
                        }
                        catch { }
                    }
                }
                else if (isSevere) {
                    target = Math.max(this.config.minConcurrency, previous - 3);
                    if (typeof global.gc === 'function') {
                        try {
                            global.gc();
                        }
                        catch { }
                    }
                }
                else {
                    target = Math.max(this.config.minConcurrency, previous - 1);
                }
                action = target < previous ? 'SCALED_DOWN' : 'STRESS_DETECTED';
                this.currentConcurrency = target;
                this.globalChapterSemaphore.setCapacity(target);
                this.logger.warn(`[Autotuner STRESS] Memory stress detected: ${stressReason}. Scaled down: ${previous} -> ${target}`, {
                    previous,
                    target,
                    stressReason,
                    isEmergency,
                    cooldownSeconds: Math.round(this.config.cooldownPeriodMs / 1000),
                    memory: mem,
                    lag,
                });
            }
            else {
                // Network/error pattern stress: maintain concurrency while applying cooldown
                this.logger.warn(`[Autotuner STRESS] Stress detected: ${stressReason}. Concurrency maintained at ${this.currentConcurrency} during cooldown.`, {
                    concurrency: this.currentConcurrency,
                    stressReason,
                    cooldownSeconds: Math.round(this.config.cooldownPeriodMs / 1000),
                    memory: mem,
                    lag,
                });
            }
            return { concurrency: target, action, reason: stressReason };
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
            // Memory proximity hold: do NOT scale up if close to soft limit or committed buffers are high
            const totalCommitted = this.activeBufferedBytes + this.reservedBufferedBytes;
            if (mem.rssMb >= (this.config.rssSoftLimitMb - 20) || totalCommitted > (this.config.maxBufferedBytes * 0.7)) {
                this.logger.info(`[Autotuner HOLD] Concurrency maintained at ${this.currentConcurrency} due to memory proximity (RSS: ${mem.rssMb}MB, Committed: ${Math.round(totalCommitted / 1024 / 1024)}MB)`);
                return {
                    concurrency: this.currentConcurrency,
                    action: 'STABLE',
                    reason: `Holding concurrency at ${this.currentConcurrency} (RSS: ${mem.rssMb}MB, Committed: ${Math.round(totalCommitted / 1024 / 1024)}MB)`,
                };
            }
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
