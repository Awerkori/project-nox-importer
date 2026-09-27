import { performance, PerformanceObserver } from 'node:perf_hooks';
import { Logger } from './logger.js';
import { BoundedSamples } from './bounded-samples.js';
import { maintenanceScheduler } from './maintenance-scheduler.js';
import { Session as InspectorSession } from 'node:inspector';
function percentile(arr, p) {
    if (!arr || arr.length === 0)
        return 0;
    const sorted = [...arr].sort((a, b) => a - b);
    const idx = Math.min(sorted.length - 1, Math.floor(sorted.length * p));
    return Math.round(sorted[idx] * 10) / 10;
}
function avg(arr) {
    if (!arr || arr.length === 0)
        return 0;
    const sum = arr.reduce((a, b) => a + b, 0);
    return Math.round((sum / arr.length) * 10) / 10;
}
export class TelemetryCollector {
    static instance;
    logger = new Logger('TelemetryCollector');
    activeSessionId = null;
    sessionStartTime = 0;
    sessionExpiresAt = 0;
    configuredChapterSlots = 0;
    effectiveCapacity = () => this.configuredChapterSlots;
    flushing = false;
    nextSessionCheck = 0;
    lastFlushWarningAt = 0;
    gcObserver = null;
    runtimeFingerprint = {};
    cpuProfile = null;
    profiledSession = null;
    async captureBoundedCpuProfile(sessionId) {
        if (this.profiledSession === sessionId)
            return;
        this.profiledSession = sessionId;
        const session = new InspectorSession();
        const post = (method, params = {}) => new Promise((resolve, reject) => session.post(method, params, (error, result) => error ? reject(error) : resolve(result)));
        try {
            session.connect();
            await post('Profiler.enable');
            await post('Profiler.setSamplingInterval', { interval: 2000 });
            await post('Profiler.start');
            await new Promise(resolve => setTimeout(resolve, 5000));
            const { profile } = await post('Profiler.stop');
            const nodes = new Map(profile.nodes.map((n) => [n.id, n]));
            const stacks = new Map();
            let totalUs = 0;
            for (let i = 0; i < (profile.samples?.length || 0); i++) {
                const node = nodes.get(profile.samples[i]);
                const frame = node?.callFrame;
                const name = String(frame?.functionName || '(anonymous)').slice(0, 120);
                const file = String(frame?.url || '').split(/[/?#]/).pop()?.slice(0, 100) || '';
                const key = `${name} @ ${file}:${frame?.lineNumber ?? 0}`;
                const us = profile.timeDeltas?.[i] || 0;
                totalUs += us;
                stacks.set(key, (stacks.get(key) || 0) + us);
            }
            this.cpuProfile = { sessionId, durationMs: (profile.endTime - profile.startTime) / 1000,
                samples: profile.samples?.length || 0, topStacks: [...stacks].sort((a, b) => b[1] - a[1]).slice(0, 20)
                    .map(([frame, us]) => ({ frame, percent: totalUs ? Math.round(us / totalUs * 1000) / 10 : 0 })) };
        }
        catch (error) {
            this.cpuProfile = { sessionId, error: error?.code || error?.name };
        }
        finally {
            session.disconnect();
        }
    }
    setRuntimeFingerprint(value) { this.runtimeFingerprint = value; }
    limiterProviders = new Map();
    configureChapterSlots(count, effectiveCapacity = () => count) {
        this.configuredChapterSlots = Math.max(0, Math.floor(count));
        this.effectiveCapacity = effectiveCapacity;
        for (const index of this.slots.keys())
            if (index >= count)
                this.slots.delete(index);
    }
    registerLimiter(name, snapshot) {
        if (this.limiterProviders.size < 128 || this.limiterProviders.has(name))
            this.limiterProviders.set(name, snapshot);
    }
    unregisterLimiter(name) { this.limiterProviders.delete(name); this.limiters.delete(name); }
    stop() {
        if (this.samplerTimer)
            clearTimeout(this.samplerTimer);
        if (this.flushTimer)
            clearInterval(this.flushTimer);
        this.gcObserver?.disconnect();
        this.samplerTimer = this.flushTimer = null;
    }
    // 1. Slot Utilization & Worker State Tracking
    slots = new Map();
    activeWorkersSamples = new BoundedSamples();
    activeWorkersDistribution = {};
    sourceActiveSamples = new Map();
    slotStateDistributionSamples = new BoundedSamples();
    samplerTimer = null;
    // 2. DB Pool Telemetry
    dbPoolWaitSamples = new BoundedSamples();
    dbPoolQueuedSamples = new BoundedSamples();
    dbPoolActiveQueries = 0;
    dbPoolTotalWaitMs = 0;
    dbPoolMaxWaitMs = 0;
    dbSqlSamples = new BoundedSamples();
    dbHoldSamples = new BoundedSamples();
    dbTransactionSamples = new BoundedSamples();
    dbQueryCount = 0;
    dbSqlTotalMs = 0;
    completedChapterCount = 0;
    recordDbQuery(ms) { this.dbSqlSamples.push(ms); this.dbQueryCount++; this.dbSqlTotalMs += ms; }
    recordDbHold(ms) { this.dbHoldSamples.push(ms); }
    recordDbTransaction(ms) { this.dbTransactionSamples.push(ms); }
    // 3. Telegram Storage Telemetry
    telegramActiveUploads = 0;
    telegramActiveUploadsSamples = new BoundedSamples();
    telegramPageUploadMsSamples = new BoundedSamples();
    telegramSemaphoreWaitSamples = new BoundedSamples();
    telegramTotalBytesUploaded = 0;
    // 4. Image Download Telemetry
    downloadActiveRequests = 0;
    downloadActiveSamples = new BoundedSamples();
    downloadPageMsSamples = new BoundedSamples();
    downloadSemaphoreWaitSamples = new BoundedSamples();
    downloadTotalBytes = 0;
    downloadErrorsCount = 0;
    downloadRetriesCount = 0;
    // 5. Host Rate Limiter Telemetry
    hostRateLimitWaitSamples = new Map();
    // 6. Internal Limiters & Semaphores Audit
    limiters = new Map();
    // 7. Chapter Jobs
    chapters = new BoundedSamples(256);
    // 8. Event Loop & Node Runtime Telemetry
    eventLoopLagSamples = new BoundedSamples();
    lastELU = performance.eventLoopUtilization ? performance.eventLoopUtilization() : null;
    eluHistory = new BoundedSamples();
    gcPauseSamples = new BoundedSamples();
    lastCpuUsage = process.cpuUsage();
    lastCpuTime = performance.now();
    cpuPercentSamples = new BoundedSamples();
    // Persistence
    poolRef = null;
    flushTimer = null;
    constructor() {
        this.startRuntimeSampling();
        this.initGcObserver();
    }
    static getInstance() {
        if (!TelemetryCollector.instance) {
            TelemetryCollector.instance = new TelemetryCollector();
        }
        return TelemetryCollector.instance;
    }
    setPool(pool) {
        this.poolRef = pool;
    }
    startSession(sessionId, expiresAt = Date.now() + 5 * 60_000) {
        this.activeSessionId = sessionId;
        this.sessionExpiresAt = Math.min(expiresAt, Date.now() + 5 * 60_000);
        this.sessionStartTime = performance.now();
        this.activeWorkersSamples = new BoundedSamples();
        this.activeWorkersDistribution = {};
        this.limiters.clear();
        this.telegramTotalBytesUploaded = this.downloadTotalBytes = 0;
        this.dbPoolWaitSamples = new BoundedSamples();
        this.dbPoolQueuedSamples = new BoundedSamples();
        this.dbPoolTotalWaitMs = 0;
        this.dbPoolMaxWaitMs = 0;
        this.dbQueryCount = this.dbSqlTotalMs = this.completedChapterCount = 0;
        this.dbSqlSamples = new BoundedSamples();
        this.dbHoldSamples = new BoundedSamples();
        this.dbTransactionSamples = new BoundedSamples();
        this.telegramActiveUploadsSamples = new BoundedSamples();
        this.telegramPageUploadMsSamples = new BoundedSamples();
        this.telegramSemaphoreWaitSamples = new BoundedSamples();
        this.downloadActiveSamples = new BoundedSamples();
        this.downloadPageMsSamples = new BoundedSamples();
        this.downloadSemaphoreWaitSamples = new BoundedSamples();
        this.downloadErrorsCount = 0;
        this.downloadRetriesCount = 0;
        this.hostRateLimitWaitSamples.clear();
        this.chapters = new BoundedSamples(256);
        this.gcPauseSamples = new BoundedSamples();
        this.eventLoopLagSamples = new BoundedSamples();
        this.eluHistory = new BoundedSamples();
        this.cpuPercentSamples = new BoundedSamples();
        this.sourceActiveSamples.clear();
        this.slotStateDistributionSamples = new BoundedSamples();
        // Reset slot timers
        const now = performance.now();
        for (const [_, slot] of this.slots.entries()) {
            slot.stateEnteredAt = now;
            slot.stateDurationMs = {
                WAITING_MUTEX: 0,
                WAITING_CLAIM_DB: 0,
                WAITING_SOURCE_PERMIT: 0,
                ACTIVE_SOURCE: 0,
                ACTIVE_DOWNLOAD: 0,
                ACTIVE_ENCODE: 0,
                ACTIVE_TELEGRAM: 0,
                ACTIVE_DB: 0,
                WAITING_BARRIER: 0,
                IDLE: 0,
            };
        }
        this.logger.info(`Started diagnostic telemetry session: ${sessionId}`);
    }
    getSessionId() {
        return this.activeSessionId;
    }
    // --- Slot State Tracking ---
    registerSlot(slotIndex) {
        if (!this.slots.has(slotIndex)) {
            this.slots.set(slotIndex, {
                slotIndex,
                currentState: 'IDLE',
                stateEnteredAt: performance.now(),
                stateDurationMs: {
                    WAITING_MUTEX: 0,
                    WAITING_CLAIM_DB: 0,
                    WAITING_SOURCE_PERMIT: 0,
                    ACTIVE_SOURCE: 0,
                    ACTIVE_DOWNLOAD: 0,
                    ACTIVE_ENCODE: 0,
                    ACTIVE_TELEGRAM: 0,
                    ACTIVE_DB: 0,
                    WAITING_BARRIER: 0,
                    IDLE: 0,
                },
            });
        }
    }
    setSlotState(slotIndex, newState, context) {
        let slot = this.slots.get(slotIndex);
        if (!slot) {
            this.registerSlot(slotIndex);
            slot = this.slots.get(slotIndex);
        }
        // Normalize legacy state names to guaranteed 10 mutually exclusive states
        let normalizedState = newState;
        if (newState === 'WAITING_FOR_JOB')
            normalizedState = 'WAITING_MUTEX';
        else if (newState === 'WAITING_FOR_SOURCE' || newState === 'WAITING_FOR_SOURCE_RATE_LIMIT')
            normalizedState = 'WAITING_SOURCE_PERMIT';
        else if (newState === 'WAITING_FOR_DOWNLOAD')
            normalizedState = 'ACTIVE_DOWNLOAD';
        else if (newState === 'WAITING_FOR_TELEGRAM')
            normalizedState = 'ACTIVE_TELEGRAM';
        else if (newState === 'WAITING_FOR_DATABASE' || newState === 'WAITING_FOR_DB_POOL')
            normalizedState = 'ACTIVE_DB';
        else if (newState === 'WAITING_FOR_PUBLICATION_BARRIER' || newState === 'PROTECTIVE_STOP')
            normalizedState = 'WAITING_BARRIER';
        else if (newState === 'ACTIVE_PROCESSING')
            normalizedState = 'ACTIVE_SOURCE';
        const now = performance.now();
        const elapsed = now - slot.stateEnteredAt;
        slot.stateDurationMs[slot.currentState] = (slot.stateDurationMs[slot.currentState] || 0) + elapsed;
        slot.currentState = normalizedState;
        slot.context = context;
        slot.stateEnteredAt = now;
    }
    /**
     * Authoritative calculation of productive vs busy vs idle slot occupancy.
     * Productive slots: ACTIVE_DOWNLOAD, ACTIVE_TELEGRAM, ACTIVE_DB, ACTIVE_SOURCE, ACTIVE_ENCODE.
     * Busy slots: all non-IDLE slots (including mutex/permit/db wait).
     */
    getSlotProductivitySnapshot() {
        const configuredSlots = this.configuredChapterSlots || this.slots.size;
        let busySlots = 0;
        let productiveSlots = 0;
        let idleSlots = Math.max(0, configuredSlots - this.slots.size);
        for (const slot of this.slots.values()) {
            if (slot.currentState === 'IDLE') {
                idleSlots++;
            }
            else {
                busySlots++;
                if (slot.currentState === 'ACTIVE_DOWNLOAD' ||
                    slot.currentState === 'ACTIVE_TELEGRAM' ||
                    slot.currentState === 'ACTIVE_DB' ||
                    slot.currentState === 'ACTIVE_SOURCE' ||
                    slot.currentState === 'ACTIVE_ENCODE') {
                    productiveSlots++;
                }
            }
        }
        const productiveSlotRatio = configuredSlots > 0
            ? Math.round((productiveSlots / configuredSlots) * 1000) / 10
            : 0;
        return {
            configuredSlots,
            busySlots,
            productiveSlots,
            idleSlots,
            productiveSlotRatio,
        };
    }
    // --- DB Pool Telemetry ---
    recordDbPoolWait(waitMs, waitingCount) {
        this.dbPoolWaitSamples.push(waitMs);
        this.dbPoolQueuedSamples.push(waitingCount);
        this.dbPoolTotalWaitMs += waitMs;
        if (waitMs > this.dbPoolMaxWaitMs)
            this.dbPoolMaxWaitMs = waitMs;
    }
    trackActiveDbQuery(delta) {
        this.dbPoolActiveQueries = Math.max(0, this.dbPoolActiveQueries + delta);
    }
    // --- Telegram Telemetry ---
    trackActiveTelegramUpload(delta) {
        this.telegramActiveUploads = Math.max(0, this.telegramActiveUploads + delta);
    }
    recordTelegramUpload(latencyMs, bytes) {
        this.telegramPageUploadMsSamples.push(latencyMs);
        this.telegramTotalBytesUploaded += bytes;
    }
    recordTelegramSemaphoreWait(waitMs) {
        this.telegramSemaphoreWaitSamples.push(waitMs);
    }
    // --- Image Download Telemetry ---
    trackActiveDownload(delta) {
        this.downloadActiveRequests = Math.max(0, this.downloadActiveRequests + delta);
    }
    recordImageDownload(source, latencyMs, bytes) {
        this.downloadPageMsSamples.push(latencyMs);
        this.downloadTotalBytes += bytes;
    }
    recordDownloadSemaphoreWait(waitMs) {
        this.downloadSemaphoreWaitSamples.push(waitMs);
    }
    recordDownloadError(retried) {
        if (retried)
            this.downloadRetriesCount++;
        else
            this.downloadErrorsCount++;
    }
    // --- Rate Limiter Telemetry ---
    recordRateLimitWait(host, waitMs) {
        let list = this.hostRateLimitWaitSamples.get(host);
        if (!list) {
            if (this.hostRateLimitWaitSamples.size >= 128)
                return;
            list = new BoundedSamples();
            this.hostRateLimitWaitSamples.set(host, list);
        }
        list.push(waitMs);
        this.recordLimiterWait(`host_rate_limiter:${host}`, waitMs, 'dynamic');
    }
    // --- Generic Limiter Audit ---
    recordLimiterWait(name, waitMs, limit = 'unknown') {
        let rec = this.limiters.get(name);
        if (!rec) {
            if (this.limiters.size >= 128)
                return;
            rec = {
                name,
                configuredLimit: limit,
                observedConcurrencyPeak: 0,
                observedConcurrencyAvg: 0,
                hitCount: 0,
                waitSamples: new BoundedSamples(),
                totalWaitMs: 0,
                maxWaitMs: 0,
            };
            this.limiters.set(name, rec);
        }
        rec.waitSamples.push(waitMs);
        rec.totalWaitMs += waitMs;
        if (waitMs > 0)
            rec.hitCount++;
        if (waitMs > rec.maxWaitMs)
            rec.maxWaitMs = waitMs;
    }
    updateLimiterConcurrency(name, current, limit) {
        let rec = this.limiters.get(name);
        if (!rec) {
            if (this.limiters.size >= 128)
                return;
            rec = {
                name,
                configuredLimit: limit ?? 'unknown',
                observedConcurrencyPeak: current,
                observedConcurrencyAvg: current,
                hitCount: 0,
                waitSamples: new BoundedSamples(),
                totalWaitMs: 0,
                maxWaitMs: 0,
            };
            this.limiters.set(name, rec);
        }
        if (current > rec.observedConcurrencyPeak) {
            rec.observedConcurrencyPeak = current;
        }
        if (limit !== undefined) {
            rec.configuredLimit = limit;
        }
    }
    // --- Chapter Profile Recording ---
    recordChapterMetric(record) {
        this.completedChapterCount++;
        this.chapters.push(record);
        this.logger.debug('CHAPTER_DIAGNOSTIC', record);
    }
    // --- Background Sampling ---
    startRuntimeSampling() {
        const sample = () => {
            if (this.activeSessionId && Date.now() >= this.sessionExpiresAt)
                this.activeSessionId = null;
            // Sample the configured runner pool, including idle runners after downscale.
            let activeCount = 0;
            const sourceCounts = new Map();
            const currentStatesCount = {
                WAITING_MUTEX: 0,
                WAITING_CLAIM_DB: 0,
                WAITING_SOURCE_PERMIT: 0,
                ACTIVE_SOURCE: 0,
                ACTIVE_DOWNLOAD: 0,
                ACTIVE_ENCODE: 0,
                ACTIVE_TELEGRAM: 0,
                ACTIVE_DB: 0,
                WAITING_BARRIER: 0,
                IDLE: 0,
            };
            for (let i = 0; i < (this.configuredChapterSlots || this.slots.size); i++) {
                const slot = this.slots.get(i);
                const st = slot?.currentState || 'IDLE';
                currentStatesCount[st] = (currentStatesCount[st] || 0) + 1;
                if (st !== 'IDLE') {
                    activeCount++;
                }
                if (slot?.context) {
                    const src = slot.context.split(' ')[0];
                    if (src) {
                        sourceCounts.set(src, (sourceCounts.get(src) || 0) + 1);
                    }
                }
            }
            this.slotStateDistributionSamples.push(currentStatesCount);
            this.activeWorkersSamples.push(activeCount);
            const bucket = Math.max(0, activeCount);
            this.activeWorkersDistribution[bucket] = (this.activeWorkersDistribution[bucket] || 0) + 1;
            for (const src of new Set([...this.sourceActiveSamples.keys(), ...sourceCounts.keys()])) {
                const c = sourceCounts.get(src) || 0;
                let arr = this.sourceActiveSamples.get(src);
                if (!arr) {
                    if (this.sourceActiveSamples.size >= 128)
                        continue;
                    arr = new BoundedSamples();
                    this.sourceActiveSamples.set(src, arr);
                }
                arr.push(c);
            }
            // 2. Telegram concurrency sample
            this.telegramActiveUploadsSamples.push(this.telegramActiveUploads);
            // 3. Download concurrency sample
            this.downloadActiveSamples.push(this.downloadActiveRequests);
            for (const [name, provider] of this.limiterProviders) {
                const state = provider();
                this.updateLimiterConcurrency(name, state.active, state.configuredCapacity);
                const limiter = this.limiters.get(name);
                if (limiter)
                    (limiter.concurrencySamples ||= new BoundedSamples()).push(state.active);
            }
            // 4. Event loop lag sample
            // Checked via lag monitor if needed
            // 5. CPU usage sample
            const cpuNow = process.cpuUsage();
            const timeNow = performance.now();
            const elapsedMs = timeNow - this.lastCpuTime;
            if (elapsedMs > 500) {
                const userDiff = (cpuNow.user - this.lastCpuUsage.user) / 1000;
                const sysDiff = (cpuNow.system - this.lastCpuUsage.system) / 1000;
                const totalCpuMs = userDiff + sysDiff;
                const percent = Math.round((totalCpuMs / elapsedMs) * 100 * 10) / 10;
                this.cpuPercentSamples.push(percent);
                this.lastCpuUsage = cpuNow;
                this.lastCpuTime = timeNow;
            }
            // 6. ELU sample
            if (performance.eventLoopUtilization && this.lastELU) {
                const currentELU = performance.eventLoopUtilization();
                const elu = performance.eventLoopUtilization(currentELU, this.lastELU);
                this.lastELU = currentELU;
                this.eluHistory.push(Math.round(elu.utilization * 1000) / 10);
            }
            this.samplerTimer = setTimeout(sample, this.activeSessionId ? 200 : 1000);
            this.samplerTimer.unref();
        };
        this.samplerTimer = setTimeout(sample, 1000);
        this.samplerTimer.unref();
        // Session polling is separately paced to 30s; diagnostic flushes use 5s.
        this.flushTimer = setInterval(async () => {
            await this.flushTelemetryToDb();
        }, 5000);
        this.flushTimer.unref();
    }
    initGcObserver() {
        try {
            const obs = new PerformanceObserver((list) => {
                for (const entry of list.getEntries()) {
                    this.gcPauseSamples.push(entry.duration);
                }
            });
            obs.observe({ entryTypes: ['gc'] });
            this.gcObserver = obs;
        }
        catch {
            // GC observation not supported in all environments
        }
    }
    recordEventLoopLag(lagMs) {
        this.eventLoopLagSamples.push(lagMs);
    }
    // --- Snapshot Generation ---
    getSnapshotReport() {
        const mem = process.memoryUsage();
        const totalSlotSamples = this.activeWorkersSamples.length || 1;
        const configuredSlots = this.configuredChapterSlots || this.slots.size;
        const distribution = {};
        for (let i = 0; i <= configuredSlots; i++)
            distribution[i] = 0;
        for (const active of this.activeWorkersSamples)
            distribution[active] = (distribution[active] || 0) + 1;
        const timeAtConfiguredCapacityPercent = Math.round(((distribution[configuredSlots] || 0) / totalSlotSamples) * 1000) / 10;
        const totalSlotDistributionSamples = this.slotStateDistributionSamples.length || 1;
        const rawSums = {
            WAITING_MUTEX: 0,
            WAITING_CLAIM_DB: 0,
            WAITING_SOURCE_PERMIT: 0,
            ACTIVE_SOURCE: 0,
            ACTIVE_DOWNLOAD: 0,
            ACTIVE_ENCODE: 0,
            ACTIVE_TELEGRAM: 0,
            ACTIVE_DB: 0,
            WAITING_BARRIER: 0,
            IDLE: 0,
        };
        for (const sample of this.slotStateDistributionSamples) {
            for (const [st, count] of Object.entries(sample)) {
                rawSums[st] = (rawSums[st] || 0) + count;
            }
        }
        const avgSlotStates = {
            WAITING_MUTEX: Math.round((rawSums.WAITING_MUTEX / totalSlotDistributionSamples) * 100) / 100,
            WAITING_CLAIM_DB: Math.round((rawSums.WAITING_CLAIM_DB / totalSlotDistributionSamples) * 100) / 100,
            WAITING_SOURCE_PERMIT: Math.round((rawSums.WAITING_SOURCE_PERMIT / totalSlotDistributionSamples) * 100) / 100,
            ACTIVE_SOURCE: Math.round((rawSums.ACTIVE_SOURCE / totalSlotDistributionSamples) * 100) / 100,
            ACTIVE_DOWNLOAD: Math.round((rawSums.ACTIVE_DOWNLOAD / totalSlotDistributionSamples) * 100) / 100,
            ACTIVE_ENCODE: Math.round((rawSums.ACTIVE_ENCODE / totalSlotDistributionSamples) * 100) / 100,
            ACTIVE_TELEGRAM: Math.round((rawSums.ACTIVE_TELEGRAM / totalSlotDistributionSamples) * 100) / 100,
            ACTIVE_DB: Math.round((rawSums.ACTIVE_DB / totalSlotDistributionSamples) * 100) / 100,
            WAITING_BARRIER: Math.round((rawSums.WAITING_BARRIER / totalSlotDistributionSamples) * 100) / 100,
            IDLE: Math.round((rawSums.IDLE / totalSlotDistributionSamples) * 100) / 100,
        };
        const sumStates = Object.values(avgSlotStates).reduce((a, b) => a + b, 0);
        const slotStatesAggregated = {
            WAITING_MUTEX: 0,
            WAITING_CLAIM_DB: 0,
            WAITING_SOURCE_PERMIT: 0,
            ACTIVE_SOURCE: 0,
            ACTIVE_DOWNLOAD: 0,
            ACTIVE_ENCODE: 0,
            ACTIVE_TELEGRAM: 0,
            ACTIVE_DB: 0,
            WAITING_BARRIER: 0,
            IDLE: 0,
        };
        let totalBusyMs = 0;
        let totalIdleMs = 0;
        let totalBlockedMs = 0;
        let totalAllMs = 0;
        const now = performance.now();
        for (const [_, slot] of this.slots.entries()) {
            const currentElapsed = now - slot.stateEnteredAt;
            for (const st of Object.keys(slot.stateDurationMs)) {
                let ms = slot.stateDurationMs[st] || 0;
                if (st === slot.currentState)
                    ms += currentElapsed;
                slotStatesAggregated[st] = (slotStatesAggregated[st] || 0) + ms;
                totalAllMs += ms;
                if (st === 'IDLE') {
                    totalIdleMs += ms;
                }
                else if (st.startsWith('ACTIVE_')) {
                    totalBusyMs += ms;
                }
                else {
                    totalBlockedMs += ms;
                }
            }
        }
        const safeTotalAll = totalAllMs || 1;
        const workerBusyPercent = Math.round((totalBusyMs / safeTotalAll) * 1000) / 10;
        const workerIdlePercent = Math.round((totalIdleMs / safeTotalAll) * 1000) / 10;
        const workerBlockedPercent = Math.round((totalBlockedMs / safeTotalAll) * 1000) / 10;
        // Chapter stages stats
        const mutexWaitTimes = this.chapters.map(c => c.mutex_wait_ms || 0);
        const schedulerAcquireTimes = this.chapters.map(c => c.scheduler_acquire_ms ?? c.claim_db_ms ?? 0);
        const poolWaitTimes = this.chapters.map(c => c.pool_wait_ms ?? 0);
        const sqlExecTimes = this.chapters.map(c => c.sql_exec_ms ?? 0);
        const claimSqlTimes = this.chapters.map(c => c.claim_sql_ms ?? c.sql_exec_ms ?? 0);
        const claimLockPoolWaitTimes = this.chapters.map(c => c.claim_lock_pool_wait_ms ?? 0);
        const claimLockSqlExecTimes = this.chapters.map(c => c.claim_lock_sql_exec_ms ?? c.claim_sql_ms ?? 0);
        const queriesPerClaim = this.chapters.map(c => c.total_queries ?? 1);
        const worksTestedList = this.chapters.map(c => c.works_tested ?? 1);
        const staffCheckTimes = this.chapters.map(c => c.staff_check_ms ?? 0);
        const p0ProbeTimes = this.chapters.map(c => c.p0_probe_ms ?? 0);
        const criticalWorkTimes = this.chapters.map(c => c.critical_work_time_ms ?? 0);
        const p1WorkTimes = this.chapters.map(c => c.p1_work_time_ms ?? 0);
        const p2WorkTimes = this.chapters.map(c => c.p2_work_time_ms ?? 0);
        const activeFallbackTimes = this.chapters.map(c => c.active_fallback_ms ?? 0);
        const admissionOnDemandTimes = this.chapters.map(c => c.admission_on_demand_ms ?? 0);
        const catalogFallbackTimes = this.chapters.map(c => c.catalog_fallback_ms ?? 0);
        const claimDbTimes = schedulerAcquireTimes;
        const claimTimes = this.chapters.map(c => c.claim_acquire_ms);
        const metadataTimes = this.chapters.map(c => c.metadata_load_ms);
        const sourceFetchTimes = this.chapters.map(c => c.source_fetch_ms);
        const pageResTimes = this.chapters.map(c => c.page_resolution_ms);
        const downloadTimes = this.chapters.map(c => c.download_ms);
        const encodeTimes = this.chapters.map(c => c.encode_ms || 0);
        const telegramTimes = this.chapters.map(c => c.telegram_upload_ms);
        const dbWaitTimes = this.chapters.map(c => c.db_wait_ms);
        const dbPublishTimes = this.chapters.map(c => c.db_publish_ms);
        const rateLimitTimes = this.chapters.map(c => c.rate_limit_wait_ms);
        const semWaitTimes = this.chapters.map(c => c.semaphore_wait_ms);
        const otherWaitTimes = this.chapters.map(c => c.other_wait_ms);
        const totalJobTimes = this.chapters.map(c => c.totalDurationMs);
        const totalSlotOccupancyTimes = this.chapters.map(c => c.totalSlotOccupancyMs || (c.totalDurationMs + c.claim_acquire_ms));
        // Service time & theoretical capacity
        const occupancyTimesSec = totalSlotOccupancyTimes.map(ms => ms / 1000);
        const meanSlotOccupancySec = avg(occupancyTimesSec);
        const p50SlotOccupancySec = percentile(occupancyTimesSec, 0.50);
        const p95SlotOccupancySec = percentile(occupancyTimesSec, 0.95);
        const avgBusyWorkers = avg(this.activeWorkersSamples);
        const theoreticalCapacityPerMin = meanSlotOccupancySec > 0 ? (avgBusyWorkers * 60) / meanSlotOccupancySec : 0;
        // Source distributions
        const sourceDist = {};
        for (const ch of this.chapters) {
            if (!sourceDist[ch.source]) {
                sourceDist[ch.source] = {
                    count: 0,
                    totalPages: 0,
                    totalBytes: 0,
                    totalDurationMs: 0,
                    avgDurationMs: 0,
                    avgDownloadMs: 0,
                    avgUploadMs: 0,
                    avgDbMs: 0,
                    avgSemWaitMs: 0,
                    avgRateLimitWaitMs: 0,
                };
            }
            const s = sourceDist[ch.source];
            s.count++;
            s.totalPages += ch.pageCount;
            s.totalBytes += ch.totalBytes;
            s.totalDurationMs += ch.totalDurationMs;
        }
        for (const [src, s] of Object.entries(sourceDist)) {
            const srcChapters = this.chapters.filter(c => c.source === src);
            s.avgDurationMs = avg(srcChapters.map(c => c.totalDurationMs));
            s.avgDownloadMs = avg(srcChapters.map(c => c.download_ms));
            s.avgUploadMs = avg(srcChapters.map(c => c.telegram_upload_ms));
            s.avgDbMs = avg(srcChapters.map(c => c.db_publish_ms));
            s.avgSemWaitMs = avg(srcChapters.map(c => c.semaphore_wait_ms));
            s.avgRateLimitWaitMs = avg(srcChapters.map(c => c.rate_limit_wait_ms));
        }
        // Top 10 slowest chapters
        const slowestChapters = [...this.chapters]
            .sort((a, b) => b.totalDurationMs - a.totalDurationMs)
            .slice(0, 10)
            .map(c => {
            let reason = 'Normal execution';
            if (c.semaphore_wait_ms > c.totalDurationMs * 0.4) {
                reason = `Blocked on source/global semaphore (${c.semaphore_wait_ms}ms)`;
            }
            else if (c.telegram_upload_ms > c.totalDurationMs * 0.5) {
                reason = `Telegram upload latency (${c.telegram_upload_ms}ms for ${c.pageCount} pages)`;
            }
            else if (c.download_ms > c.totalDurationMs * 0.5) {
                reason = `Source image download CDN latency (${c.download_ms}ms for ${c.pageCount} pages)`;
            }
            else if (c.rate_limit_wait_ms > c.totalDurationMs * 0.3) {
                reason = `Host rate limiter throttle (${c.rate_limit_wait_ms}ms)`;
            }
            else if (c.db_publish_ms > c.totalDurationMs * 0.4) {
                reason = `Database publish/lock wait (${c.db_publish_ms}ms)`;
            }
            return {
                ...c,
                slowReason: reason,
            };
        });
        // Limiters audit summary
        const limitersSummary = {};
        for (const [name, rec] of this.limiters.entries()) {
            limitersSummary[name] = {
                configuredLimit: rec.configuredLimit,
                observedConcurrencyPeak: rec.observedConcurrencyPeak,
                observedConcurrencyAvg: avg(rec.concurrencySamples || []),
                hitCount: rec.hitCount,
                waitAvgMs: avg(rec.waitSamples),
                waitP50Ms: percentile(rec.waitSamples, 0.50),
                waitP95Ms: percentile(rec.waitSamples, 0.95),
                waitMaxMs: rec.maxWaitMs,
            };
        }
        for (const [name, provider] of this.limiterProviders) {
            const state = provider();
            limitersSummary[name] = { waitP50Ms: 0, waitP95Ms: 0, ...limitersSummary[name], ...state,
                saturationPercent: state.currentCapacity > 0 ? state.active / state.currentCapacity * 100 : 0 };
        }
        const avgActiveProcessing = Math.round((avgSlotStates.ACTIVE_SOURCE +
            avgSlotStates.ACTIVE_DOWNLOAD +
            avgSlotStates.ACTIVE_ENCODE +
            avgSlotStates.ACTIVE_TELEGRAM +
            avgSlotStates.ACTIVE_DB) * 100) / 100;
        const avgBlocked = Math.round((avgSlotStates.WAITING_MUTEX +
            avgSlotStates.WAITING_CLAIM_DB +
            avgSlotStates.WAITING_SOURCE_PERMIT +
            avgSlotStates.WAITING_BARRIER) * 100) / 100;
        const avgIdle = avgSlotStates.IDLE;
        return {
            sessionId: this.activeSessionId,
            timestamp: new Date().toISOString(),
            telemetryMode: this.activeSessionId ? 'DIAGNOSTIC' : 'NORMAL',
            runtimeFingerprint: this.runtimeFingerprint,
            maintenance: maintenanceScheduler.snapshot(),
            boundedCpuProfile: this.cpuProfile,
            diagnosticExpiresAt: this.activeSessionId ? new Date(this.sessionExpiresAt).toISOString() : null,
            sampleCapacity: 2048,
            slotsConfigured: configuredSlots,
            effectiveConcurrency: this.effectiveCapacity(),
            database: {
                queries: this.dbQueryCount, sqlTotalMs: Math.round(this.dbSqlTotalMs),
                completedChapters: this.completedChapterCount,
                amortizedQueriesPerCompletedChapter: this.completedChapterCount ? this.dbQueryCount / this.completedChapterCount : null,
                amortizedSqlMsPerCompletedChapter: this.completedChapterCount ? this.dbSqlTotalMs / this.completedChapterCount : null,
                sqlP50Ms: percentile(this.dbSqlSamples, 0.50), sqlP95Ms: percentile(this.dbSqlSamples, 0.95),
                clientHoldP50Ms: percentile(this.dbHoldSamples, 0.50), clientHoldP95Ms: percentile(this.dbHoldSamples, 0.95),
                transactionP50Ms: percentile(this.dbTransactionSamples, 0.50), transactionP95Ms: percentile(this.dbTransactionSamples, 0.95),
                poolMax: this.poolRef?.options.max, totalConnections: this.poolRef?.totalCount,
                idleConnections: this.poolRef?.idleCount, waitingClients: this.poolRef?.waitingCount,
            },
            avgSlotStates: {
                ...avgSlotStates,
                ACTIVE_PROCESSING: avgActiveProcessing,
                BLOCKED: avgBlocked,
                SUM: Math.round(sumStates * 100) / 100,
            },
            slotOccupancy: {
                meanSec: Math.round(meanSlotOccupancySec * 100) / 100,
                p50Sec: Math.round(p50SlotOccupancySec * 100) / 100,
                p95Sec: Math.round(p95SlotOccupancySec * 100) / 100,
                avgBusyWorkers: avgActiveProcessing,
                avgActiveProcessing,
                avgBlocked,
                avgIdle,
                theoreticalCapacityPerMin: meanSlotOccupancySec > 0 ? Math.round(((this.effectiveCapacity() * 60) / meanSlotOccupancySec) * 100) / 100 : 0,
            },
            activeWorkers: {
                avg: avg(this.activeWorkersSamples),
                p50: percentile(this.activeWorkersSamples, 0.50),
                p75: percentile(this.activeWorkersSamples, 0.75),
                p95: percentile(this.activeWorkersSamples, 0.95),
                peak: this.activeWorkersSamples.length ? Math.max(...this.activeWorkersSamples) : 0,
                distribution,
                timeAtConfiguredCapacityPercent,
            },
            perSourceActive: Object.fromEntries([...this.sourceActiveSamples].map(([source, samples]) => [source, { avg: avg(samples), peak: samples.length ? Math.max(...samples) : 0 }])),
            workerTimeBreakdown: {
                workerBusyPercent,
                workerIdlePercent,
                workerBlockedPercent,
                statesAggregatedMs: slotStatesAggregated,
            },
            jobProfile: {
                wallTimeBreakdownMs: Object.fromEntries(['metadata_load_ms', 'source_fetch_ms', 'page_resolution_ms',
                    'media_pipeline_wall_ms', 'cover_check_ms', 'db_publish_ms', 'other_wait_ms'].map(key => [key, avg(this.chapters.map(c => Number(c[key] || 0)))])),
                totalCompleted: this.chapters.length,
                totalDuration: {
                    avg: avg(totalJobTimes),
                    p50: percentile(totalJobTimes, 0.50),
                    p95: percentile(totalJobTimes, 0.95),
                    max: totalJobTimes.length ? Math.max(...totalJobTimes) : 0,
                },
                stages: {
                    schedulerAcquire: { avg: avg(schedulerAcquireTimes), p50: percentile(schedulerAcquireTimes, 0.50), p95: percentile(schedulerAcquireTimes, 0.95), p99: percentile(schedulerAcquireTimes, 0.99), max: schedulerAcquireTimes.length ? Math.max(...schedulerAcquireTimes) : 0 },
                    claimDb: { avg: avg(schedulerAcquireTimes), p50: percentile(schedulerAcquireTimes, 0.50), p95: percentile(schedulerAcquireTimes, 0.95), p99: percentile(schedulerAcquireTimes, 0.99), max: schedulerAcquireTimes.length ? Math.max(...schedulerAcquireTimes) : 0 },
                    poolWait: { avg: avg(poolWaitTimes), p50: percentile(poolWaitTimes, 0.50), p95: percentile(poolWaitTimes, 0.95), max: poolWaitTimes.length ? Math.max(...poolWaitTimes) : 0 },
                    sqlExec: { avg: avg(sqlExecTimes), p50: percentile(sqlExecTimes, 0.50), p95: percentile(sqlExecTimes, 0.95), max: sqlExecTimes.length ? Math.max(...sqlExecTimes) : 0 },
                    schedulerSqlTotal: { avg: avg(sqlExecTimes), p50: percentile(sqlExecTimes, 0.50), p95: percentile(sqlExecTimes, 0.95), max: sqlExecTimes.length ? Math.max(...sqlExecTimes) : 0 },
                    claimSql: { avg: avg(claimSqlTimes), p50: percentile(claimSqlTimes, 0.50), p95: percentile(claimSqlTimes, 0.95), max: claimSqlTimes.length ? Math.max(...claimSqlTimes) : 0 },
                    claimLockSql: { avg: avg(claimLockSqlExecTimes), p50: percentile(claimLockSqlExecTimes, 0.50), p95: percentile(claimLockSqlExecTimes, 0.95), max: claimLockSqlExecTimes.length ? Math.max(...claimLockSqlExecTimes) : 0 },
                    claimLockPoolWait: { avg: avg(claimLockPoolWaitTimes), p50: percentile(claimLockPoolWaitTimes, 0.50), p95: percentile(claimLockPoolWaitTimes, 0.95), max: claimLockPoolWaitTimes.length ? Math.max(...claimLockPoolWaitTimes) : 0 },
                    claimLockSqlExec: { avg: avg(claimLockSqlExecTimes), p50: percentile(claimLockSqlExecTimes, 0.50), p95: percentile(claimLockSqlExecTimes, 0.95), max: claimLockSqlExecTimes.length ? Math.max(...claimLockSqlExecTimes) : 0 },
                    queriesPerClaim: { avg: avg(queriesPerClaim), p50: percentile(queriesPerClaim, 0.50), p95: percentile(queriesPerClaim, 0.95) },
                    worksTested: { avg: avg(worksTestedList), p50: percentile(worksTestedList, 0.50), p95: percentile(worksTestedList, 0.95) },
                    mutexWait: { avg: avg(mutexWaitTimes), p50: percentile(mutexWaitTimes, 0.50), p95: percentile(mutexWaitTimes, 0.95), p99: percentile(mutexWaitTimes, 0.99), max: mutexWaitTimes.length ? Math.max(...mutexWaitTimes) : 0 },
                    claim: { avg: avg(claimTimes), p50: percentile(claimTimes, 0.50), p75: percentile(claimTimes, 0.75), p95: percentile(claimTimes, 0.95), p99: percentile(claimTimes, 0.99), max: claimTimes.length ? Math.max(...claimTimes) : 0 },
                    metadata: { avg: avg(metadataTimes), p50: percentile(metadataTimes, 0.50), p75: percentile(metadataTimes, 0.75), p95: percentile(metadataTimes, 0.95), p99: percentile(metadataTimes, 0.99), max: metadataTimes.length ? Math.max(...metadataTimes) : 0 },
                    sourceFetch: { avg: avg(sourceFetchTimes), p50: percentile(sourceFetchTimes, 0.50), p75: percentile(sourceFetchTimes, 0.75), p95: percentile(sourceFetchTimes, 0.95), p99: percentile(sourceFetchTimes, 0.99), max: sourceFetchTimes.length ? Math.max(...sourceFetchTimes) : 0 },
                    pageResolution: { avg: avg(pageResTimes), p50: percentile(pageResTimes, 0.50), p75: percentile(pageResTimes, 0.75), p95: percentile(pageResTimes, 0.95), p99: percentile(pageResTimes, 0.99), max: pageResTimes.length ? Math.max(...pageResTimes) : 0 },
                    download: { avg: avg(downloadTimes), p50: percentile(downloadTimes, 0.50), p75: percentile(downloadTimes, 0.75), p95: percentile(downloadTimes, 0.95), p99: percentile(downloadTimes, 0.99), max: downloadTimes.length ? Math.max(...downloadTimes) : 0 },
                    encode: { avg: avg(encodeTimes), p50: percentile(encodeTimes, 0.50), p75: percentile(encodeTimes, 0.75), p95: percentile(encodeTimes, 0.95), p99: percentile(encodeTimes, 0.99), max: encodeTimes.length ? Math.max(...encodeTimes) : 0 },
                    telegramUpload: { avg: avg(telegramTimes), p50: percentile(telegramTimes, 0.50), p75: percentile(telegramTimes, 0.75), p95: percentile(telegramTimes, 0.95), p99: percentile(telegramTimes, 0.99), max: telegramTimes.length ? Math.max(...telegramTimes) : 0 },
                    dbWait: { avg: avg(dbWaitTimes), p50: percentile(dbWaitTimes, 0.50), p75: percentile(dbWaitTimes, 0.75), p95: percentile(dbWaitTimes, 0.95), p99: percentile(dbWaitTimes, 0.99), max: dbWaitTimes.length ? Math.max(...dbWaitTimes) : 0 },
                    dbPublish: { avg: avg(dbPublishTimes), p50: percentile(dbPublishTimes, 0.50), p75: percentile(dbPublishTimes, 0.75), p95: percentile(dbPublishTimes, 0.95), p99: percentile(dbPublishTimes, 0.99), max: dbPublishTimes.length ? Math.max(...dbPublishTimes) : 0 },
                    rateLimitWait: { avg: avg(rateLimitTimes), p50: percentile(rateLimitTimes, 0.50), p75: percentile(rateLimitTimes, 0.75), p95: percentile(rateLimitTimes, 0.95), p99: percentile(rateLimitTimes, 0.99), max: rateLimitTimes.length ? Math.max(...rateLimitTimes) : 0 },
                    semaphoreWait: { avg: avg(semWaitTimes), p50: percentile(semWaitTimes, 0.50), p75: percentile(semWaitTimes, 0.75), p95: percentile(semWaitTimes, 0.95), p99: percentile(semWaitTimes, 0.99), max: semWaitTimes.length ? Math.max(...semWaitTimes) : 0 },
                    otherWait: { avg: avg(otherWaitTimes), p50: percentile(otherWaitTimes, 0.50), p75: percentile(otherWaitTimes, 0.75), p95: percentile(otherWaitTimes, 0.95), p99: percentile(otherWaitTimes, 0.99), max: otherWaitTimes.length ? Math.max(...otherWaitTimes) : 0 },
                },
            },
            schedulerAcquireBreakdown: {
                totalMs: { avg: avg(schedulerAcquireTimes), p50: percentile(schedulerAcquireTimes, 0.50), p95: percentile(schedulerAcquireTimes, 0.95) },
                poolWaitMs: { avg: avg(poolWaitTimes), p50: percentile(poolWaitTimes, 0.50), p95: percentile(poolWaitTimes, 0.95) },
                sqlExecMs: { avg: avg(sqlExecTimes), p50: percentile(sqlExecTimes, 0.50), p95: percentile(sqlExecTimes, 0.95) },
                claimLockPoolWaitMs: { avg: avg(claimLockPoolWaitTimes), p50: percentile(claimLockPoolWaitTimes, 0.50), p95: percentile(claimLockPoolWaitTimes, 0.95) },
                claimLockSqlExecMs: { avg: avg(claimLockSqlExecTimes), p50: percentile(claimLockSqlExecTimes, 0.50), p95: percentile(claimLockSqlExecTimes, 0.95) },
                queriesCount: { avg: avg(queriesPerClaim), p50: percentile(queriesPerClaim, 0.50), p95: percentile(queriesPerClaim, 0.95) },
                worksTestedCount: { avg: avg(worksTestedList), p50: percentile(worksTestedList, 0.50), p95: percentile(worksTestedList, 0.95) },
                staffCheckMs: { avg: avg(staffCheckTimes), p50: percentile(staffCheckTimes, 0.50), p95: percentile(staffCheckTimes, 0.95) },
                p0ProbeMs: { avg: avg(p0ProbeTimes), p50: percentile(p0ProbeTimes, 0.50), p95: percentile(p0ProbeTimes, 0.95) },
                criticalWorkTimeMs: { avg: avg(criticalWorkTimes), p50: percentile(criticalWorkTimes, 0.50), p95: percentile(criticalWorkTimes, 0.95) },
                p1WorkTimeMs: { avg: avg(p1WorkTimes), p50: percentile(p1WorkTimes, 0.50), p95: percentile(p1WorkTimes, 0.95) },
                p2WorkTimeMs: { avg: avg(p2WorkTimes), p50: percentile(p2WorkTimes, 0.50), p95: percentile(p2WorkTimes, 0.95) },
                activeFallbackMs: { avg: avg(activeFallbackTimes), p50: percentile(activeFallbackTimes, 0.50), p95: percentile(activeFallbackTimes, 0.95) },
                admissionOnDemandMs: { avg: avg(admissionOnDemandTimes), p50: percentile(admissionOnDemandTimes, 0.50), p95: percentile(admissionOnDemandTimes, 0.95) },
                catalogFallbackMs: { avg: avg(catalogFallbackTimes), p50: percentile(catalogFallbackTimes, 0.50), p95: percentile(catalogFallbackTimes, 0.95) },
            },
            slowestChapters,
            sourceDistribution: sourceDist,
            limitersAudit: limitersSummary,
            yugabyteDbPool: {
                configuredMax: Number(this.poolRef?.options?.max || 2),
                waitAvgMs: avg(this.dbPoolWaitSamples),
                waitP50Ms: percentile(this.dbPoolWaitSamples, 0.50),
                waitP95Ms: percentile(this.dbPoolWaitSamples, 0.95),
                waitMaxMs: this.dbPoolMaxWaitMs,
                queuedWaitingAvg: avg(this.dbPoolQueuedSamples),
                queuedWaitingPeak: this.dbPoolQueuedSamples.length ? Math.max(...this.dbPoolQueuedSamples) : 0,
                totalQueriesSampled: this.dbPoolWaitSamples.length,
            },
            telegramStorage: {
                activeUploadsAvg: avg(this.telegramActiveUploadsSamples),
                activeUploadsP95: percentile(this.telegramActiveUploadsSamples, 0.95),
                activeUploadsPeak: this.telegramActiveUploadsSamples.length ? Math.max(...this.telegramActiveUploadsSamples) : 0,
                pageUploadDurationAvg: avg(this.telegramPageUploadMsSamples),
                pageUploadDurationP95: percentile(this.telegramPageUploadMsSamples, 0.95),
                semaphoreWaitAvgMs: avg(this.telegramSemaphoreWaitSamples),
                semaphoreWaitP95Ms: percentile(this.telegramSemaphoreWaitSamples, 0.95),
                totalBytesUploaded: this.telegramTotalBytesUploaded,
            },
            imageDownload: {
                activeRequestsAvg: avg(this.downloadActiveSamples),
                activeRequestsP95: percentile(this.downloadActiveSamples, 0.95),
                activeRequestsPeak: this.downloadActiveSamples.length ? Math.max(...this.downloadActiveSamples) : 0,
                pageDownloadDurationAvg: avg(this.downloadPageMsSamples),
                pageDownloadDurationP95: percentile(this.downloadPageMsSamples, 0.95),
                semaphoreWaitAvgMs: avg(this.downloadSemaphoreWaitSamples),
                semaphoreWaitP95Ms: percentile(this.downloadSemaphoreWaitSamples, 0.95),
                totalBytesDownloaded: this.downloadTotalBytes,
                errorsCount: this.downloadErrorsCount,
                retriesCount: this.downloadRetriesCount,
            },
            eventLoopAndNode: {
                uptimeSeconds: process.uptime(),
                cpuTotalMicroseconds: process.cpuUsage().user + process.cpuUsage().system,
                eventLoopLagAvg: avg(this.eventLoopLagSamples),
                eventLoopLagP95: percentile(this.eventLoopLagSamples, 0.95),
                eventLoopLagMax: this.eventLoopLagSamples.length ? Math.max(...this.eventLoopLagSamples) : 0,
                eventLoopUtilizationAvg: avg(this.eluHistory),
                processCpuPercentAvg: avg(this.cpuPercentSamples),
                processCpuPercentPeak: this.cpuPercentSamples.length ? Math.max(...this.cpuPercentSamples) : 0,
                gcPausesCount: this.gcPauseSamples.length,
                gcPausesTotalMs: Math.round(this.gcPauseSamples.reduce((a, b) => a + b, 0)),
                gcPausesMaxMs: this.gcPauseSamples.length ? Math.max(...this.gcPauseSamples) : 0,
                rssMb: Math.round(mem.rss / 1024 / 1024),
                heapUsedMb: Math.round(mem.heapUsed / 1024 / 1024),
                heapTotalMb: Math.round(mem.heapTotal / 1024 / 1024),
                externalMb: Math.round(mem.external / 1024 / 1024),
                arrayBuffersMb: Math.round(mem.arrayBuffers / 1024 / 1024),
                gcPauseP50Ms: percentile(this.gcPauseSamples, 0.50),
                gcPauseP95Ms: percentile(this.gcPauseSamples, 0.95),
            },
        };
    }
    // --- Persistence to DB ---
    async flushTelemetryToDb() {
        if (!this.poolRef || this.flushing)
            return;
        if (!this.activeSessionId && Date.now() < this.nextSessionCheck)
            return;
        this.flushing = true;
        try {
            // 1. Check if an active diagnostic session has been requested via settings
            const settingRes = await this.poolRef.query("SELECT value FROM settings WHERE key = 'active_diagnostic_session' LIMIT 1");
            this.nextSessionCheck = Date.now() + 30_000;
            const persistedValue = settingRes.rows[0]?.value;
            let requestedSession = persistedValue;
            if (typeof requestedSession === 'string' && (requestedSession.startsWith('"') || requestedSession.startsWith('{'))) {
                try {
                    requestedSession = JSON.parse(requestedSession);
                }
                catch { }
            }
            if (requestedSession && requestedSession !== 'IDLE') {
                const id = typeof requestedSession === 'object' ? requestedSession.id : requestedSession;
                const started = typeof requestedSession === 'object' ? Date.parse(requestedSession.started_at) : Number(String(id).match(/(\d{13})$/)?.[1] || Date.now());
                const expires = Math.min(started + 5 * 60_000, typeof requestedSession === 'object' ? Date.parse(requestedSession.expires_at) : started + 5 * 60_000);
                if (!Number.isFinite(expires) || Date.now() >= expires) {
                    await this.poolRef.query("UPDATE settings SET value = $1 WHERE key = 'active_diagnostic_session' AND value = $2", ['IDLE', persistedValue]);
                    this.activeSessionId = null;
                }
                else if (id !== this.activeSessionId) {
                    const session = { id, started_at: new Date(started).toISOString(), expires_at: new Date(expires).toISOString(),
                        cpu_profile: requestedSession?.cpu_profile === true };
                    await this.poolRef.query("UPDATE settings SET value = $1 WHERE key = 'active_diagnostic_session' AND value = $2", [JSON.stringify(session), persistedValue]);
                    this.startSession(id, expires);
                    if (session.cpu_profile)
                        void this.captureBoundedCpuProfile(id);
                }
            }
            else {
                this.activeSessionId = null;
            }
            const report = this.getSnapshotReport();
            await this.poolRef.query(`
        INSERT INTO importer_diagnostic_telemetry (id, session_id, data, created_at)
        VALUES ($1, $2, $3::jsonb, NOW())
        ON CONFLICT (id) DO UPDATE SET data = EXCLUDED.data, created_at = NOW()
      `, [`session-${this.activeSessionId || 'runtime'}`, this.activeSessionId || 'runtime', JSON.stringify(report)]);
        }
        catch (err) {
            if (Date.now() - this.lastFlushWarningAt >= 60_000) {
                this.lastFlushWarningAt = Date.now();
                this.logger.warn('Telemetry persistence failed', { code: err?.code || err?.name });
            }
        }
        finally {
            this.flushing = false;
        }
    }
}
export const telemetryCollector = TelemetryCollector.getInstance();
