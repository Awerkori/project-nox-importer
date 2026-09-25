import { performance, PerformanceObserver } from 'node:perf_hooks';
import { Logger } from './logger.js';
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
    // 1. Slot Utilization & Worker State Tracking
    slots = new Map();
    activeWorkersSamples = [];
    activeWorkersDistribution = {
        0: 0, 1: 0, 2: 0, 3: 0, 4: 0, 5: 0, 6: 0, 7: 0, 8: 0
    };
    sourceActiveSamples = new Map();
    slotStateDistributionSamples = [];
    samplerTimer = null;
    // 2. DB Pool Telemetry
    dbPoolWaitSamples = [];
    dbPoolQueuedSamples = [];
    dbPoolActiveQueries = 0;
    dbPoolTotalWaitMs = 0;
    dbPoolMaxWaitMs = 0;
    // 3. Telegram Storage Telemetry
    telegramActiveUploads = 0;
    telegramActiveUploadsSamples = [];
    telegramPageUploadMsSamples = [];
    telegramSemaphoreWaitSamples = [];
    telegramTotalBytesUploaded = 0;
    // 4. Image Download Telemetry
    downloadActiveRequests = 0;
    downloadActiveSamples = [];
    downloadPageMsSamples = [];
    downloadSemaphoreWaitSamples = [];
    downloadTotalBytes = 0;
    downloadErrorsCount = 0;
    downloadRetriesCount = 0;
    // 5. Host Rate Limiter Telemetry
    hostRateLimitWaitSamples = new Map();
    // 6. Internal Limiters & Semaphores Audit
    limiters = new Map();
    // 7. Chapter Jobs
    chapters = [];
    // 8. Event Loop & Node Runtime Telemetry
    eventLoopLagSamples = [];
    lastELU = performance.eventLoopUtilization ? performance.eventLoopUtilization() : null;
    eluHistory = [];
    gcPauseSamples = [];
    lastCpuUsage = process.cpuUsage();
    lastCpuTime = performance.now();
    cpuPercentSamples = [];
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
    startSession(sessionId) {
        this.activeSessionId = sessionId;
        this.sessionStartTime = performance.now();
        this.activeWorkersSamples = [];
        this.activeWorkersDistribution = {
            0: 0, 1: 0, 2: 0, 3: 0, 4: 0, 5: 0, 6: 0, 7: 0, 8: 0
        };
        this.dbPoolWaitSamples = [];
        this.dbPoolQueuedSamples = [];
        this.dbPoolTotalWaitMs = 0;
        this.dbPoolMaxWaitMs = 0;
        this.telegramActiveUploadsSamples = [];
        this.telegramPageUploadMsSamples = [];
        this.telegramSemaphoreWaitSamples = [];
        this.downloadActiveSamples = [];
        this.downloadPageMsSamples = [];
        this.downloadSemaphoreWaitSamples = [];
        this.downloadErrorsCount = 0;
        this.downloadRetriesCount = 0;
        this.hostRateLimitWaitSamples.clear();
        this.chapters = [];
        this.gcPauseSamples = [];
        this.eventLoopLagSamples = [];
        this.eluHistory = [];
        this.cpuPercentSamples = [];
        this.sourceActiveSamples.clear();
        this.slotStateDistributionSamples = [];
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
        this.recordLimiterWait('telegram_semaphore', waitMs, 6);
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
        this.recordLimiterWait('download_semaphore', waitMs, 8);
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
            list = [];
            this.hostRateLimitWaitSamples.set(host, list);
        }
        list.push(waitMs);
        this.recordLimiterWait(`host_rate_limiter:${host}`, waitMs, 'dynamic');
    }
    // --- Generic Limiter Audit ---
    recordLimiterWait(name, waitMs, limit = 'unknown') {
        let rec = this.limiters.get(name);
        if (!rec) {
            rec = {
                name,
                configuredLimit: limit,
                observedConcurrencyPeak: 0,
                observedConcurrencyAvg: 0,
                hitCount: 0,
                waitSamples: [],
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
            rec = {
                name,
                configuredLimit: limit ?? 'unknown',
                observedConcurrencyPeak: current,
                observedConcurrencyAvg: current,
                hitCount: 0,
                waitSamples: [],
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
        this.chapters.push(record);
        this.logger.info(`[CHAPTER_DIAGNOSTIC] ${record.source} ch ${record.chapterNumber}: duration=${record.totalDurationMs}ms (down=${record.download_ms}ms, up=${record.telegram_upload_ms}ms, db=${record.db_publish_ms}ms, sem_wait=${record.semaphore_wait_ms}ms, rl_wait=${record.rate_limit_wait_ms}ms)`);
    }
    // --- Background Sampling ---
    startRuntimeSampling() {
        this.samplerTimer = setInterval(() => {
            // 1. Sample 8 Chapter Worker Slots (mutually exclusive across 10 states)
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
            for (let i = 0; i < 8; i++) {
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
            const bucket = Math.min(8, Math.max(0, activeCount));
            this.activeWorkersDistribution[bucket] = (this.activeWorkersDistribution[bucket] || 0) + 1;
            for (const src of ['hanamiheaven', 'fleurblanche', 'mangalivreto']) {
                const c = sourceCounts.get(src) || 0;
                let arr = this.sourceActiveSamples.get(src);
                if (!arr) {
                    arr = [];
                    this.sourceActiveSamples.set(src, arr);
                }
                arr.push(c);
            }
            // 2. Telegram concurrency sample
            this.telegramActiveUploadsSamples.push(this.telegramActiveUploads);
            this.updateLimiterConcurrency('telegram_semaphore', this.telegramActiveUploads, 6);
            // 3. Download concurrency sample
            this.downloadActiveSamples.push(this.downloadActiveRequests);
            this.updateLimiterConcurrency('download_semaphore', this.downloadActiveRequests, 8);
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
                const percent = Math.min(100, Math.round((totalCpuMs / elapsedMs) * 100 * 10) / 10);
                this.cpuPercentSamples.push(percent);
                this.lastCpuUsage = cpuNow;
                this.lastCpuTime = timeNow;
            }
            // 6. ELU sample
            if (performance.eventLoopUtilization && this.lastELU) {
                const elu = performance.eventLoopUtilization(this.lastELU);
                this.eluHistory.push(Math.round(elu.utilization * 1000) / 10);
            }
        }, 100);
        this.samplerTimer.unref();
        // Background persistence flush every 3s
        this.flushTimer = setInterval(async () => {
            await this.flushTelemetryToDb();
        }, 3000);
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
        let timeWith8ActiveCount = this.activeWorkersDistribution[8] || 0;
        let timeWithLessThan6Count = 0;
        for (let i = 0; i < 6; i++) {
            timeWithLessThan6Count += this.activeWorkersDistribution[i] || 0;
        }
        const timeWith8ActivePercent = Math.round((timeWith8ActiveCount / totalSlotSamples) * 1000) / 10;
        const timeWithLessThan6Percent = Math.round((timeWithLessThan6Count / totalSlotSamples) * 1000) / 10;
        // Slot breakdown across all 8 chapter slots (mutually exclusive)
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
        // Guarantee SUM is strictly 8.00 by balancing rounding drift on IDLE
        const sumStates = Object.values(avgSlotStates).reduce((a, b) => a + b, 0);
        const roundingDiff = Math.round((8.00 - sumStates) * 100) / 100;
        if (roundingDiff !== 0) {
            avgSlotStates.IDLE = Math.max(0, Math.round((avgSlotStates.IDLE + roundingDiff) * 100) / 100);
        }
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
                observedConcurrencyAvg: avg(rec.waitSamples),
                hitCount: rec.hitCount,
                waitAvgMs: avg(rec.waitSamples),
                waitP95Ms: percentile(rec.waitSamples, 0.95),
                waitMaxMs: rec.maxWaitMs,
            };
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
            slotsConfigured: 8,
            avgSlotStates: {
                ...avgSlotStates,
                ACTIVE_PROCESSING: avgActiveProcessing,
                BLOCKED: avgBlocked,
                SUM: 8.00,
            },
            slotOccupancy: {
                meanSec: Math.round(meanSlotOccupancySec * 100) / 100,
                p50Sec: Math.round(p50SlotOccupancySec * 100) / 100,
                p95Sec: Math.round(p95SlotOccupancySec * 100) / 100,
                avgBusyWorkers: avgActiveProcessing,
                avgActiveProcessing,
                avgBlocked,
                avgIdle,
                theoreticalCapacityPerMin: meanSlotOccupancySec > 0 ? Math.round(((8.0 * 60) / meanSlotOccupancySec) * 100) / 100 : 0,
            },
            activeWorkers: {
                avg: avg(this.activeWorkersSamples),
                p50: percentile(this.activeWorkersSamples, 0.50),
                p75: percentile(this.activeWorkersSamples, 0.75),
                p95: percentile(this.activeWorkersSamples, 0.95),
                peak: this.activeWorkersSamples.length ? Math.max(...this.activeWorkersSamples) : 0,
                distribution: this.activeWorkersDistribution,
                timeWith8ActivePercent,
                timeWithLessThan6Percent,
            },
            perSourceActive: {
                hanamiheaven: {
                    avg: avg(this.sourceActiveSamples.get('hanamiheaven') || []),
                    peak: (this.sourceActiveSamples.get('hanamiheaven') || []).length ? Math.max(...(this.sourceActiveSamples.get('hanamiheaven') || [0])) : 0,
                },
                fleurblanche: {
                    avg: avg(this.sourceActiveSamples.get('fleurblanche') || []),
                    peak: (this.sourceActiveSamples.get('fleurblanche') || []).length ? Math.max(...(this.sourceActiveSamples.get('fleurblanche') || [0])) : 0,
                },
                mangalivreto: {
                    avg: avg(this.sourceActiveSamples.get('mangalivreto') || []),
                    peak: (this.sourceActiveSamples.get('mangalivreto') || []).length ? Math.max(...(this.sourceActiveSamples.get('mangalivreto') || [0])) : 0,
                },
            },
            workerTimeBreakdown: {
                workerBusyPercent,
                workerIdlePercent,
                workerBlockedPercent,
                statesAggregatedMs: slotStatesAggregated,
            },
            jobProfile: {
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
                    claimLockSql: { avg: avg(claimSqlTimes), p50: percentile(claimSqlTimes, 0.50), p95: percentile(claimSqlTimes, 0.95), max: claimSqlTimes.length ? Math.max(...claimSqlTimes) : 0 },
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
            },
        };
    }
    // --- Persistence to DB ---
    async flushTelemetryToDb() {
        if (!this.poolRef)
            return;
        try {
            // 1. Check if an active diagnostic session has been requested via settings
            const settingRes = await this.poolRef.query("SELECT value FROM settings WHERE key = 'active_diagnostic_session' LIMIT 1");
            let requestedSession = settingRes.rows[0]?.value;
            if (typeof requestedSession === 'string' && requestedSession.startsWith('"') && requestedSession.endsWith('"')) {
                try {
                    requestedSession = JSON.parse(requestedSession);
                }
                catch { }
            }
            if (requestedSession && requestedSession !== 'IDLE' && requestedSession !== this.activeSessionId) {
                this.startSession(requestedSession);
            }
            if (!this.activeSessionId)
                return;
            const report = this.getSnapshotReport();
            await this.poolRef.query(`
        INSERT INTO importer_diagnostic_telemetry (id, session_id, data, created_at)
        VALUES ($1, $2, $3::jsonb, NOW())
        ON CONFLICT (id) DO UPDATE SET data = EXCLUDED.data, created_at = NOW()
      `, [`session-${this.activeSessionId}`, this.activeSessionId, JSON.stringify(report)]);
        }
        catch (err) {
            // Non-fatal telemetry flush error
        }
    }
}
export const telemetryCollector = TelemetryCollector.getInstance();
