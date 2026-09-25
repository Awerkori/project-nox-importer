import type pg from 'pg';
export type SlotStateType = 'WAITING_MUTEX' | 'WAITING_CLAIM_DB' | 'WAITING_SOURCE_PERMIT' | 'ACTIVE_SOURCE' | 'ACTIVE_DOWNLOAD' | 'ACTIVE_ENCODE' | 'ACTIVE_TELEGRAM' | 'ACTIVE_DB' | 'WAITING_BARRIER' | 'IDLE';
export interface ChapterMetricRecord {
    jobId: string;
    source: string;
    chapterNumber: number;
    pageCount: number;
    totalBytes: number;
    totalDurationMs: number;
    totalSlotOccupancyMs: number;
    claim_acquire_ms: number;
    mutex_wait_ms: number;
    claim_db_ms: number;
    scheduler_acquire_ms?: number;
    pool_wait_ms?: number;
    sql_exec_ms?: number;
    claim_sql_ms?: number;
    total_queries?: number;
    works_tested?: number;
    staff_check_ms?: number;
    p0_probe_ms?: number;
    critical_work_attempts?: number;
    critical_work_time_ms?: number;
    p1_work_attempts?: number;
    p1_work_time_ms?: number;
    p2_work_attempts?: number;
    p2_work_time_ms?: number;
    active_fallback_ms?: number;
    admission_on_demand_ms?: number;
    catalog_fallback_ms?: number;
    metadata_load_ms: number;
    source_fetch_ms: number;
    page_resolution_ms: number;
    download_ms: number;
    encode_ms: number;
    telegram_upload_ms: number;
    db_wait_ms: number;
    db_publish_ms: number;
    rate_limit_wait_ms: number;
    semaphore_wait_ms: number;
    other_wait_ms: number;
    barrier_wait_ms?: number;
    slowReason?: string;
    timestamp: string;
}
export interface LimiterAuditRecord {
    name: string;
    configuredLimit: number | string;
    observedConcurrencyPeak: number;
    observedConcurrencyAvg: number;
    hitCount: number;
    waitSamples: number[];
    totalWaitMs: number;
    maxWaitMs: number;
}
export interface SlotStateRecord {
    slotIndex: number;
    currentState: SlotStateType;
    context?: string;
    stateEnteredAt: number;
    stateDurationMs: Record<SlotStateType, number>;
}
export declare class TelemetryCollector {
    private static instance;
    private logger;
    private activeSessionId;
    private sessionStartTime;
    private slots;
    private activeWorkersSamples;
    private activeWorkersDistribution;
    private sourceActiveSamples;
    private slotStateDistributionSamples;
    private samplerTimer;
    private dbPoolWaitSamples;
    private dbPoolQueuedSamples;
    private dbPoolActiveQueries;
    private dbPoolTotalWaitMs;
    private dbPoolMaxWaitMs;
    private telegramActiveUploads;
    private telegramActiveUploadsSamples;
    private telegramPageUploadMsSamples;
    private telegramSemaphoreWaitSamples;
    private telegramTotalBytesUploaded;
    private downloadActiveRequests;
    private downloadActiveSamples;
    private downloadPageMsSamples;
    private downloadSemaphoreWaitSamples;
    private downloadTotalBytes;
    private downloadErrorsCount;
    private downloadRetriesCount;
    private hostRateLimitWaitSamples;
    private limiters;
    private chapters;
    private eventLoopLagSamples;
    private lastELU;
    private eluHistory;
    private gcPauseSamples;
    private lastCpuUsage;
    private lastCpuTime;
    private cpuPercentSamples;
    private poolRef;
    private flushTimer;
    private constructor();
    static getInstance(): TelemetryCollector;
    setPool(pool: pg.Pool): void;
    startSession(sessionId: string): void;
    getSessionId(): string | null;
    registerSlot(slotIndex: number): void;
    setSlotState(slotIndex: number, newState: SlotStateType | string, context?: string): void;
    recordDbPoolWait(waitMs: number, waitingCount: number): void;
    trackActiveDbQuery(delta: number): void;
    trackActiveTelegramUpload(delta: number): void;
    recordTelegramUpload(latencyMs: number, bytes: number): void;
    recordTelegramSemaphoreWait(waitMs: number): void;
    trackActiveDownload(delta: number): void;
    recordImageDownload(source: string, latencyMs: number, bytes: number): void;
    recordDownloadSemaphoreWait(waitMs: number): void;
    recordDownloadError(retried: boolean): void;
    recordRateLimitWait(host: string, waitMs: number): void;
    recordLimiterWait(name: string, waitMs: number, limit?: number | string): void;
    updateLimiterConcurrency(name: string, current: number, limit?: number | string): void;
    recordChapterMetric(record: ChapterMetricRecord): void;
    private startRuntimeSampling;
    private initGcObserver;
    recordEventLoopLag(lagMs: number): void;
    getSnapshotReport(): {
        sessionId: string | null;
        timestamp: string;
        slotsConfigured: number;
        avgSlotStates: {
            ACTIVE_PROCESSING: number;
            BLOCKED: number;
            SUM: number;
            WAITING_MUTEX: number;
            WAITING_CLAIM_DB: number;
            WAITING_SOURCE_PERMIT: number;
            ACTIVE_SOURCE: number;
            ACTIVE_DOWNLOAD: number;
            ACTIVE_ENCODE: number;
            ACTIVE_TELEGRAM: number;
            ACTIVE_DB: number;
            WAITING_BARRIER: number;
            IDLE: number;
        };
        slotOccupancy: {
            meanSec: number;
            p50Sec: number;
            p95Sec: number;
            avgBusyWorkers: number;
            avgActiveProcessing: number;
            avgBlocked: number;
            avgIdle: number;
            theoreticalCapacityPerMin: number;
        };
        activeWorkers: {
            avg: number;
            p50: number;
            p75: number;
            p95: number;
            peak: number;
            distribution: Record<number, number>;
            timeWith8ActivePercent: number;
            timeWithLessThan6Percent: number;
        };
        perSourceActive: {
            hanamiheaven: {
                avg: number;
                peak: number;
            };
            fleurblanche: {
                avg: number;
                peak: number;
            };
            mangalivreto: {
                avg: number;
                peak: number;
            };
        };
        workerTimeBreakdown: {
            workerBusyPercent: number;
            workerIdlePercent: number;
            workerBlockedPercent: number;
            statesAggregatedMs: Record<SlotStateType, number>;
        };
        jobProfile: {
            totalCompleted: number;
            totalDuration: {
                avg: number;
                p50: number;
                p95: number;
                max: number;
            };
            stages: {
                schedulerAcquire: {
                    avg: number;
                    p50: number;
                    p95: number;
                    p99: number;
                    max: number;
                };
                claimDb: {
                    avg: number;
                    p50: number;
                    p95: number;
                    p99: number;
                    max: number;
                };
                poolWait: {
                    avg: number;
                    p50: number;
                    p95: number;
                    max: number;
                };
                sqlExec: {
                    avg: number;
                    p50: number;
                    p95: number;
                    max: number;
                };
                claimSql: {
                    avg: number;
                    p50: number;
                    p95: number;
                    max: number;
                };
                queriesPerClaim: {
                    avg: number;
                    p50: number;
                    p95: number;
                };
                worksTested: {
                    avg: number;
                    p50: number;
                    p95: number;
                };
                mutexWait: {
                    avg: number;
                    p50: number;
                    p95: number;
                    p99: number;
                    max: number;
                };
                claim: {
                    avg: number;
                    p50: number;
                    p75: number;
                    p95: number;
                    p99: number;
                    max: number;
                };
                metadata: {
                    avg: number;
                    p50: number;
                    p75: number;
                    p95: number;
                    p99: number;
                    max: number;
                };
                sourceFetch: {
                    avg: number;
                    p50: number;
                    p75: number;
                    p95: number;
                    p99: number;
                    max: number;
                };
                pageResolution: {
                    avg: number;
                    p50: number;
                    p75: number;
                    p95: number;
                    p99: number;
                    max: number;
                };
                download: {
                    avg: number;
                    p50: number;
                    p75: number;
                    p95: number;
                    p99: number;
                    max: number;
                };
                encode: {
                    avg: number;
                    p50: number;
                    p75: number;
                    p95: number;
                    p99: number;
                    max: number;
                };
                telegramUpload: {
                    avg: number;
                    p50: number;
                    p75: number;
                    p95: number;
                    p99: number;
                    max: number;
                };
                dbWait: {
                    avg: number;
                    p50: number;
                    p75: number;
                    p95: number;
                    p99: number;
                    max: number;
                };
                dbPublish: {
                    avg: number;
                    p50: number;
                    p75: number;
                    p95: number;
                    p99: number;
                    max: number;
                };
                rateLimitWait: {
                    avg: number;
                    p50: number;
                    p75: number;
                    p95: number;
                    p99: number;
                    max: number;
                };
                semaphoreWait: {
                    avg: number;
                    p50: number;
                    p75: number;
                    p95: number;
                    p99: number;
                    max: number;
                };
                otherWait: {
                    avg: number;
                    p50: number;
                    p75: number;
                    p95: number;
                    p99: number;
                    max: number;
                };
            };
        };
        schedulerAcquireBreakdown: {
            totalMs: {
                avg: number;
                p50: number;
                p95: number;
            };
            poolWaitMs: {
                avg: number;
                p50: number;
                p95: number;
            };
            sqlExecMs: {
                avg: number;
                p50: number;
                p95: number;
            };
            queriesCount: {
                avg: number;
                p50: number;
                p95: number;
            };
            worksTestedCount: {
                avg: number;
                p50: number;
                p95: number;
            };
            staffCheckMs: {
                avg: number;
                p50: number;
                p95: number;
            };
            p0ProbeMs: {
                avg: number;
                p50: number;
                p95: number;
            };
            criticalWorkTimeMs: {
                avg: number;
                p50: number;
                p95: number;
            };
            p1WorkTimeMs: {
                avg: number;
                p50: number;
                p95: number;
            };
            p2WorkTimeMs: {
                avg: number;
                p50: number;
                p95: number;
            };
            activeFallbackMs: {
                avg: number;
                p50: number;
                p95: number;
            };
            admissionOnDemandMs: {
                avg: number;
                p50: number;
                p95: number;
            };
            catalogFallbackMs: {
                avg: number;
                p50: number;
                p95: number;
            };
        };
        slowestChapters: {
            slowReason: string;
            jobId: string;
            source: string;
            chapterNumber: number;
            pageCount: number;
            totalBytes: number;
            totalDurationMs: number;
            totalSlotOccupancyMs: number;
            claim_acquire_ms: number;
            mutex_wait_ms: number;
            claim_db_ms: number;
            scheduler_acquire_ms?: number;
            pool_wait_ms?: number;
            sql_exec_ms?: number;
            claim_sql_ms?: number;
            total_queries?: number;
            works_tested?: number;
            staff_check_ms?: number;
            p0_probe_ms?: number;
            critical_work_attempts?: number;
            critical_work_time_ms?: number;
            p1_work_attempts?: number;
            p1_work_time_ms?: number;
            p2_work_attempts?: number;
            p2_work_time_ms?: number;
            active_fallback_ms?: number;
            admission_on_demand_ms?: number;
            catalog_fallback_ms?: number;
            metadata_load_ms: number;
            source_fetch_ms: number;
            page_resolution_ms: number;
            download_ms: number;
            encode_ms: number;
            telegram_upload_ms: number;
            db_wait_ms: number;
            db_publish_ms: number;
            rate_limit_wait_ms: number;
            semaphore_wait_ms: number;
            other_wait_ms: number;
            barrier_wait_ms?: number;
            timestamp: string;
        }[];
        sourceDistribution: Record<string, {
            count: number;
            totalPages: number;
            totalBytes: number;
            totalDurationMs: number;
            avgDurationMs: number;
            avgDownloadMs: number;
            avgUploadMs: number;
            avgDbMs: number;
            avgSemWaitMs: number;
            avgRateLimitWaitMs: number;
        }>;
        limitersAudit: Record<string, any>;
        yugabyteDbPool: {
            configuredMax: number;
            waitAvgMs: number;
            waitP50Ms: number;
            waitP95Ms: number;
            waitMaxMs: number;
            queuedWaitingAvg: number;
            queuedWaitingPeak: number;
            totalQueriesSampled: number;
        };
        telegramStorage: {
            activeUploadsAvg: number;
            activeUploadsP95: number;
            activeUploadsPeak: number;
            pageUploadDurationAvg: number;
            pageUploadDurationP95: number;
            semaphoreWaitAvgMs: number;
            semaphoreWaitP95Ms: number;
            totalBytesUploaded: number;
        };
        imageDownload: {
            activeRequestsAvg: number;
            activeRequestsP95: number;
            activeRequestsPeak: number;
            pageDownloadDurationAvg: number;
            pageDownloadDurationP95: number;
            semaphoreWaitAvgMs: number;
            semaphoreWaitP95Ms: number;
            totalBytesDownloaded: number;
            errorsCount: number;
            retriesCount: number;
        };
        eventLoopAndNode: {
            eventLoopLagAvg: number;
            eventLoopLagP95: number;
            eventLoopLagMax: number;
            eventLoopUtilizationAvg: number;
            processCpuPercentAvg: number;
            processCpuPercentPeak: number;
            gcPausesCount: number;
            gcPausesTotalMs: number;
            gcPausesMaxMs: number;
            rssMb: number;
            heapUsedMb: number;
            heapTotalMb: number;
        };
    };
    flushTelemetryToDb(): Promise<void>;
}
export declare const telemetryCollector: TelemetryCollector;
