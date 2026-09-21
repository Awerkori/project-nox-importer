import { performance, PerformanceObserver } from 'node:perf_hooks';
import type pg from 'pg';
import { Logger } from './logger.js';

export type SlotStateType =
  | 'ACTIVE_PROCESSING'
  | 'IDLE'
  | 'WAITING_FOR_JOB'
  | 'WAITING_FOR_SOURCE'
  | 'WAITING_FOR_SOURCE_RATE_LIMIT'
  | 'WAITING_FOR_DOWNLOAD'
  | 'WAITING_FOR_TELEGRAM'
  | 'WAITING_FOR_DATABASE'
  | 'WAITING_FOR_DB_POOL'
  | 'WAITING_FOR_PUBLICATION_BARRIER'
  | 'WAITING_FOR_RETRY_BACKOFF'
  | 'WAITING_FOR_MUTEX'
  | 'PROTECTIVE_STOP';

export interface ChapterMetricRecord {
  jobId: string;
  source: string;
  chapterNumber: number;
  pageCount: number;
  totalBytes: number;
  totalDurationMs: number;
  claim_acquire_ms: number;
  metadata_load_ms: number;
  source_fetch_ms: number;
  page_resolution_ms: number;
  download_ms: number;
  telegram_upload_ms: number;
  db_wait_ms: number;
  db_publish_ms: number;
  rate_limit_wait_ms: number;
  semaphore_wait_ms: number;
  other_wait_ms: number;
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

function percentile(arr: number[], p: number): number {
  if (!arr || arr.length === 0) return 0;
  const sorted = [...arr].sort((a, b) => a - b);
  const idx = Math.min(sorted.length - 1, Math.floor(sorted.length * p));
  return Math.round(sorted[idx] * 10) / 10;
}

function avg(arr: number[]): number {
  if (!arr || arr.length === 0) return 0;
  const sum = arr.reduce((a, b) => a + b, 0);
  return Math.round((sum / arr.length) * 10) / 10;
}

export class TelemetryCollector {
  private static instance: TelemetryCollector;
  private logger = new Logger('TelemetryCollector');

  private activeSessionId: string | null = null;
  private sessionStartTime = 0;

  // 1. Slot Utilization & Worker State Tracking
  private slots = new Map<number, SlotStateRecord>();
  private activeWorkersSamples: number[] = [];
  private activeWorkersDistribution: Record<number, number> = {
    0: 0, 1: 0, 2: 0, 3: 0, 4: 0, 5: 0, 6: 0, 7: 0, 8: 0
  };
  private sourceActiveSamples = new Map<string, number[]>();
  private samplerTimer: NodeJS.Timeout | null = null;

  // 2. DB Pool Telemetry
  private dbPoolWaitSamples: number[] = [];
  private dbPoolQueuedSamples: number[] = [];
  private dbPoolActiveQueries = 0;
  private dbPoolTotalWaitMs = 0;
  private dbPoolMaxWaitMs = 0;

  // 3. Telegram Storage Telemetry
  private telegramActiveUploads = 0;
  private telegramActiveUploadsSamples: number[] = [];
  private telegramPageUploadMsSamples: number[] = [];
  private telegramSemaphoreWaitSamples: number[] = [];
  private telegramTotalBytesUploaded = 0;

  // 4. Image Download Telemetry
  private downloadActiveRequests = 0;
  private downloadActiveSamples: number[] = [];
  private downloadPageMsSamples: number[] = [];
  private downloadSemaphoreWaitSamples: number[] = [];
  private downloadTotalBytes = 0;
  private downloadErrorsCount = 0;
  private downloadRetriesCount = 0;

  // 5. Host Rate Limiter Telemetry
  private hostRateLimitWaitSamples = new Map<string, number[]>();

  // 6. Internal Limiters & Semaphores Audit
  private limiters = new Map<string, LimiterAuditRecord>();

  // 7. Chapter Jobs
  private chapters: ChapterMetricRecord[] = [];

  // 8. Event Loop & Node Runtime Telemetry
  private eventLoopLagSamples: number[] = [];
  private lastELU = performance.eventLoopUtilization ? performance.eventLoopUtilization() : null;
  private eluHistory: number[] = [];
  private gcPauseSamples: number[] = [];
  private lastCpuUsage = process.cpuUsage();
  private lastCpuTime = performance.now();
  private cpuPercentSamples: number[] = [];

  // Persistence
  private poolRef: pg.Pool | null = null;
  private flushTimer: NodeJS.Timeout | null = null;

  private constructor() {
    this.startRuntimeSampling();
    this.initGcObserver();
  }

  public static getInstance(): TelemetryCollector {
    if (!TelemetryCollector.instance) {
      TelemetryCollector.instance = new TelemetryCollector();
    }
    return TelemetryCollector.instance;
  }

  public setPool(pool: pg.Pool) {
    this.poolRef = pool;
  }

  public startSession(sessionId: string) {
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

    // Reset slot timers
    const now = performance.now();
    for (const [_, slot] of this.slots.entries()) {
      slot.stateEnteredAt = now;
      slot.stateDurationMs = {
        ACTIVE_PROCESSING: 0,
        IDLE: 0,
        WAITING_FOR_JOB: 0,
        WAITING_FOR_SOURCE: 0,
        WAITING_FOR_SOURCE_RATE_LIMIT: 0,
        WAITING_FOR_DOWNLOAD: 0,
        WAITING_FOR_TELEGRAM: 0,
        WAITING_FOR_DATABASE: 0,
        WAITING_FOR_DB_POOL: 0,
        WAITING_FOR_PUBLICATION_BARRIER: 0,
        WAITING_FOR_RETRY_BACKOFF: 0,
        WAITING_FOR_MUTEX: 0,
        PROTECTIVE_STOP: 0,
      };
    }

    this.logger.info(`Started diagnostic telemetry session: ${sessionId}`);
  }

  public getSessionId(): string | null {
    return this.activeSessionId;
  }

  // --- Slot State Tracking ---
  public registerSlot(slotIndex: number) {
    if (!this.slots.has(slotIndex)) {
      this.slots.set(slotIndex, {
        slotIndex,
        currentState: 'IDLE',
        stateEnteredAt: performance.now(),
        stateDurationMs: {
          ACTIVE_PROCESSING: 0,
          IDLE: 0,
          WAITING_FOR_JOB: 0,
          WAITING_FOR_SOURCE: 0,
          WAITING_FOR_SOURCE_RATE_LIMIT: 0,
          WAITING_FOR_DOWNLOAD: 0,
          WAITING_FOR_TELEGRAM: 0,
          WAITING_FOR_DATABASE: 0,
          WAITING_FOR_DB_POOL: 0,
          WAITING_FOR_PUBLICATION_BARRIER: 0,
          WAITING_FOR_RETRY_BACKOFF: 0,
          WAITING_FOR_MUTEX: 0,
          PROTECTIVE_STOP: 0,
        },
      });
    }
  }

  public setSlotState(slotIndex: number, newState: SlotStateType, context?: string) {
    let slot = this.slots.get(slotIndex);
    if (!slot) {
      this.registerSlot(slotIndex);
      slot = this.slots.get(slotIndex)!;
    }

    const now = performance.now();
    const elapsed = now - slot.stateEnteredAt;
    slot.stateDurationMs[slot.currentState] = (slot.stateDurationMs[slot.currentState] || 0) + elapsed;
    slot.currentState = newState;
    slot.context = context;
    slot.stateEnteredAt = now;
  }

  // --- DB Pool Telemetry ---
  public recordDbPoolWait(waitMs: number, waitingCount: number) {
    this.dbPoolWaitSamples.push(waitMs);
    this.dbPoolQueuedSamples.push(waitingCount);
    this.dbPoolTotalWaitMs += waitMs;
    if (waitMs > this.dbPoolMaxWaitMs) this.dbPoolMaxWaitMs = waitMs;
  }

  public trackActiveDbQuery(delta: number) {
    this.dbPoolActiveQueries = Math.max(0, this.dbPoolActiveQueries + delta);
  }

  // --- Telegram Telemetry ---
  public trackActiveTelegramUpload(delta: number) {
    this.telegramActiveUploads = Math.max(0, this.telegramActiveUploads + delta);
  }

  public recordTelegramUpload(latencyMs: number, bytes: number) {
    this.telegramPageUploadMsSamples.push(latencyMs);
    this.telegramTotalBytesUploaded += bytes;
  }

  public recordTelegramSemaphoreWait(waitMs: number) {
    this.telegramSemaphoreWaitSamples.push(waitMs);
    this.recordLimiterWait('telegram_semaphore', waitMs, 6);
  }

  // --- Image Download Telemetry ---
  public trackActiveDownload(delta: number) {
    this.downloadActiveRequests = Math.max(0, this.downloadActiveRequests + delta);
  }

  public recordImageDownload(source: string, latencyMs: number, bytes: number) {
    this.downloadPageMsSamples.push(latencyMs);
    this.downloadTotalBytes += bytes;
  }

  public recordDownloadSemaphoreWait(waitMs: number) {
    this.downloadSemaphoreWaitSamples.push(waitMs);
    this.recordLimiterWait('download_semaphore', waitMs, 8);
  }

  public recordDownloadError(retried: boolean) {
    if (retried) this.downloadRetriesCount++;
    else this.downloadErrorsCount++;
  }

  // --- Rate Limiter Telemetry ---
  public recordRateLimitWait(host: string, waitMs: number) {
    let list = this.hostRateLimitWaitSamples.get(host);
    if (!list) {
      list = [];
      this.hostRateLimitWaitSamples.set(host, list);
    }
    list.push(waitMs);
    this.recordLimiterWait(`host_rate_limiter:${host}`, waitMs, 'dynamic');
  }

  // --- Generic Limiter Audit ---
  public recordLimiterWait(name: string, waitMs: number, limit: number | string = 'unknown') {
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
    if (waitMs > 0) rec.hitCount++;
    if (waitMs > rec.maxWaitMs) rec.maxWaitMs = waitMs;
  }

  public updateLimiterConcurrency(name: string, current: number, limit?: number | string) {
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
  public recordChapterMetric(record: ChapterMetricRecord) {
    this.chapters.push(record);
    this.logger.info(`[CHAPTER_DIAGNOSTIC] ${record.source} ch ${record.chapterNumber}: duration=${record.totalDurationMs}ms (down=${record.download_ms}ms, up=${record.telegram_upload_ms}ms, db=${record.db_publish_ms}ms, sem_wait=${record.semaphore_wait_ms}ms, rl_wait=${record.rate_limit_wait_ms}ms)`);
  }

  // --- Background Sampling ---
  private startRuntimeSampling() {
    this.samplerTimer = setInterval(() => {
      // 1. Sample Active Workers
      let activeCount = 0;
      const sourceCounts = new Map<string, number>();
      const now = performance.now();
      for (const [_, slot] of this.slots.entries()) {
        if (slot.currentState === 'ACTIVE_PROCESSING') {
          activeCount++;
          if (slot.context) {
            const src = slot.context.split(' ')[0];
            if (src) {
              sourceCounts.set(src, (sourceCounts.get(src) || 0) + 1);
            }
          }
        }
      }
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

  private initGcObserver() {
    try {
      const obs = new PerformanceObserver((list) => {
        for (const entry of list.getEntries()) {
          this.gcPauseSamples.push(entry.duration);
        }
      });
      obs.observe({ entryTypes: ['gc'] });
    } catch {
      // GC observation not supported in all environments
    }
  }

  public recordEventLoopLag(lagMs: number) {
    this.eventLoopLagSamples.push(lagMs);
  }

  // --- Snapshot Generation ---
  public getSnapshotReport() {
    const mem = process.memoryUsage();
    const totalSlotSamples = this.activeWorkersSamples.length || 1;

    let timeWith8ActiveCount = this.activeWorkersDistribution[8] || 0;
    let timeWithLessThan6Count = 0;
    for (let i = 0; i < 6; i++) {
      timeWithLessThan6Count += this.activeWorkersDistribution[i] || 0;
    }

    const timeWith8ActivePercent = Math.round((timeWith8ActiveCount / totalSlotSamples) * 1000) / 10;
    const timeWithLessThan6Percent = Math.round((timeWithLessThan6Count / totalSlotSamples) * 1000) / 10;

    // Slot breakdown across all slots
    let totalBusyMs = 0;
    let totalIdleMs = 0;
    let totalBlockedMs = 0;
    let totalAllMs = 0;

    const slotStatesAggregated: Record<SlotStateType, number> = {
      ACTIVE_PROCESSING: 0,
      IDLE: 0,
      WAITING_FOR_JOB: 0,
      WAITING_FOR_SOURCE: 0,
      WAITING_FOR_SOURCE_RATE_LIMIT: 0,
      WAITING_FOR_DOWNLOAD: 0,
      WAITING_FOR_TELEGRAM: 0,
      WAITING_FOR_DATABASE: 0,
      WAITING_FOR_DB_POOL: 0,
      WAITING_FOR_PUBLICATION_BARRIER: 0,
      WAITING_FOR_RETRY_BACKOFF: 0,
      WAITING_FOR_MUTEX: 0,
      PROTECTIVE_STOP: 0,
    };

    const now = performance.now();
    for (const [_, slot] of this.slots.entries()) {
      const currentElapsed = now - slot.stateEnteredAt;
      for (const st of Object.keys(slot.stateDurationMs) as SlotStateType[]) {
        let ms = slot.stateDurationMs[st] || 0;
        if (st === slot.currentState) ms += currentElapsed;
        slotStatesAggregated[st] += ms;
        totalAllMs += ms;

        if (st === 'ACTIVE_PROCESSING') {
          totalBusyMs += ms;
        } else if (st === 'IDLE' || st === 'WAITING_FOR_JOB') {
          totalIdleMs += ms;
        } else {
          totalBlockedMs += ms;
        }
      }
    }

    const safeTotalAll = totalAllMs || 1;
    const workerBusyPercent = Math.round((totalBusyMs / safeTotalAll) * 1000) / 10;
    const workerIdlePercent = Math.round((totalIdleMs / safeTotalAll) * 1000) / 10;
    const workerBlockedPercent = Math.round((totalBlockedMs / safeTotalAll) * 1000) / 10;

    // Chapter stages stats
    const claimTimes = this.chapters.map(c => c.claim_acquire_ms);
    const metadataTimes = this.chapters.map(c => c.metadata_load_ms);
    const sourceFetchTimes = this.chapters.map(c => c.source_fetch_ms);
    const pageResTimes = this.chapters.map(c => c.page_resolution_ms);
    const downloadTimes = this.chapters.map(c => c.download_ms);
    const telegramTimes = this.chapters.map(c => c.telegram_upload_ms);
    const dbWaitTimes = this.chapters.map(c => c.db_wait_ms);
    const dbPublishTimes = this.chapters.map(c => c.db_publish_ms);
    const rateLimitTimes = this.chapters.map(c => c.rate_limit_wait_ms);
    const semWaitTimes = this.chapters.map(c => c.semaphore_wait_ms);
    const otherWaitTimes = this.chapters.map(c => c.other_wait_ms);
    const totalJobTimes = this.chapters.map(c => c.totalDurationMs);

    // Source distributions
    const sourceDist: Record<string, {
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
    }> = {};

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
        } else if (c.telegram_upload_ms > c.totalDurationMs * 0.5) {
          reason = `Telegram upload latency (${c.telegram_upload_ms}ms for ${c.pageCount} pages)`;
        } else if (c.download_ms > c.totalDurationMs * 0.5) {
          reason = `Source image download CDN latency (${c.download_ms}ms for ${c.pageCount} pages)`;
        } else if (c.rate_limit_wait_ms > c.totalDurationMs * 0.3) {
          reason = `Host rate limiter throttle (${c.rate_limit_wait_ms}ms)`;
        } else if (c.db_publish_ms > c.totalDurationMs * 0.4) {
          reason = `Database publish/lock wait (${c.db_publish_ms}ms)`;
        }
        return {
          ...c,
          slowReason: reason,
        };
      });

    // Limiters audit summary
    const limitersSummary: Record<string, any> = {};
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

    return {
      sessionId: this.activeSessionId,
      timestamp: new Date().toISOString(),
      slotsConfigured: 8,
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
          claim: { avg: avg(claimTimes), p50: percentile(claimTimes, 0.50), p75: percentile(claimTimes, 0.75), p95: percentile(claimTimes, 0.95), p99: percentile(claimTimes, 0.99), max: claimTimes.length ? Math.max(...claimTimes) : 0 },
          metadata: { avg: avg(metadataTimes), p50: percentile(metadataTimes, 0.50), p75: percentile(metadataTimes, 0.75), p95: percentile(metadataTimes, 0.95), p99: percentile(metadataTimes, 0.99), max: metadataTimes.length ? Math.max(...metadataTimes) : 0 },
          sourceFetch: { avg: avg(sourceFetchTimes), p50: percentile(sourceFetchTimes, 0.50), p75: percentile(sourceFetchTimes, 0.75), p95: percentile(sourceFetchTimes, 0.95), p99: percentile(sourceFetchTimes, 0.99), max: sourceFetchTimes.length ? Math.max(...sourceFetchTimes) : 0 },
          pageResolution: { avg: avg(pageResTimes), p50: percentile(pageResTimes, 0.50), p75: percentile(pageResTimes, 0.75), p95: percentile(pageResTimes, 0.95), p99: percentile(pageResTimes, 0.99), max: pageResTimes.length ? Math.max(...pageResTimes) : 0 },
          imageDownload: { avg: avg(downloadTimes), p50: percentile(downloadTimes, 0.50), p75: percentile(downloadTimes, 0.75), p95: percentile(downloadTimes, 0.95), p99: percentile(downloadTimes, 0.99), max: downloadTimes.length ? Math.max(...downloadTimes) : 0 },
          telegramUpload: { avg: avg(telegramTimes), p50: percentile(telegramTimes, 0.50), p75: percentile(telegramTimes, 0.75), p95: percentile(telegramTimes, 0.95), p99: percentile(telegramTimes, 0.99), max: telegramTimes.length ? Math.max(...telegramTimes) : 0 },
          dbWait: { avg: avg(dbWaitTimes), p50: percentile(dbWaitTimes, 0.50), p75: percentile(dbWaitTimes, 0.75), p95: percentile(dbWaitTimes, 0.95), p99: percentile(dbWaitTimes, 0.99), max: dbWaitTimes.length ? Math.max(...dbWaitTimes) : 0 },
          dbPublish: { avg: avg(dbPublishTimes), p50: percentile(dbPublishTimes, 0.50), p75: percentile(dbPublishTimes, 0.75), p95: percentile(dbPublishTimes, 0.95), p99: percentile(dbPublishTimes, 0.99), max: dbPublishTimes.length ? Math.max(...dbPublishTimes) : 0 },
          rateLimitWait: { avg: avg(rateLimitTimes), p50: percentile(rateLimitTimes, 0.50), p75: percentile(rateLimitTimes, 0.75), p95: percentile(rateLimitTimes, 0.95), p99: percentile(rateLimitTimes, 0.99), max: rateLimitTimes.length ? Math.max(...rateLimitTimes) : 0 },
          semaphoreWait: { avg: avg(semWaitTimes), p50: percentile(semWaitTimes, 0.50), p75: percentile(semWaitTimes, 0.75), p95: percentile(semWaitTimes, 0.95), p99: percentile(semWaitTimes, 0.99), max: semWaitTimes.length ? Math.max(...semWaitTimes) : 0 },
          otherWait: { avg: avg(otherWaitTimes), p50: percentile(otherWaitTimes, 0.50), p75: percentile(otherWaitTimes, 0.75), p95: percentile(otherWaitTimes, 0.95), p99: percentile(otherWaitTimes, 0.99), max: otherWaitTimes.length ? Math.max(...otherWaitTimes) : 0 },
        },
      },
      slowestChapters,
      sourceDistribution: sourceDist,
      limitersAudit: limitersSummary,
      yugabyteDbPool: {
        configuredMax: Number((this.poolRef as any)?.options?.max || 2),
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
  public async flushTelemetryToDb(): Promise<void> {
    if (!this.poolRef) return;
    try {
      // 1. Check if an active diagnostic session has been requested via settings
      const settingRes = await this.poolRef.query(
        "SELECT value FROM settings WHERE key = 'active_diagnostic_session' LIMIT 1"
      );
      const requestedSession = settingRes.rows[0]?.value;
      if (requestedSession && requestedSession !== 'IDLE' && requestedSession !== this.activeSessionId) {
        this.startSession(requestedSession);
      }

      if (!this.activeSessionId) return;

      const report = this.getSnapshotReport();
      await this.poolRef.query(`
        INSERT INTO importer_diagnostic_telemetry (id, session_id, data, created_at)
        VALUES ($1, $2, $3::jsonb, NOW())
        ON CONFLICT (id) DO UPDATE SET data = EXCLUDED.data, created_at = NOW()
      `, [`session-${this.activeSessionId}`, this.activeSessionId, JSON.stringify(report)]);
    } catch (err: any) {
      // Non-fatal telemetry flush error
    }
  }
}

export const telemetryCollector = TelemetryCollector.getInstance();
