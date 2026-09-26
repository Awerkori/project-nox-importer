import { Logger } from './logger.js';
import { diagnostics } from './diagnostics.js';
import { telemetryCollector } from './telemetry-collector.js';
import { performance } from 'node:perf_hooks';
import type { PressureSnapshot, SiteHealthState } from './protective-sentinel.js';
import { getYugabytePool } from '../db/yugabyte-direct.js';

export class AsyncSemaphore {
  private activePermits = 0;
  private maxPermits: number;
  private waitQueue: Array<() => void> = [];
  public name: string;

  constructor(maxPermits: number, name: string = 'unnamed_semaphore') {
    this.maxPermits = Math.max(1, maxPermits);
    this.name = name;
  }

  tryAcquire(): boolean {
    if (this.activePermits < this.maxPermits && this.waitQueue.length === 0) {
      this.activePermits++;
      telemetryCollector.recordLimiterWait(this.name, 0, this.maxPermits);
      telemetryCollector.updateLimiterConcurrency(this.name, this.activePermits, this.maxPermits);
      return true;
    }
    return false;
  }

  async acquire(signal?: AbortSignal): Promise<void> {
    signal?.throwIfAborted();
    telemetryCollector.updateLimiterConcurrency(this.name, this.activePermits, this.maxPermits);
    if (this.activePermits < this.maxPermits) {
      this.activePermits++;
      telemetryCollector.recordLimiterWait(this.name, 0, this.maxPermits);
      telemetryCollector.updateLimiterConcurrency(this.name, this.activePermits, this.maxPermits);
      return;
    }
    const t0 = performance.now();
    return new Promise<void>((resolve, reject) => {
      const granted = () => {
        signal?.removeEventListener('abort', cancelled);
        const waitMs = performance.now() - t0;
        telemetryCollector.recordLimiterWait(this.name, waitMs, this.maxPermits);
        telemetryCollector.updateLimiterConcurrency(this.name, this.activePermits, this.maxPermits);
        resolve();
      };
      const cancelled = () => {
        const index = this.waitQueue.indexOf(granted);
        if (index >= 0) this.waitQueue.splice(index, 1);
        reject(signal?.reason || new Error('Semaphore acquisition aborted'));
      };
      signal?.addEventListener('abort', cancelled, { once: true });
      this.waitQueue.push(granted);
    });
  }

  release(): void {
    if (this.activePermits === 0) throw new Error('Semaphore released without an active permit');
    this.activePermits--;
    telemetryCollector.updateLimiterConcurrency(this.name, this.activePermits, this.maxPermits);
    this.drain();
  }

  private drain(): void {
    while (this.activePermits < this.maxPermits && this.waitQueue.length > 0) {
      this.activePermits++;
      this.waitQueue.shift()!();
    }
  }

  public waitSamples: number[] = [];
  public holdSamples: number[] = [];

  async runExclusive<T>(fn: () => Promise<T>): Promise<T> {
    const t0Wait = performance.now();
    await this.acquire();
    const waitMs = performance.now() - t0Wait;
    this.waitSamples.push(waitMs);
    if (this.waitSamples.length > 300) this.waitSamples.shift();

    const t0Hold = performance.now();
    try {
      return await fn();
    } finally {
      const holdMs = performance.now() - t0Hold;
      this.holdSamples.push(holdMs);
      if (this.holdSamples.length > 300) this.holdSamples.shift();
      this.release();
    }
  }

  getMetrics() {
    const sortedWait = [...this.waitSamples].sort((a, b) => a - b);
    const sortedHold = [...this.holdSamples].sort((a, b) => a - b);
    return {
      waitP50: sortedWait.length ? Math.round(sortedWait[Math.floor(sortedWait.length * 0.5)] * 10) / 10 : 0,
      waitP95: sortedWait.length ? Math.round(sortedWait[Math.floor(sortedWait.length * 0.95)] * 10) / 10 : 0,
      holdP50: sortedHold.length ? Math.round(sortedHold[Math.floor(sortedHold.length * 0.5)] * 10) / 10 : 0,
      holdP95: sortedHold.length ? Math.round(sortedHold[Math.floor(sortedHold.length * 0.95)] * 10) / 10 : 0,
      samples: this.waitSamples.length
    };
  }

  getLastWaitMs(): number {
    return this.waitSamples.length ? this.waitSamples[this.waitSamples.length - 1] : 0;
  }

  /**
   * Updates semaphore capacity.
   * ABSOLUTE INVARIANT: capacity cannot be set lower than 1.
   * In-flight holders drain naturally; never reissue their permits.
   */
  setCapacity(newCapacity: number): void {
    const target = Math.max(1, newCapacity);
    this.maxPermits = target;
    this.drain();
  }

  get capacity(): number {
    return this.maxPermits;
  }

  get available(): number {
    return Math.max(0, this.maxPermits - this.activePermits);
  }

  get active(): number {
    return this.activePermits;
  }

  get queued(): number {
    return this.waitQueue.length;
  }
}

// Every runner must acquire source before global capacity to avoid lock inversion.
export async function withSourceChapterPermits<T>(
  source: AsyncSemaphore, global: AsyncSemaphore, fn: () => Promise<T>, signal?: AbortSignal
): Promise<T> {
  await source.acquire(signal);
  try {
    await global.acquire(signal);
    try { return await fn(); }
    finally { global.release(); }
  } finally { source.release(); }
}

export interface SourceConcurrencyConfig {
  maxChapters: number;
  maxPagesPerChapter: number;
}

export const SOURCE_CONCURRENCY_LIMITS: Record<string, SourceConcurrencyConfig> = {
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

export const DEFAULT_SOURCE_LIMIT: SourceConcurrencyConfig = {
  maxChapters: 2,
  maxPagesPerChapter: 4,
};

export const TESTED_CONCURRENCY_CEILING = 32;

export interface AutotunerConfig {
  minConcurrency: number;
  maxConcurrency: number;
  initialConcurrency: number;
  requiredStableCycles: number;
  cooldownPeriodMs: number;
  maxRssMb: number;
  maxHeapMb: number;
  maxExternalAndBuffersMb: number;
  maxEventLoopLagMs: number;
  rssSoftLimitMb: number;
  rssHardLimitMb: number;
  rssEmergencyLimitMb: number;
  maxBufferedBytes: number;
  adaptiveEnabled: boolean;
  scaleUpDwellTimeMs: number; // Minimum dwell time between scale-ups (60-90s)
}

const DEFAULT_AUTOTUNER_CONFIG: AutotunerConfig = {
  minConcurrency: 1,
  maxConcurrency: Math.min(TESTED_CONCURRENCY_CEILING, parseInt(process.env.ADAPTIVE_MAX_CONCURRENCY || '18', 10)),
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
  adaptiveEnabled: true,
  scaleUpDwellTimeMs: 0, // Controlled by requiredStableCycles (3 cycles = 90s in prod)
};

export type AdaptiveCapacityState =
  | 'RUNNING_ACCELERATING'
  | 'RUNNING_STABLE'
  | 'RUNNING_THROTTLED'
  | 'SURVIVAL'
  | 'WAITING_DEPENDENCY'
  | 'WAITING_SOURCES'
  | 'RECOVERING'
  | 'MANUAL_STOP';

export interface AutotunerCycleResult {
  concurrency: number;
  targetConcurrency?: number;
  action: 'SCALED_UP' | 'SCALED_DOWN' | 'STABLE' | 'HOLD' | 'COOLDOWN' | 'STRESS_DETECTED' | 'SURVIVAL';
  state: AdaptiveCapacityState;
  reason: string;
  pressureScore: number;
  pressureBreakdown: PressureSnapshot['pressureBreakdown'];
  siteHealth: SiteHealthState;
}

export interface AutotunerEvaluationContext {
  allSourcesBlocked?: boolean;
  dbUnavailable?: boolean;
  manualStopActive?: boolean;
  stagedDebt?: number;
  storageUnavailable?: boolean;
}

// Cost-aware load weighting
export class WorkCostEstimator {
  static estimateCost(pageCount?: number | null, historicalBytes?: number | null): number {
    if (!pageCount || pageCount <= 0) return 1;
    if (pageCount <= 25) return 1; // Light
    if (pageCount <= 60) return 2; // Medium
    return 3; // Heavy
  }
}

// RAII Token representing an atomic slice of the memory buffer budget.
export class BufferReservation {
  private _released = false;
  private _committed = false;

  constructor(
    private autotuner: AdaptiveAutotuner,
    private _reservedBytes: number
  ) {}

  get reservedBytes(): number {
    return this._reservedBytes;
  }

  get isCommitted(): boolean {
    return this._committed;
  }

  get isReleased(): boolean {
    return this._released;
  }

  async upgrade(newBytes: number, signal?: AbortSignal): Promise<void> {
    if (this._released || this._committed) return;
    if (newBytes <= this._reservedBytes) return;
    const additional = newBytes - this._reservedBytes;
    await this.autotuner.upgradeReservation(additional, signal);
    this._reservedBytes = newBytes;
  }

  commit(actualBytes: number): void {
    if (this._released || this._committed) return;
    if (actualBytes > this._reservedBytes) {
      throw new Error(
        `BufferReservation invariant violation: cannot commit ${actualBytes} bytes exceeding reserved budget ${this._reservedBytes} bytes without prior upgrade`
      );
    }
    this._committed = true;
    this.autotuner.commitReservation(this._reservedBytes, actualBytes);
  }

  release(): void {
    if (this._released || this._committed) return;
    this._released = true;
    this.autotuner.releaseReservation(this._reservedBytes);
  }
}

/**
 * AdaptiveAutotuner: The SINGLE Authority for Global Chapter Concurrency.
 * INVARIANT: GLOBAL_CONCURRENCY_WRITERS = 1.
 * Automatic performance stop is strictly prohibited; capacity never drops below 1.
 */
export class AdaptiveAutotuner {
  private logger = new Logger('Autotuner');
  private globalChapterSemaphore: AsyncSemaphore;
  private sourceSemaphores = new Map<string, AsyncSemaphore>();
  private globalMediaSemaphore: AsyncSemaphore;
  private globalInflightRequestSemaphore: AsyncSemaphore;
  private bufferedPageSemaphore: AsyncSemaphore;
  private currentConcurrency: number;
  private stableCycleCount = 0;
  private cooldownUntil = 0;
  private config: AutotunerConfig;

  // Single authority and hysteresis state
  private currentState: AdaptiveCapacityState = 'RUNNING_STABLE';
  private lastCapacityChangeAt = Date.now();
  private lastStableConcurrency = 8;
  private lastStableAt = Date.now();

  // Active and reserved buffer tracking & backpressure waiters
  private activeBufferedBytes = 0;
  private reservedBufferedBytes = 0;
  private maxCommittedBytesObserved = 0;
  private reservationWaiters: Array<{
    requestedBytes: number;
    t0: number;
    signal?: AbortSignal;
    resolve: (reservation: BufferReservation) => void;
    reject: (err: any) => void;
  }> = [];

  // Window error counters
  private cycleErrors = 0;
  private cycleRateLimits = 0;
  private cycleTimeouts = 0;

  // Cache latest result
  private latestResult: AutotunerCycleResult = {
    concurrency: 8,
    action: 'STABLE',
    state: 'RUNNING_STABLE',
    reason: 'Initial boot state',
    pressureScore: 0,
    pressureBreakdown: {
      sitePressure: 0,
      dbPressure: 0,
      memoryPressure: 0,
      eventLoopPressure: 0,
      storagePressure: 0,
      sourcePressure: 0,
      publicationPressure: 0,
    },
    siteHealth: 'GREEN',
  };

  constructor(config: Partial<AutotunerConfig> = {}) {
    this.config = { ...DEFAULT_AUTOTUNER_CONFIG, ...config };
    this.config.maxConcurrency = Math.min(TESTED_CONCURRENCY_CEILING, this.config.maxConcurrency);
    this.config.minConcurrency = Math.max(1, this.config.minConcurrency);

    // Warm start from configured initial concurrency (minimum 1, maximum maxConcurrency)
    this.currentConcurrency = Math.max(
      this.config.minConcurrency,
      Math.min(this.config.maxConcurrency, this.config.initialConcurrency)
    );
    this.lastStableConcurrency = this.currentConcurrency;

    const mediaConcurrency = parseInt(process.env.TELEGRAM_MEDIA_CONCURRENCY || '12', 10);
    const inflightConcurrency = parseInt(process.env.DOWNLOAD_INFLIGHT_CONCURRENCY || '16', 10);
    const bufferedConcurrency = parseInt(process.env.BUFFERED_PAGE_CONCURRENCY || '32', 10);
    this.bufferedPageSemaphore = new AsyncSemaphore(bufferedConcurrency, 'buffered_page_semaphore');
    this.globalChapterSemaphore = new AsyncSemaphore(this.currentConcurrency, 'global_chapter_semaphore');
    this.globalMediaSemaphore = new AsyncSemaphore(mediaConcurrency, 'telegram_media_semaphore');
    this.globalInflightRequestSemaphore = new AsyncSemaphore(inflightConcurrency, 'global_download_inflight_semaphore');
  }

  getGlobalChapterSemaphore(): AsyncSemaphore {
    return this.globalChapterSemaphore;
  }

  getGlobalMediaSemaphore(): AsyncSemaphore {
    return this.globalMediaSemaphore;
  }

  getGlobalInflightRequestSemaphore(): AsyncSemaphore {
    return this.globalInflightRequestSemaphore;
  }

  getBufferedPageSemaphore(): AsyncSemaphore {
    return this.bufferedPageSemaphore;
  }

  canAdmitReservation(requestedBytes: number): boolean {
    const mem = diagnostics.getMemorySnapshot();
    const totalCommitted = this.activeBufferedBytes + this.reservedBufferedBytes;

    if (totalCommitted + requestedBytes > this.config.maxBufferedBytes) {
      return false;
    }

    if (mem.rssMb >= this.config.rssSoftLimitMb) {
      if (totalCommitted === 0) {
        return true; // Forward progress exception
      }
      return false;
    }

    return true;
  }

  async reserveBufferBudget(
    requestedBytes: number = 2 * 1024 * 1024,
    signal?: AbortSignal
  ): Promise<BufferReservation> {
    signal?.throwIfAborted();

    if (this.reservationWaiters.length === 0 && this.canAdmitReservation(requestedBytes)) {
      this.reservedBufferedBytes += requestedBytes;
      this.updateMaxCommittedObserved();
      return new BufferReservation(this, requestedBytes);
    }

    const t0 = performance.now();
    return new Promise<BufferReservation>((resolve, reject) => {
      let intervalTimer: NodeJS.Timeout | null = null;

      const waiter = {
        requestedBytes,
        t0,
        signal,
        resolve: (reservation: BufferReservation) => {
          cleanup();
          const waitedMs = performance.now() - t0;
          telemetryCollector.recordLimiterWait('memory_backpressure', waitedMs, this.config.maxBufferedBytes);
          if (waitedMs > 1000) {
            this.logger.info(
              `[Memory Backpressure] Admitted after ${Math.round(waitedMs)}ms wait (Active: ${Math.round(this.activeBufferedBytes / 1024 / 1024)}MB, Reserved: ${Math.round(this.reservedBufferedBytes / 1024 / 1024)}MB, RSS: ${diagnostics.getMemorySnapshot().rssMb}MB)`
            );
          }
          resolve(reservation);
        },
        reject: (err: any) => {
          cleanup();
          reject(err);
        },
      };

      const cleanup = () => {
        if (intervalTimer) clearInterval(intervalTimer);
        signal?.removeEventListener('abort', onAbort);
        const idx = this.reservationWaiters.indexOf(waiter);
        if (idx >= 0) this.reservationWaiters.splice(idx, 1);
      };

      const onAbort = () => {
        cleanup();
        reject(signal?.reason || new Error('Buffer reservation aborted'));
      };

      signal?.addEventListener('abort', onAbort, { once: true });
      this.reservationWaiters.push(waiter);

      intervalTimer = setInterval(() => {
        this.drainReservationWaiters();
      }, 200);
    });
  }

  async upgradeReservation(additionalBytes: number, signal?: AbortSignal): Promise<void> {
    if (additionalBytes <= 0) return;
    signal?.throwIfAborted();

    if (this.reservationWaiters.length === 0 && this.canAdmitReservation(additionalBytes)) {
      this.reservedBufferedBytes += additionalBytes;
      this.updateMaxCommittedObserved();
      return;
    }

    const t0 = performance.now();
    return new Promise<void>((resolve, reject) => {
      let intervalTimer: NodeJS.Timeout | null = null;
      const waiter = {
        requestedBytes: additionalBytes,
        t0,
        signal,
        resolve: () => {
          cleanup();
          resolve();
        },
        reject: (err: any) => {
          cleanup();
          reject(err);
        },
      };

      const cleanup = () => {
        if (intervalTimer) clearInterval(intervalTimer);
        signal?.removeEventListener('abort', onAbort);
        const idx = this.reservationWaiters.indexOf(waiter as any);
        if (idx >= 0) this.reservationWaiters.splice(idx, 1);
      };

      const onAbort = () => {
        cleanup();
        reject(signal?.reason || new Error('Buffer reservation upgrade aborted'));
      };

      signal?.addEventListener('abort', onAbort, { once: true });
      this.reservationWaiters.push(waiter as any);

      intervalTimer = setInterval(() => {
        this.drainReservationWaiters();
      }, 200);
    });
  }

  commitReservation(reservedBytes: number, actualBytes: number): void {
    if (actualBytes > reservedBytes) {
      throw new Error(
        `commitReservation invariant violation: actualBytes (${actualBytes}) exceeds reservedBytes (${reservedBytes})`
      );
    }
    this.reservedBufferedBytes = Math.max(0, this.reservedBufferedBytes - reservedBytes);
    this.activeBufferedBytes += actualBytes;
    this.updateMaxCommittedObserved();
    this.drainReservationWaiters();
  }

  releaseReservation(reservedBytes: number): void {
    this.reservedBufferedBytes = Math.max(0, this.reservedBufferedBytes - reservedBytes);
    this.drainReservationWaiters();
  }

  releaseActiveBufferedBytes(actualBytes: number): void {
    if (actualBytes <= 0) return;
    this.activeBufferedBytes = Math.max(0, this.activeBufferedBytes - actualBytes);
    this.drainReservationWaiters();
  }

  private drainReservationWaiters(): void {
    while (this.reservationWaiters.length > 0) {
      const next = this.reservationWaiters[0];
      if (this.canAdmitReservation(next.requestedBytes)) {
        this.reservationWaiters.shift();
        this.reservedBufferedBytes += next.requestedBytes;
        this.updateMaxCommittedObserved();
        const reservation = new BufferReservation(this, next.requestedBytes);
        next.resolve(reservation);
      } else {
        break;
      }
    }
  }

  trackBufferedBytes(bytes: number): void {
    if (bytes <= 0) return;
    this.activeBufferedBytes += bytes;
    this.updateMaxCommittedObserved();
  }

  releaseBufferedBytes(bytes: number): void {
    this.releaseActiveBufferedBytes(bytes);
  }

  getBufferedBytes(): number {
    return this.activeBufferedBytes;
  }

  getReservedBytes(): number {
    return this.reservedBufferedBytes;
  }

  getCommittedBytes(): number {
    return this.activeBufferedBytes + this.reservedBufferedBytes;
  }

  private updateMaxCommittedObserved(): void {
    const total = this.activeBufferedBytes + this.reservedBufferedBytes;
    if (total > this.maxCommittedBytesObserved) {
      this.maxCommittedBytesObserved = total;
    }
  }

  getMaxCommittedBytesObserved(): number {
    return this.maxCommittedBytesObserved;
  }

  async waitForMemoryHeadroom(estimatedBytes: number = 1.5 * 1024 * 1024, signal?: AbortSignal): Promise<void> {
    const reservation = await this.reserveBufferBudget(estimatedBytes, signal);
    reservation.release();
  }

  getSourceLimits(source: string): SourceConcurrencyConfig {
    return SOURCE_CONCURRENCY_LIMITS[source] || DEFAULT_SOURCE_LIMIT;
  }

  getSourcePageConcurrency(source: string): number {
    return this.getSourceLimits(source).maxPagesPerChapter;
  }

  getSourceSemaphore(source: string, limitPerSource?: number): AsyncSemaphore {
    let sem = this.sourceSemaphores.get(source);
    if (!sem) {
      const configuredLimit = limitPerSource ?? this.getSourceLimits(source).maxChapters;
      sem = new AsyncSemaphore(configuredLimit, `source_semaphore:${source}`);
      this.sourceSemaphores.set(source, sem);
    }
    return sem;
  }

  isSourceCapacityAvailable(source: string): boolean {
    const sem = this.getSourceSemaphore(source);
    return sem.available > 0;
  }

  private sourceHealth = new Map<string, { consecutiveFailures: number; consecutiveSuccesses: number; currentCapacity: number }>();

  recordSourceFailure(source: string): { throttled: boolean; newCapacity: number } {
    const limits = this.getSourceLimits(source);
    let health = this.sourceHealth.get(source);
    if (!health) {
      health = { consecutiveFailures: 0, consecutiveSuccesses: 0, currentCapacity: limits.maxChapters };
      this.sourceHealth.set(source, health);
    }
    health.consecutiveFailures++;
    health.consecutiveSuccesses = 0;

    if (health.consecutiveFailures >= 2 && health.currentCapacity > 1) {
      health.currentCapacity = Math.max(1, health.currentCapacity - 1);
      const sem = this.getSourceSemaphore(source);
      sem.setCapacity(health.currentCapacity);
      this.logger.warn(`Source ${source} concurrency throttled: ${health.currentCapacity + 1} -> ${health.currentCapacity} due to ${health.consecutiveFailures} consecutive failures`);
      return { throttled: true, newCapacity: health.currentCapacity };
    }
    return { throttled: false, newCapacity: health.currentCapacity };
  }

  recordSourceSuccess(source: string): { restored: boolean; newCapacity: number } {
    const limits = this.getSourceLimits(source);
    let health = this.sourceHealth.get(source);
    if (!health) {
      health = { consecutiveFailures: 0, consecutiveSuccesses: 0, currentCapacity: limits.maxChapters };
      this.sourceHealth.set(source, health);
    }
    health.consecutiveFailures = 0;
    health.consecutiveSuccesses++;

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

  recordError(type: 'error' | 'ratelimit' | 'timeout'): void {
    if (type === 'ratelimit') this.cycleRateLimits++;
    else if (type === 'timeout') this.cycleTimeouts++;
    else this.cycleErrors++;
  }

  /**
   * Evaluates system pressure and adjusts global chapter concurrency.
   * Single authority: FAST DOWN, SLOW UP, HYSTERESIS, DWELL TIME, MIN_CONCURRENCY = 1.
   */
  evaluateCycle(
    pressureSnapshot?: PressureSnapshot,
    context: AutotunerEvaluationContext = {}
  ): AutotunerCycleResult {
    const now = Date.now();
    const mem = diagnostics.getMemorySnapshot();
    const lag = (diagnostics as any).lagMonitor?.getMetrics?.() || { avgLagMs: 0 };
    const totalExternal = mem.externalMb;

    const errors = this.cycleErrors;
    const rateLimits = this.cycleRateLimits;
    const timeouts = this.cycleTimeouts;

    // Reset cycle window counters
    this.cycleErrors = 0;
    this.cycleRateLimits = 0;
    this.cycleTimeouts = 0;

    // 0. Manual Staff Stop check
    if (context.manualStopActive) {
      this.currentState = 'MANUAL_STOP';
      return {
        concurrency: this.currentConcurrency,
        targetConcurrency: this.currentConcurrency,
        action: 'HOLD',
        state: 'MANUAL_STOP',
        reason: 'Staff manual stop active',
        pressureScore: pressureSnapshot?.pressureScore || 0,
        pressureBreakdown: pressureSnapshot?.pressureBreakdown || {
          sitePressure: 0,
          dbPressure: 0,
          memoryPressure: 0,
          eventLoopPressure: 0,
          storagePressure: 0,
          sourcePressure: 0,
          publicationPressure: 0,
        },
        siteHealth: pressureSnapshot?.siteHealth || 'GREEN',
      };
    }

    // 0b. Dependency Outage check (DB or Storage down)
    if (context.dbUnavailable || context.storageUnavailable) {
      this.currentState = 'WAITING_DEPENDENCY';
      return {
        concurrency: this.currentConcurrency,
        targetConcurrency: this.currentConcurrency,
        action: 'HOLD',
        state: 'WAITING_DEPENDENCY',
        reason: context.dbUnavailable ? 'Database unavailable (waiting with backoff)' : 'Storage provider unavailable',
        pressureScore: 80,
        pressureBreakdown: pressureSnapshot?.pressureBreakdown || {
          sitePressure: 0,
          dbPressure: 50,
          memoryPressure: 0,
          eventLoopPressure: 0,
          storagePressure: 30,
          sourcePressure: 0,
          publicationPressure: 0,
        },
        siteHealth: pressureSnapshot?.siteHealth || 'YELLOW',
      };
    }

    // 0c. Sources state check
    if (context.allSourcesBlocked) {
      this.currentState = 'WAITING_SOURCES';
      return {
        concurrency: this.currentConcurrency,
        targetConcurrency: this.currentConcurrency,
        action: 'HOLD',
        state: 'WAITING_SOURCES',
        reason: 'All sources in cooldown/blocked (waiting for automatic reprobe)',
        pressureScore: 50,
        pressureBreakdown: pressureSnapshot?.pressureBreakdown || {
          sitePressure: 0,
          dbPressure: 0,
          memoryPressure: 0,
          eventLoopPressure: 0,
          storagePressure: 0,
          sourcePressure: 50,
          publicationPressure: 0,
        },
        siteHealth: pressureSnapshot?.siteHealth || 'GREEN',
      };
    }

    // 1. Ingest pressure signals
    const siteHealth: SiteHealthState = pressureSnapshot?.siteHealth || 'GREEN';
    let pressureScore = pressureSnapshot?.pressureScore || 0;
    let pressureReason = pressureSnapshot?.pressureReason || '';

    // Local process checks
    const hasEmergencyRss = mem.rssMb >= this.config.rssEmergencyLimitMb;
    const hasHardRss = mem.rssMb >= this.config.rssHardLimitMb;
    const hasSoftRss = mem.rssMb >= this.config.rssSoftLimitMb || mem.rssMb >= this.config.maxRssMb;
    const hasHeapStress = mem.heapUsedMb >= this.config.maxHeapMb;
    const hasLagStress = lag.avgLagMs >= this.config.maxEventLoopLagMs;
    const hasStagedDebt = context.stagedDebt && context.stagedDebt >= 100;

    if (hasEmergencyRss) {
      pressureScore = Math.max(pressureScore, 85);
      pressureReason = `Emergency RSS: ${mem.rssMb}MB >= limit ${this.config.rssEmergencyLimitMb}MB`;
    } else if (hasHardRss) {
      pressureScore = Math.max(pressureScore, 65);
      pressureReason = `Hard RSS: ${mem.rssMb}MB >= limit ${this.config.rssHardLimitMb}MB`;
    } else if (hasSoftRss) {
      pressureScore = Math.max(pressureScore, 40);
      pressureReason = `High RSS: ${mem.rssMb}MB >= limit ${this.config.rssSoftLimitMb}MB`;
    } else if (hasHeapStress) {
      pressureScore = Math.max(pressureScore, 35);
      pressureReason = `High Heap: ${mem.heapUsedMb}MB >= limit ${this.config.maxHeapMb}MB`;
    } else if (hasLagStress) {
      pressureScore = Math.max(pressureScore, 50);
      pressureReason = `High Event Loop Lag: ${lag.avgLagMs}ms >= limit ${this.config.maxEventLoopLagMs}ms`;
    } else if (rateLimits > 0) {
      pressureScore = Math.max(pressureScore, 30);
      pressureReason = `Detected ${rateLimits} HTTP 429 Rate Limits in cycle`;
    } else if (errors >= 2) {
      pressureScore = Math.max(pressureScore, 25);
      pressureReason = `Detected error pattern: ${errors} errors in cycle`;
    } else if (timeouts >= 2) {
      pressureScore = Math.max(pressureScore, 25);
      pressureReason = `Detected timeout pattern: ${timeouts} network timeouts in cycle`;
    } else if (errors + timeouts >= 2) {
      pressureScore = Math.max(pressureScore, 25);
      pressureReason = `Detected repeated failures: ${errors} errors, ${timeouts} timeouts in cycle`;
    } else if (hasStagedDebt) {
      pressureScore = Math.max(pressureScore, 20);
      pressureReason = `Elevated STAGED backlog: ${context.stagedDebt} chapters awaiting publication`;
    }

    const previous = this.currentConcurrency;
    let target = previous;
    let action: AutotunerCycleResult['action'] = 'STABLE';
    let state: AdaptiveCapacityState = this.currentState;

    // 2. Decide Capacity Adjustment (AIMD)

    // CASE A: SURVIVAL / EMERGENCY (concurrency = 1)
    if (hasEmergencyRss || siteHealth === 'RED' || pressureScore >= 75) {
      target = this.config.minConcurrency; // strictly 1
      state = 'SURVIVAL';
      action = target < previous ? 'SCALED_DOWN' : 'STRESS_DETECTED';
      if (!pressureReason) pressureReason = 'Extreme system pressure; running in SURVIVAL mode (concurrency = 1)';

      if (typeof (global as any).gc === 'function') {
        try { (global as any).gc(); } catch {}
      }

      this.applyCapacityChange(target, state, action, pressureReason, siteHealth, mem, lag, pressureSnapshot);
      return this.latestResult;
    }

    // CASE B: SEVERE PRESSURE (~50% reduction or at least 3 steps down)
    if (hasHardRss || siteHealth === 'ORANGE' || pressureScore >= 50) {
      target = Math.max(this.config.minConcurrency, Math.min(Math.round(previous * 0.50), previous - 3));
      state = 'RUNNING_THROTTLED';
      action = target < previous ? 'SCALED_DOWN' : 'STRESS_DETECTED';
      if (!pressureReason) pressureReason = 'Severe pressure detected; downscaling 50%';

      if (typeof (global as any).gc === 'function') {
        try { (global as any).gc(); } catch {}
      }

      this.applyCapacityChange(target, state, action, pressureReason, siteHealth, mem, lag, pressureSnapshot);
      return this.latestResult;
    }

    // CASE C: MODERATE PRESSURE (~20% reduction)
    if (siteHealth === 'YELLOW' || (pressureScore >= 30 && rateLimits === 0)) {
      target = Math.max(this.config.minConcurrency, Math.round(previous * 0.80));
      state = 'RUNNING_THROTTLED';
      action = target < previous ? 'SCALED_DOWN' : 'STRESS_DETECTED';
      if (!pressureReason) pressureReason = 'Moderate pressure detected; downscaling 20%';

      this.applyCapacityChange(target, state, action, pressureReason, siteHealth, mem, lag, pressureSnapshot);
      return this.latestResult;
    }

    // CASE C2: Provider Rate Limits (429) without system stress: maintain concurrency, apply cooldown
    if (rateLimits > 0) {
      this.stableCycleCount = 0;
      this.cooldownUntil = now + this.config.cooldownPeriodMs;
      action = 'STRESS_DETECTED';
      state = 'RUNNING_STABLE';
      const reason = `Detected ${rateLimits} HTTP 429 Rate Limits in cycle (concurrency ${previous} maintained during cooldown)`;
      this.logger.warn(`[Autotuner RateLimit] ${reason}`);
      this.latestResult = {
        concurrency: previous,
        targetConcurrency: previous,
        action: 'STRESS_DETECTED',
        state,
        reason,
        pressureScore,
        pressureBreakdown: pressureSnapshot?.pressureBreakdown || this.latestResult.pressureBreakdown,
        siteHealth,
      };
      return this.latestResult;
    }

    // CASE C3: Error / Timeout pattern without system degradation: maintain concurrency, apply cooldown
    if (errors >= 2 || timeouts >= 2 || (errors + timeouts >= 2)) {
      this.stableCycleCount = 0;
      this.cooldownUntil = now + this.config.cooldownPeriodMs;
      action = 'STRESS_DETECTED';
      state = 'RUNNING_STABLE';
      let patternReason = pressureReason;
      if (!patternReason) {
        if (errors >= 2) patternReason = `Detected error pattern: ${errors} errors in cycle`;
        else if (timeouts >= 2) patternReason = `Detected timeout pattern: ${timeouts} network timeouts in cycle`;
        else patternReason = `Detected repeated failures: ${errors} errors, ${timeouts} timeouts in cycle`;
      }
      this.logger.warn(`[Autotuner ErrorPattern] ${patternReason} (concurrency ${previous} maintained during cooldown)`);
      this.latestResult = {
        concurrency: previous,
        targetConcurrency: previous,
        action: 'STRESS_DETECTED',
        state,
        reason: patternReason,
        pressureScore,
        pressureBreakdown: pressureSnapshot?.pressureBreakdown || this.latestResult.pressureBreakdown,
        siteHealth,
      };
      return this.latestResult;
    }

    // CASE C4: Isolated error or timeout without system stress: maintain concurrency, no cooldown
    if (errors === 1 || timeouts === 1) {
      this.stableCycleCount = 0;
      state = previous === 1 ? 'RECOVERING' : 'RUNNING_STABLE';
      action = 'STABLE';
      const reason = `Isolated ${errors === 1 ? 'error' : 'timeout'} in cycle; concurrency ${previous} maintained`;
      this.latestResult = {
        concurrency: previous,
        targetConcurrency: previous,
        action: 'STABLE',
        state,
        reason,
        pressureScore,
        pressureBreakdown: pressureSnapshot?.pressureBreakdown || this.latestResult.pressureBreakdown,
        siteHealth,
      };
      return this.latestResult;
    }

    // CASE D: MILD PRESSURE (-1)
    if (hasSoftRss || (pressureScore >= 15 && rateLimits === 0 && errors === 0 && timeouts === 0)) {
      target = Math.max(this.config.minConcurrency, previous - 1);
      state = 'RUNNING_THROTTLED';
      action = target < previous ? 'SCALED_DOWN' : 'STRESS_DETECTED';
      if (!pressureReason) pressureReason = 'Mild pressure detected; downscaling -1';

      this.applyCapacityChange(target, state, action, pressureReason, siteHealth, mem, lag, pressureSnapshot);
      return this.latestResult;
    }

    // CASE E: COOLDOWN ACTIVE
    if (now < this.cooldownUntil) {
      const remainingSeconds = Math.ceil((this.cooldownUntil - now) / 1000);
      state = previous === 1 ? 'RECOVERING' : 'RUNNING_STABLE';
      return {
        concurrency: this.currentConcurrency,
        targetConcurrency: this.currentConcurrency,
        action: 'COOLDOWN',
        state,
        reason: `In stabilization cooldown for ${remainingSeconds}s`,
        pressureScore,
        pressureBreakdown: pressureSnapshot?.pressureBreakdown || this.latestResult.pressureBreakdown,
        siteHealth,
      };
    }

    // CASE F: SYSTEM HEALTHY — SLOW UP (+1 step, dwell time enforced)
    this.stableCycleCount++;

    const dwellTimeSatisfied = now - this.lastCapacityChangeAt >= this.config.scaleUpDwellTimeMs;
    const stableCyclesSatisfied = this.stableCycleCount >= this.config.requiredStableCycles;

    if (stableCyclesSatisfied && dwellTimeSatisfied && this.currentConcurrency < this.config.maxConcurrency) {
      // Memory proximity hold: do NOT scale up if close to soft limit or committed buffers are high
      const totalCommitted = this.activeBufferedBytes + this.reservedBufferedBytes;
      if (mem.rssMb >= (this.config.rssSoftLimitMb - 20) || totalCommitted > (this.config.maxBufferedBytes * 0.7)) {
        state = 'RUNNING_STABLE';
        return {
          concurrency: this.currentConcurrency,
          targetConcurrency: this.currentConcurrency,
          action: 'STABLE',
          state,
          reason: `Holding concurrency at ${this.currentConcurrency} due to memory proximity (RSS: ${mem.rssMb}MB, Committed: ${Math.round(totalCommitted / 1024 / 1024)}MB)`,
          pressureScore,
          pressureBreakdown: pressureSnapshot?.pressureBreakdown || this.latestResult.pressureBreakdown,
          siteHealth,
        };
      }

      target = Math.min(this.config.maxConcurrency, previous + 1);
      state = previous === 1 ? 'RECOVERING' : 'RUNNING_ACCELERATING';
      action = 'SCALED_UP';
      const reason = `System healthy across ${this.stableCycleCount} cycles and dwell window satisfied. Scaled up: ${previous} -> ${target}`;

      this.applyCapacityChange(target, state, action, reason, siteHealth, mem, lag, pressureSnapshot);
      return this.latestResult;
    }

    // Stable holding
    state = previous === 1 ? 'RECOVERING' : 'RUNNING_STABLE';
    action = 'STABLE';
    const reason = `Stable (${this.stableCycleCount}/${this.config.requiredStableCycles} cycles, dwell: ${Math.round((now - this.lastCapacityChangeAt) / 1000)}s/${Math.round(this.config.scaleUpDwellTimeMs / 1000)}s)`;

    this.latestResult = {
      concurrency: this.currentConcurrency,
      targetConcurrency: this.currentConcurrency,
      action,
      state,
      reason,
      pressureScore,
      pressureBreakdown: pressureSnapshot?.pressureBreakdown || this.latestResult.pressureBreakdown,
      siteHealth,
    };
    return this.latestResult;
  }

  private applyCapacityChange(
    target: number,
    state: AdaptiveCapacityState,
    action: AutotunerCycleResult['action'],
    reason: string,
    siteHealth: SiteHealthState,
    mem: any,
    lag: any,
    pressureSnapshot?: PressureSnapshot
  ): void {
    const previous = this.currentConcurrency;
    const clampedTarget = Math.max(this.config.minConcurrency, Math.min(this.config.maxConcurrency, target));

    this.currentConcurrency = clampedTarget;
    this.currentState = state;
    this.lastCapacityChangeAt = Date.now();
    this.stableCycleCount = 0;

    if (action === 'SCALED_DOWN' || action === 'STRESS_DETECTED' || action === 'SURVIVAL') {
      this.cooldownUntil = Date.now() + this.config.cooldownPeriodMs;
    }

    if (state === 'RUNNING_STABLE' || action === 'SCALED_UP') {
      this.lastStableConcurrency = clampedTarget;
      this.lastStableAt = Date.now();
    }

    // SINGLE AUTHORITY: Update the global chapter semaphore
    this.globalChapterSemaphore.setCapacity(clampedTarget);

    // STRUCTURED DECISION LOG
    if (clampedTarget !== previous) {
      this.logger.warn(
        `[CAPACITY_DECISION] Concurrency changed from ${previous} to ${clampedTarget} | State: ${state} | Action: ${action} | Reason: ${reason} | Site: ${siteHealth} | RSS: ${mem.rssMb}MB | Lag: ${lag.avgLagMs}ms | YSQL: ${pressureSnapshot?.ysqlTotal ?? '?'}/${pressureSnapshot?.ysqlActive ?? '?'}`
      );
    }

    this.latestResult = {
      concurrency: clampedTarget,
      targetConcurrency: clampedTarget,
      action,
      state,
      reason,
      pressureScore: pressureSnapshot?.pressureScore || 0,
      pressureBreakdown: pressureSnapshot?.pressureBreakdown || {
        sitePressure: 0,
        dbPressure: 0,
        memoryPressure: 0,
        eventLoopPressure: 0,
        storagePressure: 0,
        sourcePressure: 0,
        publicationPressure: 0,
      },
      siteHealth,
    };
  }

  getCurrentConcurrency(): number {
    return this.currentConcurrency;
  }

  getAdaptiveState(): AdaptiveCapacityState {
    return this.currentState;
  }

  getState(): AdaptiveCapacityState {
    return this.currentState;
  }

  getMaxConcurrency(): number {
    return this.config.maxConcurrency;
  }

  getLatestResult(): AutotunerCycleResult {
    return this.latestResult;
  }

  getLastStableConcurrency(): number {
    return this.lastStableConcurrency;
  }

  setCapacity(
    newCapacity: number,
    stateOrReason?: AdaptiveCapacityState | string,
    optionalReason?: string
  ): void {
    let state: AdaptiveCapacityState = 'RUNNING_STABLE';
    let reason = 'Manual capacity adjustment';

    if (stateOrReason) {
      if (
        stateOrReason === 'RUNNING_ACCELERATING' ||
        stateOrReason === 'RUNNING_STABLE' ||
        stateOrReason === 'RUNNING_THROTTLED' ||
        stateOrReason === 'SURVIVAL' ||
        stateOrReason === 'WAITING_DEPENDENCY' ||
        stateOrReason === 'WAITING_SOURCES' ||
        stateOrReason === 'RECOVERING' ||
        stateOrReason === 'MANUAL_STOP'
      ) {
        state = stateOrReason;
        reason = optionalReason || 'Manual state and capacity adjustment';
      } else {
        reason = stateOrReason;
      }
    }

    const target = Math.max(this.config.minConcurrency, Math.min(this.config.maxConcurrency, newCapacity));
    const mem = diagnostics.getMemorySnapshot();
    const lag = (diagnostics as any).lagMonitor?.getMetrics?.() || { avgLagMs: 0 };
    const action = state === 'SURVIVAL' ? 'SURVIVAL' : (target < this.currentConcurrency ? 'SCALED_DOWN' : 'STABLE');
    this.applyCapacityChange(
      target,
      state,
      action,
      reason,
      state === 'SURVIVAL' ? 'RED' : 'GREEN',
      mem,
      lag
    );
  }
}
