import { Logger } from './logger.js';
import { diagnostics } from './diagnostics.js';

export class AsyncSemaphore {
  private activePermits = 0;
  private maxPermits: number;
  private waitQueue: Array<() => void> = [];

  constructor(maxPermits: number) {
    this.maxPermits = Math.max(1, maxPermits);
  }

  async acquire(signal?: AbortSignal): Promise<void> {
    signal?.throwIfAborted();
    if (this.activePermits < this.maxPermits) {
      this.activePermits++;
      return;
    }
    return new Promise<void>((resolve, reject) => {
      const granted = () => { signal?.removeEventListener('abort', cancelled); resolve(); };
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
    this.drain();
  }

  private drain(): void {
    while (this.activePermits < this.maxPermits && this.waitQueue.length > 0) {
      this.activePermits++;
      this.waitQueue.shift()!();
    }
  }

  async runExclusive<T>(fn: () => Promise<T>): Promise<T> {
    await this.acquire();
    try {
      return await fn();
    } finally {
      this.release();
    }
  }

  setCapacity(newCapacity: number): void {
    const target = Math.max(1, newCapacity);
    this.maxPermits = target;
    // Existing holders drain naturally after a downscale; never reissue their permits.
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

export const DEFAULT_SOURCE_LIMIT: SourceConcurrencyConfig = {
  maxChapters: 2,
  maxPagesPerChapter: 4,
};

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
}

const DEFAULT_AUTOTUNER_CONFIG: AutotunerConfig = {
  minConcurrency: 1,
  maxConcurrency: 4,
  initialConcurrency: 2,
  requiredStableCycles: 3,
  cooldownPeriodMs: 25 * 1000,
  maxRssMb: 260,
  maxHeapMb: 160,
  maxExternalAndBuffersMb: 60,
  maxEventLoopLagMs: 100,
};

export class AdaptiveAutotuner {
  private logger = new Logger('Autotuner');
  private globalChapterSemaphore: AsyncSemaphore;
  private sourceSemaphores = new Map<string, AsyncSemaphore>();
  private globalMediaSemaphore: AsyncSemaphore;
  private globalInflightRequestSemaphore: AsyncSemaphore;
  private bufferedPageSemaphore = new AsyncSemaphore(6);
  private currentConcurrency: number;
  private stableCycleCount = 0;
  private cooldownUntil = 0;
  private config: AutotunerConfig;

  // Window error counters
  private cycleErrors = 0;
  private cycleRateLimits = 0;
  private cycleTimeouts = 0;

  constructor(config: Partial<AutotunerConfig> = {}) {
    this.config = { ...DEFAULT_AUTOTUNER_CONFIG, ...config };
    this.currentConcurrency = this.config.initialConcurrency;
    this.globalChapterSemaphore = new AsyncSemaphore(this.currentConcurrency);
    this.globalMediaSemaphore = new AsyncSemaphore(6); // Safe bounded concurrent image uploads
    this.globalInflightRequestSemaphore = new AsyncSemaphore(16); // Bounded network download budget
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

  // Hold a slot from before downloading until the page has finished uploading.
  getBufferedPageSemaphore(): AsyncSemaphore {
    return this.bufferedPageSemaphore;
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
      sem = new AsyncSemaphore(configuredLimit);
      this.sourceSemaphores.set(source, sem);
    }
    return sem;
  }

  recordError(type: 'error' | 'ratelimit' | 'timeout'): void {
    if (type === 'ratelimit') this.cycleRateLimits++;
    else if (type === 'timeout') this.cycleTimeouts++;
    else this.cycleErrors++;
  }

  evaluateCycle(): {
    concurrency: number;
    action: 'SCALED_UP' | 'SCALED_DOWN' | 'STABLE' | 'COOLDOWN';
    reason: string;
  } {
    const mem = diagnostics.getMemorySnapshot();
    const lag = (diagnostics as any).lagMonitor.getMetrics();
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
    let stressReason: string | null = null;
    if (mem.rssMb >= this.config.maxRssMb) {
      stressReason = `High RSS: ${mem.rssMb}MB >= limit ${this.config.maxRssMb}MB`;
    } else if (mem.heapUsedMb >= this.config.maxHeapMb) {
      stressReason = `High Heap: ${mem.heapUsedMb}MB >= limit ${this.config.maxHeapMb}MB`;
    } else if (totalExternal >= this.config.maxExternalAndBuffersMb) {
      stressReason = `High External/Buffers: ${totalExternal}MB >= limit ${this.config.maxExternalAndBuffersMb}MB`;
    } else if (lag.avgLagMs >= this.config.maxEventLoopLagMs) {
      stressReason = `High Event Loop Lag: ${lag.avgLagMs}ms >= limit ${this.config.maxEventLoopLagMs}ms`;
    } else if (rateLimits > 0) {
      stressReason = `Detected ${rateLimits} HTTP 429 Rate Limits in cycle`;
    } else if (errors >= 2) {
      stressReason = `Detected error pattern: ${errors} errors in cycle`;
    } else if (timeouts >= 2) {
      stressReason = `Detected timeout pattern: ${timeouts} network timeouts in cycle`;
    } else if (errors + timeouts >= 2) {
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
      this.logger.info(
        `[Autotuner ISOLATED] Single error/timeout in cycle (errors: ${errors}, timeouts: ${timeouts}). Maintaining concurrency at ${this.currentConcurrency} without cooldown.`
      );
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

    if (
      this.stableCycleCount >= this.config.requiredStableCycles &&
      this.currentConcurrency < this.config.maxConcurrency
    ) {
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

  getCurrentConcurrency(): number {
    return this.currentConcurrency;
  }
}
