import { Logger } from './logger.js';
import { diagnostics } from './diagnostics.js';

export class AsyncSemaphore {
  private currentPermits: number;
  private maxPermits: number;
  private waitQueue: Array<() => void> = [];

  constructor(maxPermits: number) {
    this.maxPermits = Math.max(1, maxPermits);
    this.currentPermits = this.maxPermits;
  }

  async acquire(): Promise<void> {
    if (this.currentPermits > 0) {
      this.currentPermits--;
      return;
    }
    return new Promise<void>((resolve) => {
      this.waitQueue.push(resolve);
    });
  }

  release(): void {
    if (this.waitQueue.length > 0) {
      const next = this.waitQueue.shift();
      if (next) next();
    } else {
      if (this.currentPermits < this.maxPermits) {
        this.currentPermits++;
      }
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
    const diff = target - this.maxPermits;
    this.maxPermits = target;

    if (diff > 0) {
      // Release waiting callers for newly added capacity
      for (let i = 0; i < diff && this.waitQueue.length > 0; i++) {
        const next = this.waitQueue.shift();
        if (next) next();
      }
      this.currentPermits = Math.min(this.maxPermits, this.currentPermits + diff);
    } else if (diff < 0) {
      this.currentPermits = Math.max(0, this.currentPermits + diff);
    }
  }

  get capacity(): number {
    return this.maxPermits;
  }

  get available(): number {
    return this.currentPermits;
  }

  get active(): number {
    return this.maxPermits - this.currentPermits;
  }

  get queued(): number {
    return this.waitQueue.length;
  }
}

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
  maxConcurrency: 5,
  initialConcurrency: 1,
  requiredStableCycles: 4, // 4 cycles * 30s = 2 minutes of continuous stability
  cooldownPeriodMs: 2 * 60 * 1000, // 2 minutes cooldown after any stress/scale-down
  maxRssMb: 360, // Container is 512MB: keep RSS comfortably below 360MB (152MB margin)
  maxHeapMb: 240, // Heap threshold
  maxExternalAndBuffersMb: 120, // Native buffers + external (accommodates heavy Mango Toons webtoons)
  maxEventLoopLagMs: 100, // Maximum tolerated event loop lag
};

export class AdaptiveAutotuner {
  private logger = new Logger('Autotuner');
  private globalChapterSemaphore: AsyncSemaphore;
  private sourceSemaphores = new Map<string, AsyncSemaphore>();
  private globalMediaSemaphore: AsyncSemaphore;
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
    this.globalMediaSemaphore = new AsyncSemaphore(6); // Concurrent image upload limit to Telegram
  }

  getGlobalChapterSemaphore(): AsyncSemaphore {
    return this.globalChapterSemaphore;
  }

  getGlobalMediaSemaphore(): AsyncSemaphore {
    return this.globalMediaSemaphore;
  }

  getSourceSemaphore(source: string, limitPerSource = 2): AsyncSemaphore {
    let sem = this.sourceSemaphores.get(source);
    if (!sem) {
      sem = new AsyncSemaphore(limitPerSource);
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
    } else if (errors > 0) {
      stressReason = `Detected ${errors} unexpected errors in cycle`;
    } else if (timeouts > 0) {
      stressReason = `Detected ${timeouts} network timeouts in cycle`;
    }

    if (stressReason) {
      // Immediate scale-down
      this.stableCycleCount = 0;
      this.cooldownUntil = now + this.config.cooldownPeriodMs;

      const previous = this.currentConcurrency;
      const target = Math.max(this.config.minConcurrency, previous - 1);
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

    if (
      this.stableCycleCount >= this.config.requiredStableCycles &&
      this.currentConcurrency < this.config.maxConcurrency
    ) {
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

  getCurrentConcurrency(): number {
    return this.currentConcurrency;
  }
}
