import { Logger } from './logger.js';

export interface ActiveJobContext {
  jobId: string;
  taskType: string;
  source: string;
  workId?: string;
  workTitle?: string;
  chapterNumber?: number;
  totalExpectedPages?: number;
  completedPages?: number;
  startedAt: number;
  initialMemoryMb: { rss: number; heapUsed: number };
}

export interface MemorySnapshot {
  rssMb: number;
  heapUsedMb: number;
  heapTotalMb: number;
  externalMb: number;
  arrayBuffersMb: number;
}

export interface ForensicReport {
  timestamp: string;
  reason: string;
  error?: {
    message: string;
    stack?: string;
    name?: string;
    code?: string | number;
  };
  process: {
    pid: number;
    uptimeSeconds: number;
    nodeVersion: string;
    memory: MemorySnapshot;
    eventLoopLag: {
      avgLagMs: number;
      maxLagMs: number;
      recentLagMs: number;
    };
  };
  activeJobsCount: number;
  activeJobs: Array<Omit<ActiveJobContext, 'initialMemoryMb'> & { durationSeconds: number }>;
}

export class EventLoopLagMonitor {
  private timer: NodeJS.Timeout | null = null;
  private lastCheck = Date.now();
  private readonly intervalMs = 500;
  private recentLags: number[] = [];
  private maxLagMs = 0;
  private readonly maxSamples = 20;

  start(): void {
    if (this.timer) return;
    this.lastCheck = Date.now();
    this.timer = setInterval(() => {
      const now = Date.now();
      const expected = this.lastCheck + this.intervalMs;
      const lag = Math.max(0, now - expected);
      this.lastCheck = now;

      this.recentLags.push(lag);
      if (this.recentLags.length > this.maxSamples) {
        this.recentLags.shift();
      }
      if (lag > this.maxLagMs) {
        this.maxLagMs = lag;
      }
    }, this.intervalMs);
    this.timer.unref();
  }

  stop(): void {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
  }

  getMetrics(): { avgLagMs: number; maxLagMs: number; recentLagMs: number } {
    if (this.recentLags.length === 0) {
      return { avgLagMs: 0, maxLagMs: this.maxLagMs, recentLagMs: 0 };
    }
    const sum = this.recentLags.reduce((a, b) => a + b, 0);
    const avgLagMs = Math.round(sum / this.recentLags.length);
    const recentLagMs = this.recentLags[this.recentLags.length - 1] || 0;
    return {
      avgLagMs,
      maxLagMs: this.maxLagMs,
      recentLagMs,
    };
  }

  resetMax(): void {
    this.maxLagMs = 0;
  }
}

export class DiagnosticsManager {
  private static instance: DiagnosticsManager;
  private logger = new Logger('Diagnostics');
  private activeJobs = new Map<string, ActiveJobContext>();
  private lagMonitor = new EventLoopLagMonitor();
  private shutdownInitiated = false;

  private constructor() {
    this.lagMonitor.start();
  }

  static getInstance(): DiagnosticsManager {
    if (!DiagnosticsManager.instance) {
      DiagnosticsManager.instance = new DiagnosticsManager();
    }
    return DiagnosticsManager.instance;
  }

  getMemorySnapshot(): MemorySnapshot {
    const mem = process.memoryUsage();
    return {
      rssMb: Math.round(mem.rss / 1024 / 1024),
      heapUsedMb: Math.round(mem.heapUsed / 1024 / 1024),
      heapTotalMb: Math.round(mem.heapTotal / 1024 / 1024),
      externalMb: Math.round(mem.external / 1024 / 1024),
      arrayBuffersMb: Math.round((mem.arrayBuffers || 0) / 1024 / 1024),
    };
  }

  registerJob(job: Omit<ActiveJobContext, 'startedAt' | 'initialMemoryMb'>): void {
    const mem = this.getMemorySnapshot();
    const fullJob: ActiveJobContext = {
      ...job,
      startedAt: Date.now(),
      initialMemoryMb: { rss: mem.rssMb, heapUsed: mem.heapUsedMb },
    };
    this.activeJobs.set(job.jobId, fullJob);
  }

  updateJobProgress(jobId: string, completedPages: number): void {
    const existing = this.activeJobs.get(jobId);
    if (existing) {
      existing.completedPages = completedPages;
    }
  }

  unregisterJob(jobId: string): void {
    this.activeJobs.delete(jobId);
  }

  getActiveJobsCount(): number {
    return this.activeJobs.size;
  }

  getActiveJobs(): ActiveJobContext[] {
    return Array.from(this.activeJobs.values());
  }

  generateForensicReport(reason: string, err?: any): ForensicReport {
    const now = Date.now();
    const memory = this.getMemorySnapshot();
    const lagMetrics = this.lagMonitor.getMetrics();

    const jobsSummary = Array.from(this.activeJobs.values()).map((job) => ({
      jobId: job.jobId,
      taskType: job.taskType,
      source: job.source,
      workId: job.workId,
      workTitle: job.workTitle,
      chapterNumber: job.chapterNumber,
      totalExpectedPages: job.totalExpectedPages,
      completedPages: job.completedPages,
      startedAt: job.startedAt,
      durationSeconds: Math.floor((now - job.startedAt) / 1000),
    }));

    return {
      timestamp: new Date().toISOString(),
      reason,
      error: err
        ? {
            message: err?.message || String(err),
            stack: err?.stack,
            name: err?.name,
            code: err?.code || err?.statusCode || err?.status,
          }
        : undefined,
      process: {
        pid: process.pid,
        uptimeSeconds: Math.floor(process.uptime()),
        nodeVersion: process.version,
        memory,
        eventLoopLag: lagMetrics,
      },
      activeJobsCount: this.activeJobs.size,
      activeJobs: jobsSummary,
    };
  }

  dumpForensics(reason: string, err?: any): void {
    const report = this.generateForensicReport(reason, err);
    this.logger.error(`[FORENSIC CRASH DUMP] Triggered by: ${reason}`, report);
  }

  initProcessHandlers(onGracefulShutdown: (signal: string) => Promise<void> | void): void {
    const handleShutdown = async (signal: string) => {
      if (this.shutdownInitiated) return;
      this.shutdownInitiated = true;
      this.logger.info(`Received shutdown signal: ${signal}. Running graceful shutdown handlers...`);

      try {
        await Promise.resolve(onGracefulShutdown(signal));
      } catch (err: any) {
        this.logger.error('Error during graceful shutdown callback', {
          error: err?.message,
          stack: err?.stack,
        });
      }
    };

    process.on('SIGINT', () => handleShutdown('SIGINT'));
    process.on('SIGTERM', () => handleShutdown('SIGTERM'));

    process.on('unhandledRejection', (reason: any) => {
      this.dumpForensics('unhandledRejection', reason);
    });

    process.on('uncaughtException', (err: Error) => {
      this.dumpForensics('uncaughtException', err);
      // Clean exit with code 1 so orchestrator (DIScloud) knows process failed
      setTimeout(() => process.exit(1), 1000).unref();
    });

    process.on('warning', (warning: Error) => {
      this.logger.warn('Node process warning detected', {
        name: warning.name,
        message: warning.message,
        stack: warning.stack,
      });
    });
  }

  stop(): void {
    this.lagMonitor.stop();
  }
}

export const diagnostics = DiagnosticsManager.getInstance();
