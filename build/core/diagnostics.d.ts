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
    initialMemoryMb: {
        rss: number;
        heapUsed: number;
    };
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
    activeJobs: Array<Omit<ActiveJobContext, 'initialMemoryMb'> & {
        durationSeconds: number;
    }>;
}
export declare class EventLoopLagMonitor {
    private timer;
    private lastCheck;
    private readonly intervalMs;
    private recentLags;
    private maxLagMs;
    private readonly maxSamples;
    start(): void;
    stop(): void;
    getMetrics(): {
        avgLagMs: number;
        maxLagMs: number;
        recentLagMs: number;
    };
    resetMax(): void;
}
export declare class DiagnosticsManager {
    private static instance;
    private logger;
    private activeJobs;
    private lagMonitor;
    private shutdownInitiated;
    private constructor();
    static getInstance(): DiagnosticsManager;
    getMemorySnapshot(): MemorySnapshot;
    registerJob(job: Omit<ActiveJobContext, 'startedAt' | 'initialMemoryMb'>): void;
    updateJobProgress(jobId: string, completedPages: number): void;
    unregisterJob(jobId: string): void;
    getActiveJobsCount(): number;
    getActiveJobs(): ActiveJobContext[];
    generateForensicReport(reason: string, err?: any): ForensicReport;
    dumpForensics(reason: string, err?: any): void;
    initProcessHandlers(onGracefulShutdown: (signal: string) => Promise<void> | void): void;
    stop(): void;
}
export declare const diagnostics: DiagnosticsManager;
