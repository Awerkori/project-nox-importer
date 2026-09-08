import type { SupabaseClient } from '@supabase/supabase-js';
import { SourceRegistry } from '../sources/registry.js';
import { StorageProvider } from '../storage/provider.js';
import { HostRateLimiter } from './rate-limiter.js';
import { Config } from '../config.js';
import { AdaptiveAutotuner } from './concurrency.js';
export declare function computeCanonicalChapterKey(chapterNumber: number | string, chapterTitle?: string): {
    normalizedNumber: number;
    sortKey: number;
    isSpecial: boolean;
    specialCategory?: 'prologue' | 'extra' | 'special' | 'side';
};
export declare class ImporterEngine {
    private supabase;
    private storage;
    private registry;
    private rateLimiter;
    private config;
    private logger;
    private queue;
    private deduplication;
    private checkpoints;
    private autotuner;
    private isRunning;
    private stopSignal;
    private abortController;
    constructor(supabase: SupabaseClient, storage: StorageProvider, registry: SourceRegistry, rateLimiter: HostRateLimiter, config: Config);
    getAutotuner(): AdaptiveAutotuner;
    start(): Promise<void>;
    runStartupRecovery(): Promise<void>;
    stop(): void;
    /**
     * Periodic discovery scheduler running in the background
     */
    private runDiscoveryLoop;
    private autotunerCycleCount;
    /**
     * Periodic autotuner telemetry & evaluation loop (every 30s)
     */
    private runAutotunerLoop;
    /**
     * Dedicated worker loop for a specific source
     */
    private runSourceWorker;
    /**
     * General worker loop to process jobs with no source filter
     */
    private runGeneralWorker;
    /**
     * Executes a job respecting global and per-source concurrency semaphores
     */
    private executeJobWithLimits;
    /**
     * Discrete step method preserved for unit tests & single iterations
     */
    step(source?: string): Promise<boolean>;
    private scheduleSources;
    private processJob;
    private handleDiscoverWorks;
    private handleSyncWork;
    computeCanonicalChapterKey(chapterNumber: number | string, chapterTitle?: string): {
        normalizedNumber: number;
        sortKey: number;
        isSpecial: boolean;
        specialCategory?: "prologue" | "extra" | "special" | "side";
    };
    private computeChapterSortKey;
    private handleImportChapter;
    private recordJobMetric;
    private recordTelemetrySnapshot;
    private pruneTelemetry;
    private sanitizeErrorMessage;
    private downloadAndRegisterImage;
    private fetchImageBytes;
    private cachedBotUserId;
    private resolveBotUserId;
    private sleep;
}
