import type { SupabaseClient } from '@supabase/supabase-js';
import { SourceRegistry } from '../sources/registry.js';
import { StorageProvider } from '../storage/provider.js';
import { HostRateLimiter } from './rate-limiter.js';
import { Config } from '../config.js';
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
    private isRunning;
    private stopSignal;
    constructor(supabase: SupabaseClient, storage: StorageProvider, registry: SourceRegistry, rateLimiter: HostRateLimiter, config: Config);
    start(): Promise<void>;
    runStartupRecovery(): Promise<void>;
    stop(): void;
    /**
     * Run a single discrete engine iteration (also used in tests)
     */
    step(): Promise<boolean>;
    private scheduleSources;
    private processJob;
    private handleDiscoverWorks;
    private handleSyncWork;
    private handleImportChapter;
    private downloadAndRegisterImage;
    private fetchImageBytes;
    private cachedBotUserId;
    private resolveBotUserId;
    private sleep;
}
