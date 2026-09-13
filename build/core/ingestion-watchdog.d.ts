import type { SupabaseClient } from '@supabase/supabase-js';
import { GlobalStorageRateLimiter } from './rate-limiter.js';
export type WatchdogStatus = 'HEALTHY_IDLE' | 'HEALTHY_ACTIVE' | 'STALLED' | 'WORKER_OFFLINE' | 'DEGRADED_RATE_LIMITED';
export interface IngestionWatchdogInput {
    upstreamHasNewChapters: boolean;
    activeWorkerCount: number;
    pendingJobs: number;
    processingJobs: number;
    completedJobs: number;
    failedJobs: number;
    stalledJobs: number;
    rateLimitCooldownActive: boolean;
    rateLimitWaitMs: number;
    discoveryStarvation?: boolean;
    overdueDiscoverySourcesCount?: number;
    activeDiscoveryJobs?: number;
}
export interface WatchdogEvaluation {
    status: WatchdogStatus;
    scenarioName: string;
    description: string;
    alertNeeded: boolean;
    actionRequired: 'NONE' | 'WAIT_COOLDOWN' | 'SPAWN_WORKER' | 'RECOVER_STALLED';
    details: IngestionWatchdogInput;
    timestamp: string;
}
export declare class IngestionWatchdog {
    private supabase?;
    private rateLimiter?;
    private logger;
    constructor(supabase?: SupabaseClient | undefined, rateLimiter?: GlobalStorageRateLimiter | undefined);
    /**
     * Pure evaluation of ingestion health based on input metrics.
     * Accurately determines which operational scenario the system is in.
     */
    evaluate(input: IngestionWatchdogInput): WatchdogEvaluation;
    /**
     * Queries Supabase and local rateLimiter state to generate a real-time watchdog evaluation.
     */
    checkLiveState(upstreamHasNewChapters?: boolean): Promise<WatchdogEvaluation>;
}
