import type { SupabaseClient } from '@supabase/supabase-js';
export interface ProtectiveStopInfo {
    active: boolean;
    reason?: string | null;
    details?: any;
    triggered_at?: string | null;
    resumed_at?: string | null;
    resumed_by?: string | null;
}
export interface SentinelThresholds {
    homeTtfbPreSlaMs: number;
    readerTtfbPreSlaMs: number;
    mediaTtfbPreSlaMs: number;
    ysqlConnTripwire: number;
    maxRssMb: number;
    maxEventLoopLagMs: number;
    maxTelegramFloodWaitSec: number;
}
export declare const DEFAULT_SENTINEL_THRESHOLDS: SentinelThresholds;
export declare class ProtectiveSentinel {
    private supabase;
    private thresholds;
    private siteUrl?;
    private logger;
    private cachedInfo;
    private lastFetchMs;
    private cacheTtlMs;
    private isRunning;
    private stopSignal;
    private consecutivePreSlaViolations;
    constructor(supabase: SupabaseClient, thresholds?: SentinelThresholds, siteUrl?: string | undefined);
    /**
     * Checks whether the protective stop is currently active.
     * Reads from database 'settings' table with a 3s TTL cache.
     */
    isProtectiveStopActive(): Promise<boolean>;
    /**
     * Retrieves full protective stop details from the settings table.
     */
    getProtectiveStopInfo(forceFresh?: boolean): Promise<ProtectiveStopInfo>;
    /**
     * Triggers a persistent PROTECTIVE STOP.
     * Halts all new job claims, allows in-flight jobs to safely drain,
     * keeps publication barrier alive, and requires manual resumption.
     */
    triggerProtectiveStop(reason: string, details: any): Promise<void>;
    /**
     * Resumes normal operation (intended for explicit manual/staff resumption).
     */
    resumeProtectiveStop(resumedBy?: string): Promise<void>;
    /**
     * Background sentinel watchdog loop.
     * Probes pre-SLA metrics every 15s. If pre-SLA stress is detected, trips PROTECTIVE_STOP.
     */
    startWatchdogLoop(): void;
    stop(): void;
    /**
     * Evaluates all Pre-SLA guard rails.
     */
    evaluatePreSlaGuardRails(): Promise<void>;
    /**
     * Probes site route latency. Requires 2 consecutive violations before tripping to eliminate transient network blips.
     */
    private probeSiteLatency;
}
