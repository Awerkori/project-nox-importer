import type { SupabaseClient } from '@supabase/supabase-js';
export type IncidentClassification = 'TRANSIENT_EDGE_INCIDENT' | 'REAL_SYSTEM_PRESSURE' | 'YSQL_PRESSURE' | 'IMPORTER_PRESSURE' | 'MANUAL_STOP';
export interface ProtectiveStopInfo {
    active: boolean;
    reason?: string | null;
    classification?: IncidentClassification | null;
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
    private consecutive5xxErrors;
    private consecutiveLatencyViolations;
    private consecutiveProbeErrors;
    private homeAgent;
    private readerAgent;
    private httpAgent;
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
     * Triggers a persistent PROTECTIVE STOP with incident classification.
     * Halts all new job claims, allows in-flight jobs to safely drain,
     * keeps publication barrier alive.
     */
    triggerProtectiveStop(reason: string, details: any, classification?: IncidentClassification): Promise<void>;
    /**
     * Resumes normal operation.
     */
    resumeProtectiveStop(resumedBy?: string): Promise<void>;
    /**
     * Background sentinel watchdog loop.
     * Probes metrics every 15s. If stopped, triggers rapid auto-heal checks.
     */
    startWatchdogLoop(): void;
    stop(): void;
    /**
     * Evaluates whether a currently stopped importer can safely auto-resume.
     * Distinguishes transient edge incidents from sustained pressure.
     * Checks 2 consecutive healthy samples with 5s debounce for rapid recovery (15-30s).
     * NEVER auto-resumes manual staff stops or active ongoing degradation.
     */
    evaluateAutoResume(): Promise<void>;
    private measureRoute;
    /**
     * Evaluates all Pre-SLA guard rails.
     */
    evaluatePreSlaGuardRails(): Promise<void>;
    /**
     * Probes site route latency using keep-alive connection.
     */
    private probeSiteLatency;
    /**
     * Evaluates probe responses, strictly distinguishing:
     * A) TRANSIENT EDGE INCIDENTS:
     *    - 1 isolated 5xx
     *    - normal WAN latency jitter
     *    - healthy DB and importer
     *    => DO NOT STOP, log warning and observe.
     *
     * B) REAL SYSTEM PRESSURE:
     *    - >= 3 consecutive 5xx errors (sustained edge/Worker failure)
     *    - >= 2 consecutive 5xx errors WITH confirmed infra pressure
     *    - Sustained severe latency (>= 3000ms) for 3+ consecutive probes
     *    => Trip PROTECTIVE_STOP with appropriate classification.
     */
    handleProbeResult(label: 'home' | 'reader' | 'media', url: string, ttfbMs: number, thresholdMs: number, slaTargetMs: number, statusCode: number): Promise<void>;
    handleProbeError(label: 'home' | 'reader' | 'media', url: string, err: any): Promise<void>;
}
