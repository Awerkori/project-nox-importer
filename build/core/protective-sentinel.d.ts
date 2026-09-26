import type { SupabaseClient } from '@supabase/supabase-js';
export type IncidentClassification = 'MANUAL_STOP' | 'TRANSIENT_EDGE_INCIDENT' | 'REAL_SYSTEM_PRESSURE' | 'YSQL_PRESSURE' | 'IMPORTER_PRESSURE';
export type SiteHealthState = 'GREEN' | 'YELLOW' | 'ORANGE' | 'RED';
export interface PressureSnapshot {
    timestamp: number;
    siteHealth: SiteHealthState;
    homeP50: number;
    homeP95: number;
    readerP50: number;
    readerP95: number;
    consecutive5xx: number;
    lastHttp5xx: number | null;
    ysqlTotal: number;
    ysqlActive: number;
    poolWait: number;
    rssMb: number;
    heapUsedMb: number;
    eventLoopLagMs: number;
    pressureScore: number;
    pressureBreakdown: {
        sitePressure: number;
        dbPressure: number;
        memoryPressure: number;
        eventLoopPressure: number;
        storagePressure: number;
        sourcePressure: number;
        publicationPressure: number;
    };
    pressureReason: string;
}
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
    private homeSamples;
    private readerSamples;
    private consecutive5xxCount;
    private last5xxTimestamp;
    private consecutiveProbeFailures;
    private cachedReaderChapterId;
    private cachedReaderChapterAt;
    private latestSnapshot;
    private homeAgent;
    private readerAgent;
    private httpAgent;
    constructor(supabase: SupabaseClient, thresholds?: SentinelThresholds, siteUrl?: string | undefined);
    /**
     * Checks whether a MANUAL staff protective stop is active.
     * STRICT INVARIANT: Automatic performance stops CANNOT make this return true.
     * If a legacy automatic stop exists in DB, it is auto-cleared on discovery.
     */
    isProtectiveStopActive(): Promise<boolean>;
    /**
     * Retrieves full protective stop details from the settings table.
     */
    getProtectiveStopInfo(forceFresh?: boolean): Promise<ProtectiveStopInfo>;
    /**
     * On startup, auto-clears any legacy automatic protective stop if active.
     */
    clearLegacyProtectiveStopOnStartup(): Promise<void>;
    /**
     * Triggers a MANUAL staff protective stop.
     * AUTOMATIC PERFORMANCE STOPS ARE STRICTLY FORBIDDEN.
     * If called with classification != 'MANUAL_STOP', it is rejected and forwarded to adaptive pressure.
     */
    triggerProtectiveStop(reason: string, details: any, classification?: IncidentClassification): Promise<void>;
    /**
     * Resumes normal operation after manual stop.
     */
    resumeProtectiveStop(resumedBy?: string): Promise<void>;
    /**
     * Returns the current computed pressure snapshot for AdaptiveAutotuner.
     */
    getPressureSnapshot(): PressureSnapshot;
    /**
     * Background sentinel monitoring loop.
     * Periodically measures site latency and system metrics to update PressureSnapshot.
     */
    startWatchdogLoop(): void;
    stop(): void;
    /**
     * Resolves a valid published chapter ID dynamically to probe the reader.
     * Avoids querying on dead hardcoded chapters.
     */
    private getValidReaderChapterId;
    /**
     * Evaluates all Pre-SLA guard rails and updates PressureSnapshot.
     * Does NOT trigger global stops.
     */
    evaluatePreSlaGuardRails(): Promise<void>;
    private getPercentile;
    /**
     * Probes site route latency using keep-alive connection.
     */
    private probeSiteLatency;
    private recordProbeResult;
    private recordProbeFailure;
    private updatePressureState;
    /**
     * Compatibility method for auto-heal watchdog
     */
    evaluateAutoResume(): Promise<void>;
}
export { ProtectiveSentinel as AdaptivePressureMonitor };
