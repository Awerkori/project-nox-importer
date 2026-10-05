/**
 * Persistent State Store for Work-Oriented Scheduler.
 *
 * Persists active work sets, watermarks, dynamic configuration, and metrics
 * into YugabyteDB Aeon (`importer_scheduler_state` table).
 * Ensures zero loss of state across container restarts, deploys, or crashes.
 */
import { ActiveWork, SchedulerConfig, SchedulerMetrics, WorkWatermark } from './types.js';
export declare class SchedulerStateStore {
    private logger;
    private pool;
    private activeWorksCache;
    private watermarksCache;
    private p1AdmissionCursorsCache;
    private configCache;
    private metricsCache;
    private isLoaded;
    private saveDebounceTimer;
    private p1CursorSaveDebounceTimer;
    constructor(pool?: any);
    /** Settings refresh is control-plane work and must yield to chapter claims. */
    private isPoolUnderClaimPressure;
    /**
     * Initializes state by creating table if missing and loading existing records.
     */
    initialize(): Promise<void>;
    getConfig(): SchedulerConfig;
    updateConfig(partial: Partial<SchedulerConfig>): Promise<void>;
    getActiveWorks(): ActiveWork[];
    getActiveWork(workId: string): ActiveWork | undefined;
    setActiveWork(work: ActiveWork): void;
    getP1AdmissionCursors(): Record<string, string>;
    setP1AdmissionCursor(source: string, workId: string): void;
    removeActiveWork(workId: string): boolean;
    private scheduleSaveActiveWorks;
    getWatermarkKey(workId: string, source: string): string;
    getWatermark(workId: string, source: string): WorkWatermark | undefined;
    setWatermark(watermark: WorkWatermark): Promise<void>;
    saveMetrics(metrics: SchedulerMetrics): Promise<void>;
    getLatestMetrics(): SchedulerMetrics | null;
    refreshSettingsFromDb(): Promise<void>;
    private persistKey;
}
