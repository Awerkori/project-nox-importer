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
    private configCache;
    private metricsCache;
    private isLoaded;
    private saveDebounceTimer;
    constructor();
    /**
     * Initializes state by creating table if missing and loading existing records.
     */
    initialize(): Promise<void>;
    getConfig(): SchedulerConfig;
    updateConfig(partial: Partial<SchedulerConfig>): Promise<void>;
    getActiveWorks(): ActiveWork[];
    getActiveWork(workId: string): ActiveWork | undefined;
    setActiveWork(work: ActiveWork): void;
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
