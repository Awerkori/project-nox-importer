/**
 * Persistent State Store for Work-Oriented Scheduler.
 * 
 * Persists active work sets, watermarks, dynamic configuration, and metrics
 * into YugabyteDB Aeon (`importer_scheduler_state` table).
 * Ensures zero loss of state across container restarts, deploys, or crashes.
 */

import { getYugabytePool } from '../../db/yugabyte-direct.js';
import { Logger } from '../logger.js';
import {
  ActiveWork,
  SchedulerConfig,
  SchedulerMetrics,
  WorkWatermark,
} from './types.js';

export class SchedulerStateStore {
  private logger = new Logger('SchedulerStateStore');
  private pool = getYugabytePool();

  private activeWorksCache: Map<string, ActiveWork> = new Map();
  private watermarksCache: Map<string, WorkWatermark> = new Map();
  private configCache: SchedulerConfig = {
    enabled: true,
    shadowMode: false,
    maxActiveNewWorks: 8,
    maxActiveBackfillWorks: 10,
    maxInflightPerWork: 2,
    slidingWindowSize: 8,
    slidingWindowMin: 3,
    antiStarvationRatio: 4,
  };
  private metricsCache: SchedulerMetrics | null = null;
  private isLoaded = false;
  private saveDebounceTimer: NodeJS.Timeout | null = null;

  constructor() {}

  /**
   * Initializes state by creating table if missing and loading existing records.
   */
  async initialize(): Promise<void> {
    try {
      await this.pool.query(`
        CREATE TABLE IF NOT EXISTS importer_scheduler_state (
          key text PRIMARY KEY,
          value jsonb NOT NULL,
          updated_at timestamptz NOT NULL DEFAULT now()
        );
      `);

      // Load config
      const cfgRes = await this.pool.query(
        `SELECT value FROM importer_scheduler_state WHERE key = 'config'`
      );
      if (cfgRes.rows.length > 0) {
        const stored = cfgRes.rows[0].value;
        this.configCache = { ...this.configCache, ...stored };
      }

      // Check public.settings table overrides
      const settingsRes = await this.pool.query(
        `SELECT key, value FROM settings WHERE key IN ('work_affinity_scheduler_enabled', 'work_affinity_scheduler_shadow')`
      );
      for (const r of settingsRes.rows) {
        if (r.key === 'work_affinity_scheduler_enabled') {
          this.configCache.enabled = r.value === 'true';
        }
        if (r.key === 'work_affinity_scheduler_shadow') {
          this.configCache.shadowMode = r.value === 'true';
        }
      }

      // Check environment variables overrides
      if (process.env.WORK_AFFINITY_SCHEDULER_ENABLED !== undefined) {
        this.configCache.enabled = process.env.WORK_AFFINITY_SCHEDULER_ENABLED === 'true';
      }
      if (process.env.SCHEDULER_SHADOW_MODE !== undefined) {
        this.configCache.shadowMode = process.env.SCHEDULER_SHADOW_MODE === 'true';
      }
      if (process.env.MAX_ACTIVE_NEW_WORKS) {
        this.configCache.maxActiveNewWorks = parseInt(process.env.MAX_ACTIVE_NEW_WORKS, 10) || 8;
      }
      if (process.env.MAX_ACTIVE_BACKFILL_WORKS) {
        this.configCache.maxActiveBackfillWorks = parseInt(process.env.MAX_ACTIVE_BACKFILL_WORKS, 10) || 10;
      }
      if (process.env.MAX_INFLIGHT_PER_WORK) {
        this.configCache.maxInflightPerWork = parseInt(process.env.MAX_INFLIGHT_PER_WORK, 10) || 2;
      }

      // Start periodic settings refresh loop
      setInterval(async () => {
        try {
          await this.refreshSettingsFromDb();
        } catch {}
      }, 10000);

      // Load active works
      const worksRes = await this.pool.query(
        `SELECT value FROM importer_scheduler_state WHERE key = 'active_works'`
      );
      if (worksRes.rows.length > 0 && Array.isArray(worksRes.rows[0].value)) {
        for (const item of worksRes.rows[0].value) {
          if (item?.workId) {
            this.activeWorksCache.set(item.workId, item);
          }
        }
      }

      // Load watermarks
      const wmRes = await this.pool.query(
        `SELECT value FROM importer_scheduler_state WHERE key = 'watermarks'`
      );
      if (wmRes.rows.length > 0 && typeof wmRes.rows[0].value === 'object') {
        const wmObj = wmRes.rows[0].value;
        for (const k of Object.keys(wmObj)) {
          this.watermarksCache.set(k, wmObj[k]);
        }
      }

      this.isLoaded = true;
      this.logger.info('SchedulerStateStore initialized successfully', {
        activeWorks: this.activeWorksCache.size,
        watermarks: this.watermarksCache.size,
        enabled: this.configCache.enabled,
        shadowMode: this.configCache.shadowMode,
      });
    } catch (err: any) {
      this.logger.error('Failed to initialize SchedulerStateStore from DB', { error: err?.message });
      // Keep memory fallback
      this.isLoaded = true;
    }
  }

  // --- Configuration ---

  getConfig(): SchedulerConfig {
    return { ...this.configCache };
  }

  async updateConfig(partial: Partial<SchedulerConfig>): Promise<void> {
    this.configCache = { ...this.configCache, ...partial };
    await this.persistKey('config', this.configCache);
  }

  // --- Active Works ---

  getActiveWorks(): ActiveWork[] {
    return Array.from(this.activeWorksCache.values());
  }

  getActiveWork(workId: string): ActiveWork | undefined {
    return this.activeWorksCache.get(workId);
  }

  setActiveWork(work: ActiveWork): void {
    this.activeWorksCache.set(work.workId, work);
    this.scheduleSaveActiveWorks();
  }

  removeActiveWork(workId: string): boolean {
    const deleted = this.activeWorksCache.delete(workId);
    if (deleted) {
      this.scheduleSaveActiveWorks();
    }
    return deleted;
  }

  private scheduleSaveActiveWorks(): void {
    if (this.saveDebounceTimer) return;
    this.saveDebounceTimer = setTimeout(async () => {
      this.saveDebounceTimer = null;
      const list = Array.from(this.activeWorksCache.values());
      await this.persistKey('active_works', list);
    }, 1000);
  }

  // --- Watermarks ---

  getWatermarkKey(workId: string, source: string): string {
    return `${source}:${workId}`;
  }

  getWatermark(workId: string, source: string): WorkWatermark | undefined {
    return this.watermarksCache.get(this.getWatermarkKey(workId, source));
  }

  async setWatermark(watermark: WorkWatermark): Promise<void> {
    const key = this.getWatermarkKey(watermark.workId, watermark.source);
    this.watermarksCache.set(key, watermark);

    // Persist to DB in background
    const wmObj: Record<string, WorkWatermark> = {};
    for (const [k, v] of this.watermarksCache.entries()) {
      wmObj[k] = v;
    }
    await this.persistKey('watermarks', wmObj);
  }

  // --- Metrics ---

  async saveMetrics(metrics: SchedulerMetrics): Promise<void> {
    this.metricsCache = metrics;
    await this.persistKey('metrics', metrics);
  }

  getLatestMetrics(): SchedulerMetrics | null {
    return this.metricsCache;
  }

  // --- Dynamic Settings Refresh ---

  async refreshSettingsFromDb(): Promise<void> {
    try {
      const res = await this.pool.query(
        `SELECT key, value FROM settings WHERE key IN ('work_affinity_scheduler_enabled', 'work_affinity_scheduler_shadow')`
      );
      for (const r of res.rows) {
        if (r.key === 'work_affinity_scheduler_enabled') {
          const val = r.value === 'true';
          if (this.configCache.enabled !== val) {
            this.logger.info(`Dynamic settings update: work_affinity_scheduler_enabled -> ${val}`);
            this.configCache.enabled = val;
          }
        }
        if (r.key === 'work_affinity_scheduler_shadow') {
          const val = r.value === 'true';
          if (this.configCache.shadowMode !== val) {
            this.logger.info(`Dynamic settings update: work_affinity_scheduler_shadow -> ${val}`);
            this.configCache.shadowMode = val;
          }
        }
      }
    } catch {}
  }

  // --- Persistence Helper ---

  private async persistKey(key: string, value: any): Promise<void> {
    try {
      await this.pool.query(
        `INSERT INTO importer_scheduler_state (key, value, updated_at)
         VALUES ($1, $2::jsonb, NOW())
         ON CONFLICT (key) DO UPDATE SET
           value = EXCLUDED.value,
           updated_at = NOW()`,
        [key, JSON.stringify(value)]
      );
    } catch (err: any) {
      this.logger.warn(`Failed to persist scheduler state key '${key}'`, { error: err?.message });
    }
  }
}
