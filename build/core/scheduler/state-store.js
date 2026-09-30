/**
 * Persistent State Store for Work-Oriented Scheduler.
 *
 * Persists active work sets, watermarks, dynamic configuration, and metrics
 * into YugabyteDB Aeon (`importer_scheduler_state` table).
 * Ensures zero loss of state across container restarts, deploys, or crashes.
 */
import { getYugabytePool } from '../../db/yugabyte-direct.js';
import { Logger } from '../logger.js';
import { maintenanceScheduler } from '../maintenance-scheduler.js';
export class SchedulerStateStore {
    logger = new Logger('SchedulerStateStore');
    pool;
    activeWorksCache = new Map();
    watermarksCache = new Map();
    // A small durable cursor lets P1 admission rotate through works of the
    // same source.  It is scheduler control-plane state, not editorial data.
    p1AdmissionCursorsCache = new Map();
    configCache = {
        enabled: true,
        shadowMode: false,
        maxActiveNewWorks: 8,
        maxActiveBackfillWorks: 24,
        maxInflightPerWork: 2,
        slidingWindowSize: 12,
        slidingWindowMin: 4,
        antiStarvationRatio: 4,
    };
    metricsCache = null;
    isLoaded = false;
    saveDebounceTimer = null;
    p1CursorSaveDebounceTimer = null;
    constructor(pool) {
        // State is replaced by a local double in scheduler unit tests. Avoid
        // resolving production credentials merely to create that empty instance.
        this.pool = pool || (process.env.NODE_ENV === 'test'
            ? { query: async () => ({ rows: [] }) }
            : getYugabytePool());
    }
    /**
     * Initializes state by creating table if missing and loading existing records.
     */
    async initialize() {
        try {
            await this.pool.query(`
        CREATE TABLE IF NOT EXISTS importer_scheduler_state (
          key text PRIMARY KEY,
          value jsonb NOT NULL,
          updated_at timestamptz NOT NULL DEFAULT now()
        );
      `);
            // Load config
            const cfgRes = await this.pool.query(`SELECT value FROM importer_scheduler_state WHERE key = 'config'`);
            if (cfgRes.rows.length > 0) {
                const stored = cfgRes.rows[0].value;
                this.configCache = { ...this.configCache, ...stored };
            }
            // Check public.settings table overrides
            const settingsRes = await this.pool.query(`SELECT key, value FROM settings WHERE key IN ('work_affinity_scheduler_enabled', 'work_affinity_scheduler_shadow')`);
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
            maintenanceScheduler.register('scheduler-settings', 10000, 7000, async () => {
                try {
                    await this.refreshSettingsFromDb();
                }
                catch { }
            });
            // Load active works (importer_scheduler_state with fallback to settings)
            let rawWorks = [];
            const worksRes = await this.pool.query(`SELECT value FROM importer_scheduler_state WHERE key = 'active_works'`);
            if (worksRes.rows.length > 0 && Array.isArray(worksRes.rows[0].value) && worksRes.rows[0].value.length > 0) {
                rawWorks = worksRes.rows[0].value;
            }
            else {
                try {
                    const settRes = await this.pool.query(`SELECT value FROM settings WHERE key = 'active_works'`);
                    if (settRes.rows.length > 0) {
                        const v = settRes.rows[0].value;
                        rawWorks = typeof v === 'string' ? JSON.parse(v) : v;
                    }
                }
                catch { }
            }
            if (Array.isArray(rawWorks)) {
                for (const item of rawWorks) {
                    if (item?.workId) {
                        this.activeWorksCache.set(item.workId, item);
                    }
                }
            }
            // Load watermarks
            const wmRes = await this.pool.query(`SELECT value FROM importer_scheduler_state WHERE key = 'watermarks'`);
            if (wmRes.rows.length > 0 && typeof wmRes.rows[0].value === 'object') {
                const wmObj = wmRes.rows[0].value;
                for (const k of Object.keys(wmObj)) {
                    this.watermarksCache.set(k, wmObj[k]);
                }
            }
            const p1CursorRes = await this.pool.query(`SELECT value FROM importer_scheduler_state WHERE key = 'p1_admission_cursors'`);
            if (p1CursorRes.rows.length > 0 && p1CursorRes.rows[0].value && typeof p1CursorRes.rows[0].value === 'object') {
                for (const [source, workId] of Object.entries(p1CursorRes.rows[0].value)) {
                    if (typeof workId === 'string' && workId)
                        this.p1AdmissionCursorsCache.set(source, workId);
                }
            }
            this.isLoaded = true;
            this.logger.info('SchedulerStateStore initialized successfully', {
                activeWorks: this.activeWorksCache.size,
                watermarks: this.watermarksCache.size,
                enabled: this.configCache.enabled,
                shadowMode: this.configCache.shadowMode,
            });
        }
        catch (err) {
            this.logger.error('Failed to initialize SchedulerStateStore from DB', { error: err?.message });
            // Keep memory fallback
            this.isLoaded = true;
        }
    }
    // --- Configuration ---
    getConfig() {
        return { ...this.configCache };
    }
    async updateConfig(partial) {
        this.configCache = { ...this.configCache, ...partial };
        await this.persistKey('config', this.configCache);
    }
    // --- Active Works ---
    getActiveWorks() {
        return Array.from(this.activeWorksCache.values());
    }
    getActiveWork(workId) {
        return this.activeWorksCache.get(workId);
    }
    setActiveWork(work) {
        this.activeWorksCache.set(work.workId, work);
        this.scheduleSaveActiveWorks();
    }
    // --- P1 admission fairness cursors ---
    getP1AdmissionCursors() {
        return Object.fromEntries(this.p1AdmissionCursorsCache);
    }
    setP1AdmissionCursor(source, workId) {
        if (!source || !workId || this.p1AdmissionCursorsCache.get(source) === workId)
            return;
        this.p1AdmissionCursorsCache.set(source, workId);
        // Sources are a bounded configuration set. Retain a hard ceiling anyway
        // so a malformed source cannot turn this tiny cursor into an uptime leak.
        while (this.p1AdmissionCursorsCache.size > 128) {
            const oldest = this.p1AdmissionCursorsCache.keys().next().value;
            if (!oldest)
                break;
            this.p1AdmissionCursorsCache.delete(oldest);
        }
        if (this.p1CursorSaveDebounceTimer)
            return;
        this.p1CursorSaveDebounceTimer = setTimeout(() => {
            this.p1CursorSaveDebounceTimer = null;
            void this.persistKey('p1_admission_cursors', this.getP1AdmissionCursors());
        }, 1000);
    }
    removeActiveWork(workId) {
        const deleted = this.activeWorksCache.delete(workId);
        if (deleted) {
            this.scheduleSaveActiveWorks();
        }
        return deleted;
    }
    scheduleSaveActiveWorks() {
        if (this.saveDebounceTimer)
            return;
        this.saveDebounceTimer = setTimeout(async () => {
            this.saveDebounceTimer = null;
            const list = Array.from(this.activeWorksCache.values());
            await this.persistKey('active_works', list);
        }, 1000);
    }
    // --- Watermarks ---
    getWatermarkKey(workId, source) {
        return `${source}:${workId}`;
    }
    getWatermark(workId, source) {
        return this.watermarksCache.get(this.getWatermarkKey(workId, source));
    }
    async setWatermark(watermark) {
        const key = this.getWatermarkKey(watermark.workId, watermark.source);
        this.watermarksCache.set(key, watermark);
        // Persist to DB in background
        const wmObj = {};
        for (const [k, v] of this.watermarksCache.entries()) {
            wmObj[k] = v;
        }
        await this.persistKey('watermarks', wmObj);
    }
    // --- Metrics ---
    async saveMetrics(metrics) {
        this.metricsCache = metrics;
        await this.persistKey('metrics', metrics);
    }
    getLatestMetrics() {
        return this.metricsCache;
    }
    // --- Dynamic Settings Refresh ---
    async refreshSettingsFromDb() {
        try {
            const cfgRes = await this.pool.query(`SELECT value FROM importer_scheduler_state WHERE key = 'config'`);
            if (cfgRes.rows.length > 0 && typeof cfgRes.rows[0].value === 'object') {
                this.configCache = { ...this.configCache, ...cfgRes.rows[0].value };
            }
            const res = await this.pool.query(`SELECT key, value FROM settings WHERE key IN ('work_affinity_scheduler_enabled', 'work_affinity_scheduler_shadow')`);
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
        }
        catch { }
    }
    // --- Persistence Helper ---
    async persistKey(key, value) {
        try {
            await this.pool.query(`INSERT INTO importer_scheduler_state (key, value, updated_at)
         VALUES ($1, $2::jsonb, NOW())
         ON CONFLICT (key) DO UPDATE SET
           value = EXCLUDED.value,
           updated_at = NOW()`, [key, JSON.stringify(value)]);
            if (key === 'active_works') {
                try {
                    await this.pool.query(`INSERT INTO settings (key, value, updated_at)
             VALUES ('active_works', $1, NOW())
             ON CONFLICT (key) DO UPDATE SET
               value = EXCLUDED.value,
               updated_at = NOW()`, [JSON.stringify(value)]);
                }
                catch { }
            }
        }
        catch (err) {
            this.logger.warn(`Failed to persist scheduler state key '${key}'`, { error: err?.message });
        }
    }
}
