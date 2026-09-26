import { Logger } from './logger.js';
export class RateBucketTracker {
    logger = new Logger('RateBucketTracker');
    pool;
    pendingFresh = 0;
    pendingCompleted = 0;
    flushTimer = null;
    isFlushing = false;
    cachedRates = null;
    cachedRatesAt = 0;
    CACHE_TTL_MS = 10_000;
    constructor(pool) {
        this.pool = pool;
    }
    recordFreshPublication() {
        this.pendingFresh++;
    }
    recordJobCompletion() {
        this.pendingCompleted++;
    }
    startPeriodicFlush(intervalMs = 15_000) {
        if (this.flushTimer)
            return;
        this.flushTimer = setInterval(() => {
            void this.flush();
        }, intervalMs);
        this.flushTimer.unref?.();
    }
    stop() {
        if (this.flushTimer) {
            clearInterval(this.flushTimer);
            this.flushTimer = null;
        }
    }
    async flush() {
        if (this.isFlushing)
            return;
        if (this.pendingFresh === 0 && this.pendingCompleted === 0)
            return;
        this.isFlushing = true;
        const toFlushFresh = this.pendingFresh;
        const toFlushCompleted = this.pendingCompleted;
        this.pendingFresh = 0;
        this.pendingCompleted = 0;
        try {
            await this.pool.query(`INSERT INTO importer_rate_buckets (bucket_minute, completed_jobs, fresh_visible, updated_at)
         VALUES (date_trunc('minute', NOW()), $1, $2, NOW())
         ON CONFLICT (bucket_minute) DO UPDATE
         SET completed_jobs = importer_rate_buckets.completed_jobs + EXCLUDED.completed_jobs,
             fresh_visible = importer_rate_buckets.fresh_visible + EXCLUDED.fresh_visible,
             updated_at = NOW()`, [toFlushCompleted, toFlushFresh]);
        }
        catch (err) {
            // Re-queue pending counts on failure so they are retried next cycle
            this.pendingFresh += toFlushFresh;
            this.pendingCompleted += toFlushCompleted;
            this.logger.warn('Failed flushing rate buckets to Yugabyte', { error: err?.message });
        }
        finally {
            this.isFlushing = false;
        }
    }
    async getRecentRates(forceFresh = false) {
        const now = Date.now();
        if (!forceFresh && this.cachedRates && now - this.cachedRatesAt < this.CACHE_TTL_MS) {
            return this.cachedRates;
        }
        try {
            const res = await this.pool.query(`
        SELECT
          COALESCE(SUM(fresh_visible) FILTER (WHERE bucket_minute >= NOW() - INTERVAL '5 minutes'), 0)::int AS fresh_5m,
          COALESCE(SUM(completed_jobs) FILTER (WHERE bucket_minute >= NOW() - INTERVAL '5 minutes'), 0)::int AS completed_5m,
          COALESCE(SUM(fresh_visible) FILTER (WHERE bucket_minute >= NOW() - INTERVAL '30 minutes'), 0)::int AS fresh_30m,
          COALESCE(SUM(completed_jobs) FILTER (WHERE bucket_minute >= NOW() - INTERVAL '30 minutes'), 0)::int AS completed_30m
        FROM importer_rate_buckets
        WHERE bucket_minute >= NOW() - INTERVAL '30 minutes'
      `);
            const row = res.rows[0] || {};
            const fresh5m = Number(row.fresh_5m) || 0;
            const completed5m = Number(row.completed_5m) || 0;
            const fresh30m = Number(row.fresh_30m) || 0;
            const completed30m = Number(row.completed_30m) || 0;
            const rate5m = Math.round((fresh5m / 5.0) * 10) / 10;
            const rate30m = Math.round((fresh30m / 30.0) * 10) / 10;
            const completedRate5m = Math.round((completed5m / 5.0) * 10) / 10;
            const completedRate30m = Math.round((completed30m / 30.0) * 10) / 10;
            const metrics = {
                rate5m,
                rate30m,
                fresh5m,
                fresh30m,
                completedRate5m,
                completedRate30m,
                completed5m,
                completed30m,
            };
            this.cachedRates = metrics;
            this.cachedRatesAt = now;
            return metrics;
        }
        catch (err) {
            this.logger.warn('Failed querying recent rates from importer_rate_buckets', { error: err?.message });
            return (this.cachedRates || {
                rate5m: 0,
                rate30m: 0,
                fresh5m: 0,
                fresh30m: 0,
                completedRate5m: 0,
                completedRate30m: 0,
                completed5m: 0,
                completed30m: 0,
            });
        }
    }
    async pruneOldBuckets() {
        try {
            const res = await this.pool.query("DELETE FROM importer_rate_buckets WHERE bucket_minute < NOW() - INTERVAL '48 hours'");
            return res.rowCount || 0;
        }
        catch (err) {
            this.logger.warn('Failed pruning old rate buckets', { error: err?.message });
            return 0;
        }
    }
}
