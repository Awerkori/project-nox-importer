import type { Pool } from 'pg';
import { Logger } from './logger.js';

export interface RateMetrics {
  rate1m: number;          // Canonical visible publications / min in the rolling 1m window
  rate5m: number;          // Visible published chapters / min (over 5m window) — Operational Target
  rate10m: number;         // Visible published chapters / min (over 10m window)
  rate30m: number;         // Visible published chapters / min (over 30m window)
  visible1m: number;
  visible5m: number;       // Total visible published chapters in last 5m
  visible10m: number;
  visible30m: number;      // Total visible published chapters in last 30m
  freshRate5m: number;     // Genuine fresh releases / min (over 5m window)
  freshRate30m: number;    // Genuine fresh releases / min (over 30m window)
  fresh5m: number;         // Total genuine fresh releases in last 5m
  fresh30m: number;        // Total genuine fresh releases in last 30m
  backfill5m: number;      // Total backfill visible chapters in last 5m
  backfill30m: number;     // Total backfill visible chapters in last 30m
  completedRate5m: number; // Completed chapter jobs / min (over 5m window)
  completedRate30m: number;// Completed chapter jobs / min (over 30m window)
  completed5m: number;     // Total completed chapter jobs in last 5m
  completed30m: number;    // Total completed chapter jobs in last 30m
}

export class RateBucketTracker {
  private logger = new Logger('RateBucketTracker');
  private pool: Pool;
  private pendingVisible = 0;
  private pendingFresh = 0;
  private pendingCompleted = 0;
  private flushTimer: NodeJS.Timeout | null = null;
  private isFlushing = false;
  private cachedRates: RateMetrics | null = null;
  private cachedRatesAt = 0;
  private readonly CACHE_TTL_MS = 10_000;

  constructor(pool: Pool) {
    this.pool = pool;
  }

  /**
   * Records a visible publication on the site.
   * If isFreshRelease is true, it is also credited as a genuine fresh release.
   */
  recordVisiblePublication(isFreshRelease: boolean = false): void {
    this.pendingVisible++;
    if (isFreshRelease) {
      this.pendingFresh++;
    }
  }

  /**
   * Backward-compatible alias for genuine fresh releases.
   */
  recordFreshPublication(): void {
    this.recordVisiblePublication(true);
  }

  recordJobCompletion(): void {
    this.pendingCompleted++;
  }

  startPeriodicFlush(intervalMs = 15_000): void {
    if (this.flushTimer) return;
    this.flushTimer = setInterval(() => {
      void this.flush();
    }, intervalMs);
    this.flushTimer.unref?.();
  }

  stop(): void {
    if (this.flushTimer) {
      clearInterval(this.flushTimer);
      this.flushTimer = null;
    }
  }

  async flush(): Promise<void> {
    if (this.isFlushing) return;
    if (this.pendingVisible === 0 && this.pendingFresh === 0 && this.pendingCompleted === 0) return;

    this.isFlushing = true;
    const toFlushVisible = this.pendingVisible;
    const toFlushFresh = this.pendingFresh;
    const toFlushCompleted = this.pendingCompleted;
    this.pendingVisible = 0;
    this.pendingFresh = 0;
    this.pendingCompleted = 0;

    try {
      await this.pool.query(
        `INSERT INTO importer_rate_buckets (bucket_minute, completed_jobs, fresh_visible, visible_published, updated_at)
         VALUES (date_trunc('minute', NOW()), $1, $2, $3, NOW())
         ON CONFLICT (bucket_minute) DO UPDATE
         SET completed_jobs = importer_rate_buckets.completed_jobs + EXCLUDED.completed_jobs,
             fresh_visible = importer_rate_buckets.fresh_visible + EXCLUDED.fresh_visible,
             visible_published = importer_rate_buckets.visible_published + EXCLUDED.visible_published,
             updated_at = NOW()`,
        [toFlushCompleted, toFlushFresh, toFlushVisible]
      );
    } catch (err: any) {
      // Re-queue pending counts on failure so they are retried next cycle
      this.pendingVisible += toFlushVisible;
      this.pendingFresh += toFlushFresh;
      this.pendingCompleted += toFlushCompleted;
      this.logger.warn('Failed flushing rate buckets to Yugabyte', { error: err?.message });
    } finally {
      this.isFlushing = false;
    }
  }

  async getRecentRates(forceFresh = false): Promise<RateMetrics> {
    const now = Date.now();
    if (!forceFresh && this.cachedRates && now - this.cachedRatesAt < this.CACHE_TTL_MS) {
      return this.cachedRates;
    }

    try {
      // Publication events are the canonical source for visible throughput.
      // The derived minute bucket is intentionally not read for publication
      // counts: concurrent publishers used to contend on its single current
      // row while committing a chapter. Keep bucket reads only for the
      // non-canonical completed-job counters.
      let res;
      try {
        res = await this.pool.query(`
          SELECT
            COALESCE(COUNT(*) FILTER (WHERE e.transition_at >= NOW() - INTERVAL '1 minute'), 0)::int AS visible_1m,
            COALESCE(COUNT(*) FILTER (WHERE e.transition_at >= NOW() - INTERVAL '5 minutes'), 0)::int AS visible_5m,
            COALESCE(COUNT(*) FILTER (WHERE e.transition_at >= NOW() - INTERVAL '10 minutes'), 0)::int AS visible_10m,
            COALESCE(COUNT(*) FILTER (WHERE e.is_fresh_release AND e.transition_at >= NOW() - INTERVAL '5 minutes'), 0)::int AS fresh_5m,
            COALESCE((SELECT SUM(completed_jobs) FROM importer_rate_buckets WHERE bucket_minute >= NOW() - INTERVAL '5 minutes'), 0)::int AS completed_5m,
            COALESCE(COUNT(*) FILTER (WHERE e.transition_at >= NOW() - INTERVAL '30 minutes'), 0)::int AS visible_30m,
            COALESCE(COUNT(*) FILTER (WHERE e.is_fresh_release AND e.transition_at >= NOW() - INTERVAL '30 minutes'), 0)::int AS fresh_30m,
            COALESCE((SELECT SUM(completed_jobs) FROM importer_rate_buckets WHERE bucket_minute >= NOW() - INTERVAL '30 minutes'), 0)::int AS completed_30m
          FROM importer_publication_events e
          WHERE e.bucket_minute >= NOW() - INTERVAL '30 minutes'
        `);
      } catch (eventErr) {
        // Older installations may not have the durable event table yet. Keep
        // the pre-event bucket query as a compatibility fallback; production
        // uses the event-backed path above.
        res = await this.pool.query(`
          SELECT
            COALESCE(SUM(visible_published) FILTER (WHERE bucket_minute >= NOW() - INTERVAL '1 minute'), 0)::int AS visible_1m,
            COALESCE(SUM(visible_published) FILTER (WHERE bucket_minute >= NOW() - INTERVAL '5 minutes'), 0)::int AS visible_5m,
            COALESCE(SUM(visible_published) FILTER (WHERE bucket_minute >= NOW() - INTERVAL '10 minutes'), 0)::int AS visible_10m,
            COALESCE(SUM(fresh_visible) FILTER (WHERE bucket_minute >= NOW() - INTERVAL '5 minutes'), 0)::int AS fresh_5m,
            COALESCE(SUM(completed_jobs) FILTER (WHERE bucket_minute >= NOW() - INTERVAL '5 minutes'), 0)::int AS completed_5m,
            COALESCE(SUM(visible_published) FILTER (WHERE bucket_minute >= NOW() - INTERVAL '30 minutes'), 0)::int AS visible_30m,
            COALESCE(SUM(fresh_visible) FILTER (WHERE bucket_minute >= NOW() - INTERVAL '30 minutes'), 0)::int AS fresh_30m,
            COALESCE(SUM(completed_jobs) FILTER (WHERE bucket_minute >= NOW() - INTERVAL '30 minutes'), 0)::int AS completed_30m
          FROM importer_rate_buckets
          WHERE bucket_minute >= NOW() - INTERVAL '30 minutes'
        `);
      }

      const row = res.rows[0] || {};
      const visible1m = Number(row.visible_1m) || 0;
      const visible5m = Number(row.visible_5m) || 0;
      const visible10m = Number(row.visible_10m) || 0;
      const fresh5m = Number(row.fresh_5m) || 0;
      const completed5m = Number(row.completed_5m) || 0;
      const visible30m = Number(row.visible_30m) || 0;
      const fresh30m = Number(row.fresh_30m) || 0;
      const completed30m = Number(row.completed_30m) || 0;

      // Rate calculations
      const rate1m = visible1m;
      const rate5m = Math.round((visible5m / 5.0) * 10) / 10;
      const rate10m = Math.round((visible10m / 10.0) * 10) / 10;
      const rate30m = Math.round((visible30m / 30.0) * 10) / 10;
      const freshRate5m = Math.round((fresh5m / 5.0) * 10) / 10;
      const freshRate30m = Math.round((fresh30m / 30.0) * 10) / 10;
      const backfill5m = Math.max(0, visible5m - fresh5m);
      const backfill30m = Math.max(0, visible30m - fresh30m);
      const completedRate5m = Math.round((completed5m / 5.0) * 10) / 10;
      const completedRate30m = Math.round((completed30m / 30.0) * 10) / 10;

      const metrics: RateMetrics = {
        rate1m,
        rate5m,
        rate10m,
        rate30m,
        visible1m,
        visible5m,
        visible10m,
        visible30m,
        freshRate5m,
        freshRate30m,
        fresh5m,
        fresh30m,
        backfill5m,
        backfill30m,
        completedRate5m,
        completedRate30m,
        completed5m,
        completed30m,
      };

      this.cachedRates = metrics;
      this.cachedRatesAt = now;
      return metrics;
    } catch (err: any) {
      this.logger.warn('Failed querying recent rates from importer_rate_buckets', { error: err?.message });
      return (
        this.cachedRates || {
          rate1m: 0,
          rate5m: 0,
          rate10m: 0,
          rate30m: 0,
          visible1m: 0,
          visible5m: 0,
          visible10m: 0,
          visible30m: 0,
          freshRate5m: 0,
          freshRate30m: 0,
          fresh5m: 0,
          fresh30m: 0,
          backfill5m: 0,
          backfill30m: 0,
          completedRate5m: 0,
          completedRate30m: 0,
          completed5m: 0,
          completed30m: 0,
        }
      );
    }
  }

  async pruneOldBuckets(): Promise<number> {
    try {
      const res = await this.pool.query(
        "DELETE FROM importer_rate_buckets WHERE bucket_minute < NOW() - INTERVAL '48 hours'"
      );
      return res.rowCount || 0;
    } catch (err: any) {
      this.logger.warn('Failed pruning old rate buckets', { error: err?.message });
      return 0;
    }
  }
}
