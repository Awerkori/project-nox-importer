import { describe, it, expect, vi, beforeEach } from 'vitest';
import { RateBucketTracker } from '../src/core/rate-bucket-tracker.js';

describe('RateBucketTracker', () => {
  let mockPool: any;
  let queries: { sql: string; params?: any[] }[];

  beforeEach(() => {
    queries = [];
    mockPool = {
      query: vi.fn(async (sql: string, params?: any[]) => {
        queries.push({ sql, params });
        if (sql.includes('SELECT')) {
          return {
            rows: [
              {
                fresh_5m: 10,
                completed_5m: 15,
                fresh_30m: 45,
                completed_30m: 60,
              },
            ],
            rowCount: 1,
          };
        }
        if (sql.includes('DELETE')) {
          return { rowCount: 5 };
        }
        return { rows: [], rowCount: 1 };
      }),
    };
  });

  it('increments in-memory counters and flushes to database with upsert', async () => {
    const tracker = new RateBucketTracker(mockPool);

    tracker.recordFreshPublication();
    tracker.recordFreshPublication();
    tracker.recordJobCompletion();
    tracker.recordJobCompletion();
    tracker.recordJobCompletion();

    await tracker.flush();

    expect(mockPool.query).toHaveBeenCalledTimes(1);
    const insertQuery = queries[0];
    expect(insertQuery.sql).toContain('INSERT INTO importer_rate_buckets');
    expect(insertQuery.sql).toContain('ON CONFLICT (bucket_minute) DO UPDATE');
    // params: [completed_jobs, fresh_visible]
    expect(insertQuery.params).toEqual([3, 2]);

    // Subsequent flush with 0 pending should not execute query
    await tracker.flush();
    expect(mockPool.query).toHaveBeenCalledTimes(1);
  });

  it('calculates 5m and 30m chapter throughput rates accurately', async () => {
    const tracker = new RateBucketTracker(mockPool);

    const rates = await tracker.getRecentRates(true);

    expect(mockPool.query).toHaveBeenCalledTimes(1);
    // fresh_5m = 10 -> 10 / 5 = 2.0 chapters/min
    expect(rates.rate5m).toBe(2);
    // fresh_30m = 45 -> 45 / 30 = 1.5 chapters/min
    expect(rates.rate30m).toBe(1.5);
    // completed_5m = 15 -> 15 / 5 = 3.0 jobs/min
    expect(rates.completedRate5m).toBe(3);
    // completed_30m = 60 -> 60 / 30 = 2.0 jobs/min
    expect(rates.completedRate30m).toBe(2);
    expect(rates.fresh5m).toBe(10);
    expect(rates.fresh30m).toBe(45);
  });

  it('caches rate metrics for CACHE_TTL duration', async () => {
    const tracker = new RateBucketTracker(mockPool);

    await tracker.getRecentRates();
    await tracker.getRecentRates();
    await tracker.getRecentRates();

    expect(mockPool.query).toHaveBeenCalledTimes(1);
  });

  it('prunes buckets older than 48 hours', async () => {
    const tracker = new RateBucketTracker(mockPool);

    const deleted = await tracker.pruneOldBuckets();

    expect(deleted).toBe(5);
    expect(queries[0].sql).toContain("DELETE FROM importer_rate_buckets WHERE bucket_minute < NOW() - INTERVAL '48 hours'");
  });
});
