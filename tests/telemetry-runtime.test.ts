import { afterEach, describe, expect, it, vi } from 'vitest';
import { TelemetryCollector } from '../src/core/telemetry-collector.js';
import { BoundedSamples } from '../src/core/bounded-samples.js';
import { PGlite } from '@electric-sql/pglite';

describe('bounded runtime telemetry', () => {
  afterEach(() => vi.useRealTimers());
  function collector() { return new (TelemetryCollector as any)() as TelemetryCollector; }

  it('persists TTL metadata in the production TEXT settings column and expires it', async () => {
    const db = new PGlite();
    const c = collector();
    try {
      await db.exec(`CREATE TABLE settings (key text PRIMARY KEY, value text);
        CREATE TABLE importer_diagnostic_telemetry (id text PRIMARY KEY,session_id text,data jsonb,created_at timestamptz);
        INSERT INTO settings VALUES ('active_diagnostic_session','old-1700000000000');`);
      c.setPool({ query: (s:string,p?:any[]) => db.query(s,p), options:{max:4} } as any);
      await c.flushTelemetryToDb();
      expect((await db.query('SELECT value FROM settings')).rows).toEqual([{value:'IDLE'}]);
      expect((await db.query('SELECT session_id FROM importer_diagnostic_telemetry')).rows).toEqual([{session_id:'runtime'}]);
      await db.query('UPDATE settings SET value=$1', [`fresh-${Date.now()}`]);
      (c as any).nextSessionCheck = 0;
      await c.flushTelemetryToDb();
      const value = (await db.query<{value:string}>('SELECT value FROM settings')).rows[0].value;
      expect(Date.parse(JSON.parse(value).expires_at)-Date.parse(JSON.parse(value).started_at)).toBe(300000);
      expect(c.getSessionId()).toMatch(/^fresh-/);
    } finally { c.stop(); await db.close(); }
  });

  it('keeps the last bounded population without changing array statistics', () => {
    const samples = new BoundedSamples<number>(3);
    for (let i = 1; i <= 10000; i++) samples.push(i);
    expect([...samples].sort((a,b) => a-b)).toEqual([9998, 9999, 10000]);
    expect(samples.map(x => x * 2)).toHaveLength(3);
  });

  it('counts all ten runners, supports downsizing, and never invents idle samples', async () => {
    vi.useFakeTimers();
    const c = collector();
    try {
      c.configureChapterSlots(10, () => 9);
      for (let i = 0; i < 10; i++) c.setSlotState(i, 'ACTIVE_DOWNLOAD', 'source chapter');
      await vi.advanceTimersByTimeAsync(1000);
      let report = c.getSnapshotReport();
      expect(report.slotsConfigured).toBe(10);
      expect(report.effectiveConcurrency).toBe(9);
      expect(report.activeWorkers.peak).toBe(10);
      expect(report.avgSlotStates.SUM).toBe(10);
      expect(report.avgSlotStates.IDLE).toBe(0);
      c.configureChapterSlots(6);
      expect(c.getSlotProductivitySnapshot()).toMatchObject({configuredSlots: 6, productiveSlots: 6, productiveSlotRatio: 100});
    } finally { c.stop(); }
  });

  it('measures productivity against effective autotuner capacity during a ramp', () => {
    const c = collector();
    try {
      c.configureChapterSlots(5, () => 4);
      for (let i = 0; i < 4; i++) c.setSlotState(i, 'ACTIVE_DOWNLOAD', 'source chapter');
      c.setSlotState(4, 'IDLE');
      expect(c.getSlotProductivitySnapshot()).toMatchObject({
        configuredSlots: 5,
        effectiveSlots: 4,
        productiveSlots: 4,
        productiveSlotRatio: 100,
      });
    } finally { c.stop(); }
  });

  it('bounds every numeric sample even after a long diagnostic and returns to normal automatically', async () => {
    vi.useFakeTimers();
    const c = collector();
    try {
      c.startSession('bounded-test');
      for (let i = 0; i < 150000; i++) { c.recordDbPoolWait(i, 0); c.recordEventLoopLag(i); c.recordTelegramUpload(i, 10); }
      expect(() => c.getSnapshotReport()).not.toThrow();
      expect((c as any).dbPoolWaitSamples.length).toBe(2048);
      await vi.advanceTimersByTimeAsync(301000);
      expect(c.getSnapshotReport().telemetryMode).toBe('NORMAL');
    } finally { c.stop(); }
  });

  it('uses limiter capacity and queues supplied by the runtime', () => {
    const c = collector();
    try {
      c.registerLimiter('test', () => ({configuredCapacity: 12, currentCapacity: 6, active: 3, available: 3, waiters: 2}));
      const report = c.getSnapshotReport() as any;
      expect(report.limitersAudit.test).toMatchObject({configuredCapacity:12,currentCapacity:6,active:3,waiters:2,saturationPercent:50});
    } finally { c.stop(); }
  });

  it('reports page-buffer pressure separately from additive chapter wall time', () => {
    const c = collector();
    try {
      c.recordChapterMetric({
        jobId: 'buffer-job', source: 'source', chapterNumber: 1, pageCount: 2, totalBytes: 200,
        totalDurationMs: 1000, totalSlotOccupancyMs: 1000, claim_acquire_ms: 0, mutex_wait_ms: 0,
        claim_db_ms: 0, metadata_load_ms: 0, source_fetch_ms: 0, page_resolution_ms: 0,
        download_ms: 0, encode_ms: 0, telegram_upload_ms: 0, db_wait_ms: 0, db_publish_ms: 0,
        rate_limit_wait_ms: 0, semaphore_wait_ms: 0, other_wait_ms: 0,
        buffer_reservation_wait_aggregate_ms: 400,
        buffered_page_permit_wait_aggregate_ms: 50,
        buffer_reservation_hold_aggregate_ms: 900,
        ready_queue_dwell_aggregate_ms: 300,
        buffer_reservation_wait_events: 2,
        buffered_page_permit_wait_events: 1,
        timestamp: new Date().toISOString(),
      });
      const pageBuffer = (c.getSnapshotReport() as any).jobProfile.pageBuffer;
      expect(pageBuffer.reservationWaitAggregateMs).toMatchObject({p50: 400, p95: 400});
      expect(pageBuffer.readyQueueDwellAggregateMs).toMatchObject({p50: 300, p95: 300});
      expect(pageBuffer.reservationWaitEvents).toMatchObject({total: 2});
    } finally { c.stop(); }
  });

  it('keeps only bounded parameter-free database query classes', () => {
    const c = collector();
    try {
      c.recordDbQuery(50, 'SELECT chapters');
      c.recordDbQuery(25, 'SELECT chapters');
      c.recordDbQuery(100, 'UPDATE importer_queue');
      const top = (c.getSnapshotReport() as any).database.topQueryClasses;
      expect(top[0]).toMatchObject({fingerprint: 'UPDATE importer_queue', count: 1, totalMs: 100});
      expect(top[1]).toMatchObject({fingerprint: 'SELECT chapters', count: 2, totalMs: 75});
      expect(top[1].p95Ms).toBe(50);
    } finally { c.stop(); }
  });

  it('attributes empty scheduler scans separately from claims', () => {
    const c = collector();
    try {
      c.recordSchedulerAcquireAttempt(120, 'CLAIMED');
      c.recordSchedulerAcquireAttempt(800, 'EMPTY');
      c.recordSchedulerAcquireAttempt(1000, 'EMPTY');
      c.recordSchedulerAcquireAttempt(40, 'ERROR');

      expect((c.getSnapshotReport() as any).schedulerAcquireOutcomes).toMatchObject({
        attempts: 4,
        claimed: 1,
        empty: 2,
        errors: 1,
        emptyPercent: 50,
        claimedMs: { p50: 120 },
        emptyMs: { p50: 1000, p95: 1000 },
        errorMs: { p50: 40 },
      });
    } finally { c.stop(); }
  });

  it('coalesces flushes and expires old persisted sessions across restarts', async () => {
    const c = collector();
    const queries: string[] = [];
    c.setPool({query: vi.fn(async (sql: string) => {
      queries.push(sql);
      return {rows: sql.startsWith('SELECT') ? [{value: 'legacy-1700000000000'}] : []};
    }), options: {max: 4}} as any);
    try {
      await Promise.all([c.flushTelemetryToDb(), c.flushTelemetryToDb()]);
      expect(queries.filter(q => q.startsWith('SELECT'))).toHaveLength(1);
      expect(queries.some(q => q.startsWith('UPDATE settings'))).toBe(true);
      expect(c.getSessionId()).toBe(null);
      await c.flushTelemetryToDb();
      expect(queries.filter(q => q.startsWith('SELECT'))).toHaveLength(1);
    } finally { c.stop(); }
  });
});
