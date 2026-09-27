import { afterEach, describe, expect, it, vi } from 'vitest';
import { TelemetryCollector } from '../src/core/telemetry-collector.js';
import { BoundedSamples } from '../src/core/bounded-samples.js';

describe('bounded runtime telemetry', () => {
  afterEach(() => vi.useRealTimers());
  function collector() { return new (TelemetryCollector as any)() as TelemetryCollector; }

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
