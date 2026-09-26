import { describe, it, expect, vi, beforeEach } from 'vitest';

let mockSettings: Record<string, any> = {};
let mockDbConns = { total: 4, active: 1 };

const mockPool = {
  query: vi.fn().mockImplementation((sql: string, params?: any[]) => {
    if (sql.includes('SELECT value FROM settings')) {
      const val = mockSettings['importer_protective_stop'];
      return Promise.resolve({ rows: val ? [{ value: val }] : [] });
    }
    if (sql.includes('INSERT INTO settings')) {
      mockSettings['importer_protective_stop'] = params?.[0];
      return Promise.resolve({ rowCount: 1 });
    }
    if (sql.includes('pg_stat_activity')) {
      return Promise.resolve({
        rows: [{ total: String(mockDbConns.total), active: String(mockDbConns.active) }],
      });
    }
    if (sql.includes('SELECT id FROM chapters')) {
      return Promise.resolve({ rows: [{ id: 'test-chapter-uuid-1' }] });
    }
    return Promise.resolve({ rows: [] });
  }),
};

vi.mock('../src/db/yugabyte-direct.js', () => ({
  getYugabytePool: () => mockPool,
}));

import { ProtectiveSentinel } from '../src/core/protective-sentinel.js';
import { diagnostics } from '../src/core/diagnostics.js';

describe('ProtectiveSentinel Always-On Adaptive Capacity Tests', () => {
  const mockSupabase = {
    from: vi.fn().mockImplementation(() => ({
      select: vi.fn().mockImplementation(() => ({
        eq: vi.fn().mockImplementation((_col: string, val: string) => ({
          maybeSingle: vi.fn().mockResolvedValue({
            data: mockSettings[val] ? { value: mockSettings[val] } : null,
            error: null,
          }),
        })),
      })),
      upsert: vi.fn().mockImplementation((row: any) => {
        mockSettings[row.key] = typeof row.value === 'string' ? row.value : JSON.stringify(row.value);
        return Promise.resolve({ error: null });
      }),
    })),
    rpc: vi.fn().mockImplementation(() => Promise.resolve({ data: mockDbConns.total, error: null })),
  };

  beforeEach(() => {
    mockSettings = {};
    mockDbConns = { total: 4, active: 1 };
    vi.spyOn(diagnostics, 'getMemorySnapshot').mockReturnValue({
      rssMb: 240,
      heapUsedMb: 120,
      heapTotalMb: 180,
      externalMb: 10,
      arrayBuffersMb: 0,
    });
  });

  it('Requirement 1 & 2: Automatic 5xx errors NEVER trigger global protective stop', async () => {
    const sentinel = new ProtectiveSentinel(mockSupabase as any, undefined, 'https://test-site.workers.dev');

    // 1st, 2nd, 3rd probe 503
    (sentinel as any).recordProbeResult('home', 90, 503);
    (sentinel as any).recordProbeResult('home', 92, 503);
    (sentinel as any).recordProbeResult('home', 94, 503);

    // isProtectiveStopActive MUST remain FALSE (no global stop on 5xx)
    expect(await sentinel.isProtectiveStopActive()).toBe(false);

    // But pressure snapshot reports pressure to the autotuner
    await sentinel.evaluatePreSlaGuardRails();
    const snap = sentinel.getPressureSnapshot();
    expect(snap.siteHealth).toBe('RED');
    expect(snap.pressureScore).toBeGreaterThanOrEqual(60);
    expect(snap.pressureReason).toContain('Sustained HTTP 5xx');
  });

  it('Requirement 1 & 2: YSQL connection spikes NEVER trigger global protective stop', async () => {
    const sentinel = new ProtectiveSentinel(mockSupabase as any, undefined);

    mockDbConns = { total: 12, active: 8 };
    await sentinel.evaluatePreSlaGuardRails();

    // Must NEVER trigger global stop
    expect(await sentinel.isProtectiveStopActive()).toBe(false);

    // But pressure snapshot reports DB pressure to the autotuner
    const snap = sentinel.getPressureSnapshot();
    expect(snap.pressureBreakdown.dbPressure).toBe(30);
    expect(snap.pressureReason).toContain('Elevated YSQL load');
  });

  it('Requirement 2: Legacy automatic protective stop in database is auto-cleared', async () => {
    const sentinel = new ProtectiveSentinel(mockSupabase as any, undefined);

    // Simulate an old automatic stop from 6 hours ago in DB
    mockSettings['importer_protective_stop'] = JSON.stringify({
      active: true,
      reason: 'Sustained HTTP 503 on READER',
      classification: 'REAL_SYSTEM_PRESSURE',
      triggered_at: '2026-09-26T01:02:32.400Z',
    });

    // On check, must ignore legacy stop and auto-clear it
    const isActive = await sentinel.isProtectiveStopActive();
    expect(isActive).toBe(false);

    // On startup check, explicitly clears
    await sentinel.clearLegacyProtectiveStopOnStartup();
    const info = await sentinel.getProtectiveStopInfo(true);
    expect(info.active).toBe(false);
  });

  it('Requirement 2: Manual staff stop IS preserved and active', async () => {
    const sentinel = new ProtectiveSentinel(mockSupabase as any, undefined);

    await sentinel.triggerProtectiveStop('Manual maintenance by staff', {}, 'MANUAL_STOP');

    expect(await sentinel.isProtectiveStopActive()).toBe(true);
    const info = await sentinel.getProtectiveStopInfo(true);
    expect(info.active).toBe(true);
    expect(info.classification).toBe('MANUAL_STOP');

    // Resuming manual stop
    await sentinel.resumeProtectiveStop('staff_user');
    expect(await sentinel.isProtectiveStopActive()).toBe(false);
  });

  it('Requirement 63: CI Anti-regression — Automatic performance stops cannot be triggered', async () => {
    const sentinel = new ProtectiveSentinel(mockSupabase as any, undefined);

    // Attempting to trigger with non-MANUAL_STOP classification must be blocked
    await sentinel.triggerProtectiveStop('High RAM', {}, 'IMPORTER_PRESSURE');
    expect(await sentinel.isProtectiveStopActive()).toBe(false);

    await sentinel.triggerProtectiveStop('High latency', {}, 'REAL_SYSTEM_PRESSURE');
    expect(await sentinel.isProtectiveStopActive()).toBe(false);

    await sentinel.triggerProtectiveStop('DB tripwire', {}, 'YSQL_PRESSURE');
    expect(await sentinel.isProtectiveStopActive()).toBe(false);
  });
});
