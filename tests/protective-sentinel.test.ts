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
    return Promise.resolve({ rows: [] });
  }),
};

vi.mock('../src/db/yugabyte-direct.js', () => ({
  getYugabytePool: () => mockPool,
}));

import { ProtectiveSentinel } from '../src/core/protective-sentinel.js';
import { diagnostics } from '../src/core/diagnostics.js';

describe('ProtectiveSentinel Targeted Auto-Heal & Discrimination Tests', () => {
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
    });
  });

  it('Case A: Isolated 503 with healthy infra does NOT trigger PROTECTIVE_STOP and resets on 200', async () => {
    const sentinel = new ProtectiveSentinel(mockSupabase as any, undefined, 'https://test-site.workers.dev');

    // Initially active should be false
    expect(await sentinel.isProtectiveStopActive()).toBe(false);

    // 1st probe: 503 transient edge error
    await sentinel.handleProbeResult('home', 'https://test-site.workers.dev/', 85, 250, 350, 503);

    // Must NOT trip protective stop on a single isolated 503
    expect(await sentinel.isProtectiveStopActive()).toBe(false);

    // 2nd probe: 200 OK
    await sentinel.handleProbeResult('home', 'https://test-site.workers.dev/', 95, 250, 350, 200);

    expect(await sentinel.isProtectiveStopActive()).toBe(false);
  });

  it('Case B: Sustained 3x consecutive 503 errors trips PROTECTIVE_STOP with REAL_SYSTEM_PRESSURE', async () => {
    const sentinel = new ProtectiveSentinel(mockSupabase as any, undefined, 'https://test-site.workers.dev');

    // 1st probe: 503 -> no stop
    await sentinel.handleProbeResult('home', 'https://test-site.workers.dev/', 90, 250, 350, 503);
    expect(await sentinel.isProtectiveStopActive()).toBe(false);

    // 2nd probe: 503 -> no stop (infra healthy)
    await sentinel.handleProbeResult('home', 'https://test-site.workers.dev/', 92, 250, 350, 503);
    expect(await sentinel.isProtectiveStopActive()).toBe(false);

    // 3rd probe: 503 -> MUST trip REAL_SYSTEM_PRESSURE
    await sentinel.handleProbeResult('home', 'https://test-site.workers.dev/', 94, 250, 350, 503);
    expect(await sentinel.isProtectiveStopActive()).toBe(true);

    const info = await sentinel.getProtectiveStopInfo(true);
    expect(info.active).toBe(true);
    expect(info.classification).toBe('REAL_SYSTEM_PRESSURE');
    expect(info.reason).toContain('Sustained HTTP 503');
  });

  it('Case C: YSQL pressure trips YSQL_PRESSURE immediately and blocks auto-resume', async () => {
    const sentinel = new ProtectiveSentinel(mockSupabase as any, undefined);

    // Simulate DB connection spike (12 of 13)
    mockDbConns = { total: 12, active: 8 };

    await sentinel.evaluatePreSlaGuardRails();

    expect(await sentinel.isProtectiveStopActive()).toBe(true);
    const info = await sentinel.getProtectiveStopInfo(true);
    expect(info.classification).toBe('YSQL_PRESSURE');
    expect(info.reason).toContain('YSQL Connection Tripwire Exceeded');

    // evaluateAutoResume must refuse to resume while YSQL is elevated
    await sentinel.evaluateAutoResume();
    expect(await sentinel.isProtectiveStopActive()).toBe(true);
  });

  it('Case D: Auto-resume succeeds automatically without human intervention when edge recovers', async () => {
    const sentinel = new ProtectiveSentinel(mockSupabase as any, undefined, 'https://test-site.workers.dev');

    // Start in PROTECTIVE_STOP due to edge incident
    await sentinel.triggerProtectiveStop(
      'Edge transient failure',
      { status: 503 },
      'REAL_SYSTEM_PRESSURE'
    );
    expect(await sentinel.isProtectiveStopActive()).toBe(true);

    // Mock measureRoute returning 200 OK within WAN threshold
    vi.spyOn(sentinel as any, 'measureRoute').mockResolvedValue({
      statusCode: 200,
      ttfbMs: 120,
    });

    // Mock timer so 5s debounce passes instantly
    vi.useFakeTimers();
    const resumePromise = sentinel.evaluateAutoResume();
    await vi.advanceTimersByTimeAsync(5500);
    await resumePromise;
    vi.useRealTimers();

    expect(await sentinel.isProtectiveStopActive()).toBe(false);
    const stopInfo = await sentinel.getProtectiveStopInfo(true);
    expect(stopInfo.active).toBe(false);
    expect(stopInfo.resumed_by).toBe('auto_healing_sentinel_recovery');
  });

  it('Case E: Manual staff stop is NEVER auto-resumed', async () => {
    const sentinel = new ProtectiveSentinel(mockSupabase as any, undefined, 'https://test-site.workers.dev');

    // Manual stop
    await sentinel.triggerProtectiveStop(
      'Manual maintenance by staff',
      {},
      'MANUAL_STOP'
    );
    expect(await sentinel.isProtectiveStopActive()).toBe(true);

    vi.spyOn(sentinel as any, 'measureRoute').mockResolvedValue({
      statusCode: 200,
      ttfbMs: 80,
    });

    vi.useFakeTimers();
    const resumePromise = sentinel.evaluateAutoResume();
    await vi.advanceTimersByTimeAsync(6000);
    await resumePromise;
    vi.useRealTimers();

    // Must still be stopped!
    expect(await sentinel.isProtectiveStopActive()).toBe(true);
  });
});
