import { describe, expect, it, vi } from 'vitest';
import { AutoHealWatchdog, type HealthPanelMetrics } from '../src/core/auto-heal-watchdog.js';

const stalledMetrics: HealthPanelMetrics = {
  status: 'CRITICAL_STALL', autoHealState: 'MONITORING', processingHealth: 'CRITICAL_STALL', publicationHealth: 'CRITICAL_STALL',
  lastStartedAgeSec: 2000, lastCompletedAgeSec: 2000, lastFreshVisibleAgeSec: 2000,
  startedLast15m: 0, completedLast15m: 0, freshLast15m: 0, eligibleJobs: 10, claimableWorks: 1,
  activeWorksCount: 1, zombieWorksCount: 0, importingCount: 0, retryCount: 0, stagedUnique: 0,
  publishableStaged: 0, waitingPredecessorStaged: 0, stuckStaged: 0, lastAutoHealAt: null,
  autoRestartCount1h: 0, circuitBreakerOpen: false, protectiveStopActive: false, rssMb: 0, pid: 1,
  timestamp: new Date().toISOString(),
};

describe('soft restart accounting', () => {
  it('restarts a sustained STALLED pipeline at 12m after prior recovery ran', async () => {
    const pool: any = { query: vi.fn(async (sql: string) => {
      if (sql.includes("key = 'importer_auto_restarts'")) return { rows: [{ value: '[]' }] };
      return { rows: [] };
    }) };
    const onControlledRestart = vi.fn().mockResolvedValue(true);
    const watchdog = new AutoHealWatchdog({ pool, onControlledRestart });
    (watchdog as any).lastLevel1At = Date.now() - 60_000;

    await watchdog.executeRecoveryLadder({
      ...stalledMetrics,
      status: 'STALLED',
      processingHealth: 'STALLED',
      publicationHealth: 'STALLED',
      lastCompletedAgeSec: 12 * 60,
      lastFreshVisibleAgeSec: 12 * 60,
    });

    expect(onControlledRestart).toHaveBeenCalledOnce();
    expect(pool.query.mock.calls.some(([sql]: [string]) => sql.includes("VALUES ('importer_auto_restarts'"))).toBe(true);
  });

  it('does not consume restart circuit budget when the engine defers an unsafe restart', async () => {
    const pool: any = { query: vi.fn(async (sql: string) => {
      if (sql.includes("key = 'importer_auto_restarts'")) return { rows: [{ value: '[]' }] };
      return { rows: [] };
    }) };
    const onControlledRestart = vi.fn().mockResolvedValue(false);
    const watchdog = new AutoHealWatchdog({ pool, onControlledRestart });
    (watchdog as any).lastLevel1At = Date.now();
    (watchdog as any).lastLevel2At = Date.now();

    await watchdog.executeRecoveryLadder(stalledMetrics);

    expect(onControlledRestart).toHaveBeenCalledOnce();
    expect(pool.query.mock.calls.some(([sql]: [string]) => sql.includes("VALUES ('importer_auto_restarts'"))).toBe(false);
  });
});
