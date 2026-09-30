import { describe, expect, it } from 'vitest';
import { HostRateLimiter } from '../src/core/rate-limiter.js';
import { ExistingWorksReconciler } from '../src/core/reconciliation.js';

describe('long-lived runtime cache bounds', () => {
  it('evicts old dynamic media hosts instead of retaining every hostname forever', () => {
    const limiter = new HostRateLimiter();
    for (let i = 0; i < 300; i++) limiter.setHostRate(`cdn-${i}.example`, 1);
    expect((limiter as any).buckets.size).toBeLessThanOrEqual(256);
  });

  it('bounds reconciliation de-duplication memory while preserving the active window', () => {
    const reconciler = new ExistingWorksReconciler({} as any, {} as any, {} as any);
    const now = Date.now();
    for (let i = 0; i < 4200; i++) {
      (reconciler as any).rememberReconciliation(`work-${i}`, now);
    }
    const cache = (reconciler as any).lastReconciliationAt as Map<string, number>;
    expect(cache.size).toBeLessThanOrEqual(4096);
    expect(cache.has('work-4199')).toBe(true);
  });
});
