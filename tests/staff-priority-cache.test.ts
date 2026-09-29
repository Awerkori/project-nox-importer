import { describe, expect, it, vi } from 'vitest';
import { WorkAffinityScheduler } from '../src/core/scheduler/work-affinity-scheduler.js';

function schedulerWith(query: ReturnType<typeof vi.fn>) {
  return new WorkAffinityScheduler(
    { getConfig: () => ({}) } as any,
    {} as any,
    {} as any,
    { query },
  );
}

describe('staff priority presence cache', () => {
  it('coalesces empty STAFF probes instead of running a claim CTE per slot', async () => {
    const previous = process.env.NODE_ENV;
    process.env.NODE_ENV = 'production';
    try {
      const query = vi.fn(async () => ({ rows: [] }));
      const scheduler: any = schedulerWith(query);

      const values = await Promise.all([
        scheduler.hasStaffForcedCandidate(),
        scheduler.hasStaffForcedCandidate(),
        scheduler.hasStaffForcedCandidate(),
      ]);

      expect(values).toEqual([false, false, false]);
      // One request lookup and one explicit priority>=1000 probe, shared by
      // all concurrent general slots.
      expect(query).toHaveBeenCalledTimes(2);

      await scheduler.hasStaffForcedCandidate();
      expect(query).toHaveBeenCalledTimes(2);
    } finally {
      process.env.NODE_ENV = previous;
    }
  });

  it('keeps an active STAFF request immediately eligible', async () => {
    const previous = process.env.NODE_ENV;
    process.env.NODE_ENV = 'production';
    try {
      const query = vi.fn(async () => ({ rows: [{ work_id: 'staff-work' }] }));
      const scheduler: any = schedulerWith(query);

      await expect(scheduler.hasStaffForcedCandidate()).resolves.toBe(true);
      expect((scheduler as any).cachedStaffWorkIds).toEqual(['staff-work']);
      expect(query).toHaveBeenCalledTimes(1);
    } finally {
      process.env.NODE_ENV = previous;
    }
  });
});
