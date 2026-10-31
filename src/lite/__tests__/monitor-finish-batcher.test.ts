import { describe, it, expect, vi } from 'vitest';
import { MonitorFinishBatcher } from '../monitor-finish-batcher.js';
import type { LiteTask } from '../types.js';

describe('monitor finish batching', () => {
  it('coalesces observations and returns the fenced result for each task', async () => {
    const finish = vi.fn(async () => new Set(['one']));
    const batcher = new MonitorFinishBatcher(finish);
    const value = (id: string) => ({ task: { id } as LiteTask, workerId: 'worker',
      nextRunAt: new Date(), checkpoint: { lastCheckedAt: '2026-10-31T23:00:00Z' } });
    const results = await Promise.all([batcher.submit(value('one')), batcher.submit(value('two'))]);
    expect(results).toEqual([true, false]);
    expect(finish).toHaveBeenCalledTimes(1);
    expect(finish.mock.calls[0][0]).toHaveLength(2);
  });

  it('propagates database failure to every observation for durable retry', async () => {
    const batcher = new MonitorFinishBatcher(async () => { throw new Error('db unavailable'); });
    const value = (id: string) => ({ task: { id } as LiteTask, workerId: 'worker',
      nextRunAt: new Date(), checkpoint: {} });
    const results = await Promise.allSettled([batcher.submit(value('one')), batcher.submit(value('two'))]);
    expect(results.map((result) => result.status)).toEqual(['rejected', 'rejected']);
  });
});
