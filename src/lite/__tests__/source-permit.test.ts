import { describe, expect, it, vi } from 'vitest';
import type { Pool } from 'pg';
import { AsyncSemaphore } from '../../core/concurrency.js';
import { LiteExecutor } from '../executor.js';
import type { LiteRepository } from '../repository.js';
import type { SourceRegistry } from '../../sources/registry.js';
import type { StorageProvider } from '../../storage/provider.js';
import type { LiteTask } from '../types.js';

describe('LiteExecutor source permit acquisition & fairness', () => {
  function makeExecutor(yieldMock: any = vi.fn().mockResolvedValue(true)) {
    const queue = {
      recordUploadReceipts: vi.fn().mockResolvedValue(undefined),
      recordUploadReservations: vi.fn().mockResolvedValue(undefined),
      yieldForSourceFairness: yieldMock,
    } as unknown as LiteRepository;

    const downloads = new AsyncSemaphore(10, 'test_downloads');
    const executor = new LiteExecutor(
      {} as Pool,
      queue,
      {} as SourceRegistry,
      {} as StorageProvider,
      '00000000-0000-0000-0000-000000000000',
      downloads,
    );
    return { executor, queue, yieldMock };
  }

  it('acquires permit immediately when sourceSlot has capacity', async () => {
    const { executor } = makeExecutor();
    const task: LiteTask = {
      taskId: 'task-1',
      priority: 'P1',
      stage: 'CHAPTER_IMPORT',
      state: 'RUNNING',
      workId: 'work-1',
      attempts: 1,
      checkpoint: {},
    } as any;

    const permit = await (executor as any).acquireSourcePermit(task, 'worker-1', 'mangaflix');
    expect(permit).not.toBeNull();
    expect(permit.sourceSlot).toBeDefined();
    expect(permit.waitMs).toBeGreaterThanOrEqual(0);

    // Release permit
    permit.sourceSlot.release();
  });

  it('releases prioritySourceWaitGate exactly once on timeout without throwing double-release error', async () => {
    const { executor, yieldMock } = makeExecutor();
    // Simulate fair demand with multiple sources so hasFairAlternative is true
    executor.setFairSourceDemandCounts(new Map([['mangaflix', 5], ['vulcannovel', 5]]));

    const sourceSlot = (executor as any).sourceSlot('mangaflix');
    // Saturate sourceSlot completely
    while (sourceSlot.available > 0) {
      sourceSlot.tryAcquire();
    }

    const priorityGate: AsyncSemaphore = (executor as any).prioritySourceWaitGate;
    expect(priorityGate.available).toBe(1);

    const p0Task: LiteTask = {
      taskId: 'task-p0',
      priority: 'P0',
      stage: 'CHAPTER_IMPORT',
      state: 'RUNNING',
      workId: 'work-p0',
      attempts: 1,
      checkpoint: {},
    } as any;

    // Spy on sourceSlot.acquire to simulate timeout
    vi.spyOn(sourceSlot, 'acquire').mockRejectedValueOnce(new Error('Timeout'));

    // Should NOT throw "Semaphore released without an active permit"
    const result = await (executor as any).acquireSourcePermit(p0Task, 'worker-1', 'mangaflix');

    expect(result).toBeNull();
    expect(yieldMock).toHaveBeenCalledWith(p0Task, 'worker-1', 1_000);
    // Gate must be released back to 1 available
    expect(priorityGate.available).toBe(1);

    // Verify it doesn't throw if we call tryAcquire and release directly
    expect(priorityGate.tryAcquire()).toBe(true);
    priorityGate.release();
  });

  it('yields for fairness for non-priority tasks when source is saturated and alternatives exist', async () => {
    const { executor, yieldMock } = makeExecutor();
    executor.setFairSourceDemandCounts(new Map([['mangaflix', 5], ['vulcannovel', 5]]));

    const sourceSlot = (executor as any).sourceSlot('mangaflix');
    while (sourceSlot.available > 0) {
      sourceSlot.tryAcquire();
    }

    const p1Task: LiteTask = {
      taskId: 'task-p1',
      priority: 'P1',
      stage: 'CHAPTER_IMPORT',
      state: 'RUNNING',
      workId: 'work-p1',
      attempts: 1,
      checkpoint: {},
    } as any;

    vi.spyOn(sourceSlot, 'acquire').mockRejectedValueOnce(new Error('Timeout'));

    const result = await (executor as any).acquireSourcePermit(p1Task, 'worker-1', 'mangaflix');
    expect(result).toBeNull();
    expect(yieldMock).toHaveBeenCalledWith(p1Task, 'worker-1', 3_000);
  });
});
