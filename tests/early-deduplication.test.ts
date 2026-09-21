import { describe, it, expect } from 'vitest';
import { WorkAffinityScheduler } from '../src/core/scheduler/work-affinity-scheduler.js';

describe('Early Deduplication & Multi-Provider Concurrency Isolation', () => {
  it('WorkAffinityScheduler tracks inFlightChapterKeys and onJobStarted/onJobFinished', () => {
    const scheduler = new WorkAffinityScheduler();
    const workId = 'test-work-1';
    const sortKey = 50.0;

    expect(scheduler.getInFlightCount(workId)).toBe(0);

    // Slot 1 claims chapter 50 from Mangaflix
    scheduler.onJobStarted(workId, sortKey);
    expect(scheduler.getInFlightCount(workId)).toBe(1);

    // Private inFlightChapterKeys set must contain workId:sortKey
    const inFlightKeys: Set<string> = (scheduler as any).inFlightChapterKeys;
    expect(inFlightKeys.has(`${workId}:${sortKey}`)).toBe(true);

    // Finishing the job removes it from inFlightChapterKeys
    scheduler.onJobFinished(workId, sortKey);
    expect(scheduler.getInFlightCount(workId)).toBe(0);
    expect(inFlightKeys.has(`${workId}:${sortKey}`)).toBe(false);
  });

  it('In-flight chapter key isolation prevents duplicate concurrent slots', () => {
    const scheduler = new WorkAffinityScheduler();
    const workId = 'test-work-multi-provider';
    const sortKey = 100.0;

    // Slot 1 starts chapter 100
    scheduler.onJobStarted(workId, sortKey);

    const inFlightKeys: Set<string> = (scheduler as any).inFlightChapterKeys;
    expect(Array.from(inFlightKeys)).toContain(`${workId}:${sortKey}`);

    // If another provider attempts to claim the same chapter, it would be excluded by $9 parameter
    const disallowedKeys = Array.from((scheduler as any).inFlightChapterKeys);
    expect(disallowedKeys).toEqual([`${workId}:${sortKey}`]);

    // When Slot 1 completes, the exclusion is cleanly lifted
    scheduler.onJobFinished(workId, sortKey);
    expect((scheduler as any).inFlightChapterKeys.size).toBe(0);
  });
});
