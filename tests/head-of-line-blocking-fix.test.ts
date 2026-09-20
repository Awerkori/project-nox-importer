import { describe, it, expect } from 'vitest';
import { AsyncSemaphore, AdaptiveAutotuner, SOURCE_CONCURRENCY_LIMITS } from '../src/core/concurrency.js';

describe('Head-of-Line Blocking Fix Verification', () => {
  it('AsyncSemaphore.tryAcquire works atomically without blocking', () => {
    const sem = new AsyncSemaphore(2, 'test_sem');
    expect(sem.available).toBe(2);
    expect(sem.active).toBe(0);

    expect(sem.tryAcquire()).toBe(true);
    expect(sem.available).toBe(1);
    expect(sem.active).toBe(1);

    expect(sem.tryAcquire()).toBe(true);
    expect(sem.available).toBe(0);
    expect(sem.active).toBe(2);

    // Third acquire must fail immediately without creating queue waiters
    expect(sem.tryAcquire()).toBe(false);
    expect(sem.available).toBe(0);
    expect(sem.active).toBe(2);
    expect(sem.queued).toBe(0);

    // After release, tryAcquire succeeds again
    sem.release();
    expect(sem.available).toBe(1);
    expect(sem.active).toBe(1);
    expect(sem.tryAcquire()).toBe(true);
    expect(sem.available).toBe(0);
  });

  it('AdaptiveAutotuner.isSourceCapacityAvailable respects per-source limits', () => {
    const autotuner = new AdaptiveAutotuner({ initialConcurrency: 8 });

    // hanamiheaven has maxChapters = 1
    expect(SOURCE_CONCURRENCY_LIMITS.hanamiheaven.maxChapters).toBe(1);
    expect(autotuner.isSourceCapacityAvailable('hanamiheaven')).toBe(true);

    const hanamiSem = autotuner.getSourceSemaphore('hanamiheaven');
    expect(hanamiSem.tryAcquire()).toBe(true);

    // Once acquired, capacity is full
    expect(autotuner.isSourceCapacityAvailable('hanamiheaven')).toBe(false);
    expect(hanamiSem.tryAcquire()).toBe(false);

    // Meanwhile fleurblanche (maxChapters = 2) remains available
    expect(SOURCE_CONCURRENCY_LIMITS.fleurblanche.maxChapters).toBe(2);
    expect(autotuner.isSourceCapacityAvailable('fleurblanche')).toBe(true);

    const fleurSem = autotuner.getSourceSemaphore('fleurblanche');
    expect(fleurSem.tryAcquire()).toBe(true);
    expect(autotuner.isSourceCapacityAvailable('fleurblanche')).toBe(true); // 1 permit remaining
    expect(fleurSem.tryAcquire()).toBe(true);
    expect(autotuner.isSourceCapacityAvailable('fleurblanche')).toBe(false); // full
    expect(fleurSem.tryAcquire()).toBe(false);

    // Release hanami
    hanamiSem.release();
    expect(autotuner.isSourceCapacityAvailable('hanamiheaven')).toBe(true);

    // Release fleur
    fleurSem.release();
    fleurSem.release();
    expect(autotuner.isSourceCapacityAvailable('fleurblanche')).toBe(true);
  });

  it('Simulates 8 runner slots acquiring without Head-of-Line Blocking', async () => {
    const autotuner = new AdaptiveAutotuner({ initialConcurrency: 8 });
    const claimMutex = new AsyncSemaphore(1, 'claim_mutex');

    // Sources: hanami (1), fleur (2), manga (2) -> sum = 5 permits
    const activeSources = ['hanamiheaven', 'fleurblanche', 'mangalivreto'];

    // Simulated queue with priority:
    // hanami has priority 80 (many items)
    // fleur has priority 20
    // manga has priority 20
    const queue = [
      { id: 'h1', source: 'hanamiheaven', priority: 80 },
      { id: 'h2', source: 'hanamiheaven', priority: 80 },
      { id: 'h3', source: 'hanamiheaven', priority: 80 },
      { id: 'h4', source: 'hanamiheaven', priority: 80 },
      { id: 'f1', source: 'fleurblanche', priority: 20 },
      { id: 'f2', source: 'fleurblanche', priority: 20 },
      { id: 'f3', source: 'fleurblanche', priority: 20 },
      { id: 'm1', source: 'mangalivreto', priority: 20 },
      { id: 'm2', source: 'mangalivreto', priority: 20 },
    ];

    const claimedJobs: Array<{ slot: number; job: any }> = [];

    // Simulate 8 slots claiming concurrently
    const slotClaims = Array.from({ length: 8 }, async (_, slotIndex) => {
      await claimMutex.runExclusive(async () => {
        // 1. Determine sources with available capacity
        const eligible = activeSources.filter(s => autotuner.isSourceCapacityAvailable(s));
        if (eligible.length === 0) return;

        // 2. Query queue for highest priority job matching eligible sources
        const matchIdx = queue.findIndex(q => eligible.includes(q.source));
        if (matchIdx < 0) return;

        const candidateJob = queue[matchIdx];
        const sem = autotuner.getSourceSemaphore(candidateJob.source);
        if (sem.tryAcquire()) {
          queue.splice(matchIdx, 1);
          claimedJobs.push({ slot: slotIndex, job: candidateJob });
        }
      });
    });

    await Promise.all(slotClaims);

    // Exactly 5 jobs should be claimed (1 hanami, 2 fleur, 2 manga)
    expect(claimedJobs.length).toBe(5);

    const sourcesClaimed = claimedJobs.map(c => c.job.source);
    expect(sourcesClaimed.filter(s => s === 'hanamiheaven').length).toBe(1);
    expect(sourcesClaimed.filter(s => s === 'fleurblanche').length).toBe(2);
    expect(sourcesClaimed.filter(s => s === 'mangalivreto').length).toBe(2);

    // Slots 5, 6, 7 did NOT claim or block on hanami!
    expect(autotuner.getSourceSemaphore('hanamiheaven').queued).toBe(0);
    expect(autotuner.getSourceSemaphore('fleurblanche').queued).toBe(0);
    expect(autotuner.getSourceSemaphore('mangalivreto').queued).toBe(0);
  });
});
