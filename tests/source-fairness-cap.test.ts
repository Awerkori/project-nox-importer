import { describe, expect, it } from 'vitest';
import { AdaptiveAutotuner } from '../src/core/concurrency.js';

describe('source fair-share chapter capacity', () => {
  it('keeps a slow source from consuming a small global importer when alternatives are healthy', () => {
    const autotuner = new AdaptiveAutotuner({
      minConcurrency: 1,
      healthyConcurrencyFloor: 5,
      initialConcurrency: 5,
      maxConcurrency: 5,
    });

    autotuner.setEligibleSourceCountForFairness(4);
    const slowSource = autotuner.getSourceSemaphore('mundohentai');
    expect(slowSource.capacity).toBe(2);
    expect(slowSource.tryAcquire()).toBe(true);
    expect(slowSource.tryAcquire()).toBe(true);
    expect(slowSource.tryAcquire()).toBe(false);

    // Capacity stays local: a different healthy source can still claim work.
    const otherSource = autotuner.getSourceSemaphore('manhastro');
    expect(otherSource.capacity).toBe(2);
    expect(otherSource.tryAcquire()).toBe(true);
  });

  it('remains work-conserving when only one source is healthy', () => {
    const autotuner = new AdaptiveAutotuner({
      minConcurrency: 1,
      healthyConcurrencyFloor: 5,
      initialConcurrency: 5,
      maxConcurrency: 5,
    });

    autotuner.setEligibleSourceCountForFairness(3);
    const source = autotuner.getSourceSemaphore('mundohentai');
    expect(source.capacity).toBe(2);

    autotuner.setEligibleSourceCountForFairness(1);
    expect(source.capacity).toBe(4); // provider's configured ceiling
  });

  it('rebalances source capacity when the autotuner reduces global pressure', () => {
    const autotuner = new AdaptiveAutotuner({
      minConcurrency: 1,
      healthyConcurrencyFloor: 5,
      initialConcurrency: 5,
      maxConcurrency: 5,
    });
    autotuner.setEligibleSourceCountForFairness(3);
    const source = autotuner.getSourceSemaphore('mundohentai');
    expect(source.capacity).toBe(2);

    autotuner.setCapacity(3, 'RUNNING_THROTTLED', 'test pressure');
    expect(source.capacity).toBe(1);
  });
});
