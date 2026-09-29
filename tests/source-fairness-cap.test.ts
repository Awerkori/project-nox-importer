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

  it('keeps one large source from consuming every global page-download permit', () => {
    const autotuner = new AdaptiveAutotuner({
      minConcurrency: 1,
      healthyConcurrencyFloor: 5,
      initialConcurrency: 5,
      maxConcurrency: 5,
      downloadInflightConcurrency: 8,
    });

    autotuner.setEligibleSourceCountForFairness(3);
    const nexus = autotuner.getSourceDownloadSemaphore('nexus');
    const other = autotuner.getSourceDownloadSemaphore('hentaihome');

    // Nexus can run two chapters at four page requests each, but while other
    // sources are healthy it receives only a fair share of the global eight.
    expect(nexus.capacity).toBe(3);
    expect(other.capacity).toBe(3);
    expect(nexus.tryAcquire()).toBe(true);
    expect(nexus.tryAcquire()).toBe(true);
    expect(nexus.tryAcquire()).toBe(true);
    expect(nexus.tryAcquire()).toBe(false);
    expect(other.tryAcquire()).toBe(true);
  });

  it('lets a sole healthy source use its configured page fan-out', () => {
    const autotuner = new AdaptiveAutotuner({
      minConcurrency: 1,
      healthyConcurrencyFloor: 5,
      initialConcurrency: 5,
      maxConcurrency: 5,
      downloadInflightConcurrency: 8,
    });

    autotuner.setEligibleSourceCountForFairness(3);
    const nexus = autotuner.getSourceDownloadSemaphore('nexus');
    expect(nexus.capacity).toBe(3);

    autotuner.setEligibleSourceCountForFairness(1);
    expect(nexus.capacity).toBe(8);
  });

  it('uses active chapter sources for fairness so idle providers do not strand capacity', () => {
    const autotuner = new AdaptiveAutotuner({
      minConcurrency: 1,
      healthyConcurrencyFloor: 5,
      initialConcurrency: 5,
      maxConcurrency: 5,
      downloadInflightConcurrency: 8,
    });

    // A catalog may have many healthy providers even though only Manhastro
    // currently has queued chapter work. It should retain its configured
    // chapter capacity and page fan-out until another source is active.
    autotuner.setEligibleSourceCountForFairness(53);
    const manhastro = autotuner.getSourceSemaphore('manhastro');
    expect(manhastro.capacity).toBe(2);
    expect(manhastro.tryAcquire()).toBe(true);
    expect(autotuner.refreshSourceFairnessFromActiveSources(['manhastro', 'nexus', 'hanamiheaven'])).toBe(1);
    expect(manhastro.capacity).toBe(3);
    expect(autotuner.getSourceDownloadSemaphore('manhastro').capacity).toBe(8);

    // As independent sources receive work, local caps narrow again without
    // changing global chapter, Telegram, download, or memory budgets.
    expect(autotuner.getSourceSemaphore('nexus').tryAcquire()).toBe(true);
    expect(autotuner.refreshSourceFairnessFromActiveSources(['manhastro', 'nexus', 'hanamiheaven'])).toBe(2);
    expect(autotuner.getSourceDownloadSemaphore('manhastro').capacity).toBe(4);

    expect(autotuner.getSourceSemaphore('hanamiheaven').tryAcquire()).toBe(true);
    expect(autotuner.refreshSourceFairnessFromActiveSources(['manhastro', 'nexus', 'hanamiheaven'])).toBe(3);
    expect(manhastro.capacity).toBe(2);
    expect(autotuner.getSourceDownloadSemaphore('manhastro').capacity).toBe(3);
  });
});
