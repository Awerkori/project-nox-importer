import { describe, expect, it } from 'vitest';
import { ImporterEngine } from '../src/core/engine.js';
import { shouldDeferHeavyStagedClassification } from '../src/core/auto-heal-watchdog.js';

describe('claim pressure protects the bounded YSQL pool', () => {
  it('defers deep staged classification only when cached telemetry exists and claims are pressured', () => {
    expect(shouldDeferHeavyStagedClassification({
      cachedTelemetryAvailable: true,
      healthyState: false,
      claimPressureHigh: true,
    })).toBe(true);
    expect(shouldDeferHeavyStagedClassification({
      cachedTelemetryAvailable: false,
      healthyState: false,
      claimPressureHigh: true,
    })).toBe(false);
    expect(shouldDeferHeavyStagedClassification({
      cachedTelemetryAvailable: true,
      healthyState: true,
      claimPressureHigh: true,
    })).toBe(false);
  });

  it('defers catalog maintenance when nearly all chapter-claim permits are active', () => {
    const engine = Object.create(ImporterEngine.prototype) as any;
    engine.chapterClaimPhaseReady = true;
    engine.chapterClaimGate = { active: 4, queued: 0, capacity: 5 };
    expect(engine.shouldDeferCatalogMaintenance()).toBe(true);
    engine.chapterClaimGate = { active: 3, queued: 0, capacity: 5 };
    expect(engine.shouldDeferCatalogMaintenance()).toBe(false);
    engine.chapterClaimGate = { active: 0, queued: 1, capacity: 5 };
    expect(engine.shouldDeferCatalogMaintenance()).toBe(true);
    engine.chapterClaimPhaseReady = false;
    engine.chapterClaimGate = { active: 0, queued: 0, capacity: 5 };
    expect(engine.shouldDeferCatalogMaintenance()).toBe(true);
  });

  it('keeps maintenance deferred until every startup chapter slot has attempted a claim', () => {
    const engine = Object.create(ImporterEngine.prototype) as any;
    engine.config = { MAX_CONCURRENT_CHAPTERS: 5, TESTED_CONCURRENCY_CEILING: 32 };
    engine.chapterClaimPhaseReady = false;
    engine.chapterClaimStartupSlots = new Set<number>();

    for (const slot of [0, 1, 2, 3]) {
      engine.markChapterClaimPhaseAttempt(slot);
      expect(engine.chapterClaimPhaseReady).toBe(false);
      engine.chapterClaimGate = { active: 0, queued: 0, capacity: 5 };
      expect(engine.shouldDeferCatalogMaintenance()).toBe(true);
    }

    engine.markChapterClaimPhaseAttempt(4);
    expect(engine.chapterClaimPhaseReady).toBe(true);
    engine.chapterClaimGate = { active: 0, queued: 0, capacity: 5 };
    expect(engine.shouldDeferCatalogMaintenance()).toBe(false);
  });
});
