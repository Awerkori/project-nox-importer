import { describe, expect, it } from 'vitest';
import { ImporterEngine } from '../src/core/engine.js';
import { shouldDeferHeavyStagedClassification } from '../src/core/auto-heal-watchdog.js';
import { AsyncSemaphore } from '../src/core/concurrency.js';

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

  it('defers catalog maintenance only before the initial chapter claim phase', () => {
    const engine = Object.create(ImporterEngine.prototype) as any;
    engine.chapterClaimPhaseReady = true;
    expect(engine.shouldDeferCatalogMaintenance()).toBe(false);
    engine.chapterClaimPhaseReady = false;
    expect(engine.shouldDeferCatalogMaintenance()).toBe(true);
  });

  it('gives a waiting maintenance claim turn priority over fresh nonblocking claims', async () => {
    const gate = new AsyncSemaphore(1, 'test_maintenance_claim_fairness');
    expect(gate.tryAcquire()).toBe(true);

    let maintenanceAcquired = false;
    const maintenanceTurn = gate.acquire().then(() => {
      maintenanceAcquired = true;
    });
    expect(gate.queued).toBe(1);
    expect(gate.tryAcquire()).toBe(false);

    gate.release();
    await maintenanceTurn;
    expect(maintenanceAcquired).toBe(true);
    expect(gate.active).toBe(1);
    gate.release();
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
