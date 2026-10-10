import { describe, expect, it, vi } from 'vitest';
import {
  ImporterEngine,
  legacySameSourceClaimRecoveryDedupeKey,
  selectCatalogMaintenanceProbeSources,
} from '../src/core/engine.js';
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

  it('releases maintenance after a confirmed empty canonical scan', () => {
    const engine = Object.create(ImporterEngine.prototype) as any;
    engine.chapterClaimPhaseReady = false;
    engine.chapterClaimStartupSlots = new Set([0]);

    engine.markInitialChapterScanEmpty();

    expect(engine.chapterClaimPhaseReady).toBe(true);
    expect(engine.shouldDeferCatalogMaintenance()).toBe(false);
  });

  it('rotates bounded maintenance probes across eligible sources', () => {
    const sources = ['zeta', 'mangaflix', 'alpha', 'mangaflix'];

    const first = selectCatalogMaintenanceProbeSources(sources, 0, 2);
    expect(first).toEqual({ sources: ['alpha', 'mangaflix'], nextCursor: 2 });

    const second = selectCatalogMaintenanceProbeSources(sources, first.nextCursor, 2);
    expect(second).toEqual({ sources: ['zeta', 'alpha'], nextCursor: 1 });
  });

  it('claims from the whole rotating probe window atomically', async () => {
    const engine = Object.create(ImporterEngine.prototype) as any;
    const acquireNextJob = vi.fn().mockResolvedValue({ id: 'mangaflix-recovery' });
    engine.chapterClaimGate = new AsyncSemaphore(1, 'test_catalog_claim_gate');
    engine.config = { QUEUE_LEASE_DURATION_SECONDS: 300 };
    engine.stopSignal = false;
    engine.chapterClaimPhaseReady = true;
    engine.catalogMaintenanceSourceCursor = { DISCOVER_WORKS: 0, SYNC_WORK: 0 };
    engine.getEligibleCatalogSources = vi.fn().mockResolvedValue([
      'zeta', 'mangaflix', 'littletyrant', 'alpha',
    ]);
    engine.queue = { acquireNextJob };

    const job = await engine.acquireCatalogMaintenanceJob('SYNC_WORK');

    expect(job).toEqual({ id: 'mangaflix-recovery' });
    expect(acquireNextJob).toHaveBeenCalledTimes(1);
    expect(acquireNextJob).toHaveBeenCalledWith(
      5,
      ['alpha', 'littletyrant', 'mangaflix', 'zeta'],
      'SYNC_WORK',
      true,
    );
    expect(engine.catalogMaintenanceSourceCursor.SYNC_WORK).toBe(0);
    expect(engine.chapterClaimGate.active).toBe(0);
  });

  it('reserves one bounded sync turn for queued legacy ambiguity recoveries', async () => {
    const engine = Object.create(ImporterEngine.prototype) as any;
    const acquireNextJob = vi.fn().mockResolvedValue({
      id: 'mangaflix-recovery',
      source: 'mangaflix',
    });
    engine.chapterClaimGate = new AsyncSemaphore(1, 'test_catalog_recovery_turn');
    engine.config = { QUEUE_LEASE_DURATION_SECONDS: 300 };
    engine.stopSignal = false;
    engine.chapterClaimPhaseReady = true;
    engine.catalogMaintenanceSourceCursor = { DISCOVER_WORKS: 0, SYNC_WORK: 0 };
    engine.catalogSyncClaimsSinceLegacyRecovery = 3;
    engine.catalogLegacyRecoverySourceCursor = 0;
    engine.nextLegacyRecoveryProbeAt = 0;
    engine.getEligibleCatalogSources = vi.fn().mockResolvedValue([
      'zeta', 'mangaflix', 'littletyrant', 'alpha',
    ]);
    engine.queue = { acquireNextJob };

    const job = await engine.acquireCatalogMaintenanceJob('SYNC_WORK');

    expect(job).toEqual({ id: 'mangaflix-recovery', source: 'mangaflix' });
    expect(acquireNextJob).toHaveBeenCalledWith(
      5,
      ['alpha', 'littletyrant', 'mangaflix', 'zeta'],
      'SYNC_WORK',
      true,
      { onlyLegacySameSourceClaimRecovery: true },
    );
    expect(engine.catalogSyncClaimsSinceLegacyRecovery).toBe(0);
    expect(engine.catalogLegacyRecoverySourceCursor).toBe(3);
    expect(engine.chapterClaimGate.active).toBe(0);
  });

  it('rotates winners inside a probe window without slowing the global window sweep', async () => {
    const engine = Object.create(ImporterEngine.prototype) as any;
    const acquireNextJob = vi.fn().mockResolvedValue({ id: 'mangaflix-recovery', source: 'mangaflix' });
    engine.chapterClaimGate = new AsyncSemaphore(1, 'test_catalog_source_order');
    engine.config = { QUEUE_LEASE_DURATION_SECONDS: 300 };
    engine.stopSignal = false;
    engine.chapterClaimPhaseReady = true;
    engine.catalogMaintenanceSourceCursor = { DISCOVER_WORKS: 16, SYNC_WORK: 16 };
    engine.getEligibleCatalogSources = vi.fn().mockResolvedValue([
      'apenasumafa', 'apecomics', 'brasilhentai', 'cafecomyaoi',
      'euphoriascan', 'fleurblanche', 'hanamiheaven', 'hentaifusion',
      'hentaihome', 'hentaiseason', 'hentaitokyo', 'hipercool',
      'kamisamaexplorer', 'kuro', 'ler999', 'littletyrant',
      'mangaonlinetv', 'mangalivreto', 'mangaflix', 'megahentai',
    ]);
    engine.queue = { acquireNextJob };

    await engine.acquireCatalogMaintenanceJob('SYNC_WORK');

    expect(acquireNextJob).toHaveBeenCalledWith(
      5,
      ['mangaflix', 'mangalivreto', 'mangaonlinetv', 'megahentai'],
      'SYNC_WORK',
      true,
    );
    // MangaFlix won its probe, so its next visit starts at the following
    // source; the global cursor still advances by the whole four-source
    // window instead of degrading to a one-source cycle.
    expect(engine.catalogMaintenanceSourceCursor.SYNC_WORK).toBe(0);
    expect(engine.catalogMaintenanceWindowCursors.SYNC_WORK.get('mangaflix\u001fmangalivreto\u001fmangaonlinetv\u001fmegahentai')).toBe(1);

    engine.catalogMaintenanceSourceCursor.SYNC_WORK = 16;
    await engine.acquireCatalogMaintenanceJob('SYNC_WORK');

    expect(acquireNextJob).toHaveBeenLastCalledWith(
      5,
      ['mangalivreto', 'mangaonlinetv', 'megahentai', 'mangaflix'],
      'SYNC_WORK',
      true,
    );
    expect(engine.catalogMaintenanceSourceCursor.SYNC_WORK).toBe(0);
    expect(engine.chapterClaimGate.active).toBe(0);
  });

  it('keeps the maintenance probe empty when no source is eligible', () => {
    expect(selectCatalogMaintenanceProbeSources([], 4, 6)).toEqual({ sources: [], nextCursor: 0 });
  });

  it('uses a distinct idempotency key for a legacy ambiguity recovery sync', () => {
    expect(legacySameSourceClaimRecoveryDedupeKey('mangaflix', 'source-work-42'))
      .toBe('mangaflix:legacy-same-source-claim-recovery:source-work-42');
  });

  it('caps outstanding, separately deduplicated legacy ambiguity re-syncs per source', async () => {
    const query = vi.fn().mockResolvedValue({
      rows: [{ source_work_id: 'source-work-42', source_slug: 'legacy-work', source_title: 'Legacy Work' }],
    });
    const enqueueBatch = vi.fn().mockResolvedValue(1);
    const engine = Object.create(ImporterEngine.prototype) as any;
    engine.dbPool = { query };
    engine.queue = { enqueueBatch };
    engine.logger = { info: vi.fn(), warn: vi.fn() };

    await engine.scheduleLegacySameSourceClaimAmbiguityRecovery('mangaflix');

    expect(query).toHaveBeenCalledWith(
      expect.stringContaining("wm.sync_status = 'AMBIGUOUS'"),
      ['mangaflix', 'Work already claimed by another ID from the same source',
        'mangaflix:legacy-same-source-claim-recovery:', 4],
    );
    expect(query.mock.calls[0][0]).toContain("q.status IN ('QUEUED', 'RETRY', 'IMPORTING')");
    expect(query.mock.calls[0][0]).toContain('c.candidate_rank <= GREATEST(0, $4 - o.count)');
    expect(enqueueBatch).toHaveBeenCalledWith([{
      taskType: 'SYNC_WORK',
      source: 'mangaflix',
      dedupeKey: 'mangaflix:legacy-same-source-claim-recovery:source-work-42',
      payload: {
        sourceWorkId: 'source-work-42',
        slug: 'legacy-work',
        title: 'Legacy Work',
        legacySameSourceClaimRecovery: true,
      },
      priority: 65,
    }]);
  });
});
