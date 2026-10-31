import { describe, expect, it } from 'vitest';
import type { Pool } from 'pg';
import { AsyncSemaphore } from '../../core/concurrency.js';
import { readImageBody } from '../../core/bounded-body.js';
import { HostRateLimiter } from '../../core/rate-limiter.js';
import type { SourceRegistry } from '../../sources/registry.js';
import type { StorageProvider } from '../../storage/provider.js';
import { LiteExecutor } from '../executor.js';
import { LiteRepository } from '../repository.js';
import { LiteMemoryPressureGuard } from '../memory-pressure.js';
import { PageByteBudget } from '../page-byte-budget.js';
import { SourcePerformanceTracker } from '../source-performance.js';
import { selectChapterSource } from '../source-selector.js';
import type { ChapterSourceRoute } from '../types.js';

const MIB = 1024 * 1024;

function makeRoute(patch: Partial<ChapterSourceRoute>): ChapterSourceRoute {
  return {
    workId: '00000000-0000-4000-8000-000000000001',
    canonicalChapterKey: 'pt-br|default|chapter|1|-',
    source: 'nexus',
    sourceWorkId: 'w1',
    sourceChapterId: 'c1',
    language: 'pt-BR',
    edition: 'default',
    sourceRank: 100,
    confirmed: true,
    hasChapter: true,
    health: 'HEALTHY',
    cooldownUntil: null,
    ...patch,
  };
}

describe('SourcePerformanceTracker & Dynamic Fast/Slow Scheduling', () => {
  it('avoids premature classification on tiny samples and dynamically promotes/demotes with real evidence', () => {
    const tracker = new SourcePerformanceTracker({ useWarmStartPriors: false });
    expect(tracker.getTier('unknown-scan')).toBe('NORMAL');

    // Only 3 slow pages: must remain NORMAL (no permanent punishment from small sample).
    for (let i = 0; i < 3; i++) {
      tracker.recordPageDownload('unknown-scan', 14_000, 500_000);
    }
    expect(tracker.getTier('unknown-scan')).toBe('NORMAL');

    // Sustained slow latency across >=12 pages transitions to SLOW.
    for (let i = 0; i < 12; i++) {
      tracker.recordPageDownload('unknown-scan', 14_000, 500_000);
    }
    expect(tracker.getTier('unknown-scan')).toBe('SLOW');

    // If the source genuinely recovers and sustains fast downloads, it promotes out of SLOW -> FAST.
    for (let i = 0; i < 30; i++) {
      tracker.recordPageDownload('unknown-scan', 900, 450_000);
    }
    tracker.recordChapterOutcome('unknown-scan', { succeeded: true, durationMs: 14_000, pageCount: 16 });
    tracker.recordChapterOutcome('unknown-scan', { succeeded: true, durationMs: 15_000, pageCount: 16 });
    expect(tracker.getTier('unknown-scan')).toBe('FAST');
  });

  it('dynamically demotes a warm-start FAST source when upstream stalls or fails repeatedly', () => {
    const tracker = new SourcePerformanceTracker({ useWarmStartPriors: true });
    expect(tracker.getTier('nexus')).toBe('FAST');
    expect(tracker.getTier('mangaflix')).toBe('SLOW');

    for (let i = 0; i < 4; i++) {
      tracker.recordChapterOutcome('nexus', { succeeded: false, durationMs: 35_000 });
    }
    expect(tracker.getTier('nexus')).toBe('SLOW');
  });

  it('prefers healthy FAST routes over SLOW routes even when SLOW has lower legacy sourceRank', () => {
    const tracker = new SourcePerformanceTracker({ useWarmStartPriors: true });
    const routes: ChapterSourceRoute[] = [
      makeRoute({ source: 'mangaflix', sourceRank: 10, sourceChapterId: 'mf-1' }),
      makeRoute({ source: 'nexus', sourceRank: 100, sourceChapterId: 'nx-1' }),
    ];

    const selected = selectChapterSource(routes, { language: 'pt-BR', edition: 'default' }, new Date(), (s) => tracker.getSpeedRank(s));
    expect(selected.route?.source).toBe('nexus');
  });

  it('prioritizes fast sources on 75% of workers while keeping slow sources active in background and expanding when fast demand drops', () => {
    const tracker = new SourcePerformanceTracker({ useWarmStartPriors: true });
    const executor = new LiteExecutor(
      {} as Pool,
      {} as LiteRepository,
      {} as SourceRegistry,
      {} as StorageProvider,
      '00000000-0000-4000-8000-000000000009',
      new AsyncSemaphore(24, 'test-downloads'),
      8,
      new AsyncSemaphore(80, 'test-buffers'),
      new HostRateLimiter(5),
      20,
    );
    executor.setPerformanceTracker(tracker);

    // High fast + slow demand across 20 workers:
    executor.setFairSourceDemandCounts(new Map([
      ['nexus', 50],
      ['taimumangas', 6],
      ['pizzariascan', 4],
      ['mangaflix', 30],
      ['mangotoons', 8],
      ['hipercool', 6],
    ]));

    const nexusCap = (executor as any).sourceSlot('nexus').capacity;
    const taimuCap = (executor as any).sourceSlot('taimumangas').capacity;
    const pizzariaCap = (executor as any).sourceSlot('pizzariascan').capacity;
    const mangaflixCap = (executor as any).sourceSlot('mangaflix').capacity;
    const mangotoonsCap = (executor as any).sourceSlot('mangotoons').capacity;
    const hipercoolCap = (executor as any).sourceSlot('hipercool').capacity;

    // Fast sources receive the bulk (15 of 20 workers), slow sources stay active in bounded background (1-2 each, 5 pool).
    expect(nexusCap + taimuCap + pizzariaCap).toBeGreaterThanOrEqual(15);
    expect(nexusCap).toBeGreaterThanOrEqual(10);
    expect(mangaflixCap).toBeGreaterThanOrEqual(1);
    expect(mangaflixCap).toBeLessThanOrEqual(2);
    expect(mangotoonsCap).toBeGreaterThanOrEqual(1);
    expect(hipercoolCap).toBeGreaterThanOrEqual(1);

    // Page wave concurrency: fast nexus gets 12, slow mangaflix gets 4 while fast demand exists, mega-chapters cap at 4.
    expect(executor.getPageConcurrencyForSource('nexus', 20)).toBe(12);
    expect(executor.getPageConcurrencyForSource('mangaflix', 20)).toBe(4);
    expect(executor.getPageConcurrencyForSource('nexus', 120)).toBe(4);

    // Work-conserving expansion: when fast sources drain to 0, slow sources expand automatically to use idle workers!
    executor.setFairSourceDemandCounts(new Map([
      ['mangaflix', 30],
      ['mangotoons', 8],
      ['hipercool', 6],
    ]));
    expect((executor as any).sourceSlot('mangaflix').capacity).toBe(6);
    expect((executor as any).sourceSlot('mangotoons').capacity).toBe(4);
    expect((executor as any).sourceSlot('hipercool').capacity).toBe(5);
    expect(executor.getPageConcurrencyForSource('mangaflix', 20)).toBe(8);
  });

  it('caps typical lengthless page upgrades at 4 MiB instead of locking 20 MiB per page', async () => {
    const budget = new PageByteBudget(48 * MIB);
    const reservation = await budget.reserve(512 * 1024, undefined, { completionHeadroomBytes: 1 * MIB });

    let chunks = 0;
    const stream = new ReadableStream<Uint8Array>({
      pull(controller) {
        if (chunks >= 3) {
          controller.close();
          return;
        }
        chunks++;
        controller.enqueue(new Uint8Array(512 * 1024)); // 1.5 MiB total
      },
    });

    const bytes = await readImageBody(new Response(stream), { reservation });
    expect(bytes.byteLength).toBe(1.5 * MIB);
    expect(reservation.reservedBytes).toBe(4 * MIB);
    reservation.completeBody(bytes.byteLength);
    expect(reservation.reservedBytes).toBe(1.5 * MIB);
    reservation.release();
  });

  it('keeps 624 MiB DIScloud container in NORMAL state at 405 MiB RSS with 128 MiB byte budget', () => {
    const buffers = new AsyncSemaphore(80, 'mem-624-buffers');
    const bytes = new PageByteBudget(128 * MIB);
    const guard = new LiteMemoryPressureGuard(buffers, bytes, 80, 128 * MIB, 624);

    const normalAt405 = guard.sample({
      rss: 405 * MIB,
      heapUsed: 140 * MIB,
      heapTotal: 190 * MIB,
      external: 24 * MIB,
      arrayBuffers: 18 * MIB,
    });
    expect(normalAt405.state).toBe('NORMAL');
    expect(buffers.capacity).toBe(80);
    expect(bytes.capacityBytes).toBe(128 * MIB);

    const reducedAt495 = guard.sample({
      rss: 495 * MIB,
      heapUsed: 200 * MIB,
      heapTotal: 250 * MIB,
      external: 48 * MIB,
      arrayBuffers: 40 * MIB,
    });
    expect(reducedAt495.state).toBe('REDUCED');
    expect(buffers.capacity).toBe(48);
    expect(bytes.capacityBytes).toBe(64 * MIB);
  });

  it('includes manhastro in warm-start slow priors and bounds slow sources in P1 Focus Group relaxation', async () => {
    const tracker = new SourcePerformanceTracker();
    expect(tracker.getTier('manhastro')).toBe('SLOW');
    expect(tracker.getTier('nocturnesummer')).toBe('FAST');
    expect(tracker.getTier('taimumangas')).toBe('FAST');
    expect(tracker.getTier('mrtenzus')).toBe('NORMAL');

    const rows = [
      ...Array.from({ length: 20 }, (_, idx) => ({
        work_key: `work:00000000-0000-4000-8000-0000000001${String(idx).padStart(2, '0')}`,
        work_id: `00000000-0000-4000-8000-0000000001${String(idx).padStart(2, '0')}`,
        remaining_chapters: 10,
        oldest_task_at: new Date(Date.now() - 3600_000).toISOString(),
        runnable_sources: ['mangaflix'],
        has_recorded_routes: true,
      })),
      ...Array.from({ length: 15 }, (_, idx) => ({
        work_key: `work:00000000-0000-4000-8000-0000000002${String(idx).padStart(2, '0')}`,
        work_id: `00000000-0000-4000-8000-0000000002${String(idx).padStart(2, '0')}`,
        remaining_chapters: 10,
        oldest_task_at: new Date(Date.now() - 3600_000).toISOString(),
        runnable_sources: ['nocturnesummer'],
        has_recorded_routes: true,
      })),
      ...Array.from({ length: 15 }, (_, idx) => ({
        work_key: `work:00000000-0000-4000-8000-0000000003${String(idx).padStart(2, '0')}`,
        work_id: `00000000-0000-4000-8000-0000000003${String(idx).padStart(2, '0')}`,
        remaining_chapters: 10,
        oldest_task_at: new Date(Date.now() - 3600_000).toISOString(),
        runnable_sources: ['mrtenzus'],
        has_recorded_routes: true,
      })),
    ];
    const mockDb = {
      query: async () => ({ rows }),
    } as unknown as Pool;
    const repo = new LiteRepository(mockDb, mockDb);
    const snap = await repo.refreshP1FocusGroup({
      targetSize: 28,
      maxPerSource: 8,
      sourceTiers: tracker.getTierMap(['mangaflix', 'nocturnesummer', 'mrtenzus']),
    });
    expect(snap.focusWorkCount).toBe(28);
    expect(snap.focusSources.mangaflix || 0).toBeLessThanOrEqual(2);
    expect((snap.focusSources.nocturnesummer || 0) + (snap.focusSources.mrtenzus || 0)).toBeGreaterThanOrEqual(26);
  });
});
