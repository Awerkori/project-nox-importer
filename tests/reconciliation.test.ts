import { describe, it, expect, vi, beforeEach } from 'vitest';
import { ExistingWorksReconciler } from '../src/core/reconciliation.js';
import { SourceRegistry } from '../src/sources/registry.js';
import { SourceAdapter } from '../src/sources/types.js';

describe('ExistingWorksReconciler - Canonical Gap & Fresh Release Discovery', () => {
  let reconciler: ExistingWorksReconciler;
  let mockSupabase: any;
  let mockQueue: any;
  let registry: SourceRegistry;
  let enqueuedJobs: any[] = [];

  const workId = '00000000-0000-0000-0000-000000000001';

  beforeEach(() => {
    enqueuedJobs = [];
    registry = new SourceRegistry();

    mockQueue = {
      enqueue: vi.fn(async (taskType, source, dedupeKey, payload, priority, sortKey) => {
        enqueuedJobs.push({ taskType, source, dedupeKey, payload, priority, sortKey });
        return true;
      }),
    };
  });

  it('enqueues confirmed gaps with Priority 70 in strict ascending sort order and fresh releases with Priority 80', async () => {
    // Published: Ch 1, 2, 5 (Max published = 5).
    // Gaps in between: Ch 3, Ch 4.
    // Fresh release: Ch 6.
    const adapter: SourceAdapter = {
      id: 'nexus',
      name: 'Nexus',
      baseUrl: 'https://nexus.com',
      fetchUpdatedWorks: vi.fn(),
      fetchWorkDetails: vi.fn(),
      fetchChapters: vi.fn(async () => [
        { sourceChapterId: 'ch-1', number: 1, title: 'Cap 1', pageCount: 20 },
        { sourceChapterId: 'ch-2', number: 2, title: 'Cap 2', pageCount: 20 },
        { sourceChapterId: 'ch-3', number: 3, title: 'Cap 3', pageCount: 20 },
        { sourceChapterId: 'ch-4', number: 4, title: 'Cap 4', pageCount: 20 },
        { sourceChapterId: 'ch-5', number: 5, title: 'Cap 5', pageCount: 20 },
        { sourceChapterId: 'ch-6', number: 6, title: 'Cap 6', pageCount: 20 },
      ]),
      fetchChapterPages: vi.fn(),
    };
    registry.register(adapter);

    mockSupabase = {
      from: vi.fn((table: string) => {
        if (table === 'importer_work_mappings') {
          return {
            select: vi.fn().mockReturnThis(),
            eq: vi.fn().mockReturnThis(),
            order: vi.fn().mockReturnThis(),
            limit: vi.fn().mockResolvedValue({
              data: [
                {
                  id: 'map-1',
                  work_id: workId,
                  source: 'nexus',
                  source_work_id: 'nx-1',
                  updated_at: new Date().toISOString(),
                  works: { title: 'Test Work', slug: 'test-work' },
                },
              ],
              error: null,
            }),
          };
        }
        if (table === 'chapters') {
          return {
            select: vi.fn().mockReturnThis(),
            eq: vi.fn().mockReturnThis(),
            not: vi.fn().mockResolvedValue({
              data: [
                { id: 'c1', number: 1, published_at: '2026-09-01T00:00:00Z' },
                { id: 'c2', number: 2, published_at: '2026-09-01T00:00:00Z' },
                { id: 'c5', number: 5, published_at: '2026-09-01T00:00:00Z' },
              ],
            }),
          };
        }
        if (table === 'importer_chapter_mappings') {
          return {
            select: vi.fn().mockReturnThis(),
            eq: vi.fn().mockResolvedValue({ data: [] }),
            upsert: vi.fn().mockResolvedValue({ error: null }),
          };
        }
        if (table === 'importer_queue') {
          return {
            select: vi.fn().mockReturnThis(),
            eq: vi.fn().mockReturnThis(),
            in: vi.fn().mockResolvedValue({ data: [] }),
          };
        }
        return {};
      }),
    };

    reconciler = new ExistingWorksReconciler(mockSupabase, mockQueue, registry);
    const stats = await reconciler.reconcileExistingWorks(10);

    expect(stats.worksScanned).toBe(1);
    expect(stats.worksWithConfirmedGaps).toBe(1);
    expect(stats.confirmedGapsDiscovered).toBe(2); // Ch 3, Ch 4
    expect(stats.newChaptersDiscovered).toBe(1); // Ch 6
    expect(stats.jobsEnqueued).toBe(3);

    // Verify ordering: Gaps first in ASC order (3 then 4), then Fresh release (6)
    expect(enqueuedJobs[0].sortKey).toBe(3);
    expect(enqueuedJobs[0].priority).toBe(70);
    expect(enqueuedJobs[0].dedupeKey).toBe(`work:${workId}:chapter:3`);

    expect(enqueuedJobs[1].sortKey).toBe(4);
    expect(enqueuedJobs[1].priority).toBe(70);
    expect(enqueuedJobs[1].dedupeKey).toBe(`work:${workId}:chapter:4`);

    expect(enqueuedJobs[2].sortKey).toBe(6);
    expect(enqueuedJobs[2].priority).toBe(80);
    expect(enqueuedJobs[2].dedupeKey).toBe(`work:${workId}:chapter:6`);
  });

  it('never invents chapters when a numerical jump exists in the source', async () => {
    // Source legitimately jumps from Ch 10 to Ch 20 (no Ch 11..19 exist anywhere)
    const adapter: SourceAdapter = {
      id: 'nexus',
      name: 'Nexus',
      baseUrl: 'https://nexus.com',
      fetchUpdatedWorks: vi.fn(),
      fetchWorkDetails: vi.fn(),
      fetchChapters: vi.fn(async () => [
        { sourceChapterId: 'ch-10', number: 10, title: 'Cap 10' },
        { sourceChapterId: 'ch-20', number: 20, title: 'Cap 20' },
      ]),
      fetchChapterPages: vi.fn(),
    };
    registry.register(adapter);

    mockSupabase = {
      from: vi.fn((table: string) => {
        if (table === 'importer_work_mappings') {
          return {
            select: vi.fn().mockReturnThis(),
            eq: vi.fn().mockReturnThis(),
            order: vi.fn().mockReturnThis(),
            limit: vi.fn().mockResolvedValue({
              data: [
                {
                  id: 'map-1',
                  work_id: workId,
                  source: 'nexus',
                  source_work_id: 'nx-1',
                  works: { title: 'Test Work' },
                },
              ],
              error: null,
            }),
          };
        }
        if (table === 'chapters') {
          return {
            select: vi.fn().mockReturnThis(),
            eq: vi.fn().mockReturnThis(),
            not: vi.fn().mockResolvedValue({
              data: [{ id: 'c10', number: 10, published_at: '2026-09-01T00:00:00Z' }],
            }),
          };
        }
        if (table === 'importer_chapter_mappings') {
          return {
            select: vi.fn().mockReturnThis(),
            eq: vi.fn().mockResolvedValue({ data: [] }),
            upsert: vi.fn().mockResolvedValue({ error: null }),
          };
        }
        if (table === 'importer_queue') {
          return {
            select: vi.fn().mockReturnThis(),
            eq: vi.fn().mockReturnThis(),
            in: vi.fn().mockResolvedValue({ data: [] }),
          };
        }
        return {};
      }),
    };

    reconciler = new ExistingWorksReconciler(mockSupabase, mockQueue, registry);
    const stats = await reconciler.reconcileExistingWorks(10);

    // Only chapter 20 should be enqueued as fresh release (priority 80)
    // Chapters 11..19 must NEVER be invented!
    expect(stats.confirmedGapsDiscovered).toBe(0);
    expect(stats.newChaptersDiscovered).toBe(1);
    expect(enqueuedJobs.length).toBe(1);
    expect(enqueuedJobs[0].sortKey).toBe(20);
    expect(enqueuedJobs[0].priority).toBe(80);
  });

  it('protects STAGED chapters and NEVER re-enqueues them', async () => {
    // Ch 3 is STAGED (waiting for publication barrier)
    const adapter: SourceAdapter = {
      id: 'nexus',
      name: 'Nexus',
      baseUrl: 'https://nexus.com',
      fetchUpdatedWorks: vi.fn(),
      fetchWorkDetails: vi.fn(),
      fetchChapters: vi.fn(async () => [
        { sourceChapterId: 'ch-1', number: 1, title: 'Cap 1' },
        { sourceChapterId: 'ch-2', number: 2, title: 'Cap 2' },
        { sourceChapterId: 'ch-3', number: 3, title: 'Cap 3' },
      ]),
      fetchChapterPages: vi.fn(),
    };
    registry.register(adapter);

    mockSupabase = {
      from: vi.fn((table: string) => {
        if (table === 'importer_work_mappings') {
          return {
            select: vi.fn().mockReturnThis(),
            eq: vi.fn().mockReturnThis(),
            order: vi.fn().mockReturnThis(),
            limit: vi.fn().mockResolvedValue({
              data: [
                {
                  id: 'map-1',
                  work_id: workId,
                  source: 'nexus',
                  source_work_id: 'nx-1',
                  works: { title: 'Test Work' },
                },
              ],
              error: null,
            }),
          };
        }
        if (table === 'chapters') {
          return {
            select: vi.fn().mockReturnThis(),
            eq: vi.fn().mockReturnThis(),
            not: vi.fn().mockResolvedValue({
              data: [{ id: 'c1', number: 1, published_at: '2026-09-01T00:00:00Z' }],
            }),
          };
        }
        if (table === 'importer_chapter_mappings') {
          return {
            select: vi.fn().mockReturnThis(),
            eq: vi.fn().mockResolvedValue({
              data: [
                { chapter_sort_key: 3, chapter_number: 3, status: 'STAGED' },
              ],
            }),
            upsert: vi.fn().mockResolvedValue({ error: null }),
          };
        }
        if (table === 'importer_queue') {
          return {
            select: vi.fn().mockReturnThis(),
            eq: vi.fn().mockReturnThis(),
            in: vi.fn().mockResolvedValue({ data: [] }),
          };
        }
        return {};
      }),
    };

    reconciler = new ExistingWorksReconciler(mockSupabase, mockQueue, registry);
    const stats = await reconciler.reconcileExistingWorks(10);

    // Ch 3 was STAGED, so it was skipped
    expect(stats.stagedSkipped).toBe(1);
    // Only Ch 2 (the missing predecessor gap) is enqueued!
    expect(enqueuedJobs.length).toBe(1);
    expect(enqueuedJobs[0].sortKey).toBe(2);
    expect(enqueuedJobs[0].priority).toBe(70);
  });

  it('selects higher-priority source when multiple sources have the same chapter', async () => {
    // Both Kuro (prio 100) and MangaFlix (prio 40) provide Ch 4
    const kuroAdapter: SourceAdapter = {
      id: 'kuro',
      name: 'Kuro',
      baseUrl: 'https://kuro.com',
      fetchUpdatedWorks: vi.fn(),
      fetchWorkDetails: vi.fn(),
      fetchChapters: vi.fn(async () => [
        { sourceChapterId: 'kuro-ch-4', number: 4, title: 'Cap 4' },
      ]),
      fetchChapterPages: vi.fn(),
    };
    const mfAdapter: SourceAdapter = {
      id: 'mangaflix',
      name: 'MangaFlix',
      baseUrl: 'https://mangaflix.com',
      fetchUpdatedWorks: vi.fn(),
      fetchWorkDetails: vi.fn(),
      fetchChapters: vi.fn(async () => [
        { sourceChapterId: 'mf-ch-4', number: 4, title: 'Cap 4' },
      ]),
      fetchChapterPages: vi.fn(),
    };
    registry.register(kuroAdapter);
    registry.register(mfAdapter);

    mockSupabase = {
      from: vi.fn((table: string) => {
        if (table === 'importer_work_mappings') {
          return {
            select: vi.fn().mockReturnThis(),
            eq: vi.fn().mockReturnThis(),
            order: vi.fn().mockReturnThis(),
            limit: vi.fn().mockResolvedValue({
              data: [
                {
                  id: 'map-mf',
                  work_id: workId,
                  source: 'mangaflix',
                  source_work_id: 'mf-1',
                  works: { title: 'Test Work' },
                },
                {
                  id: 'map-kuro',
                  work_id: workId,
                  source: 'kuro',
                  source_work_id: 'kuro-1',
                  works: { title: 'Test Work' },
                },
              ],
              error: null,
            }),
          };
        }
        if (table === 'chapters') {
          return {
            select: vi.fn().mockReturnThis(),
            eq: vi.fn().mockReturnThis(),
            not: vi.fn().mockResolvedValue({ data: [] }),
          };
        }
        if (table === 'importer_chapter_mappings') {
          return {
            select: vi.fn().mockReturnThis(),
            eq: vi.fn().mockResolvedValue({ data: [] }),
            upsert: vi.fn().mockResolvedValue({ error: null }),
          };
        }
        if (table === 'importer_queue') {
          return {
            select: vi.fn().mockReturnThis(),
            eq: vi.fn().mockReturnThis(),
            in: vi.fn().mockResolvedValue({ data: [] }),
          };
        }
        return {};
      }),
    };

    reconciler = new ExistingWorksReconciler(mockSupabase, mockQueue, registry);
    const stats = await reconciler.reconcileExistingWorks(10);

    // Exactly 1 job enqueued with Kuro as primary source and MangaFlix as fallback
    expect(enqueuedJobs.length).toBe(1);
    expect(enqueuedJobs[0].source).toBe('kuro');
    expect(enqueuedJobs[0].payload.sourceChapterId).toBe('kuro-ch-4');
    expect(enqueuedJobs[0].payload.fallbackSources).toBeDefined();
    expect(enqueuedJobs[0].payload.fallbackSources[0].source).toBe('mangaflix');
  });
});
