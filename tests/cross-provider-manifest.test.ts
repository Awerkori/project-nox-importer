import { describe, it, expect, vi, beforeEach } from 'vitest';
import { ExistingWorksReconciler } from '../src/core/reconciliation.js';
import { SourceRegistry } from '../src/sources/registry.js';
import { SourceAdapter } from '../src/sources/types.js';

describe('Cross-Provider Work Matching & Canonical Chapter Manifest', () => {
  let reconciler: ExistingWorksReconciler;
  let mockSupabase: any;
  let mockQueue: any;
  let registry: SourceRegistry;
  let enqueuedJobs: any[] = [];
  let upsertedManifest: any[] = [];
  let upsertedHealth: any = null;
  let upsertedWorkMappings: any[] = [];

  const workId = '11111111-2222-3333-4444-555555555555';

  beforeEach(() => {
    enqueuedJobs = [];
    upsertedManifest = [];
    upsertedHealth = null;
    upsertedWorkMappings = [];
    registry = new SourceRegistry();

    mockQueue = {
      enqueue: vi.fn(async (taskType, source, dedupeKey, payload, priority, sortKey) => {
        enqueuedJobs.push({ taskType, source, dedupeKey, payload, priority, sortKey });
        return true;
      }),
    };
  });

  it('discovers cross-provider alternative mappings, builds unified manifest and marks unresolved gaps', async () => {
    // Adapter A (Primary Source e.g. manhastro): has chapters 5 to 10
    const adapterA: SourceAdapter = {
      id: 'manhastro',
      name: 'Manhastro',
      baseUrl: 'https://manhastro.net',
      fetchUpdatedWorks: vi.fn(),
      fetchWorkDetails: vi.fn(),
      fetchChapters: vi.fn(async () => [
        { sourceChapterId: 'mh-5', number: 5, title: 'Cap 5', pageCount: 15 },
        { sourceChapterId: 'mh-6', number: 6, title: 'Cap 6', pageCount: 15 },
        { sourceChapterId: 'mh-7', number: 7, title: 'Cap 7', pageCount: 15 },
        { sourceChapterId: 'mh-10', number: 10, title: 'Cap 10', pageCount: 15 },
      ]),
      fetchChapterPages: vi.fn(),
      searchWorks: vi.fn(async () => []),
    };

    // Adapter B (Alternative Source e.g. mangaflix): has chapters 1, 2, 3, 4 (missing beginning!)
    const adapterB: SourceAdapter = {
      id: 'mangaflix',
      name: 'MangaFlix',
      baseUrl: 'https://mangaflix.net',
      fetchUpdatedWorks: vi.fn(),
      fetchWorkDetails: vi.fn(),
      fetchChapters: vi.fn(async () => [
        { sourceChapterId: 'mf-1', number: 1, title: 'Cap 1', pageCount: 22 },
        { sourceChapterId: 'mf-2', number: 2, title: 'Cap 2', pageCount: 22 },
        { sourceChapterId: 'mf-3', number: 3, title: 'Cap 3', pageCount: 22 },
        { sourceChapterId: 'mf-4', number: 4, title: 'Cap 4', pageCount: 22 },
      ]),
      fetchChapterPages: vi.fn(),
      searchWorks: vi.fn(async (q) => [
        {
          sourceWorkId: 'mf-work-1',
          title: 'SandLand',
          slug: 'sandland',
          coverUrl: 'https://static.mangaflix.net/cover.jpg',
        },
      ]),
    };

    registry.register(adapterA);
    registry.register(adapterB);

    mockSupabase = {
      from: vi.fn((table: string) => {
        if (table === 'works') {
          return {
            select: vi.fn().mockReturnThis(),
            eq: vi.fn().mockReturnThis(),
            maybeSingle: vi.fn().mockResolvedValue({
              data: {
                id: workId,
                title: 'SandLand',
                slug: 'sandland',
                kind: 'MANGA',
                aliases: ['Sand Land'],
              },
            }),
          };
        }
        if (table === 'importer_work_mappings') {
          return {
            select: vi.fn().mockReturnThis(),
            eq: vi.fn((field, val) => {
              // Return existing mapping only for manhastro at start
              return {
                select: vi.fn().mockReturnThis(),
                eq: vi.fn().mockReturnThis(),
                maybeSingle: vi.fn().mockResolvedValue({ data: null }),
                then: (cb: any) =>
                  cb({
                    data: [
                      {
                        id: 'map-mh',
                        work_id: workId,
                        source: 'manhastro',
                        source_work_id: 'mh-work-1',
                        confidence_score: 1.0,
                        is_primary: true,
                        sync_status: 'SYNCED',
                      },
                      ...upsertedWorkMappings,
                    ],
                  }),
              };
            }),
            upsert: vi.fn(async (payload) => {
              upsertedWorkMappings.push(payload);
              return { error: null };
            }),
          };
        }
        if (table === 'chapters') {
          return {
            select: vi.fn().mockReturnThis(),
            eq: vi.fn().mockReturnThis(),
            not: vi.fn().mockResolvedValue({
              // Already published in Nox: only chapters 5 and 6
              data: [
                { id: 'c5', number: 5, published_at: '2026-09-01T00:00:00Z' },
                { id: 'c6', number: 6, published_at: '2026-09-01T00:00:00Z' },
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
        if (table === 'importer_chapter_manifest') {
          return {
            upsert: vi.fn(async (chunks) => {
              upsertedManifest.push(...chunks);
              return { error: null };
            }),
          };
        }
        if (table === 'importer_work_health') {
          return {
            upsert: vi.fn(async (payload) => {
              upsertedHealth = payload;
              return { error: null };
            }),
          };
        }
        return {};
      }),
    };

    reconciler = new ExistingWorksReconciler(mockSupabase, mockQueue, registry);
    const result = await reconciler.reconcileWorkManifest(workId);

    // 1. Cross-provider mapping discovery check:
    // MangaFlix was discovered and upserted into importer_work_mappings!
    expect(adapterB.searchWorks).toHaveBeenCalledWith('SandLand');
    expect(upsertedWorkMappings.length).toBeGreaterThan(0);
    expect(upsertedWorkMappings[0].source).toBe('mangaflix');
    expect(upsertedWorkMappings[0].confidence_score).toBeGreaterThanOrEqual(0.85);

    // 2. Canonical Manifest check:
    // Combined chapters: 1, 2, 3, 4 (from MangaFlix), 5, 6, 7, 10 (from Manhastro)
    // Chapters 8 and 9 are missing across all providers!
    expect(result.totalKnownChapters).toBe(8); // 1, 2, 3, 4, 5, 6, 7, 10

    // 3. Enqueued jobs check:
    // Chapters 1, 2, 3, 4 should be backfilled from MangaFlix!
    // Chapter 7 should be queued from Manhastro!
    // Chapter 10 should be queued from Manhastro!
    // (5 and 6 already published, so not re-enqueued)
    const enqueuedNumbers = enqueuedJobs.map((j) => j.sortKey);
    expect(enqueuedNumbers).toContain(1);
    expect(enqueuedNumbers).toContain(2);
    expect(enqueuedNumbers).toContain(3);
    expect(enqueuedNumbers).toContain(4);
    expect(enqueuedNumbers).toContain(7);
    expect(enqueuedNumbers).toContain(10);
    expect(enqueuedNumbers).not.toContain(5); // published!
    expect(enqueuedNumbers).not.toContain(6); // published!

    // Missing start (1, 2, 3, 4) should be enqueued with MangaFlix source!
    const job1 = enqueuedJobs.find((j) => j.sortKey === 1);
    expect(job1.source).toBe('mangaflix');

    // 4. Manifest persistence check:
    expect(upsertedManifest.length).toBe(8);

    // 5. Work Health status:
    expect(upsertedHealth).toBeDefined();
    expect(upsertedHealth.work_id).toBe(workId);
    expect(upsertedHealth.total_known_chapters).toBe(8);
    expect(upsertedHealth.total_imported_chapters).toBe(2); // 5, 6 published
  });
});
