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

  it('filters out PAUSED sources from primary and operational fallback, preserving mappings only', async () => {
    // Kuro = PAUSED, Nexus = ACTIVE, Manhastro = ACTIVE
    // All 3 have Cap 10
    const kuroAdapter: SourceAdapter = {
      id: 'kuro',
      name: 'Kuro',
      baseUrl: 'https://kuro.com',
      fetchUpdatedWorks: vi.fn(),
      fetchWorkDetails: vi.fn(),
      fetchChapters: vi.fn(async () => [
        { sourceChapterId: 'kuro-ch-10', number: 10, title: 'Cap 10', pageCount: 25 },
      ]),
      fetchChapterPages: vi.fn(),
    };
    const nexusAdapter: SourceAdapter = {
      id: 'nexus',
      name: 'Nexus',
      baseUrl: 'https://nexus.com',
      fetchUpdatedWorks: vi.fn(),
      fetchWorkDetails: vi.fn(),
      fetchChapters: vi.fn(async () => [
        { sourceChapterId: 'nexus-ch-10', number: 10, title: 'Cap 10', pageCount: 25 },
      ]),
      fetchChapterPages: vi.fn(),
    };
    const manhastroAdapter: SourceAdapter = {
      id: 'manhastro',
      name: 'Manhastro',
      baseUrl: 'https://manhastro.com',
      fetchUpdatedWorks: vi.fn(),
      fetchWorkDetails: vi.fn(),
      fetchChapters: vi.fn(async () => [
        { sourceChapterId: 'manhastro-ch-10', number: 10, title: 'Cap 10', pageCount: 25 },
      ]),
      fetchChapterPages: vi.fn(),
    };

    registry.register(kuroAdapter);
    registry.register(nexusAdapter);
    registry.register(manhastroAdapter);

    const savedMappings: any[] = [];

    mockSupabase = {
      from: vi.fn((table: string) => {
        if (table === 'importer_sources') {
          return {
            select: vi.fn().mockResolvedValue({
              data: [
                { id: 'kuro', status: 'PAUSED', enabled: true, cooldown_until: null },
                { id: 'nexus', status: 'ACTIVE', enabled: true, cooldown_until: null },
                { id: 'manhastro', status: 'ACTIVE', enabled: true, cooldown_until: null },
              ],
            }),
          };
        }
        if (table === 'importer_work_mappings') {
          return {
            select: vi.fn().mockReturnThis(),
            eq: vi.fn().mockReturnThis(),
            order: vi.fn().mockReturnThis(),
            limit: vi.fn().mockResolvedValue({
              data: [
                { id: 'map-kuro', work_id: workId, source: 'kuro', source_work_id: 'k-1', works: { title: 'Test' } },
                { id: 'map-nexus', work_id: workId, source: 'nexus', source_work_id: 'n-1', works: { title: 'Test' } },
                { id: 'map-manhastro', work_id: workId, source: 'manhastro', source_work_id: 'm-1', works: { title: 'Test' } },
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
            upsert: vi.fn((record) => {
              savedMappings.push(record);
              return Promise.resolve({ error: null });
            }),
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

    // 1. Um único canonical chapter enfileirado
    expect(enqueuedJobs.length).toBe(1);
    const job = enqueuedJobs[0];
    expect(job.dedupeKey).toBe(`work:${workId}:chapter:10`);

    // 2. Nexus vira provider operacional primário (mesmo Kuro tendo score nominal 100)
    expect(job.source).toBe('nexus');
    expect(job.payload.sourceChapterId).toBe('nexus-ch-10');

    // 3. Manhastro entra como fallback operacional executável
    expect(job.payload.fallbackSources).toBeDefined();
    expect(job.payload.fallbackSources.length).toBe(1);
    expect(job.payload.fallbackSources[0].source).toBe('manhastro');

    // 4. Kuro NÃO está presente no fallback operacional executável
    const hasKuroInFallback = job.payload.fallbackSources.some((s: any) => s.source === 'kuro');
    expect(hasKuroInFallback).toBe(false);

    // 5. Todos os 3 mappings foram preservados no banco
    expect(savedMappings.length).toBe(3);
    const kuroMapping = savedMappings.find((m) => m.source === 'kuro');
    const nexusMapping = savedMappings.find((m) => m.source === 'nexus');
    const manhastroMapping = savedMappings.find((m) => m.source === 'manhastro');

    expect(nexusMapping.is_page_provider).toBe(true);
    expect(manhastroMapping.is_page_provider).toBe(false);
    expect(kuroMapping.is_page_provider).toBe(false); // Kuro preservado no banco para uso futuro!
  });

  it('correctly orders blocking chain so lowest pending predecessor gets priority 90 first', () => {
    // Simulação determinística da query SQL de aquisição (importer_acquire_job)
    // Cenário:
    // Cap 1 -> RETRY (prio 70)
    // Cap 2 -> RETRY (prio 70)
    // Cap 3 -> STAGED
    // Cap 4 -> STAGED
    interface QueueItem {
      id: string;
      workId: string;
      sortKey: number;
      priority: number;
      status: string;
    }
    const stagedKeys = [3, 4];
    const queue: QueueItem[] = [
      { id: 'job-1', workId, sortKey: 1, priority: 70, status: 'RETRY' },
      { id: 'job-2', workId, sortKey: 2, priority: 70, status: 'RETRY' },
    ];

    function calculateDynamicPriority(job: QueueItem, activeQueue: QueueItem[], staged: number[]): number {
      const hasStagedAfter = staged.some((s) => s > job.sortKey);
      if (!hasStagedAfter) return job.priority;

      // Cabeça da cadeia: NÃO pode existir nenhum outro job ativo com sortKey < job.sortKey
      const hasPredecessor = activeQueue.some((q) => q.id !== job.id && q.sortKey < job.sortKey);
      if (hasPredecessor) return job.priority;

      return 90; // Concedido apenas para a cabeça da cadeia
    }

    // Passo 1: Inicialmente Cap 1 é a cabeça da cadeia -> recebe 90. Cap 2 continua com 70.
    const prioJob1 = calculateDynamicPriority(queue[0], queue, stagedKeys);
    const prioJob2 = calculateDynamicPriority(queue[1], queue, stagedKeys);

    expect(prioJob1).toBe(90);
    expect(prioJob2).toBe(70);

    // Passo 2: Cap 1 conclui e é removido da fila ativa
    const queueAfterJob1 = queue.filter((q) => q.id !== 'job-1');

    // Agora Cap 2 é a nova cabeça da cadeia -> recebe 90!
    const prioJob2AfterJob1 = calculateDynamicPriority(queueAfterJob1[0], queueAfterJob1, stagedKeys);
    expect(prioJob2AfterJob1).toBe(90);
  });
});
