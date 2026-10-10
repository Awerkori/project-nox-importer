/**
 * Project Nox — Work-Affinity Scheduler Test Suite
 * 
 * Verifies all 8 mandatory behavioral contracts:
 * - Test A: P0 (Fresh Release Preemption)
 * - Test B: Work Affinity (Steady progression on admitted works)
 * - Test C: Fairness (MAX_INFLIGHT_PER_WORK = 2 prevents monopoly)
 * - Test D: Slot Release on Caught-Up (Vacates slot and admits new work)
 * - Test E: Critical Gap Prioritization (BARRIER_UNBLOCK_SCORE unblocks STAGED cascade)
 * - Test F: Source Down & Auto-Healing Recovery (Work BLOCKED without global stall)
 * - Test G: P0 During P2 (P0 claims next free slot during active P2 filling)
 * - Test H: Restart Persistence (Active sets and watermarks survive re-instantiation)
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';
import { PGlite } from '@electric-sql/pglite';
import {
  SchedulerLane,
  WorkSchedulerState,
  ActiveWork,
  WorkWatermark,
} from '../src/core/scheduler/types.js';
import { SchedulerStateStore } from '../src/core/scheduler/state-store.js';
import { AdmissionController } from '../src/core/scheduler/admission-controller.js';
import { WorkAffinityScheduler } from '../src/core/scheduler/work-affinity-scheduler.js';
import {
  shouldRunCatalogFallbackAgain,
  shouldStartCatalogProbe,
  selectRotatingSourceWindow,
} from '../src/core/scheduler/work-affinity-scheduler.js';

describe('Project Nox — Work-Affinity Scheduler Tests A-H', () => {
  it('does not repeat an empty catalog fallback in the same claim attempt', () => {
    expect(shouldRunCatalogFallbackAgain(true, true)).toBe(false);
    expect(shouldRunCatalogFallbackAgain(true, false)).toBe(true);
    expect(shouldRunCatalogFallbackAgain(false, true)).toBe(true);
  });

  it('serializes catalog probes and enforces the short hand-off interval', () => {
    expect(shouldStartCatalogProbe(1_000, 0, false)).toBe(true);
    expect(shouldStartCatalogProbe(1_999, 1_000, false)).toBe(false);
    expect(shouldStartCatalogProbe(2_000, 1_000, false)).toBe(true);
    expect(shouldStartCatalogProbe(10_000, 0, true)).toBe(false);
  });

  it('rotates a bounded catalog source window without starving later sources', () => {
    expect(selectRotatingSourceWindow(['a', 'b', 'c', 'd'], 0, 2)).toEqual({
      sources: ['a', 'b'], nextCursor: 2,
    });
    expect(selectRotatingSourceWindow(['a', 'b', 'c', 'd'], 2, 2)).toEqual({
      sources: ['c', 'd'], nextCursor: 0,
    });
  });

  it('keeps the catalog fallback P1-only and filters blocked rows before its bounded source window', async () => {
    const query = vi.fn().mockResolvedValue({ rows: [] });
    const client = { query };
    const state = {
      getConfig: () => ({ maxInflightPerWork: 2 }),
      getActiveWork: () => undefined,
      setActiveWork: vi.fn(),
    } as any;
    const localScheduler = new WorkAffinityScheduler(state, {} as any, {} as any);

    await expect((localScheduler as any).executeClaimCatalogQuery(client, {
      workerId: 'catalog-test', leaseMin: 5, allowedSources: ['healthy-source'],
    })).resolves.toBeNull();

    const sql = String(query.mock.calls[0][0]);
    const sourceWindow = sql.indexOf('source_window AS MATERIALIZED');
    const sourceWindowLimit = sql.indexOf('LIMIT $6', sourceWindow);
    expect(sql.indexOf('q.priority >= 75 AND q.priority < 100', sourceWindow)).toBeLessThan(sourceWindowLimit);
    expect(sql.indexOf("COALESCE(q.payload->>'staffForced', 'false') <> 'true'", sourceWindow)).toBeLessThan(sourceWindowLimit);
    expect(sql.indexOf('canonical_chapter.published_at IS NOT NULL', sourceWindow)).toBeLessThan(sourceWindowLimit);
    expect(sql.indexOf('predecessor.chapter_sort_key < q.chapter_sort_key', sourceWindow)).toBeLessThan(sourceWindowLimit);
    expect(sql.indexOf("active_chapter.status = 'IMPORTING'", sourceWindow)).toBeLessThan(sourceWindowLimit);
    const canonicalRows = sql.indexOf('canonical_rows AS MATERIALIZED');
    const canonicalFilter = sql.indexOf('canonical_chapter.published_at IS NOT NULL', canonicalRows);
    const frontierRows = sql.indexOf('frontier_rows AS MATERIALIZED', canonicalRows);
    expect(canonicalFilter).toBeGreaterThan(canonicalRows);
    expect(frontierRows).toBeGreaterThan(canonicalFilter);
    expect(sql).toContain('SELECT DISTINCT ON (source, work_id) id');
  });

  let mockPool: any;
  let mockStateStore: any;
  let mockAdmissionController: any;
  let mockSentinel: any;
  let scheduler: WorkAffinityScheduler;

  beforeEach(() => {
    // In-memory state store for deterministic testing
    const activeWorks = new Map<string, ActiveWork>();
    const watermarks = new Map<string, WorkWatermark>();
    let config = {
      enabled: true,
      shadowMode: false,
      maxActiveNewWorks: 8,
      maxActiveBackfillWorks: 10,
      maxInflightPerWork: 2,
      slidingWindowSize: 8,
      slidingWindowMin: 3,
      antiStarvationRatio: 4,
    };

    mockStateStore = {
      initialize: vi.fn().mockResolvedValue(undefined),
      getConfig: vi.fn(() => ({ ...config })),
      updateConfig: vi.fn((patch) => { config = { ...config, ...patch }; }),
      getActiveWorks: vi.fn(() => Array.from(activeWorks.values())),
      getActiveWork: vi.fn((id: string) => activeWorks.get(id)),
      setActiveWork: vi.fn((work: ActiveWork) => { activeWorks.set(work.workId, work); }),
      removeActiveWork: vi.fn((id: string) => activeWorks.delete(id)),
      getWatermark: vi.fn((workId: string, source: string) => watermarks.get(`${source}:${workId}`)),
      setWatermark: vi.fn((wm: WorkWatermark) => { watermarks.set(`${wm.source}:${wm.workId}`, wm); }),
      saveMetrics: vi.fn().mockResolvedValue(undefined),
      getLatestMetrics: vi.fn().mockReturnValue(null),
    };

    mockSentinel = {
      isProtectiveStopActive: vi.fn().mockResolvedValue(false),
    };

    mockAdmissionController = {
      start: vi.fn(),
      stop: vi.fn(),
      runAdmissionCycle: vi.fn().mockResolvedValue(undefined),
    };

    scheduler = new WorkAffinityScheduler(mockStateStore, mockAdmissionController, mockSentinel);
  });

  // =========================================================================
  // TEST A: P0 (Preempção de capítulo novo de obra caught-up)
  // =========================================================================
  it('TEST A: P0 Fresh Release preempts immediately ahead of P1 and P2', async () => {
    // Obra Solo Leveling (already tracked) receives chapter 201
    const p0Job = {
      id: 'job-solo-201',
      source: 'mangaflix',
      priority: 100,
      chapter_sort_key: 201,
      payload: {
        workId: 'solo-leveling-id',
        chapterTitle: 'Solo Leveling #201',
        chapterNumber: 201,
        isFreshRelease: true,
      },
    };

    // Mock client returning P0 job on priority >= 100 query
    const mockClient = {
      query: vi.fn().mockImplementation((queryText: string, params: any[]) => {
        if (params && params[1] === 100) {
          // P0 query
          return { rows: [p0Job] };
        }
        return { rows: [] };
      }),
      release: vi.fn(),
    };

    (scheduler as any).pool = {
      connect: vi.fn().mockResolvedValue(mockClient),
    };

    const acquired = await scheduler.acquireNextChapterJob({
      workerId: 'worker-1',
      leaseDurationMinutes: 5,
      allowedSources: ['mangaflix'],
    });

    expect(acquired).not.toBeNull();
    expect(acquired.id).toBe('job-solo-201');
    expect(acquired.priority).toBe(100);
    expect(scheduler.getInFlightCount('solo-leveling-id')).toBe(1);
  });

  it('keeps an eligible P0 ahead of P1 after a sustained high-priority streak', async () => {
    const p1WorkId = 'p1-progress-work';
    const p0WorkId = 'p0-release-work';
    mockStateStore.setActiveWork({
      workId: p1WorkId,
      workTitle: 'P1 backfill',
      lane: 'P1',
      state: 'FILLING',
      primarySource: 'mangaflix',
      admittedAt: new Date().toISOString(),
      lastActivityAt: new Date().toISOString(),
      totalChapters: 20,
      publishedChapters: 5,
      queuedChapters: 3,
      inFlightChapters: 0,
      frontierSortKey: 6,
      criticalGapSortKey: null,
      criticalGapUnblockCount: 0,
    });
    (scheduler as any).highPriorityConsecutiveClaims = 4;

    const p0Job = {
      id: 'p0-release-job', source: 'mangaflix', priority: 100, chapter_sort_key: 201,
      payload: { workId: p0WorkId, chapterNumber: 201, isFreshRelease: true },
    };
    vi.spyOn(scheduler as any, 'hasStaffForcedCandidate').mockResolvedValue(false);
    vi.spyOn(scheduler as any, 'getP0CandidateWorkIds').mockResolvedValue([p0WorkId]);
    vi.spyOn(scheduler as any, 'claimSingleJob').mockImplementation(async (_pool: unknown, opts: any) => {
      if (opts.workId === p0WorkId) return p0Job;
      throw new Error(`lower-priority path reached before P0: ${opts.workId}`);
    });

    const acquired = await scheduler.acquireNextChapterJob({
      workerId: 'worker-strict-p0', allowedSources: ['mangaflix'],
    });

    expect(acquired?.id).toBe('p0-release-job');
    expect((scheduler as any).highPriorityConsecutiveClaims).toBe(5);
  });

  it('tries another known P0 before P1/P2 when the round-robin target races away', async () => {
    const racedP0WorkId = 'p0-raced-work';
    const readyP0WorkId = 'p0-ready-work';
    const p0Job = {
      id: 'p0-ready-job', source: 'mangaflix', priority: 100, chapter_sort_key: 202,
      payload: { workId: readyP0WorkId, chapterNumber: 202, isFreshRelease: true },
    };
    mockStateStore.setActiveWork({
      workId: 'p1-work', workTitle: 'P1 backfill', lane: 'P1', state: 'FILLING',
      primarySource: 'mangaflix', admittedAt: new Date().toISOString(), lastActivityAt: new Date().toISOString(),
      totalChapters: 2, publishedChapters: 1, queuedChapters: 1, inFlightChapters: 0,
      frontierSortKey: 2, criticalGapSortKey: null, criticalGapUnblockCount: 0,
    });
    vi.spyOn(scheduler as any, 'hasStaffForcedCandidate').mockResolvedValue(false);
    vi.spyOn(scheduler as any, 'getP0CandidateWorkIds').mockResolvedValue([racedP0WorkId, readyP0WorkId]);
    vi.spyOn(scheduler as any, 'claimSingleJob').mockImplementation(async (_pool: unknown, opts: any) => {
      if (opts.workId === racedP0WorkId) return null;
      if (opts.allowedWorkIds?.includes(readyP0WorkId) && opts.minPriority === 100) return p0Job;
      throw new Error('lower-priority path reached while P0 remained eligible');
    });

    const acquired = await scheduler.acquireNextChapterJob({
      workerId: 'worker-p0-race', allowedSources: ['mangaflix'],
    });

    expect(acquired?.id).toBe('p0-ready-job');
    expect(acquired?.priority).toBe(100);
  });

  it('keeps an eligible STAFF job ahead of P0, P1, and P2', async () => {
    const staffJob = {
      id: 'staff-job', source: 'mangaflix', priority: 1000, chapter_sort_key: 1,
      payload: { workId: 'staff-work', chapterNumber: 1, staffForced: true },
    };
    vi.spyOn(scheduler as any, 'hasStaffForcedCandidate').mockResolvedValue(true);
    vi.spyOn(scheduler as any, 'claimStaffForcedJob').mockResolvedValue(staffJob);
    vi.spyOn(scheduler as any, 'getP0CandidateWorkIds').mockImplementation(async () => {
      throw new Error('P0 must not be probed before an eligible STAFF claim');
    });
    vi.spyOn(scheduler as any, 'claimSingleJob').mockImplementation(async () => {
      throw new Error('P1/P2 must not be reached before an eligible STAFF claim');
    });

    const acquired = await scheduler.acquireNextChapterJob({
      workerId: 'worker-staff', allowedSources: ['mangaflix'],
    });

    expect(acquired?.id).toBe('staff-job');
  });

  it('claims an untracked eligible P1 catalog frontier before an active P2 work', async () => {
    const p2WorkId = 'p2-new-work';
    const catalogP1Job = {
      id: 'catalog-p1-job', source: 'mangaflix', priority: 75, chapter_sort_key: 2,
      payload: { workId: 'catalog-p1-work', chapterNumber: 2 },
    };
    mockStateStore.setActiveWork({
      workId: p2WorkId,
      workTitle: 'P2 new work',
      lane: 'P2',
      state: 'FILLING',
      primarySource: 'mangaflix',
      admittedAt: new Date().toISOString(),
      lastActivityAt: new Date().toISOString(),
      totalChapters: 8,
      publishedChapters: 0,
      queuedChapters: 1,
      inFlightChapters: 0,
      frontierSortKey: 1,
      criticalGapSortKey: null,
      criticalGapUnblockCount: 0,
    });
    vi.spyOn(scheduler as any, 'hasStaffForcedCandidate').mockResolvedValue(false);
    vi.spyOn(scheduler as any, 'getP0CandidateWorkIds').mockResolvedValue([]);
    vi.spyOn(scheduler as any, 'claimCatalogP1Job').mockResolvedValue(catalogP1Job);
    vi.spyOn(scheduler as any, 'claimSingleJob').mockImplementation(async () => {
      throw new Error('P2 claim attempted before eligible catalog P1');
    });

    const acquired = await scheduler.acquireNextChapterJob({
      workerId: 'worker-strict-p1', allowedSources: ['mangaflix'],
    });

    expect(acquired?.id).toBe('catalog-p1-job');
  });

  it('allows P2 only after STAFF, P0, and every P1 claim path is empty', async () => {
    const p2WorkId = 'p2-only-work';
    const p2Job = {
      id: 'p2-job', source: 'mangaflix', priority: 50, chapter_sort_key: 1,
      payload: { workId: p2WorkId, chapterNumber: 1 },
    };
    mockStateStore.setActiveWork({
      workId: p2WorkId,
      workTitle: 'P2 only work',
      lane: 'P2', state: 'FILLING', primarySource: 'mangaflix',
      admittedAt: new Date().toISOString(), lastActivityAt: new Date().toISOString(),
      totalChapters: 1, publishedChapters: 0, queuedChapters: 1, inFlightChapters: 0,
      frontierSortKey: 1, criticalGapSortKey: null, criticalGapUnblockCount: 0,
    });
    vi.spyOn(scheduler as any, 'hasStaffForcedCandidate').mockResolvedValue(false);
    vi.spyOn(scheduler as any, 'getP0CandidateWorkIds').mockResolvedValue([]);
    vi.spyOn(scheduler as any, 'claimCatalogP1Job').mockResolvedValue(null);
    vi.spyOn(scheduler as any, 'claimSingleJob').mockImplementation(async (_pool: unknown, opts: any) => {
      if (opts.workId === p2WorkId) return p2Job;
      throw new Error(`unexpected higher-priority claim: ${opts.workId}`);
    });

    const acquired = await scheduler.acquireNextChapterJob({
      workerId: 'worker-p2', allowedSources: ['mangaflix'],
    });

    expect(acquired?.id).toBe('p2-job');
  });

  it('moves only retry-budget-exhausted queued/retry work out of the hot queue in a bounded statement', async () => {
    const query = vi.fn().mockResolvedValue({ rows: [{ count: 2 }] });
    (scheduler as any).pool = { query };

    const result = await scheduler.runControlledExhaustedJobCleanup(999);

    expect(result).toEqual({ failed: 2 });
    const [sql, params] = query.mock.calls[0];
    expect(sql).toContain("status IN ('QUEUED', 'RETRY')");
    expect(sql).toContain('attempts >= COALESCE(max_attempts, 7)');
    expect(sql).toContain("SET status = 'FAILED'");
    expect(sql).toContain('FOR UPDATE SKIP LOCKED');
    expect(params).toEqual([500]);
  });

  it('guards direct claims behind the canonical publication frontier', async () => {
    const query = vi.fn().mockResolvedValue({ rows: [] });
    const client = { query, release: vi.fn() };
    (scheduler as any).pool = { connect: vi.fn().mockResolvedValue(client) };

    await (scheduler as any).claimSingleJob((scheduler as any).pool, {
      workerId: 'frontier-guard',
      leaseMin: 5,
      allowedSources: null,
    });

    const sql = query.mock.calls[0][0] as string;
    expect(sql).toContain('predecessor.chapter_sort_key < q.chapter_sort_key');
    expect(sql).toContain("staged_frontier.status IN ('STAGED', 'WAITING_FOR_GAP')");
    expect(sql).toContain('pub.max_published + 1.5');
    expect(sql).toContain('importer_confirmed_gaps');
  });

  it('builds the P0 work list from canonical executable rows before its 32-work bound', async () => {
    const db = new PGlite();
    const previousNodeEnv = process.env.NODE_ENV;
    const p0Query = vi.fn((sql: string, params?: any[]) => db.query(sql, params));
    const blockedWorkIds = Array.from({ length: 24 }, (_, index) =>
      `00000000-0000-0000-0000-${String(index + 1).padStart(12, '0')}`,
    );
    const nagatoroWorkId = '00000000-0000-0000-0000-000000000025';
    const retryWorkId = '00000000-0000-0000-0000-000000000026';

    try {
      await db.exec(`
        CREATE TABLE importer_queue (
          task_type text NOT NULL,
          status text NOT NULL,
          payload jsonb NOT NULL,
          attempts integer NOT NULL DEFAULT 0,
          max_attempts integer,
          priority integer NOT NULL,
          chapter_sort_key numeric NOT NULL,
          source text NOT NULL,
          next_run_at timestamptz NOT NULL
        );
        CREATE TABLE importer_sources (
          id text PRIMARY KEY,
          enabled boolean NOT NULL,
          status text NOT NULL,
          cooldown_until timestamptz,
          blocked_reason text,
          blocked_details jsonb
        );
        CREATE TABLE chapters (work_id uuid NOT NULL, number numeric NOT NULL, published_at timestamptz);
        CREATE TABLE importer_chapter_mappings (
          work_id uuid NOT NULL,
          chapter_sort_key numeric NOT NULL,
          status text NOT NULL,
          is_gap boolean NOT NULL DEFAULT false
        );
        CREATE TABLE importer_confirmed_gaps (
          work_id uuid NOT NULL,
          start_sort_key numeric NOT NULL,
          end_sort_key numeric NOT NULL
        );
        INSERT INTO importer_sources (id, enabled, status) VALUES ('healthy-source', true, 'ACTIVE');
      `);

      // These rows are deliberately ordered before the valid releases. They
      // model healthy sources such as Montetai/Nebulosa whose P0 row is behind
      // an open canonical predecessor, so a post-LIMIT filter would starve
      // the release that is ready now.
      for (const workId of blockedWorkIds) {
        await db.query(
          `INSERT INTO importer_queue
             (task_type, status, payload, attempts, max_attempts, priority, chapter_sort_key, source, next_run_at)
           VALUES
             ('IMPORT_CHAPTER', 'IMPORTING', $1::jsonb, 0, 7, 100, 1, 'healthy-source', NOW() - INTERVAL '1 day'),
             ('IMPORT_CHAPTER', 'QUEUED', $1::jsonb, 0, 7, 100, 2, 'healthy-source', NOW() - INTERVAL '1 day')`,
          [JSON.stringify({ workId })],
        );
      }

      // Mirrors MangaFlix / Nagatoro #130: max published is 129, chapter 130
      // is queued and due, and no predecessor is open.
      await db.query(
        `INSERT INTO chapters (work_id, number, published_at) VALUES ($1::uuid, 129, NOW())`,
        [nagatoroWorkId],
      );
      await db.query(
        `INSERT INTO importer_queue
           (task_type, status, payload, attempts, max_attempts, priority, chapter_sort_key, source, next_run_at)
         VALUES ('IMPORT_CHAPTER', 'QUEUED', $1::jsonb, 0, 7, 100, 130, 'healthy-source', NOW())`,
        [JSON.stringify({ workId: nagatoroWorkId })],
      );

      // RETRY remains eligible when due and canonical; the pre-list must not
      // accidentally narrow the existing QUEUED/RETRY behavior.
      await db.query(
        `INSERT INTO importer_queue
           (task_type, status, payload, attempts, max_attempts, priority, chapter_sort_key, source, next_run_at)
         VALUES ('IMPORT_CHAPTER', 'RETRY', $1::jsonb, 1, 7, 100, 1, 'healthy-source', NOW() - INTERVAL '1 minute')`,
        [JSON.stringify({ workId: retryWorkId })],
      );

      process.env.NODE_ENV = 'production';
      (scheduler as any).pool = { query: p0Query };

      const candidateWorkIds = await (scheduler as any).getP0CandidateWorkIds();
      expect(candidateWorkIds).toEqual(expect.arrayContaining([nagatoroWorkId, retryWorkId]));
      expect(candidateWorkIds).toHaveLength(2);
      expect(candidateWorkIds).not.toEqual(expect.arrayContaining(blockedWorkIds));

      const sql = p0Query.mock.calls[0][0] as string;
      const limitOffset = sql.indexOf('LIMIT 32');
      expect(sql).toContain('MAX(c.number) AS max_published');
      expect(sql).toContain('canonical_chapter.published_at IS NOT NULL');
      expect(sql).toContain('predecessor.chapter_sort_key < q.chapter_sort_key');
      expect(sql).toContain("staged_frontier.status IN ('STAGED', 'WAITING_FOR_GAP')");
      expect(sql).toContain('active_chapter.status = \'IMPORTING\'');
      expect(sql).toContain("q.status = 'RETRY' AND q.next_run_at <= NOW()");
      expect(sql.indexOf('predecessor.chapter_sort_key < q.chapter_sort_key')).toBeLessThan(limitOffset);
      expect(sql.indexOf('active_chapter.status = \'IMPORTING\'')).toBeLessThan(limitOffset);
    } finally {
      process.env.NODE_ENV = previousNodeEnv;
      await db.close();
    }
  });

  it('keeps P0 round-robin fairness within the canonical executable work list', async () => {
    const previousNodeEnv = process.env.NODE_ENV;
    const p0WorkIds = ['p0-executable-a', 'p0-executable-b'];
    const p0Jobs = new Map([
      ['p0-executable-a', {
        id: 'job-p0-a', source: 'healthy-source', priority: 100, chapter_sort_key: 130,
        payload: { workId: 'p0-executable-a', chapterNumber: 130 },
      }],
      ['p0-executable-b', {
        id: 'job-p0-b', source: 'healthy-source', priority: 100, chapter_sort_key: 131,
        payload: { workId: 'p0-executable-b', chapterNumber: 131 },
      }],
    ]);
    const client = {
      query: vi.fn((sql: string, params: any[]) => {
        if (sql.includes('importer_staff_requests') || sql.includes('priority >= 1000')) return { rows: [] };
        if (sql.includes("SELECT q.payload->>'workId' AS work_id")) {
          return { rows: p0WorkIds.map((work_id) => ({ work_id })) };
        }
        if (sql.includes('UPDATE importer_queue q')) {
          const job = p0Jobs.get(params?.[2]);
          return { rows: job ? [job] : [] };
        }
        return { rows: [] };
      }),
      release: vi.fn(),
    };

    try {
      process.env.NODE_ENV = 'production';
      (scheduler as any).pool = {
        query: vi.fn(),
        connect: vi.fn().mockResolvedValue(client),
      };

      const first = await scheduler.acquireNextChapterJob({
        workerId: 'p0-fairness-1', allowedSources: ['healthy-source'],
      });
      scheduler.onJobFinished('p0-executable-a', 130);
      const second = await scheduler.acquireNextChapterJob({
        workerId: 'p0-fairness-2', allowedSources: ['healthy-source'],
      });

      expect(first?.id).toBe('job-p0-a');
      expect(second?.id).toBe('job-p0-b');
      const claimedWorkIds = client.query.mock.calls
        .filter(([sql]: [string]) => sql.includes('UPDATE importer_queue q'))
        .map(([, params]: [string, any[]]) => params[2]);
      expect(claimedWorkIds).toEqual(p0WorkIds);
    } finally {
      process.env.NODE_ENV = previousNodeEnv;
    }
  });

  it('claims an executable catalog P1 before on-demand admission when the active set is empty', async () => {
    const catalogJob = {
      id: 'catalog-p1-job',
      source: 'pinkrosa',
      priority: 75,
      chapter_sort_key: 18,
      payload: { workId: 'catalog-work', chapterNumber: 18, chapterTitle: 'Catalog P1' },
    };
    const query = vi.fn().mockImplementation((sql: string) => {
      // Staff/P0/direct-work probes remain empty; the catalog fallback is the
      // first path that can return this published-work candidate.
      if (sql.includes('JOIN works w') && sql.includes('UPDATE importer_queue')) {
        return { rows: [catalogJob] };
      }
      return { rows: [] };
    });
    const client = { query, release: vi.fn() };
    (scheduler as any).pool = { connect: vi.fn().mockResolvedValue(client) };
    mockAdmissionController.admitNextWorkOnDemand = vi.fn().mockResolvedValue(null);

    const acquired = await scheduler.acquireNextChapterJob({
      workerId: 'catalog-first',
      leaseDurationMinutes: 5,
      allowedSources: ['pinkrosa'],
    });

    expect(acquired?.id).toBe('catalog-p1-job');
    expect(mockAdmissionController.admitNextWorkOnDemand).not.toHaveBeenCalled();
  });

  // =========================================================================
  // TEST B: Work Affinity (Progresso contínuo em obra admitida)
  // =========================================================================
  it('TEST B: Work Affinity maintains sequential progress on admitted new work', async () => {
    const workId = 'nano-machine-id';
    const activeWork: ActiveWork = {
      workId,
      workTitle: 'Nano Machine',
      lane: 'P2',
      state: 'FILLING',
      primarySource: 'kuro',
      admittedAt: new Date().toISOString(),
      lastActivityAt: new Date().toISOString(),
      totalChapters: 200,
      publishedChapters: 0,
      queuedChapters: 8,
      inFlightChapters: 0,
      frontierSortKey: 1,
      criticalGapSortKey: null,
      criticalGapUnblockCount: 0,
    };
    mockStateStore.setActiveWork(activeWork);

    const ch1 = { id: 'job-nano-1', source: 'kuro', priority: 50, chapter_sort_key: 1, payload: { workId, chapterNumber: 1 } };
    const ch2 = { id: 'job-nano-2', source: 'kuro', priority: 50, chapter_sort_key: 2, payload: { workId, chapterNumber: 2 } };

    let callCount = 0;
    const mockClient = {
      query: vi.fn().mockImplementation((queryText: string, params: any[]) => {
        // First claim gets ch1, second gets ch2
        if (params && params[2] === workId) {
          callCount++;
          return { rows: [callCount === 1 ? ch1 : ch2] };
        }
        return { rows: [] };
      }),
      release: vi.fn(),
    };

    (scheduler as any).pool = {
      connect: vi.fn().mockResolvedValue(mockClient),
    };

    // Slot 1 claims
    const claim1 = await scheduler.acquireNextChapterJob({ workerId: 'worker-1', allowedSources: ['kuro'] });
    expect(claim1.id).toBe('job-nano-1');
    expect(scheduler.getInFlightCount(workId)).toBe(1);

    // Slot 2 claims (within max inflight = 2)
    const claim2 = await scheduler.acquireNextChapterJob({ workerId: 'worker-2', allowedSources: ['kuro'] });
    expect(claim2.id).toBe('job-nano-2');
    expect(scheduler.getInFlightCount(workId)).toBe(2);
  });

  it('reserves a known work while its claim is in flight to avoid claim-and-release churn', async () => {
    const workId = 'reserved-work';
    const resolvers: Array<(value: any) => void> = [];
    const mockClient = {
      query: vi.fn(() => new Promise((resolve) => resolvers.push(resolve))),
      release: vi.fn(),
    };
    (scheduler as any).pool = { connect: vi.fn().mockResolvedValue(mockClient) };
    const opts = {
      workerId: 'worker', leaseMin: 5, allowedSources: null,
      workId, telemetry: undefined,
    };

    const first = (scheduler as any).claimSingleJob((scheduler as any).pool, opts);
    const second = (scheduler as any).claimSingleJob((scheduler as any).pool, opts);
    const third = await (scheduler as any).claimSingleJob((scheduler as any).pool, opts);

    expect(third).toBeNull();
    await Promise.resolve();
    expect(mockClient.query).toHaveBeenCalledTimes(2);
    resolvers.forEach((resolve, index) => resolve({ rows: [{
      id: `job-${index}`, payload: { workId }, chapter_sort_key: index + 1,
    }] }));
    await Promise.all([first, second]);
  });

  it('filters canonically published chapters before consuming a chapter claim', async () => {
    const sqlCalls: string[] = [];
    const mockClient = {
      query: vi.fn().mockImplementation((sql: string) => {
        sqlCalls.push(sql);
        return { rows: [] };
      }),
      release: vi.fn(),
    };
    (scheduler as any).pool = { connect: vi.fn().mockResolvedValue(mockClient) };

    await (scheduler as any).claimSingleJob((scheduler as any).pool, {
      workerId: 'canonical-filter-test',
      leaseMin: 5,
      allowedSources: ['mangaflix'],
    });

    expect(sqlCalls).toHaveLength(1);
    expect(sqlCalls[0]).toContain('canonical_chapter.work_id = (q.payload->>\'workId\')::uuid');
    expect(sqlCalls[0]).toContain('canonical_chapter.published_at IS NOT NULL');
    expect(sqlCalls[0]).toContain("canonical_chapter.number = COALESCE(NULLIF(q.payload->>'chapterNumber', '')::numeric, q.chapter_sort_key)");
  });

  it('vacates a stale P2 active entry after repeated exact claim misses', () => {
    const workId = 'stale-p2-work';
    mockStateStore.setActiveWork({
      workId,
      workTitle: 'Stale P2',
      lane: 'P2',
      state: 'FILLING',
      primarySource: 'source',
      admittedAt: new Date().toISOString(),
      lastActivityAt: new Date().toISOString(),
      totalChapters: 1,
      publishedChapters: 0,
      queuedChapters: 1,
      inFlightChapters: 0,
      frontierSortKey: 1,
      criticalGapSortKey: null,
      criticalGapUnblockCount: 0,
    });

    (scheduler as any).noteStaleActiveWorkClaimMiss(workId);
    (scheduler as any).noteStaleActiveWorkClaimMiss(workId);
    expect(mockStateStore.getActiveWork(workId)).toBeDefined();
    (scheduler as any).noteStaleActiveWorkClaimMiss(workId);
    expect(mockStateStore.getActiveWork(workId)).toBeUndefined();
    expect(mockStateStore.removeActiveWork).toHaveBeenCalledWith(workId);
  });

  it('vacates a stale P1 entry with only a persisted in-flight counter', () => {
    const workId = 'stale-p1-work';
    mockStateStore.setActiveWork({
      workId,
      workTitle: 'Stale P1',
      lane: 'P1',
      state: 'FILLING',
      primarySource: 'source',
      admittedAt: new Date().toISOString(),
      lastActivityAt: new Date().toISOString(),
      totalChapters: 1,
      publishedChapters: 0,
      queuedChapters: 0,
      inFlightChapters: 1,
      frontierSortKey: 1,
      criticalGapSortKey: null,
      criticalGapUnblockCount: 0,
    });

    (scheduler as any).noteStaleActiveWorkClaimMiss(workId);
    (scheduler as any).noteStaleActiveWorkClaimMiss(workId);
    expect(mockStateStore.getActiveWork(workId)).toBeDefined();
    (scheduler as any).noteStaleActiveWorkClaimMiss(workId);
    expect(mockStateStore.getActiveWork(workId)).toBeUndefined();
  });

  it('vacates a stale normal P1 entry after exact misses even when state reports queued work', () => {
    const workId = 'stale-normal-p1-work';
    mockStateStore.setActiveWork({
      workId,
      workTitle: 'Stale normal P1',
      lane: 'P1',
      state: 'FILLING',
      primarySource: 'source',
      admittedAt: new Date().toISOString(),
      lastActivityAt: new Date().toISOString(),
      totalChapters: 1,
      publishedChapters: 1,
      queuedChapters: 1,
      inFlightChapters: 0,
      frontierSortKey: 1,
      criticalGapSortKey: null,
      criticalGapUnblockCount: 0,
    });

    (scheduler as any).noteStaleActiveWorkClaimMiss(workId);
    (scheduler as any).noteStaleActiveWorkClaimMiss(workId);
    expect(mockStateStore.getActiveWork(workId)).toBeDefined();
    (scheduler as any).noteStaleActiveWorkClaimMiss(workId);
    expect(mockStateStore.getActiveWork(workId)).toBeUndefined();
    expect(mockStateStore.removeActiveWork).toHaveBeenCalledWith(workId);
  });

  it('rotates a stale critical P1 entry after repeated exact claim misses', async () => {
    const workId = 'stale-critical-p1-work';
    mockStateStore.setActiveWork({
      workId,
      workTitle: 'Stale critical P1',
      lane: 'P1',
      state: 'FILLING',
      primarySource: 'mangaflix',
      admittedAt: new Date().toISOString(),
      lastActivityAt: new Date().toISOString(),
      totalChapters: 1,
      publishedChapters: 49,
      queuedChapters: 1,
      inFlightChapters: 0,
      frontierSortKey: 50,
      criticalGapSortKey: 50,
      criticalGapUnblockCount: 1,
    });
    mockAdmissionController.admitNextWorkOnDemand = vi.fn().mockResolvedValue(null);
    vi.spyOn(scheduler as any, 'hasStaffForcedCandidate').mockResolvedValue(false);
    vi.spyOn(scheduler as any, 'getP0CandidateWorkIds').mockResolvedValue([]);
    vi.spyOn(scheduler as any, 'claimSingleJob').mockResolvedValue(null);
    vi.spyOn(scheduler as any, 'claimCatalogP1Job').mockResolvedValue(null);

    for (let attempt = 0; attempt < 3; attempt++) {
      await scheduler.acquireNextChapterJob({ workerId: 'critical-miss', allowedSources: ['mangaflix'] });
    }

    expect(mockStateStore.getActiveWork(workId)).toBeUndefined();
    expect(mockStateStore.removeActiveWork).toHaveBeenCalledWith(workId);
  });

  // =========================================================================
  // TEST C: Fairness & Max Inflight per Work (MAX_INFLIGHT_PER_WORK = 2)
  // =========================================================================
  it('TEST C: Fairness prevents monopolization with MAX_INFLIGHT_PER_WORK = 2', async () => {
    const workA = 'work-a-giant';
    const workB = 'work-b-fair';

    mockStateStore.setActiveWork({
      workId: workA,
      workTitle: 'Giant Backlog Work A',
      lane: 'P1',
      state: 'FILLING',
      primarySource: 'mangaflix',
      admittedAt: new Date().toISOString(),
      lastActivityAt: new Date().toISOString(),
      totalChapters: 800,
      publishedChapters: 10,
      queuedChapters: 8,
      inFlightChapters: 0,
      frontierSortKey: 11,
      criticalGapSortKey: null,
      criticalGapUnblockCount: 0,
    });

    mockStateStore.setActiveWork({
      workId: workB,
      workTitle: 'Fair Work B',
      lane: 'P1',
      state: 'FILLING',
      primarySource: 'mangaflix',
      admittedAt: new Date().toISOString(),
      lastActivityAt: new Date().toISOString(),
      totalChapters: 50,
      publishedChapters: 5,
      queuedChapters: 8,
      inFlightChapters: 0,
      frontierSortKey: 6,
      criticalGapSortKey: null,
      criticalGapUnblockCount: 0,
    });

    // Simulate workA already having 2 inflight jobs
    scheduler.onJobStarted(workA);
    scheduler.onJobStarted(workA);
    expect(scheduler.getInFlightCount(workA)).toBe(2);

    // Worker 3 attempts to claim: workA is full (inflight=2 >= maxInflight=2)
    // Scheduler must skip workA and assign workB!
    const jobB = { id: 'job-b-6', source: 'mangaflix', priority: 75, chapter_sort_key: 6, payload: { workId: workB, chapterNumber: 6 } };

    const mockClient = {
      query: vi.fn().mockImplementation((queryText: string, params: any[]) => {
        // Should only be queried for workB, never workA
        if (params && params[2] === workB) {
          return { rows: [jobB] };
        }
        return { rows: [] };
      }),
      release: vi.fn(),
    };

    (scheduler as any).pool = {
      connect: vi.fn().mockResolvedValue(mockClient),
    };

    const claim3 = await scheduler.acquireNextChapterJob({ workerId: 'worker-3', allowedSources: ['mangaflix'] });
    expect(claim3.id).toBe('job-b-6');
    expect(claim3.payload.workId).toBe(workB);
    expect(scheduler.getInFlightCount(workB)).toBe(1);
    expect(scheduler.getInFlightCount(workA)).toBe(2); // Still exactly 2
  });

  // =========================================================================
  // TEST D: Slot Release on Caught-Up (Liberação de vaga e admissão)
  // =========================================================================
  it('TEST D: Caught-up work vacates slot and admission controller admits exactly 1 new work', async () => {
    const admission = new AdmissionController(mockStateStore, mockSentinel);

    // Fill active new works to capacity (8/8)
    for (let i = 1; i <= 8; i++) {
      mockStateStore.setActiveWork({
        workId: `work-${i}`,
        workTitle: `Work ${i}`,
        lane: 'P2',
        state: 'FILLING',
        primarySource: 'kuro',
        admittedAt: new Date().toISOString(),
        lastActivityAt: new Date().toISOString(),
        totalChapters: 20,
        publishedChapters: i === 4 ? 20 : 10,
        queuedChapters: i === 4 ? 0 : 5,
        inFlightChapters: 0,
        frontierSortKey: null,
        criticalGapSortKey: null,
        criticalGapUnblockCount: 0,
      });
    }

    expect(mockStateStore.getActiveWorks().length).toBe(8);

    // Mock DB queries for reconcileActiveWorks: work-4 has 0 queued, 0 importing, 0 paused -> CAUGHT_UP
    const mockClient = {
      query: vi.fn().mockImplementation((queryText: string, params: any[]) => {
        // Candidate replenishment queries
        if (queryText.includes('w.published IS FALSE')) {
          return {
            rows: [
              { work_id: 'work-9-new', title: 'Admitted Work 9', source: 'mangaflix', pending_jobs: '30', queued_count: '2', paused_count: '28', min_sort_key: '1' },
            ],
          };
        }
        if (queryText.includes('w.published = true')) {
          return { rows: [] };
        }
        // Work queue reconciliation query
        if (queryText.includes('queued_cnt') || queryText.includes('paused_cnt')) {
          return { rows: params[0].map((work_id: string) => work_id === 'work-4'
            ? { work_id, queued_cnt:'0', importing_cnt:'0', paused_cnt:'0', min_sort_key:null, pub_cnt:'20', max_pub:'20', unimported_cnt:'0', source_status:'ACTIVE' }
            : { work_id, queued_cnt:'5', importing_cnt:'0', paused_cnt:'5', min_sort_key:'10', pub_cnt:'20', max_pub:'20', staged_cnt:'0', source_status:'ACTIVE' }) };
        }
        if (queryText.includes('SELECT q.status') || (queryText.includes('CROSS JOIN LATERAL') && queryText.includes('SELECT 1'))) {
          return { rows: [] };
        }
        if (queryText.includes('FROM chapters')) {
          return { rows: [{ pub_cnt: '20' }] };
        }
        if (queryText.includes('FROM importer_chapter_mappings')) {
          return { rows: [{ staged_cnt: '0', min_staged: null }] };
        }
        return { rows: [] };
      }),
      release: vi.fn(),
    };

    (admission as any).pool = {
      connect: vi.fn().mockResolvedValue(mockClient),
    };

    await admission.runAdmissionCycle();

    // work-4 was removed because it caught up
    expect(mockStateStore.getActiveWork('work-4')).toBeUndefined();
    // work-9-new was admitted to take its place
    expect(mockStateStore.getActiveWork('work-9-new')).toBeDefined();
    // Capacity remains exactly 8
    expect(mockStateStore.getActiveWorks().length).toBe(8);
  });

  // =========================================================================
  // TEST E: Critical Gap Prioritization (BARRIER_UNBLOCK_SCORE)
  // =========================================================================
  it('TEST E: Critical gap that unblocks STAGED cascade gets prioritized with BARRIER_UNBLOCK', async () => {
    const workId = 'work-with-gap';
    // Published: 1-40, Missing: 41, Staged: 42-60 (19 staged chapters waiting!)
    mockStateStore.setActiveWork({
      workId,
      workTitle: 'Work With Gap',
      lane: 'P1',
      state: 'FILLING',
      primarySource: 'mangaflix',
      admittedAt: new Date().toISOString(),
      lastActivityAt: new Date().toISOString(),
      totalChapters: 60,
      publishedChapters: 40,
      queuedChapters: 1,
      inFlightChapters: 0,
      frontierSortKey: 41,
      criticalGapSortKey: 41,
      criticalGapUnblockCount: 19,
    });

    const gapJob = {
      id: 'job-missing-41',
      source: 'mangaflix',
      priority: 95,
      chapter_sort_key: 41,
      payload: { workId, chapterNumber: 41 },
    };

    const mockClient = {
      query: vi.fn().mockImplementation((queryText: string, params: any[]) => {
        // Critical gap query checks workId and sortKey=41
        if (params && params[2] === workId && params[3] === 41) {
          return { rows: [gapJob] };
        }
        return { rows: [] };
      }),
      release: vi.fn(),
    };

    (scheduler as any).pool = {
      connect: vi.fn().mockResolvedValue(mockClient),
    };

    const acquired = await scheduler.acquireNextChapterJob({
      workerId: 'worker-1',
      allowedSources: ['mangaflix'],
    });

    expect(acquired).not.toBeNull();
    expect(acquired.id).toBe('job-missing-41');
    expect(acquired.chapter_sort_key).toBe(41);
    expect(scheduler.getInFlightCount(workId)).toBe(1);
  });

  // =========================================================================
  // TEST F: Source Down / Circuit Breaker Isolation (Importer Never Stalls)
  // =========================================================================
  it('TEST F: Source failure does not stall global importer; other works continue', async () => {
    // Work A uses failing source (down in cooldown)
    const workA = 'work-down-source';
    // Work B uses healthy source
    const workB = 'work-healthy-source';

    mockStateStore.setActiveWork({
      workId: workA,
      workTitle: 'Work on Down Source',
      lane: 'P1',
      state: 'BLOCKED',
      primarySource: 'down_source',
      admittedAt: new Date().toISOString(),
      lastActivityAt: new Date().toISOString(),
      totalChapters: 50,
      publishedChapters: 5,
      queuedChapters: 5,
      inFlightChapters: 0,
      frontierSortKey: 6,
      criticalGapSortKey: null,
      criticalGapUnblockCount: 0,
    });

    mockStateStore.setActiveWork({
      workId: workB,
      workTitle: 'Work on Healthy Source',
      lane: 'P1',
      state: 'FILLING',
      primarySource: 'healthy_source',
      admittedAt: new Date().toISOString(),
      lastActivityAt: new Date().toISOString(),
      totalChapters: 50,
      publishedChapters: 5,
      queuedChapters: 5,
      inFlightChapters: 0,
      frontierSortKey: 6,
      criticalGapSortKey: null,
      criticalGapUnblockCount: 0,
    });

    const jobB = {
      id: 'job-healthy-6',
      source: 'healthy_source',
      priority: 75,
      chapter_sort_key: 6,
      payload: { workId: workB, chapterNumber: 6 },
    };

    const mockClient = {
      query: vi.fn().mockImplementation((queryText: string, params: any[]) => {
        // Allowed sources only includes healthy_source
        if (params && params[0] && params[0].includes('healthy_source') && params[2] === workB) {
          return { rows: [jobB] };
        }
        return { rows: [] };
      }),
      release: vi.fn(),
    };

    (scheduler as any).pool = {
      connect: vi.fn().mockResolvedValue(mockClient),
    };

    // Workers querying with only healthy_source (auto-healer excluded down_source)
    const acquired = await scheduler.acquireNextChapterJob({
      workerId: 'worker-1',
      allowedSources: ['healthy_source'], // down_source excluded!
    });

    expect(acquired).not.toBeNull();
    expect(acquired.id).toBe('job-healthy-6');
    expect(acquired.source).toBe('healthy_source');
    // Work on down source remains BLOCKED without crashing the engine
    expect(mockStateStore.getActiveWork(workA)?.state).toBe('BLOCKED');
  });

  // =========================================================================
  // TEST G: P0 Durante P2 (Injeção de P0 com Active New Works cheios)
  // =========================================================================
  it('TEST G: P0 preempts immediately even when 8 P2 works are actively running', async () => {
    // Set 8 active P2 works running
    for (let i = 1; i <= 8; i++) {
      mockStateStore.setActiveWork({
        workId: `p2-work-${i}`,
        workTitle: `P2 Work ${i}`,
        lane: 'P2',
        state: 'FILLING',
        primarySource: 'mangaflix',
        admittedAt: new Date().toISOString(),
        lastActivityAt: new Date().toISOString(),
        totalChapters: 100,
        publishedChapters: 0,
        queuedChapters: 8,
        inFlightChapters: 1,
        frontierSortKey: 1,
        criticalGapSortKey: null,
        criticalGapUnblockCount: 0,
      });
      scheduler.onJobStarted(`p2-work-${i}`);
    }

    // Now a P0 fresh release arrives for tracked work 'eleceed-id'
    const p0Job = {
      id: 'job-eleceed-300',
      source: 'mangaflix',
      priority: 100,
      chapter_sort_key: 300,
      payload: { workId: 'eleceed-id', chapterTitle: 'Eleceed #300', chapterNumber: 300, isFreshRelease: true },
    };

    const mockClient = {
      query: vi.fn().mockImplementation((queryText: string, params: any[]) => {
        // Priority >= 100 checks P0 first!
        if (params && params[1] === 100) {
          return { rows: [p0Job] };
        }
        return { rows: [] };
      }),
      release: vi.fn(),
    };

    (scheduler as any).pool = {
      connect: vi.fn().mockResolvedValue(mockClient),
    };

    const acquired = await scheduler.acquireNextChapterJob({
      workerId: 'worker-9',
      allowedSources: ['mangaflix'],
    });

    // P0 was claimed immediately without waiting for any P2 work to finish
    expect(acquired).not.toBeNull();
    expect(acquired.id).toBe('job-eleceed-300');
    expect(acquired.priority).toBe(100);
    expect(acquired.payload.isFreshRelease).toBe(true);
  });

  // =========================================================================
  // TEST H: Restart Persistence & Watermark Integrity
  // =========================================================================
  it('TEST H: Active works and watermarks persist and reload intact across restart', async () => {
    const realStateStore = new SchedulerStateStore();
    const storedActiveWorks = [
      {
        workId: 'work-persisted-1',
        workTitle: 'Persisted Work 1',
        lane: 'P1',
        state: 'FILLING',
        primarySource: 'mangaflix',
        admittedAt: '2026-09-20T22:00:00.000Z',
        lastActivityAt: '2026-09-20T22:30:00.000Z',
        totalChapters: 120,
        publishedChapters: 80,
        queuedChapters: 8,
        inFlightChapters: 0,
        frontierSortKey: 81,
        criticalGapSortKey: null,
        criticalGapUnblockCount: 0,
      },
    ];

    const storedWatermarks = {
      'mangaflix:solo-leveling': {
        workId: 'solo-leveling',
        source: 'mangaflix',
        lastSeenChapter: 200,
        lastSeenSortKey: 200,
        lastDiscoveryAt: '2026-09-20T22:00:00.000Z',
      },
    };

    // Mock DB queries for reload
    (realStateStore as any).pool = {
      query: vi.fn().mockImplementation((sql: string, params: any[]) => {
        if (sql.includes("key = 'config'")) {
          return { rows: [{ value: { enabled: true, maxActiveNewWorks: 8 } }] };
        }
        if (sql.includes("key = 'active_works'")) {
          return { rows: [{ value: storedActiveWorks }] };
        }
        if (sql.includes("key = 'watermarks'")) {
          return { rows: [{ value: storedWatermarks }] };
        }
        return { rows: [] };
      }),
    };

    await realStateStore.initialize();

    // Verify reloaded state matches exactly
    const reloadedActive = realStateStore.getActiveWorks();
    expect(reloadedActive.length).toBe(1);
    expect(reloadedActive[0].workId).toBe('work-persisted-1');
    expect(reloadedActive[0].frontierSortKey).toBe(81);

    const reloadedWatermark = realStateStore.getWatermark('solo-leveling', 'mangaflix');
    expect(reloadedWatermark).toBeDefined();
    expect(reloadedWatermark?.lastSeenChapter).toBe(200);
    expect(reloadedWatermark?.lastSeenSortKey).toBe(200);

    // Old chapter 150 checked against watermark (200) cannot become P0!
    expect(150 > (reloadedWatermark?.lastSeenSortKey || 0)).toBe(false);
  });

  // =========================================================================
  // TEST I: Zombie Active Works Prevention (Overnight Degradation Guard)
  // Work with unimported/staged mappings but 0 queued and 0 importing MUST vacate
  // active slot immediately so healthy works with queued chapters can enter.
  // =========================================================================
  it('TEST I: Zombie Active Works Prevention — work with unimported chapters but 0 queued vacates active slot', async () => {
    const admission = new AdmissionController(mockStateStore, mockSentinel);
    const admissionInfo = vi.spyOn((admission as any).logger, 'info');

    // Set up active work that has zero queued and zero importing, but has unimported/staged mappings
    mockStateStore.setActiveWork({
      workId: 'work-zombie-candidate',
      workTitle: 'Zombie Work With Blocked Frontier',
      lane: 'P1',
      state: 'FILLING',
      primarySource: 'mangaflix',
      admittedAt: new Date().toISOString(),
      lastActivityAt: new Date().toISOString(),
      totalChapters: 100,
      publishedChapters: 10,
      queuedChapters: 0,
      inFlightChapters: 0,
      frontierSortKey: null,
      criticalGapSortKey: null,
      criticalGapUnblockCount: 0,
    });

    expect(mockStateStore.getActiveWorks().length).toBe(1);

    // Mock DB queries: work has 0 queued, 0 importing, 0 paused, but unimported = 10
    const mockClient = {
      query: vi.fn().mockImplementation((queryText: string, params: any[]) => {
        if (queryText.includes('ORDER BY CASE WHEN $2::text IS NULL OR s.id > $2::text')) {
          return { rows: [{ id: 'kuro' }] };
        }
        // Work queue reconciliation query: 0 queued, 0 importing
        if (queryText.includes('queued_cnt') || queryText.includes('paused_cnt')) {
          return { rows: params[0].map((work_id: string) => ({ work_id, queued_cnt:'0', importing_cnt:'0', paused_cnt:'0', min_sort_key:null, pub_cnt:'10', max_pub:'10', unimported_cnt:'10', source_status:'ACTIVE' })) };
        }
        // Work has 10 unimported mappings (e.g. STAGED or waiting)
        if (queryText.includes('FROM importer_chapter_mappings')) {
          return { rows: [{ unimported: '10' }] };
        }
        // Healthy candidate work query
        if (queryText.includes('w.published IS FALSE') || queryText.includes('w.published = true')) {
          return {
            rows: [
              { work_id: 'work-healthy-1', title: 'Healthy Admitted Work', source: 'kuro', pending_jobs: '50', queued_count: '10', paused_count: '40', min_sort_key: '1' },
            ],
          };
        }
        if (queryText.includes('FROM chapters')) {
          return { rows: [{ pub_cnt: '10' }] };
        }
        return { rows: [] };
      }),
      release: vi.fn(),
    };

    (admission as any).pool = {
      connect: vi.fn().mockResolvedValue(mockClient),
      query: mockClient.query,
    };

    // Run full admission cycle (reconciliation + admission)
    await admission.runAdmissionCycle();

    // The zombie work MUST be vacated from active works despite unimported > 0
    const activeAfterCycle = mockStateStore.getActiveWorks();
    const zombieStillActive = activeAfterCycle.some((w) => w.workId === 'work-zombie-candidate');
    expect(zombieStillActive).toBe(false);

    // Healthy work was admitted into the vacated slot
    const healthyAdmitted = activeAfterCycle.some((w) => w.workId === 'work-healthy-1');
    expect(healthyAdmitted).toBe(true);
    expect(activeAfterCycle.length).toBe(1);
    expect(admissionInfo).toHaveBeenCalledWith(expect.stringContaining('ACTIVE SET BEFORE: 1 -> AFTER: 0'));
  });
});
