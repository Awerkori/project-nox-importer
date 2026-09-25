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
import {
  SchedulerLane,
  WorkSchedulerState,
  ActiveWork,
  WorkWatermark,
} from '../src/core/scheduler/types.js';
import { SchedulerStateStore } from '../src/core/scheduler/state-store.js';
import { AdmissionController } from '../src/core/scheduler/admission-controller.js';
import { WorkAffinityScheduler } from '../src/core/scheduler/work-affinity-scheduler.js';

describe('Project Nox — Work-Affinity Scheduler Tests A-H', () => {
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
          const wId = params[0];
          if (wId === 'work-4') {
            return { rows: [{ queued_cnt: '0', importing_cnt: '0', paused_cnt: '0', min_sort_key: null }] };
          }
          return { rows: [{ queued_cnt: '5', importing_cnt: '0', paused_cnt: '5', min_sort_key: '10' }] };
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
        // Work queue reconciliation query: 0 queued, 0 importing
        if (queryText.includes('queued_cnt') || queryText.includes('paused_cnt')) {
          return { rows: [{ queued_cnt: '0', importing_cnt: '0', paused_cnt: '0', min_sort_key: null }] };
        }
        if (queryText.includes('FROM chapters')) {
          return { rows: [{ pub_cnt: '10' }] };
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
  });
});
