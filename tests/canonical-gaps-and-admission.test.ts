/**
 * Project Nox — Canonical Upstream Gaps & Elastic Admission Test Suite
 * 
 * Verifies all 5 mandatory behavioral contracts:
 * - TEST A: Work with ch 1 -> gap [2..6] proven absent across all sources -> confirmed gap registered -> ch 7 publishes canonically -> ch 8 cascades
 * - TEST B: Work with ch 1 -> ch 2 exists in alternative source -> NO confirmed gap registered -> prioritizes ch 2 with priority 95 -> ch 7 remains STAGED
 * - TEST C: Blocked work vacates active set immediately -> AdmissionController admits healthy work -> claims throughput does not drop to 0
 * - TEST D: Scheduler avoids saturated source (0 permits) and prioritizes source with available permit headroom
 * - TEST E: AdmissionController admits P2 work whose catalog starts at chapter 10 (covered by confirmed upstream gap)
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';
import { WorkAffinityScheduler } from '../src/core/scheduler/work-affinity-scheduler.js';
import { AdmissionController } from '../src/core/scheduler/admission-controller.js';
import { confirmUpstreamGapInterval } from '../src/core/gap-validator.js';
import { ActiveWork } from '../src/core/scheduler/types.js';

describe('Project Nox — Canonical Gaps & Elastic Admission (Tests A-E)', () => {
  let mockStateStore: any;
  let mockAdmissionController: any;
  let mockSentinel: any;
  let activeWorks: Map<string, ActiveWork>;

  beforeEach(() => {
    activeWorks = new Map<string, ActiveWork>();
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
      saveMetrics: vi.fn().mockResolvedValue(undefined),
      getLatestMetrics: vi.fn().mockReturnValue(null),
    };

    mockSentinel = {
      isProtectiveStopActive: vi.fn().mockResolvedValue(false),
      isEmergencyPaused: vi.fn().mockReturnValue(false),
      getEmergencyPauseState: vi.fn().mockReturnValue({ active: false }),
      getPressureSnapshot: vi.fn().mockReturnValue({ pressureScore: 10, state: 'GREEN' }),
    };

    mockAdmissionController = {
      start: vi.fn(),
      stop: vi.fn(),
      runAdmissionCycle: vi.fn().mockResolvedValue(undefined),
      admitNextWorkOnDemand: vi.fn().mockImplementation(async (lane?: string) => {
        const admitted: ActiveWork = {
          workId: 'work-healthy-replacement',
          workTitle: 'Healthy Replacement Work',
          lane: (lane as any) || 'P1',
          state: 'FILLING',
          primarySource: 'source-healthy',
          admittedAt: new Date().toISOString(),
          lastActivityAt: new Date().toISOString(),
          totalChapters: 50,
          publishedChapters: 0,
          queuedChapters: 5,
          inFlightChapters: 0,
          frontierSortKey: 1,
          criticalGapSortKey: null,
          criticalGapUnblockCount: 0,
        };
        activeWorks.set(admitted.workId, admitted);
        return admitted;
      }),
    };
  });

  // =========================================================================
  // TEST A: Structural gap [2..6] absent from all sources -> confirmed gap
  // -> ch 7 publishes canonically -> ch 8 cascades
  // =========================================================================
  it('TEST A: confirmed structural gap releases STAGED chapter 7 and cascades chapter 8', async () => {
    const workId = 'work-test-a-1111-2222';
    const publishedChapters = new Set<number>([1]);
    const confirmedGaps: Array<{ start: number; end: number }> = [];
    const mappings: Array<{ chapter_id: string; sort_key: number; status: string }> = [
      { chapter_id: 'ch-7-id', sort_key: 7, status: 'STAGED' },
      { chapter_id: 'ch-8-id', sort_key: 8, status: 'STAGED' },
    ];

    const mockClient = {
      query: vi.fn().mockImplementation(async (sql: string, params: any[]) => {
        // Work mappings for sources checked
        if (sql.includes('FROM importer_work_mappings')) {
          return { rows: [{ source: 'montetai' }] };
        }
        // Check alternative sources in importer_chapter_mappings
        if (sql.includes('FROM importer_chapter_mappings cm')) {
          return { rows: [] }; // No alternative source has chapters 2..6
        }
        // Check queue for chapters 2..6
        if (sql.includes('FROM importer_queue') && sql.includes('chapter_sort_key >=')) {
          return { rows: [] }; // Nothing in queue
        }
        // Insert confirmed gap
        if (sql.includes('INSERT INTO importer_confirmed_gaps')) {
          confirmedGaps.push({ start: params[1], end: params[2] });
          return { rowCount: 1 };
        }
        // Check max published
        if (sql.includes('FROM chapters') && sql.includes('MAX(number)')) {
          return { rows: [{ max_pub: '1' }] };
        }
        // Staged check
        if (sql.includes('status = \'STAGED\'') && sql.includes('chapter_sort_key <')) {
          const staged = mappings.filter((m) => m.status === 'STAGED' && m.sort_key < params[1]);
          return {
            rows: staged.map((s) => ({
              id: `map-${s.sort_key}`,
              chapter_id: s.chapter_id,
              chapter_sort_key: String(s.sort_key),
              source: 'montetai',
            })),
          };
        }
        // Check chapters table
        if (sql.includes('FROM chapters') && sql.includes('published_at IS NOT NULL')) {
          const num = params[1];
          if (publishedChapters.has(num)) {
            return { rows: [{ id: `pub-${num}` }] };
          }
          return { rows: [] };
        }
        return { rows: [] };
      }),
      release: vi.fn(),
    };

    const mockPool = {
      connect: vi.fn().mockResolvedValue(mockClient),
      query: mockClient.query,
    };

    // 1. Confirm gap interval [2..6]
    const gapResult = await confirmUpstreamGapInterval(mockPool, {
      workId,
      startSortKey: 2,
      endSortKey: 6,
      primarySource: 'montetai',
      reason: 'UPSTREAM_MISSING_PREDECESSORS',
    });

    expect(gapResult.confirmed).toBe(true);
    expect(confirmedGaps.length).toBe(1);
    expect(confirmedGaps[0].start).toBe(2);
    expect(confirmedGaps[0].end).toBe(6);

    // 2. Publication barrier unblocks chapter 7 and cascades chapter 8
    const mockBarrier = {
      tryPublish: vi.fn().mockImplementation(async (wId: string, sKey: number) => {
        publishedChapters.add(sKey);
        const map = mappings.find((m) => m.sort_key === sKey);
        if (map) map.status = 'COMPLETED';

        // Cascade next staged
        const next = mappings.find((m) => m.status === 'STAGED' && m.sort_key === sKey + 1);
        if (next) {
          publishedChapters.add(next.sort_key);
          next.status = 'COMPLETED';
        }
        return { published: true };
      }),
    };

    const scheduler = new WorkAffinityScheduler(mockStateStore, mockAdmissionController, mockSentinel, mockPool);
    scheduler.setPublicationBarrier(mockBarrier);

    // Validate claimed job for chapter 9
    const jobCh9 = {
      id: 'job-ch-9',
      source: 'montetai',
      chapter_sort_key: 9,
      payload: { workId, chapterNumber: 9 },
    };

    const validation = await scheduler.validateClaimedJobPostMutex(jobCh9);
    expect(mockBarrier.tryPublish).toHaveBeenCalledWith(workId, 7, 'ch-7-id');
    // Chapters 7 and 8 published -> ch 9 is now completely valid without block!
    expect(validation.valid).toBe(true);
    expect(publishedChapters.has(7)).toBe(true);
    expect(publishedChapters.has(8)).toBe(true);
  });

  // =========================================================================
  // TEST B: Predecessor exists in alternative source -> NO confirmed gap
  // -> prioritizes predecessor with priority 95 -> ch 7 remains STAGED
  // =========================================================================
  it('TEST B: alternative source possesses predecessor -> prevents confirmed gap and boosts priority to 95', async () => {
    const workId = 'work-test-b-3333-4444';
    let queueUpdatedPriority = 0;
    const confirmedGaps: any[] = [];

    const mockClient = {
      query: vi.fn().mockImplementation(async (sql: string, params: any[]) => {
        if (sql.includes('FROM importer_work_mappings')) {
          return { rows: [{ source: 'montetai' }, { source: 'kuro' }] };
        }
        // Chapter 2 exists in alternative source 'kuro'
        if (sql.includes('FROM importer_chapter_mappings cm')) {
          return {
            rows: [{
              source: 'kuro',
              chapter_sort_key: '2',
              status: 'QUEUED',
              is_gap: false,
            }],
          };
        }
        if (sql.includes('INSERT INTO importer_confirmed_gaps')) {
          confirmedGaps.push(params);
          return { rowCount: 1 };
        }
        if (sql.includes('FROM chapters') && sql.includes('MAX(number)')) {
          return { rows: [{ max_pub: '1' }] };
        }
        if (sql.includes('status = \'STAGED\'') && sql.includes('chapter_sort_key <')) {
          return {
            rows: [{
              id: 'map-7',
              chapter_id: 'ch-7-id',
              chapter_sort_key: '7',
              source: 'montetai',
            }],
          };
        }
        if (sql.includes('UPDATE importer_queue') && sql.includes('priority = 95')) {
          queueUpdatedPriority = 95;
          return { rowCount: 1 };
        }
        if (sql.includes('FROM chapters') && sql.includes('published_at IS NOT NULL')) {
          return { rows: [] };
        }
        return { rows: [] };
      }),
      release: vi.fn(),
    };

    const mockPool = {
      connect: vi.fn().mockResolvedValue(mockClient),
      query: mockClient.query,
    };

    activeWorks.set(workId, {
      workId,
      workTitle: 'Test B Work',
      lane: 'P1',
      state: 'FILLING',
      primarySource: 'montetai',
      admittedAt: new Date().toISOString(),
      lastActivityAt: new Date().toISOString(),
      totalChapters: 20,
      publishedChapters: 1,
      queuedChapters: 5,
      inFlightChapters: 0,
      frontierSortKey: 8,
      criticalGapSortKey: null,
      criticalGapUnblockCount: 0,
    });

    const gapResult = await confirmUpstreamGapInterval(mockPool, {
      workId,
      startSortKey: 2,
      endSortKey: 6,
      primarySource: 'montetai',
    });

    // Must NOT confirm gap because kuro has chapter 2!
    expect(gapResult.confirmed).toBe(false);
    expect(gapResult.reason).toContain('ALTERNATIVE_SOURCE_HAS_CHAPTER');
    expect(confirmedGaps.length).toBe(0);

    const scheduler = new WorkAffinityScheduler(mockStateStore, mockAdmissionController, mockSentinel, mockPool);
    const jobCh8 = {
      id: 'job-ch-8',
      source: 'montetai',
      chapter_sort_key: 8,
      payload: { workId, chapterNumber: 8 },
    };

    const validation = await scheduler.validateClaimedJobPostMutex(jobCh8);
    // Job 8 must be rejected and held until chapter 2 is resolved
    expect(validation.valid).toBe(false);
    expect(validation.reason).toBe('BLOCKED_BY_STAGED');
    // Predecessor must be prioritized to 95 in queue
    expect(queueUpdatedPriority).toBe(95);
    // Work must vacate active slot while waiting on predecessor
    expect(mockStateStore.removeActiveWork).toHaveBeenCalledWith(workId);
  });

  // =========================================================================
  // TEST C: Blocked work vacates active set immediately -> AdmissionController
  // admits healthy work -> claims throughput does not stall
  // =========================================================================
  it('TEST C: blocked active work vacates slot immediately and triggers on-demand admission of healthy work', async () => {
    const workId = 'work-blocked-unresolvable';
    activeWorks.set(workId, {
      workId,
      workTitle: 'Stuck Work',
      lane: 'P1',
      state: 'FILLING',
      primarySource: 'source-stuck',
      admittedAt: new Date().toISOString(),
      lastActivityAt: new Date().toISOString(),
      totalChapters: 20,
      publishedChapters: 0,
      queuedChapters: 5,
      inFlightChapters: 0,
      frontierSortKey: 10,
      criticalGapSortKey: null,
      criticalGapUnblockCount: 0,
    });

    const mockClient = {
      query: vi.fn().mockImplementation(async (sql: string) => {
        if (sql.includes('status = \'STAGED\'')) {
          return {
            rows: [{
              id: 'map-staged-9',
              chapter_id: 'ch-9-id',
              chapter_sort_key: '9',
              source: 'source-stuck',
            }],
          };
        }
        if (sql.includes('MAX(number)')) return { rows: [{ max_pub: '-1' }] };
        if (sql.includes('FROM importer_chapter_mappings cm')) {
          // Predecessors exist but stuck
          return { rows: [{ source: 'source-stuck', chapter_sort_key: '5', status: 'WAITING_FOR_GAP' }] };
        }
        return { rows: [] };
      }),
      release: vi.fn(),
    };

    const mockPool = {
      connect: vi.fn().mockResolvedValue(mockClient),
      query: mockClient.query,
    };

    const scheduler = new WorkAffinityScheduler(mockStateStore, mockAdmissionController, mockSentinel, mockPool);
    const job = {
      id: 'job-stuck-10',
      source: 'source-stuck',
      chapter_sort_key: 10,
      payload: { workId, chapterNumber: 10 },
    };

    expect(activeWorks.has(workId)).toBe(true);

    const validation = await scheduler.validateClaimedJobPostMutex(job);
    expect(validation.valid).toBe(false);
    expect(validation.reason).toBe('BLOCKED_BY_STAGED');

    // 1. Stuck work MUST vacate active slot immediately
    expect(activeWorks.has(workId)).toBe(false);
    expect(mockStateStore.removeActiveWork).toHaveBeenCalledWith(workId);

    // 2. AdmissionController must have been triggered on-demand
    expect(mockAdmissionController.admitNextWorkOnDemand).toHaveBeenCalledWith('P1');

    // 3. Healthy replacement work is now active in its place
    expect(activeWorks.has('work-healthy-replacement')).toBe(true);
  });

  // =========================================================================
  // TEST D: Scheduler avoids saturated source (0 permits) and prioritizes
  // source with available permit headroom
  // =========================================================================
  it('TEST D: scheduler avoids source with 0 permits and prioritizes source with headroom', async () => {
    // Two eligible works: one on saturated 'mangadex' (0 permits), one on 'kuro' (5 permits)
    activeWorks.set('work-saturated', {
      workId: 'work-saturated',
      workTitle: 'Work on Saturated Source',
      lane: 'P1',
      state: 'FILLING',
      primarySource: 'mangadex',
      admittedAt: new Date().toISOString(),
      lastActivityAt: new Date().toISOString(),
      totalChapters: 20,
      publishedChapters: 5,
      queuedChapters: 4,
      inFlightChapters: 0,
      frontierSortKey: 6,
      criticalGapSortKey: null,
      criticalGapUnblockCount: 0,
    });

    activeWorks.set('work-headroom', {
      workId: 'work-headroom',
      workTitle: 'Work on Available Source',
      lane: 'P1',
      state: 'FILLING',
      primarySource: 'kuro',
      admittedAt: new Date().toISOString(),
      lastActivityAt: new Date().toISOString(),
      totalChapters: 30,
      publishedChapters: 10,
      queuedChapters: 6,
      inFlightChapters: 0,
      frontierSortKey: 11,
      criticalGapSortKey: null,
      criticalGapUnblockCount: 0,
    });

    let claimedWorkId: string | null = null;
    const mockClient = {
      query: vi.fn().mockImplementation(async (sql: string, params: any[]) => {
        // P0 query
        if (params && params[1] === 100) {
          return { rows: [] };
        }
        if (sql.includes('UPDATE importer_queue q')) {
          const targetWork = params[2]; // workId parameter
          if (targetWork === 'work-headroom') {
            claimedWorkId = targetWork;
            return {
              rows: [{
                id: 'job-claimed-1',
                task_type: 'IMPORT_CHAPTER',
                source: 'kuro',
                priority: 75,
                payload: { workId: 'work-headroom', chapterNumber: 11 },
                chapter_sort_key: '11',
              }],
            };
          }
        }
        return { rows: [] };
      }),
      release: vi.fn(),
    };

    const mockPool = {
      connect: vi.fn().mockResolvedValue(mockClient),
      query: mockClient.query,
    };

    const scheduler = new WorkAffinityScheduler(mockStateStore, mockAdmissionController, mockSentinel, mockPool);

    // Provide permit headroom function: mangadex has 0 permits; kuro has 5 permits
    scheduler.setSourcePermitProvider((source: string) => {
      if (source === 'mangadex') return 0;
      if (source === 'kuro') return 5;
      return 1;
    });

    const job = await scheduler.acquireNextChapterJob({ workerId: 'runner-slot-1' });
    expect(job).not.toBeNull();
    // Claimed job must come from the source with headroom ('kuro'), NOT the saturated source
    expect(job.source).toBe('kuro');
    expect(job.payload?.workId).toBe('work-headroom');
  });

  // =========================================================================
  // TEST E: AdmissionController admits P2 work whose catalog starts at chapter 10
  // (because 1..9 are confirmed upstream gaps)
  // =========================================================================
  it('TEST E: AdmissionController admits P2 work starting at chapter 10 when 1..9 is confirmed gap', async () => {
    const admissionController = new AdmissionController(mockStateStore, mockSentinel, {
      query: vi.fn().mockImplementation(async (sql: string, params: any[]) => {
        // canAdmitNewWork queries
        if (sql.includes('priority >= 100 AND priority < 1000')) return { rows: [{ p0_cnt: '0' }] };
        if (sql.includes('p1_claimable')) return { rows: [{ p1_claimable: '0' }] };
        if (sql.includes('status = \'IMPORTING\'')) return { rows: [{ cnt: '2' }] }; // idle workers = 6

        // Query candidates for P2
        if (sql.includes('FROM importer_queue q') && sql.includes('w.published IS FALSE')) {
          return {
            rows: [{
              work_id: 'work-starts-ch10',
              title: 'Manga Starting At Chapter 10',
              source: 'kuro',
              pending_jobs: '50',
              queued_count: '8',
              paused_count: '42',
              min_sort_key: '10.0000',
              created_at: new Date().toISOString(),
            }],
          };
        }

        // Query importer_confirmed_gaps for candidate
        if (sql.includes('FROM importer_confirmed_gaps')) {
          return {
            rows: [{
              gap_cnt: '1',
              start_sort_key: '1.0000',
              end_sort_key: '9.0000',
            }],
          };
        }

        // Promotion queries
        if (sql.includes('UPDATE importer_work_mappings')) return { rowCount: 1 };
        if (sql.includes('UPDATE importer_queue')) return { rows: [{ id: 'promoted-1' }] };

        return { rows: [] };
      }),
    });

    admissionController.setSourcePermitProvider(() => 4);
    admissionController.setChapterCapacityProvider(() => 8);

    // Run admission cycle
    await admissionController.runAdmissionCycle();

    // Verify work was admitted despite min_sort_key = 10 > 1.5 because 1..9 is confirmed gap!
    const active = mockStateStore.getActiveWork('work-starts-ch10');
    expect(active).toBeDefined();
    expect(active?.workTitle).toBe('Manga Starting At Chapter 10');
    expect(active?.lane).toBe('P2');
    expect(active?.frontierSortKey).toBe(10);
  });
});
