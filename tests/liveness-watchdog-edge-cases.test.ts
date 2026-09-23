import { describe, it, expect, vi } from 'vitest';
import {
  computeInternalLivenessState,
  computeExternalLivenessState,
} from '../src/core/engine.js';
import { AdmissionController } from '../src/core/scheduler/admission-controller.js';

describe('Liveness Watchdog & Anti-Starvation Edge Cases (Casos A a G)', () => {
  // Case A: eligible jobs + 0 importing + sem progresso >15m => STALLED
  it('Caso A: eligible jobs + 0 importing + sem progresso >15m => STALLED', () => {
    const state = computeInternalLivenessState({
      isStopActive: false,
      rssMb: 150,
      eligibleCount: 10,
      importingCount: 0,
      activeJobsCount: 0,
      minutesSinceProgress: 16,
    });
    expect(state).toBe('STALLED');
  });

  // Case B: eligible jobs + 5 IMPORTING stale + sem progresso >15m => STALLED
  it('Caso B: eligible jobs + 5 IMPORTING stale + sem progresso >15m => STALLED', () => {
    const state = computeInternalLivenessState({
      isStopActive: false,
      rssMb: 150,
      eligibleCount: 10,
      importingCount: 5,
      activeJobsCount: 5,
      minutesSinceProgress: 20,
    });
    expect(state).toBe('STALLED');
  });

  // Case C: 5 IMPORTING com progresso recente (< 15m) => HEALTHY_WORKING
  it('Caso C: 5 IMPORTING com progresso recente => HEALTHY_WORKING', () => {
    const state = computeInternalLivenessState({
      isStopActive: false,
      rssMb: 150,
      eligibleCount: 10,
      importingCount: 5,
      activeJobsCount: 5,
      minutesSinceProgress: 2,
    });
    expect(state).toBe('HEALTHY_WORKING');
  });

  // Case D: protective stop legítimo => BACKPRESSURED
  it('Caso D: protective stop legítimo => BACKPRESSURED', () => {
    const state = computeInternalLivenessState({
      isStopActive: true,
      rssMb: 150,
      eligibleCount: 10,
      importingCount: 0,
      activeJobsCount: 0,
      minutesSinceProgress: 20,
    });
    expect(state).toBe('BACKPRESSURED');
  });

  // Case E: zero trabalho elegível => HEALTHY_IDLE
  it('Caso E: zero trabalho elegível => HEALTHY_IDLE', () => {
    const state = computeInternalLivenessState({
      isStopActive: false,
      rssMb: 120,
      eligibleCount: 0,
      importingCount: 0,
      activeJobsCount: 0,
      minutesSinceProgress: 60,
    });
    expect(state).toBe('HEALTHY_IDLE');
  });

  // Case F: heartbeat externo ausente => DEAD / restart externo
  it('Caso F: heartbeat externo ausente => DEAD', () => {
    const now = Date.now();
    const fourMinutesAgo = now - 4 * 60 * 1000;

    const deadState = computeExternalLivenessState({
      lastHeartbeatTimestamp: fourMinutesAgo,
      now,
      heartbeatTimeoutMs: 3 * 60 * 1000,
      internalState: 'HEALTHY_WORKING',
    });
    expect(deadState).toBe('DEAD');

    const aliveState = computeExternalLivenessState({
      lastHeartbeatTimestamp: now - 30 * 1000, // 30s ago
      now,
      heartbeatTimeoutMs: 3 * 60 * 1000,
      internalState: 'HEALTHY_WORKING',
    });
    expect(aliveState).toBe('HEALTHY_WORKING');
  });

  // Case G: active work com mappings pendentes e queue temporariamente zerada => NÃO pode ser removida prematuramente
  it('Caso G: active work com mappings pendentes e queue temporariamente zerada NÃO pode ser removida', async () => {
    const mockStateStore: any = {
      config: { enabled: true, shadowMode: false, maxActiveNewWorks: 4 },
      getConfig: vi.fn().mockReturnValue({ enabled: true, shadowMode: false, maxActiveNewWorks: 4 }),
      activeWorks: new Map<string, any>(),
      getActiveWorks: vi.fn().mockImplementation(() => Array.from(mockStateStore.activeWorks.values())),
      getActiveWork: vi.fn().mockImplementation((id: string) => mockStateStore.activeWorks.get(id)),
      setActiveWork: vi.fn().mockImplementation((w: any) => mockStateStore.activeWorks.set(w.workId, w)),
      removeActiveWork: vi.fn().mockImplementation((id: string) => mockStateStore.activeWorks.delete(id)),
    };

    const mockSentinel: any = {
      isProtectiveStopActive: vi.fn().mockResolvedValue(false),
    };

    const workWithPendingMappings = {
      workId: 'work-pending-mappings',
      workTitle: 'Work With Pending Mappings',
      lane: 'P2',
      state: 'FILLING',
      primarySource: 'kuro',
      admittedAt: new Date().toISOString(),
      lastActivityAt: new Date().toISOString(),
      totalChapters: 10,
      publishedChapters: 0,
      queuedChapters: 0,
      inFlightChapters: 0,
      frontierSortKey: null,
      criticalGapSortKey: null,
      criticalGapUnblockCount: 0,
    };

    mockStateStore.setActiveWork(workWithPendingMappings);

    // Mock DB queries:
    // queue has 0 queued, 0 importing, 0 paused
    // BUT importer_chapter_mappings has 5 unimported mappings (e.g. pending discovery / staged)
    const mockClient = {
      query: vi.fn().mockImplementation((queryText: string, params: any[]) => {
        if (queryText.includes('queued_cnt') || queryText.includes('paused_cnt')) {
          return { rows: [{ queued_cnt: '0', importing_cnt: '0', paused_cnt: '0', min_sort_key: null }] };
        }
        if (queryText.includes('FROM chapters')) {
          return { rows: [{ pub_cnt: '0', max_pub: '-1' }] };
        }
        if (queryText.includes('FROM importer_chapter_mappings')) {
          return { rows: [{ staged_cnt: '2', min_staged: '3', unimported_cnt: '5' }] };
        }
        if (queryText.includes('FROM importer_sources')) {
          return { rows: [{ status: 'ACTIVE', cooldown_until: null }] };
        }
        return { rows: [] };
      }),
      release: vi.fn(),
    };

    const admission = new AdmissionController(mockStateStore, mockSentinel);
    (admission as any).pool = {
      connect: vi.fn().mockResolvedValue(mockClient),
    };

    // Run active works reconciliation
    await (admission as any).reconcileActiveWorks();

    // Verify: Work was NOT removed because unimported mappings exist
    expect(mockStateStore.getActiveWork('work-pending-mappings')).toBeDefined();
    expect(mockStateStore.getActiveWork('work-pending-mappings')?.state).toBe('FILLING');
  });
});
