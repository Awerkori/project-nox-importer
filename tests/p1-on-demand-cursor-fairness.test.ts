import { describe, expect, it, vi } from 'vitest';
import { AdmissionController } from '../src/core/scheduler/admission-controller.js';

const SOURCE = 'manhastro';
const CURSOR_BEFORE_GOBLIN = '52000000-0000-4000-8000-000000000001';
const GOBLIN_POSITION = '92000000-0000-4000-8000-000000000002';
const FRONTIER_AFTER_GOBLIN = 'b0000000-0000-4000-8000-000000000003';

function makeOnDemandFixture(
  cursor: string,
  frontierWorkId = FRONTIER_AFTER_GOBLIN,
  options: { minSort?: string; maxPublished?: string; confirmedGap?: boolean; cursors?: Record<string, string> } = {},
) {
  const activeWorks = new Map<string, any>();
  const stateStore = {
    getConfig: () => ({ enabled: true, shadowMode: false, maxActiveNewWorks: 8 }),
    getActiveWorks: () => Array.from(activeWorks.values()),
    setActiveWork: vi.fn((work: any) => activeWorks.set(work.workId, work)),
    getP1AdmissionCursors: vi.fn(() => options.cursors || ({ [SOURCE]: cursor })),
    setP1AdmissionCursor: vi.fn(),
  };
  const pool = {
    query: vi.fn(async (sql: string) => {
      if (sql.includes('FROM queue_candidates q')) {
        // Models the real spare-capacity path: a frontier choice beyond an
        // unseen P1 work. It must not rewrite the circular cursor.
        return {
          rows: [{
            work_id: frontierWorkId,
            title: 'Frontier work',
            source: SOURCE,
            pending_jobs: '12',
            queued_count: '1',
            min_sort_key: options.minSort || '5.0000',
          }],
        };
      }
      if (sql.includes('FROM chapters')) {
        return { rows: [{ work_id: frontierWorkId, max_pub: options.maxPublished || '4' }] };
      }
      if (sql.includes('FROM importer_confirmed_gaps')) {
        return options.confirmedGap
          ? { rows: [{ work_id: frontierWorkId, start_sort_key: '5', end_sort_key: '9' }] }
          : { rows: [] };
      }
      if (sql.includes('WITH ranked AS')) return { rows: [] };
      if (sql.includes('WITH existing AS')) return { rows: [{ queued_count: '1' }] };
      return { rows: [] };
    }),
  };
  const sentinel = { isProtectiveStopActive: vi.fn().mockResolvedValue(false) };
  return { activeWorks, stateStore, pool, sentinel };
}

describe('P1 on-demand cursor fairness', () => {
  it('does not skip an unseen P1 work when frontier fallback is admitted on demand', async () => {
    const { stateStore, pool, sentinel } = makeOnDemandFixture(CURSOR_BEFORE_GOBLIN);
    const controller = new AdmissionController(stateStore as any, sentinel as any, pool);

    const admitted = await controller.admitNextWorkOnDemand('P1');

    expect(admitted?.workId).toBe(FRONTIER_AFTER_GOBLIN);
    expect(stateStore.setActiveWork).toHaveBeenCalledWith(expect.objectContaining({
      lane: 'P1',
      workId: FRONTIER_AFTER_GOBLIN,
    }));
    // The unseen Goblin-position work remains next in circular progress. A
    // different frontier work receiving spare capacity is not cursor progress.
    expect(GOBLIN_POSITION > CURSOR_BEFORE_GOBLIN).toBe(true);
    expect(GOBLIN_POSITION < FRONTIER_AFTER_GOBLIN).toBe(true);
    expect(stateStore.setP1AdmissionCursor).not.toHaveBeenCalled();
  });

  it('keeps the persisted cursor stable across repeated on-demand and wrap-around admissions', async () => {
    const wrappedCursor = 'f9000000-0000-4000-8000-000000000001';
    const { activeWorks, stateStore, pool, sentinel } = makeOnDemandFixture(
      wrappedCursor,
      '03000000-0000-4000-8000-000000000004',
    );
    const controller = new AdmissionController(stateStore as any, sentinel as any, pool);

    await controller.admitNextWorkOnDemand('P1');
    activeWorks.clear();
    await controller.admitNextWorkOnDemand('P1');

    expect(stateStore.getP1AdmissionCursors()).toEqual({ [SOURCE]: wrappedCursor });
    expect(stateStore.setP1AdmissionCursor).not.toHaveBeenCalled();
    expect(pool.query).toHaveBeenCalled();
  });

  it('preserves every persisted source cursor when a critical-gap frontier is admitted on demand', async () => {
    const cursors = {
      [SOURCE]: CURSOR_BEFORE_GOBLIN,
      mangaflix: '61000000-0000-4000-8000-000000000005',
    };
    const { stateStore, pool, sentinel } = makeOnDemandFixture(
      CURSOR_BEFORE_GOBLIN,
      FRONTIER_AFTER_GOBLIN,
      { minSort: '10.0000', maxPublished: '4', confirmedGap: true, cursors },
    );
    const controller = new AdmissionController(stateStore as any, sentinel as any, pool);

    const admitted = await controller.admitNextWorkOnDemand('P1');

    expect(admitted?.workId).toBe(FRONTIER_AFTER_GOBLIN);
    expect(stateStore.getP1AdmissionCursors()).toEqual(cursors);
    expect(stateStore.setP1AdmissionCursor).not.toHaveBeenCalled();
  });
});
