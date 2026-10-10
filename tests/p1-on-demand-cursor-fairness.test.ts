import { describe, expect, it, vi } from 'vitest';
import { AdmissionController } from '../src/core/scheduler/admission-controller.js';

const SOURCE = 'manhastro';
const CURSOR_BEFORE_GOBLIN = '52000000-0000-4000-8000-000000000001';
const GOBLIN_POSITION = '92000000-0000-4000-8000-000000000002';
const FRONTIER_AFTER_GOBLIN = 'b0000000-0000-4000-8000-000000000003';

function makeOnDemandFixture(options: {
  rows?: Array<Record<string, string>>;
  minSort?: string;
  maxPublished?: string;
  confirmedGap?: boolean;
  cursors?: Record<string, string>;
} = {}) {
  const activeWorks = new Map<string, any>();
  const cursors = { [SOURCE]: CURSOR_BEFORE_GOBLIN, ...(options.cursors || {}) };
  const rows = options.rows || [{
    // The old frontier path selected a later work and skipped this one. A P1
    // spare-capacity claim must now be the next circular opportunity.
    work_id: GOBLIN_POSITION,
    title: 'Goblin',
    source: SOURCE,
    pending_jobs: '12',
    queued_count: '1',
    min_sort_key: options.minSort || '5.0000',
    rotation_rank: '1',
  }];
  const stateStore = {
    getConfig: () => ({ enabled: true, shadowMode: false, maxActiveNewWorks: 8 }),
    getActiveWorks: () => Array.from(activeWorks.values()),
    setActiveWork: vi.fn((work: any) => activeWorks.set(work.workId, work)),
    getP1AdmissionCursors: vi.fn(() => ({ ...cursors })),
    setP1AdmissionCursor: vi.fn((source: string, workId: string) => { cursors[source] = workId; }),
  };
  const pool = {
    query: vi.fn(async (sql: string) => {
      if (sql.includes('FROM p1_rotation q') || sql.includes('FROM queue_candidates q')) return { rows };
      if (sql.includes('FROM importer_sources')) return { rows: [{ id: SOURCE }] };
      if (sql.includes('FROM chapters')) {
        return { rows: rows.map((row) => ({ work_id: row.work_id, max_pub: options.maxPublished || '4' })) };
      }
      if (sql.includes('FROM importer_confirmed_gaps')) {
        return options.confirmedGap
          ? { rows: rows.map((row) => ({ work_id: row.work_id, start_sort_key: '5', end_sort_key: '9' })) }
          : { rows: [] };
      }
      if (sql.includes('WITH ranked AS')) return { rows: [] };
      if (sql.includes('WITH existing AS')) return { rows: [{ queued_count: '1' }] };
      return { rows: [] };
    }),
  };
  const sentinel = { isProtectiveStopActive: vi.fn().mockResolvedValue(false) };
  return { activeWorks, cursors, stateStore, pool, sentinel };
}

describe('P1 on-demand cursor fairness', () => {
  it('bounds each P1 admission probe to a rotating two-source window', async () => {
    const sourceRows = [{ id: 'alpha' }, { id: 'beta' }];
    const stateStore = {
      getConfig: () => ({ enabled: true, shadowMode: false, maxActiveNewWorks: 8 }),
      getActiveWorks: () => [],
      getP1AdmissionCursors: () => ({}),
    };
    const pool = {
      query: vi.fn(async (sql: string) => {
        if (sql.trim().startsWith('SELECT s.id')) return { rows: sourceRows };
        return { rows: [] };
      }),
    };
    const controller = new AdmissionController(stateStore as any, {
      isProtectiveStopActive: vi.fn().mockResolvedValue(false),
    } as any, pool);

    await expect(controller.admitNextWorkOnDemand('P1', ['alpha', 'beta', 'gamma'])).resolves.toBeNull();

    const sourceWindowCall = pool.query.mock.calls.find(([sql]) => String(sql).trim().startsWith('SELECT s.id'));
    expect(sourceWindowCall?.[1]).toEqual([['alpha', 'beta', 'gamma'], null, 2]);
    const p1CandidateCalls = pool.query.mock.calls.filter(([sql]) => String(sql).includes('FROM p1_rotation q'));
    expect(p1CandidateCalls).toHaveLength(2); // queued, then bounded paused fallback
    for (const [, params] of p1CandidateCalls) {
      expect(params[4]).toEqual(['alpha', 'beta']);
    }
  });

  it('admits the next circular P1 opportunity instead of a later frontier work', async () => {
    const { stateStore, pool, sentinel } = makeOnDemandFixture();
    const controller = new AdmissionController(stateStore as any, sentinel as any, pool);

    const admitted = await controller.admitNextWorkOnDemand('P1');

    expect(admitted?.workId).toBe(GOBLIN_POSITION);
    expect(admitted?.workId).not.toBe(FRONTIER_AFTER_GOBLIN);
    expect(stateStore.setActiveWork).toHaveBeenCalledWith(expect.objectContaining({
      lane: 'P1',
      workId: GOBLIN_POSITION,
    }));
    console.log(pool.query.mock.calls.map((c) => c[0]));
    const admissionSql = pool.query.mock.calls.find(([sql]) => String(sql).includes('FROM p1_rotation q'))?.[0] as string;
    expect(admissionSql).toContain('canonical_chapter.published_at IS NOT NULL');
    expect(admissionSql).toContain("canonical_chapter.number = COALESCE(NULLIF(q.payload->>'chapterNumber', '')::numeric, q.chapter_sort_key)");
    // It received a real one-chapter window, so this is valid durable cursor
    // progress rather than the artificial leap that caused the regression.
    expect(stateStore.setP1AdmissionCursor).toHaveBeenCalledWith(SOURCE, GOBLIN_POSITION);
  });

  it('moves the cursor monotonically across repeated on-demand P1 windows', async () => {
    const { activeWorks, cursors, stateStore, pool, sentinel } = makeOnDemandFixture();
    const controller = new AdmissionController(stateStore as any, sentinel as any, pool);

    await controller.admitNextWorkOnDemand('P1');
    expect(cursors[SOURCE]).toBe(GOBLIN_POSITION);
    activeWorks.clear();
    pool.query.mockImplementation(async (sql: string) => {
      if (sql.includes('FROM p1_rotation q') || sql.includes('FROM queue_candidates q')) {
        return { rows: [{ work_id: FRONTIER_AFTER_GOBLIN, title: 'Next work', source: SOURCE, pending_jobs: '1', queued_count: '1', min_sort_key: '6', rotation_rank: '1' }] };
      }
      if (sql.includes('FROM importer_sources')) return { rows: [{ id: SOURCE }] };
      if (sql.includes('FROM chapters')) return { rows: [{ work_id: FRONTIER_AFTER_GOBLIN, max_pub: '5' }] };
      if (sql.includes('FROM importer_confirmed_gaps')) return { rows: [] };
      if (sql.includes('WITH ranked AS')) return { rows: [] };
      if (sql.includes('WITH existing AS')) return { rows: [{ queued_count: '1' }] };
      return { rows: [] };
    });

    const admitted = await controller.admitNextWorkOnDemand('P1');
    expect(admitted?.workId).toBe(FRONTIER_AFTER_GOBLIN);
    expect(cursors[SOURCE]).toBe(FRONTIER_AFTER_GOBLIN);
    expect(stateStore.setP1AdmissionCursor).toHaveBeenLastCalledWith(SOURCE, FRONTIER_AFTER_GOBLIN);
  });

  it('keeps source opportunities rotating while preserving a critical confirmed gap', async () => {
    const otherSource = 'mangaflix';
    const { stateStore, pool, sentinel } = makeOnDemandFixture({
      minSort: '10.0000',
      maxPublished: '4',
      confirmedGap: true,
      cursors: { [otherSource]: '61000000-0000-4000-8000-000000000005' },
    });
    const controller = new AdmissionController(stateStore as any, sentinel as any, pool);

    const admitted = await controller.admitNextWorkOnDemand('P1');

    expect(admitted?.workId).toBe(GOBLIN_POSITION);
    expect(stateStore.setP1AdmissionCursor).toHaveBeenCalledWith(SOURCE, GOBLIN_POSITION);
    expect(stateStore.getP1AdmissionCursors()).toMatchObject({
      [SOURCE]: GOBLIN_POSITION,
      [otherSource]: '61000000-0000-4000-8000-000000000005',
    });
  });

  it('skips a cursor work parked behind an unresolved gap and admits the next bounded frontier', async () => {
    const blockedWork = '81000000-0000-4000-8000-000000000010';
    const nextWork = '82000000-0000-4000-8000-000000000011';
    const { stateStore, pool, sentinel } = makeOnDemandFixture({
      rows: [
        {
          work_id: blockedWork,
          title: 'Blocked frontier',
          source: SOURCE,
          pending_jobs: '20',
          queued_count: '2',
          min_sort_key: '10',
          rotation_rank: '1',
          frontier_rank: '2',
        },
        {
          work_id: nextWork,
          title: 'Executable frontier',
          source: SOURCE,
          pending_jobs: '4',
          queued_count: '1',
          min_sort_key: '6',
          rotation_rank: '2',
          frontier_rank: '1',
        },
      ],
      maxPublished: '4',
    });

    pool.query.mockImplementation(async (sql: string, params?: unknown[]) => {
      if (sql.includes('FROM p1_rotation q') || sql.includes('FROM queue_candidates q')) {
        return { rows: [
          {
            work_id: blockedWork,
            title: 'Blocked frontier',
            source: SOURCE,
            pending_jobs: '20',
            queued_count: '2',
            min_sort_key: '10',
            rotation_rank: '1',
            frontier_rank: '2',
          },
          {
            work_id: nextWork,
            title: 'Executable frontier',
            source: SOURCE,
            pending_jobs: '4',
            queued_count: '1',
            min_sort_key: '6',
            rotation_rank: '2',
            frontier_rank: '1',
          },
        ] };
      }
      if (sql.includes('FROM importer_sources')) return { rows: [{ id: SOURCE }] };
      if (sql.includes('FROM chapters')) {
        return { rows: [
          { work_id: blockedWork, max_pub: '4' },
          { work_id: nextWork, max_pub: '5' },
        ] };
      }
      if (sql.includes('FROM importer_confirmed_gaps')) return { rows: [] };
      if (sql.includes('WITH ranked AS')) return { rows: [] };
      if (sql.includes('WITH existing AS')) return { rows: [{ queued_count: '1' }] };
      return { rows: [] };
    });

    const controller = new AdmissionController(stateStore as any, sentinel as any, pool);
    const admitted = await controller.admitNextWorkOnDemand('P1');

    expect(admitted?.workId).toBe(nextWork);
    expect(admitted?.workId).not.toBe(blockedWork);
    expect(stateStore.setP1AdmissionCursor).toHaveBeenCalledWith(SOURCE, nextWork);
  });
});
