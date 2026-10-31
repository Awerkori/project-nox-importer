import { describe, it, expect, vi } from 'vitest';
import { AsyncSemaphore } from '../../core/concurrency.js';
import { HanamiHeavenAdapter } from '../../sources/hanamiheaven/hanamiheaven-adapter.js';
import { HipercoolAdapter } from '../../sources/hipercool/hipercool-adapter.js';
import { TaimuMangasAdapter } from '../../sources/taimumangas/taimumangas-adapter.js';
import { HostRateLimiter } from '../../core/rate-limiter.js';
import { ImporterLiteRuntime } from '../runtime.js';

describe('P0 Universal Monitoring & Status Integrity', () => {
  it('borrows a spare SQL connection only when the Reader is fast and capacity remains', () => {
    const canBorrow=(ImporterLiteRuntime.prototype as any).mayBorrowPrimaryForMonitor;
    const ctx:any={pool:{idleCount:2,waitingCount:0},sourceMetadataPool:{waitingCount:3},
      lastSiteProbe:{state:'HEALTHY',checkedAt:new Date().toISOString(),latencyMs:300}};
    expect(canBorrow.call(ctx)).toBe(true);
    ctx.pool.idleCount=1; expect(canBorrow.call(ctx)).toBe(false);
    ctx.pool.idleCount=2; ctx.pool.waitingCount=1; expect(canBorrow.call(ctx)).toBe(false);
    ctx.pool.waitingCount=0; ctx.lastSiteProbe.latencyMs=2500; expect(canBorrow.call(ctx)).toBe(false);
    ctx.lastSiteProbe.latencyMs=300; ctx.lastSiteProbe.checkedAt=new Date(Date.now()-181_000).toISOString();
    expect(canBorrow.call(ctx)).toBe(false);
  });

  it('keeps the previous success and a bounded counter for recurring checks', () => {
    const checkpoint = (ImporterLiteRuntime.prototype as any).successfulMonitorCheckpoint;
    const first = checkpoint({ checkpoint: {} }, '2026-10-31T12:00:00.000Z');
    const second = checkpoint({ checkpoint: first }, '2026-10-31T12:30:00.000Z');
    expect(second).toEqual({ lastCheckedAt: '2026-10-31T12:30:00.000Z',
      previousCheckedAt: '2026-10-31T12:00:00.000Z', successfulChecksSinceTracking: 2 });
  });

  it('admits at most one coverage producer under sustained pool pressure', async () => {
    const runtime = Object.create(ImporterLiteRuntime.prototype) as any;
    let resolve!: (value: unknown) => void;
    const pending = new Promise((done) => { resolve = done; });
    const refill = vi.fn(() => pending);
    Object.assign(runtime, { isRuntimeLeader: true, stopped: false, manualProtectiveStop: { active: false },
      sourceMetadataPool: { waitingCount: 14 }, pool: { waitingCount: 0 },
      p0MonitorCoverageRefillGate: new AsyncSemaphore(1), queue: { refillP0MonitorCoverage: refill },
      p0CoverageZeroStreak: 46 });
    const first = runtime.refillP0MonitorCoverage();
    await runtime.refillP0MonitorCoverage();
    expect(refill).toHaveBeenCalledTimes(1);
    resolve({ source: 'healthy', admitted: 0, revived: 0 });
    await first;
    expect(runtime.p0MonitorCoverage.outcome).toBe('SOURCE_ALREADY_COVERED');
  });

  it('correctly parses ONGOING status from HanamiHeaven Portuguese Lançando metadata', async () => {
    const adapter = new HanamiHeavenAdapter(new HostRateLimiter());
    const mockHtml = `
      <div class="profile-manga">
        <div class="post-title"><h1>Regressed Warrior's Female Dominance Diary</h1></div>
        <div class="post-status">
          <div class="post-content_item">
            <div class="summary-heading"><h5>Lançamento</h5></div>
            <div class="summary-content"><a href="https://hanamiheaven.org/manga-release/2024/">2024</a></div>
          </div>
          <div class="post-content_item">
            <div class="summary-heading"><h5>Status</h5></div>
            <div class="summary-content">Lançando</div>
          </div>
        </div>
        <div class="summary_content"><div class="description-summary"><p>Sinopse completa.</p></div></div>
      </div>
    `;

    (adapter as any).fetchHtml = async () => mockHtml;

    const details = await adapter.fetchWorkDetails('regressed-warriors-female-dominance-diary');
    expect(details.status).toBe('ONGOING');
  });

  it('correctly parses ONGOING status from Hipercool series json', async () => {
    const adapter = new HipercoolAdapter(new HostRateLimiter());
    (adapter as any).fetchJson = async () => [
      { result: { data: { json: null } } },
      {
        result: {
          data: {
            json: {
              id: 29108,
              title: "Regressed Warrior's Female Dominance Diary",
              slug: "regressed-warriors-female-dominance-diary",
              status: "ongoing",
              type: "manhwa",
            }
          }
        }
      }
    ];

    const details = await adapter.fetchWorkDetails('regressed-warriors-female-dominance-diary');
    expect(details.status).toBe('ONGOING');
  });

  it('correctly parses ONGOING status from TaimuMangas api json', async () => {
    const adapter = new TaimuMangasAdapter(new HostRateLimiter());
    (adapter as any).request = async () => ({
      identifier: 'test-manga',
      title: "Test Manga",
      status: "ongoing",
    });

    const details = await adapter.fetchWorkDetails('test-manga');
    expect(details.status).toBe('ONGOING');
  });

  it('verifies universal P0 intelligent cadence rules in runtime', () => {
    const fn = (ImporterLiteRuntime.prototype as any).monitorIntervalMs;
    const dummyTask: any = { checkpoint: {} };

    for (const status of ['ONGOING', 'HIATUS', 'COMPLETED', 'UNKNOWN']) {
      for (const releases of [0, 1]) {
        expect(fn.call({}, dummyTask, status, releases)).toBe(30 * 60_000);
        expect(fn.call({}, { checkpoint: { emptyChecks: 10, lastReleaseAt: '2020-01-01T00:00:00Z' } }, status, releases)).toBe(30 * 60_000);
      }
    }
  });

  it('protects COMPLETED works from improper ONGOING promotion without reliable evidence', async () => {
    const { DeduplicationEngine } = await import('../../core/deduplication.js');
    let updatedPayload: any = null;
    const mockSupabase: any = {
      from: (table: string) => ({
        select: () => ({
          eq: () => ({
            maybeSingle: async () => ({
              data: {
                id: 'work-completed-1',
                title: 'Truly Completed Work',
                status: 'COMPLETED',
                metadata_provenance: {},
              },
              error: null,
            }),
          }),
        }),
        update: (payload: any) => ({
          eq: () => {
            updatedPayload = payload;
            return Promise.resolve({ error: null });
          },
        }),
      }),
    };

    const engine = new DeduplicationEngine(mockSupabase);

    // Case 1: Candidate with no rawMetadata or empty rawMetadata must NOT promote COMPLETED -> ONGOING
    updatedPayload = null;
    await engine.applyMetadataPrecedence('work-completed-1', {
      source: 'test-source',
      sourceWorkId: 'swid-1',
      title: 'Truly Completed Work',
      slug: 'truly-completed-work',
      status: 'ONGOING',
    }, 'test-source');
    expect(updatedPayload?.status).toBeUndefined();

    // Case 2: Candidate with rawMetadata explicitly saying 'Completo' must NOT promote
    updatedPayload = null;
    await engine.applyMetadataPrecedence('work-completed-1', {
      source: 'test-source',
      sourceWorkId: 'swid-1',
      title: 'Truly Completed Work',
      slug: 'truly-completed-work',
      status: 'ONGOING',
      rawMetadata: { status: 'Completo' },
    }, 'test-source');
    expect(updatedPayload?.status).toBeUndefined();

    // Case 3: Candidate with reliable evidence ('Lançando' / 'ongoing') PROMOTES COMPLETED -> ONGOING
    updatedPayload = null;
    await engine.applyMetadataPrecedence('work-completed-1', {
      source: 'test-source',
      sourceWorkId: 'swid-1',
      title: 'Truly Completed Work',
      slug: 'truly-completed-work',
      status: 'ONGOING',
      rawMetadata: { status: 'Lançando' },
    }, 'test-source');
    expect(updatedPayload?.status).toBe('ONGOING');
  });
});
