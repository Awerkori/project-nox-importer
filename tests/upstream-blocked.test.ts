import { describe, it, expect, vi, beforeEach } from 'vitest';
import { ImporterEngine } from '../src/core/engine.js';
import { ImporterQueue } from '../src/core/queue.js';
import { SourceRegistry } from '../src/sources/registry.js';
import { SourceAdapter } from '../src/sources/types.js';
import { ExistingWorksReconciler } from '../src/core/reconciliation.js';
import { HostRateLimiter } from '../src/core/rate-limiter.js';
import { getConfig, Config } from '../src/config.js';
import { MockStorageProvider } from '../src/storage/mock.js';

describe('UPSTREAM_BLOCKED Isolation, Job Parking & Cross-Provider Fallback', () => {
  let mockSupabase: any;
  let registry: SourceRegistry;
  let engine: ImporterEngine;
  let reconciler: ExistingWorksReconciler;
  let rateLimiter: HostRateLimiter;
  let storage: MockStorageProvider;
  let config: Config;

  beforeEach(() => {
    rateLimiter = new HostRateLimiter(100);
    storage = new MockStorageProvider();
    registry = new SourceRegistry(rateLimiter);
    config = getConfig();
  });

  it('ExistingWorksReconciler skips UPSTREAM_BLOCKED providers and uses healthy alternate', async () => {
    const blockedAdapter: SourceAdapter = {
      id: 'nexus_toons',
      name: 'Nexus Toons',
      baseUrl: 'https://nx-toons.xyz',
      fetchUpdatedWorks: vi.fn(),
      fetchWorkDetails: vi.fn(),
      fetchChapters: vi.fn(async () => [
        { sourceChapterId: 'nt-ch-1', number: 1, title: 'Cap 1', pageCount: 15 },
      ]),
      fetchChapterPages: vi.fn(),
      searchWorks: vi.fn(async () => [
        { sourceWorkId: 'nt-work-1', title: 'Solo Leveling', slug: 'solo-leveling' },
      ]),
    };

    const healthyAdapter: SourceAdapter = {
      id: 'nexus',
      name: 'Nexus Mangas',
      baseUrl: 'https://nexusmangas.com',
      fetchUpdatedWorks: vi.fn(),
      fetchWorkDetails: vi.fn(),
      fetchChapters: vi.fn(async () => [
        { sourceChapterId: 'nx-ch-1', number: 1, title: 'Cap 1', pageCount: 20 },
        { sourceChapterId: 'nx-ch-2', number: 2, title: 'Cap 2', pageCount: 20 },
      ]),
      fetchChapterPages: vi.fn(),
      searchWorks: vi.fn(async () => [
        { sourceWorkId: 'nx-work-1', title: 'Solo Leveling', slug: 'solo-leveling' },
      ]),
    };

    registry.register(blockedAdapter);
    registry.register(healthyAdapter);

    const enqueuedJobs: any[] = [];
    const mockQueue: any = {
      enqueue: vi.fn(async (taskType, source, dedupeKey, payload, priority, sortKey) => {
        enqueuedJobs.push({ taskType, source, dedupeKey, payload, priority, sortKey });
        return true;
      }),
      enqueueBatch: vi.fn(async (jobs) => {
        enqueuedJobs.push(...jobs);
        return jobs.length;
      }),
    };

    const sourcesInDb = [
      { id: 'nexus_toons', status: 'UPSTREAM_BLOCKED', enabled: false, cooldown_until: null },
      { id: 'nexus', status: 'ACTIVE', enabled: true, cooldown_until: null },
    ];

    const workMappings = [
      { id: 'map-nt', work_id: 'work-123', source: 'nexus_toons', source_work_id: 'nt-work-1', sync_status: 'SYNCED', is_primary: false },
      { id: 'map-nx', work_id: 'work-123', source: 'nexus', source_work_id: 'nx-work-1', sync_status: 'SYNCED', is_primary: true },
    ];

    const createChainableQuery = (resolvedData: any) => {
      const q: any = {
        select: vi.fn().mockReturnThis(),
        eq: vi.fn().mockReturnThis(),
        not: vi.fn().mockReturnThis(),
        order: vi.fn().mockReturnThis(),
        limit: vi.fn().mockReturnThis(),
        in: vi.fn().mockReturnThis(),
        maybeSingle: vi.fn().mockResolvedValue({ data: resolvedData }),
        upsert: vi.fn().mockResolvedValue({ error: null }),
        then: (onfulfilled: any, onrejected: any) =>
          Promise.resolve({ data: resolvedData, error: null }).then(onfulfilled, onrejected),
      };
      return q;
    };

    mockSupabase = {
      from: vi.fn((table: string) => {
        if (table === 'works') {
          return createChainableQuery({ id: 'work-123', title: 'Solo Leveling', slug: 'solo-leveling', kind: 'MANHWA' });
        }
        if (table === 'importer_sources') {
          return createChainableQuery(sourcesInDb);
        }
        if (table === 'importer_work_mappings') {
          return createChainableQuery(workMappings);
        }
        if (table === 'chapters') {
          return createChainableQuery([]);
        }
        if (table === 'importer_chapter_manifest' || table === 'importer_work_health') {
          return createChainableQuery(null);
        }
        return createChainableQuery(null);
      }),
    };

    reconciler = new ExistingWorksReconciler(mockSupabase, mockQueue, registry);

    const result = await reconciler.reconcileWorkManifest('work-123');

    // Blocked adapter chapters must NOT have been called
    expect(blockedAdapter.fetchChapters).not.toHaveBeenCalled();

    // Healthy adapter chapters MUST have been called
    expect(healthyAdapter.fetchChapters).toHaveBeenCalledWith('nx-work-1');

    // Summary should show healthy provider chapters
    expect(result.totalKnownChapters).toBe(2);
    expect(result.enqueuedCount).toBe(2);

    // Enqueued jobs should all point to healthy provider 'nexus', NONE to 'nexus_toons'
    expect(enqueuedJobs.every((j) => j.source === 'nexus')).toBe(true);
  });

  it('probeSourceHealth retains UPSTREAM_BLOCKED when HTTP 403 Cloudflare is received', async () => {
    let updatedPayload: any = null;
    mockSupabase = {
      from: vi.fn((table: string) => {
        if (table === 'importer_sources') {
          return {
            select: vi.fn().mockReturnThis(),
            in: vi.fn().mockResolvedValue({
              data: [
                {
                  id: 'nexus_toons',
                  name: 'Nexus Toons',
                  status: 'UPSTREAM_BLOCKED',
                  blocked_reason: 'CLOUDFLARE_DATACENTER_BLOCK',
                },
              ],
            }),
            update: vi.fn((payload: any) => {
              updatedPayload = payload;
              return {
                eq: vi.fn().mockResolvedValue({ error: null }),
              };
            }),
          };
        }
        return {};
      }),
    };

    const failingAdapter: SourceAdapter = {
      id: 'nexus_toons',
      name: 'Nexus Toons',
      baseUrl: 'https://nx-toons.xyz',
      fetchUpdatedWorks: vi.fn(),
      fetchWorkDetails: vi.fn(),
      fetchChapters: vi.fn(),
      fetchChapterPages: vi.fn(),
      searchWorks: vi.fn(async () => {
        throw new Error('Cloudflare 403 WAF');
      }),
    };
    registry.register(failingAdapter);

    engine = new ImporterEngine(mockSupabase, storage, registry, rateLimiter, config);

    // Mock global fetch to simulate Cloudflare 403 on DIScloud
    const originalFetch = global.fetch;
    global.fetch = vi.fn(async (url: any) => {
      return {
        ok: false,
        status: 403,
        text: async () => '<!DOCTYPE html>Cloudflare 403 WAF',
      } as any;
    });

    try {
      await engine.probeSourceHealth({
        id: 'nexus_toons',
        name: 'Nexus Toons',
        status: 'UPSTREAM_BLOCKED',
      });

      expect(updatedPayload).toBeDefined();
      expect(updatedPayload.status).toBe('UPSTREAM_BLOCKED');
      expect(updatedPayload.blocked_reason).toBe('CLOUDFLARE_DATACENTER_BLOCK');
      expect(updatedPayload.blocked_details.discloud_status).toBe(403);
      expect(updatedPayload.blocked_details.local_status).toBe(200);
    } finally {
      global.fetch = originalFetch;
    }
  });

  it('probeSourceHealth transitions RECOVERING -> ACTIVE only when all 4 stages succeed', async () => {
    const updates: any[] = [];
    mockSupabase = {
      from: vi.fn((table: string) => {
        if (table === 'importer_sources') {
          return {
            update: vi.fn((payload: any) => {
              updates.push(payload);
              return {
                eq: vi.fn().mockResolvedValue({ error: null }),
              };
            }),
          };
        }
        if (table === 'importer_queue') {
          return {
            update: vi.fn(() => ({
              eq: vi.fn(() => ({
                eq: vi.fn().mockResolvedValue({ error: null }),
              })),
            })),
          };
        }
        return {};
      }),
    };

    const mockToonsAdapter: SourceAdapter = {
      id: 'nexus_toons',
      name: 'Nexus Toons',
      baseUrl: 'https://nx-toons.xyz',
      fetchUpdatedWorks: vi.fn(),
      fetchWorkDetails: vi.fn(),
      searchWorks: vi.fn(async () => [
        { sourceWorkId: 'solo-id', title: 'Solo Leveling', slug: 'solo' },
      ]),
      fetchChapters: vi.fn(async () => [
        { sourceChapterId: 'ch-1', number: 1, title: 'Ch 1', pageCount: 10 },
      ]),
      fetchChapterPages: vi.fn(async () => [
        'https://img.nx-toons.xyz/page1.jpg',
      ]),
    };
    registry.register(mockToonsAdapter);

    engine = new ImporterEngine(mockSupabase, storage, registry, rateLimiter, config);

    const originalFetch = global.fetch;
    global.fetch = vi.fn(async (url: any) => {
      const u = String(url);
      if (u.includes('/api/mangas')) {
        return { ok: true, status: 200 } as any;
      }
      if (u.includes('img.nx-toons.xyz')) {
        const validJpeg = new Uint8Array([0xff, 0xd8, 0xff, 0xe0, 0x00, 0x10, 0x4a, 0x46, 0x49, 0x46, 0x00, 0x01]);
        return {
          ok: true,
          status: 200,
          headers: new Headers({ 'content-type': 'image/jpeg' }),
          arrayBuffer: async () => validJpeg.buffer,
        } as any;
      }
      return { ok: false, status: 404 } as any;
    });

    try {
      await engine.probeSourceHealth({
        id: 'nexus_toons',
        name: 'Nexus Toons',
        status: 'UPSTREAM_BLOCKED',
      });

      // Should first transition to PROBING, then to ACTIVE
      expect(updates.length).toBe(2);
      expect(updates[0].status).toBe('PROBING');
      expect(updates[1].status).toBe('ACTIVE');
      expect(updates[1].enabled).toBe(true);
      expect(updates[1].blocked_reason).toBeNull();
    } finally {
      global.fetch = originalFetch;
    }
  });
});
