import { describe, expect, it, vi } from 'vitest';
import type { Pool } from 'pg';
import {
  DURABLE_RECEIPT_BATCH_JOIN_MS,
  DURABLE_RESERVATION_BATCH_JOIN_MS,
  LiteExecutor,
  UPLOAD_BATCH_FLUSH_THRESHOLD,
  UploadReceiptBatcher,
  UploadReservationBatcher,
} from '../executor.js';
import { ImporterLiteRuntime } from '../runtime.js';
import { LiteRepository } from '../repository.js';
import { AsyncSemaphore } from '../../core/concurrency.js';
import { HostRateLimiter } from '../../core/rate-limiter.js';
import type { SourceRegistry } from '../../sources/registry.js';
import type { StorageProvider } from '../../storage/provider.js';

describe('YugabyteDB pool & query optimization invariants', () => {
  it('coalesces upload reservations and receipts with 350ms/900ms windows and 24-item immediate threshold', async () => {
    expect(DURABLE_RESERVATION_BATCH_JOIN_MS).toBe(350);
    expect(DURABLE_RECEIPT_BATCH_JOIN_MS).toBe(900);
    expect(UPLOAD_BATCH_FLUSH_THRESHOLD).toBe(24);

    const reserveCalls: number[] = [];
    const batcher = new UploadReservationBatcher({
      reserveUploads: async (inputs) => {
        reserveCalls.push(inputs.length);
        return inputs.map((_, idx) => ({ mediaId: `00000000-0000-4000-8000-${String(idx).padStart(12, '0')}`, state: 'INTENT' }));
      },
    } as unknown as LiteRepository);

    const results = await Promise.all(
      Array.from({ length: 24 }, (_, idx) =>
        batcher.reserve({
          taskId: '00000000-0000-4000-8000-000000000001',
          pagePosition: idx + 1,
          sha256: `sha-${idx}`,
        }),
      ),
    );
    expect(results).toHaveLength(24);
    expect(reserveCalls).toEqual([24]);

    const receiptCalls: number[] = [];
    const receiptBatcher = new UploadReceiptBatcher({
      recordUploadReceipts: async (inputs) => {
        receiptCalls.push(inputs.length);
      },
    } as unknown as LiteRepository);

    await Promise.all(
      Array.from({ length: 24 }, (_, idx) =>
        receiptBatcher.persist({
          taskId: '00000000-0000-4000-8000-000000000001',
          pagePosition: idx + 1,
          mediaId: `00000000-0000-4000-8000-${String(idx).padStart(12, '0')}`,
          sha256: `sha-${idx}`,
          bytes: 1024,
          mime: 'image/webp',
          width: 800,
          height: 1200,
          providerKey: 'file',
          botReference: 'bot-1',
          shardId: null,
          channelId: '-1001',
          messageId: idx + 1,
          createdBy: '00000000-0000-4000-8000-000000000009',
        }),
      ),
    );
    expect(receiptCalls).toEqual([24]);
  });

  it('keeps P2 getAvailablePool isolated from the primary chapter pool outside turbo mode', () => {
    const primaryPool = { waitingCount: 0, idleCount: 2, totalCount: 3, query: vi.fn(), end: vi.fn() } as unknown as Pool;
    const metadataPool = { waitingCount: 2, idleCount: 0, totalCount: 1, query: vi.fn(), end: vi.fn() } as unknown as Pool;
    const runtime = new ImporterLiteRuntime(
      primaryPool,
      {},
      { get: () => null } as unknown as SourceRegistry,
      { upload: async () => 'unused', healthCheck: async () => true, getProviderKey: () => 'telegram' } as unknown as StorageProvider,
      {
        IMPORTER_USER_ID: '00000000-0000-4000-8000-000000000009',
        WORKER_ID: 'pool-isolation-test',
        QUEUE_LEASE_DURATION_SECONDS: 60,
        DOWNLOAD_INFLIGHT_CONCURRENCY: 4,
        BUFFERED_PAGE_CONCURRENCY: 8,
        BATCH_PAGE_DOWNLOAD_CONCURRENCY: 4,
        LITE_CHAPTER_WORKERS: 4,
        MAX_CONCURRENT_CHAPTERS: 4,
        TESTED_CONCURRENCY_CEILING: 4,
      } as any,
      new HostRateLimiter(5),
      metadataPool,
    );

    expect((runtime as any).getAvailablePool()).toBe(metadataPool);
    (runtime as any).metadataTurboMode = { enabled: true };
    expect((runtime as any).getAvailablePool()).toBe(primaryPool);
  });

  it('gracefully releases runtime leadership and closes isolated metadata pool on shutdown()', async () => {
    const primaryQuery = vi.fn(async () => ({ rows: [], rowCount: 1 }));
    const metadataEnd = vi.fn(async () => undefined);
    const primaryPool = { waitingCount: 0, idleCount: 1, totalCount: 3, query: primaryQuery, end: vi.fn() } as unknown as Pool;
    const metadataPool = { waitingCount: 0, idleCount: 1, totalCount: 1, query: vi.fn(), end: metadataEnd } as unknown as Pool;
    const runtime = new ImporterLiteRuntime(
      primaryPool,
      {},
      { get: () => null } as unknown as SourceRegistry,
      { upload: async () => 'unused', healthCheck: async () => true, getProviderKey: () => 'telegram' } as unknown as StorageProvider,
      {
        IMPORTER_USER_ID: '00000000-0000-4000-8000-000000000009',
        WORKER_ID: 'shutdown-test',
        QUEUE_LEASE_DURATION_SECONDS: 60,
        DOWNLOAD_INFLIGHT_CONCURRENCY: 4,
        BUFFERED_PAGE_CONCURRENCY: 8,
        BATCH_PAGE_DOWNLOAD_CONCURRENCY: 4,
        LITE_CHAPTER_WORKERS: 4,
        MAX_CONCURRENT_CHAPTERS: 4,
        TESTED_CONCURRENCY_CEILING: 4,
      } as any,
      new HostRateLimiter(5),
      metadataPool,
    );
    (runtime as any).isRuntimeLeader = true;

    await runtime.shutdown();

    expect(primaryQuery).toHaveBeenCalledTimes(1);
    expect(metadataEnd).toHaveBeenCalledTimes(1);
    expect((runtime as any).isRuntimeLeader).toBe(false);
  });

  it('skips importer_lite_uploads subquery flag on fresh first-attempt route lookups', async () => {
    const capturedParams: unknown[][] = [];
    const mockDb = {
      query: vi.fn(async (_sql: string, params: unknown[]) => {
        capturedParams.push(params);
        return { rows: [] };
      }),
    } as unknown as Pool;
    const repo = new LiteRepository(mockDb);
    const executor = new LiteExecutor(
      mockDb,
      repo,
      { get: () => null } as unknown as SourceRegistry,
      {} as StorageProvider,
      '00000000-0000-4000-8000-000000000009',
      new AsyncSemaphore(4, 'test-dl'),
      2,
      new AsyncSemaphore(8, 'test-buf'),
    );

    await (executor as any).routes({
      id: '00000000-0000-4000-8000-000000000010',
      workId: '00000000-0000-4000-8000-000000000011',
      chapterKey: 'pt-br|default|chapter|1|-',
      attempts: 1,
      checkpoint: {},
    });
    expect(capturedParams[0]?.[3]).toBe(false);

    await (executor as any).routes({
      id: '00000000-0000-4000-8000-000000000010',
      workId: '00000000-0000-4000-8000-000000000011',
      chapterKey: 'pt-br|default|chapter|1|-',
      attempts: 2,
      checkpoint: {},
    });
    expect(capturedParams[1]?.[3]).toBe(true);
  });

  it('gates cold legacy maintenance inside reconcile when recoverLegacyMaintenance is false', async () => {
    const queries: string[] = [];
    const client = {
      query: vi.fn(async (sql: string) => {
        queries.push(sql);
        return { rows: [], rowCount: 0 };
      }),
      release: vi.fn(),
    };
    const mockDb = {
      connect: vi.fn(async () => client),
      query: vi.fn(async () => ({ rows: [], rowCount: 0 })),
    } as unknown as Pool;
    const repo = new LiteRepository(mockDb);

    await repo.reconcile(20, {
      recoverUploadOutcomes: false,
      recoverSourceCooldowns: false,
      recoverLegacyMaintenance: false,
    });
    expect(queries.some((q) => q.includes('publishIntegrityRepairV1'))).toBe(false);
    expect(queries.some((q) => q.includes('candidate_p0'))).toBe(false);

    queries.length = 0;
    await repo.reconcile(20, {
      recoverUploadOutcomes: false,
      recoverSourceCooldowns: true,
      recoverLegacyMaintenance: true,
    });
    expect(queries.some((q) => q.includes('publishIntegrityRepairV1'))).toBe(true);
    expect(queries.some((q) => q.includes('candidate_p0'))).toBe(true);
  });

  it('does not globally cool down a fast source when a single chapter has a content defect (PAGE_HTTP_404 / INVALID_PAGE_MANIFEST)', async () => {
    const capturedParams: unknown[][] = [];
    const mockDb = {
      query: vi.fn(async (_sql: string, params: unknown[]) => {
        capturedParams.push(params);
        return { rows: [], rowCount: 1 };
      }),
    } as unknown as Pool;
    const repo = new LiteRepository(mockDb);

    await repo.recordSourceFailure('nocturnesummer', new Error('INVALID_PAGE_MANIFEST'));
    await repo.recordSourceFailure('mrtenzus', new Error('PAGE_HTTP_404'));
    await repo.recordSourceFailure('mangaflix', new Error('The operation was aborted due to timeout'));

    expect(capturedParams[0]?.[4]).toBe(true);
    expect(capturedParams[1]?.[4]).toBe(true);
    expect(capturedParams[2]?.[4]).toBe(false);
  });
});
