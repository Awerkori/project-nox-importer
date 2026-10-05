import { describe, expect, it, vi } from 'vitest';
import { ImporterQueue } from '../src/core/queue.js';

function makeSupabase(existing: { id: string; status: string; priority: number; source: string }) {
  const update = vi.fn().mockReturnValue({ eq: vi.fn().mockResolvedValue({ error: null }) });
  const maybeSingle = vi.fn().mockResolvedValue({ data: existing, error: null });
  const select = vi.fn().mockReturnValue({
    eq: vi.fn().mockReturnValue({ maybeSingle }),
  });
  const insert = vi.fn().mockResolvedValue({ error: { code: '23505', message: 'duplicate' } });
  const from = vi.fn((table: string) => {
    if (table !== 'importer_queue') throw new Error(`unexpected table ${table}`);
    return { insert, select, update };
  });
  return { client: { from } as any, insert, update };
}

describe('ImporterQueue dedupe revival', () => {
  it('revives a staff-cancelled job when reconciliation enqueues its queued mapping', async () => {
    const mock = makeSupabase({ id: 'job-1', status: 'CANCELLED_BY_STAFF', priority: 70, source: 'mangaflix' });
    const queue = new ImporterQueue(mock.client, 'test-worker');

    const revived = await queue.enqueue(
      'IMPORT_CHAPTER',
      'mangaflix',
      'work:work-1:chapter:47',
      { workId: 'work-1', chapterNumber: 47 },
      70,
      47,
    );

    expect(revived).toBe(true);
    expect(mock.update).toHaveBeenCalledWith(expect.objectContaining({ status: 'QUEUED', attempts: 0 }));
  });
});

describe('ImporterQueue transient publication barrier recovery', () => {
  it('requeues only a barrier-failed job whose mapping is still pending', async () => {
    const updates: any[] = [];
    const failed = [{
      id: 'job-1', source: 'mangaflix', priority: 75, attempts: 5,
      payload: { workId: 'work-1' }, chapter_sort_key: 47,
      last_error: 'PublicationSafetyBarrier is CLOSED/RECOVERING',
    }];
    const from = vi.fn((table: string) => {
      if (table === 'importer_queue') {
        return {
          select: vi.fn().mockReturnValue({
            eq: vi.fn().mockReturnThis(),
            ilike: vi.fn().mockReturnThis(),
            order: vi.fn().mockReturnThis(),
            limit: vi.fn().mockResolvedValue({ data: failed, error: null }),
          }),
          update: vi.fn((value: any) => {
            updates.push(value);
            return { eq: vi.fn().mockReturnValue({ eq: vi.fn().mockResolvedValue({ error: null }) }) };
          }),
        };
      }
      return {
        select: vi.fn().mockReturnValue({
          eq: vi.fn().mockReturnThis(),
          maybeSingle: vi.fn().mockResolvedValue({ data: { id: 'mapping-1' }, error: null }),
        }),
      };
    });
    const queue = new ImporterQueue({ from } as any, 'test-worker');
    await expect(queue.recoverPublicationBarrierFailures()).resolves.toBe(1);
    expect(updates[0]).toMatchObject({ status: 'QUEUED', attempts: 0, retry_reason: 'PUBLICATION_BARRIER_RECOVERY' });
  });
});

describe('ImporterQueue transient reservation-limit recovery', () => {
  it('requeues only a reservation-failed job whose mapping is still pending', async () => {
    const updates: any[] = [];
    const failed = [{
      id: 'job-2', source: 'megahentai', payload: { workId: 'work-2' },
      chapter_sort_key: 3, last_error: 'Concurrent reservation limit: SOURCE_CONCURRENCY_FULL',
    }];
    const from = vi.fn((table: string) => {
      if (table === 'importer_queue') {
        return {
          select: vi.fn().mockReturnValue({
            eq: vi.fn().mockReturnThis(), ilike: vi.fn().mockReturnThis(),
            order: vi.fn().mockReturnThis(), limit: vi.fn().mockResolvedValue({ data: failed, error: null }),
          }),
          update: vi.fn((value: any) => {
            updates.push(value);
            return { eq: vi.fn().mockReturnValue({ eq: vi.fn().mockResolvedValue({ error: null }) }) };
          }),
        };
      }
      return {
        select: vi.fn().mockReturnValue({
          eq: vi.fn().mockReturnThis(),
          maybeSingle: vi.fn().mockResolvedValue({ data: { id: 'mapping-2' }, error: null }),
        }),
      };
    });
    const queue = new ImporterQueue({ from } as any, 'test-worker');
    await expect(queue.recoverReservationLimitFailures()).resolves.toBe(1);
    expect(updates[0]).toMatchObject({ status: 'QUEUED', attempts: 0, retry_reason: 'RESERVATION_LIMIT_RECOVERY' });
  });
});
