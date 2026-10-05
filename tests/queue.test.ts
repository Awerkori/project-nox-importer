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
