import { afterEach, describe, expect, it, vi } from 'vitest';

const failBatchDirect = vi.fn();
const acquireJobsDirect = vi.fn();

vi.mock('../src/db/yugabyte-direct.js', () => ({
  failBatchDirect,
  getYugabytePool: () => ({ query: vi.fn() }),
  acquireJobsDirect,
  heartbeatDirect: vi.fn(),
  recoverStalledLeasesDirect: vi.fn(),
}));

const { DirectSupabaseClient } = await import('../src/db/direct-supabase-client.js');

describe('direct release fencing', () => {
  afterEach(() => vi.clearAllMocks());

  it('reports a stale worker release as unsuccessful instead of claiming completion', async () => {
    failBatchDirect.mockResolvedValueOnce(0);
    const client = new DirectSupabaseClient({ query: vi.fn() } as any);

    const result = await client.rpc('importer_release_job', {
      p_job_id: 'job-1', p_worker_id: 'old-worker', p_status: 'COMPLETED',
    });

    expect(failBatchDirect).toHaveBeenCalledWith([
      expect.objectContaining({ jobId: 'job-1', workerId: 'old-worker', status: 'COMPLETED' }),
    ]);
    expect(result).toEqual({ data: false, error: null });
  });

  it('passes the catalog P1 filter only to the direct YSQL acquisition path', async () => {
    acquireJobsDirect.mockResolvedValueOnce([]);
    const client = new DirectSupabaseClient({ query: vi.fn() } as any);

    await client.rpc('importer_acquire_job', {
      p_worker_id: 'worker-1',
      p_allowed_sources: ['alpha', 'mangaflix'],
      p_task_type: 'SYNC_WORK',
      p_only_existing_published_work_sync: true,
    });

    expect(acquireJobsDirect).toHaveBeenCalledWith(expect.objectContaining({
      workerId: 'worker-1',
      allowedSources: ['alpha', 'mangaflix'],
      taskType: 'SYNC_WORK',
      onlyExistingPublishedWorkSync: true,
    }));
  });
});
