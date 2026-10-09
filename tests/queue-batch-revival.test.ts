import { describe, expect, it } from 'vitest';
import { ImporterQueue } from '../src/core/queue.js';

type StoredJob = Record<string, any>;

function createQueueClient(initialRows: StoredJob[] = []) {
  const rows = new Map(initialRows.map((row) => [row.dedupe_key, { ...row }]));
  const calls = { upserts: [] as StoredJob[][], inserts: 0 };

  const client = {
    from(table: string) {
      if (table !== 'importer_queue') throw new Error(`Unexpected table: ${table}`);

      return {
        async upsert(incoming: StoredJob[]) {
          calls.upserts.push(incoming);
          for (const row of incoming) {
            if (!rows.has(row.dedupe_key)) rows.set(row.dedupe_key, { ...row });
          }
          return { error: null };
        },
        async insert(row: StoredJob) {
          calls.inserts++;
          if (rows.has(row.dedupe_key)) return { error: { code: '23505' } };
          rows.set(row.dedupe_key, { ...row });
          return { error: null };
        },
        select(_columns: string) {
          let selected = [...rows.values()];
          const result = () => ({ data: selected.map((row) => ({ ...row })), error: null });
          const chain: any = {
            in(column: string, values: unknown[]) {
              selected = selected.filter((row) => values.includes(row[column]));
              return chain;
            },
            eq(column: string, value: unknown) {
              selected = selected.filter((row) => row[column] === value);
              return chain;
            },
            async maybeSingle() {
              return { data: selected[0] ? { ...selected[0] } : null, error: null };
            },
            then(resolve: (value: unknown) => unknown, reject: (reason: unknown) => unknown) {
              return Promise.resolve(result()).then(resolve, reject);
            },
          };
          return chain;
        },
        update(changes: StoredJob) {
          return {
            async eq(column: string, value: unknown) {
              for (const row of rows.values()) {
                if (row[column] === value) Object.assign(row, changes);
              }
              return { error: null };
            },
          };
        },
      };
    },
  };

  return { client, rows, calls };
}

describe('ImporterQueue.enqueueBatch', () => {
  it('uses bounded bulk inserts for a catalog-sized batch', async () => {
    const { client, rows, calls } = createQueueClient();
    const queue = new ImporterQueue(client as any, 'test-worker');
    const jobs = Array.from({ length: 100 }, (_, index) => ({
      taskType: 'SYNC_WORK' as const,
      source: 'manhastro',
      dedupeKey: `manhastro:work:${index}`,
      payload: { sourceWorkId: String(index) },
      priority: 60,
    }));

    await expect(queue.enqueueBatch(jobs)).resolves.toBe(100);
    expect(calls.upserts.map((chunk) => chunk.length)).toEqual([50, 50]);
    expect(rows.size).toBe(100);
    expect(calls.inserts).toBe(0);
  });

  it('revives an exceptional duplicate exactly as single enqueue does', async () => {
    const { client, rows, calls } = createQueueClient([{
      id: 'failed-sync',
      task_type: 'SYNC_WORK',
      source: 'manhastro',
      priority: 10,
      dedupe_key: 'manhastro:work:42',
      payload: { sourceWorkId: '42' },
      status: 'FAILED',
    }]);
    const queue = new ImporterQueue(client as any, 'test-worker');

    await queue.enqueueBatch([{
      taskType: 'SYNC_WORK',
      source: 'manhastro',
      dedupeKey: 'manhastro:work:42',
      payload: { sourceWorkId: '42', title: 'Retried work' },
      priority: 60,
    }], { reviveDuplicates: true });

    expect(calls.inserts).toBe(1);
    expect(rows.get('manhastro:work:42')).toMatchObject({
      status: 'QUEUED',
      attempts: 0,
      priority: 60,
      payload: { sourceWorkId: '42', title: 'Retried work' },
    });
  });

  it('leaves a live retry from the same source untouched', async () => {
    const { client, rows, calls } = createQueueClient([{
      id: 'live-retry',
      task_type: 'SYNC_WORK',
      source: 'mangaflix',
      priority: 60,
      dedupe_key: 'mangaflix:work:42',
      payload: { sourceWorkId: '42' },
      status: 'RETRY',
      attempts: 3,
    }]);
    const queue = new ImporterQueue(client as any, 'test-worker');

    await queue.enqueueBatch([{
      taskType: 'SYNC_WORK',
      source: 'mangaflix',
      dedupeKey: 'mangaflix:work:42',
      payload: { sourceWorkId: '42', title: 'Still retrying' },
      priority: 60,
    }], { reviveDuplicates: true });

    expect(calls.inserts).toBe(0);
    expect(rows.get('mangaflix:work:42')).toMatchObject({
      status: 'RETRY',
      attempts: 3,
      payload: { sourceWorkId: '42' },
    });
  });
});
