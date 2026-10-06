import { describe, expect, it, vi } from 'vitest';
import { ImporterEngine } from '../src/core/engine.js';

process.env.YUGABYTE_PASSWORD ??= 'test-password';

const baseConfig: any = {
  WORKER_ID: 'discloud-importer-1',
  MAX_CONCURRENT_CHAPTERS: 5,
  TESTED_CONCURRENCY_CEILING: 5,
  ADAPTIVE_INITIAL_CONCURRENCY: 3,
  DIRECT_DB_POOL_MAX: 2,
  TELEGRAM_MEDIA_CONCURRENCY: 8,
  DOWNLOAD_INFLIGHT_CONCURRENCY: 8,
  BUFFERED_PAGE_CONCURRENCY: 12,
  QUEUE_LEASE_DURATION_SECONDS: 300,
  QUEUE_HEARTBEAT_INTERVAL_SECONDS: 60,
  NOX_MANGA_URL: 'https://example.test',
};

function createEngine(pool: any) {
  const supabase = { getPool: () => pool } as any;
  return new ImporterEngine(supabase, {} as any, {} as any, {} as any, { ...baseConfig });
}

describe('runtime leadership lease', () => {
  it('uses one durable lease row and gives each boot a fenced worker identity', async () => {
    const query = vi.fn().mockResolvedValue({ rows: [{ value: '{}' }] });
    const engine = createEngine({ query });

    await expect((engine as any).tryAcquireRuntimeLeadership()).resolves.toBe(true);

    const [sql, params] = query.mock.calls[0];
    expect(sql).toContain("INSERT INTO settings AS leader");
    expect(sql).toContain("$2::text");
    expect(params[0]).toBe('importer_runtime_leader');
    expect(sql).toContain("expires_at");
    expect(sql).toContain("started_at");
    expect(params[1]).toMatch(/^discloud-importer-1@/);
    expect(params[3]).toMatch(/T/);
    expect((engine as any).config.WORKER_ID).toBe(params[1]);
  });

  it('does not claim work when another live runtime owns the lease', async () => {
    const query = vi.fn().mockResolvedValue({ rows: [] });
    const engine = createEngine({ query });

    await expect((engine as any).tryAcquireRuntimeLeadership()).resolves.toBe(false);
    expect((engine as any).isRuntimeLeader).toBe(false);
  });
});
