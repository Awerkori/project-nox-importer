import { describe, expect, it, vi } from 'vitest';
import { reclaimStartupOwnedLeases } from '../src/core/engine.js';

describe('startup owned-lease recovery', () => {
  it('only releases expired importing leases held by the restarting worker', async () => {
    const query = vi.fn().mockResolvedValue({ rowCount: 2, rows: [{ id: 'a' }, { id: 'b' }] });

    await expect(reclaimStartupOwnedLeases({ query }, 'discloud-importer-1')).resolves.toBe(2);

    const [sql, params] = query.mock.calls[0];
    expect(sql).toContain("status = 'IMPORTING'");
    expect(sql).toContain('locked_by = $1');
    expect(sql).toContain('lease_expires_at < NOW()');
    expect(sql).toContain("status = 'QUEUED'");
    expect(sql).toContain("retry_reason = 'WORKER_RESTART_RECOVERED'");
    expect(params).toEqual(['discloud-importer-1']);
  });
});
