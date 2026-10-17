import { describe, expect, it, vi } from 'vitest';
import { reconcileAuditedSources, SOURCE_AUDIT_RELEASE } from '../src/core/source-audit-bootstrap.js';

describe('source audit bootstrap', () => {
  it('adds only the validated missing providers, restores Kuro once, and retires only the two proven dead adapters', async () => {
    const query = vi.fn()
      .mockResolvedValueOnce({ rowCount: 4 })
      .mockResolvedValueOnce({ rowCount: 1 })
      .mockResolvedValueOnce({ rowCount: 2 });

    await reconcileAuditedSources({ query } as any);

    expect(query).toHaveBeenCalledTimes(3);
    const calls = query.mock.calls.map(([sql]) => String(sql));
    expect(calls[0]).toContain("'brscans'");
    expect(calls[0]).toContain("'mangastop'");
    expect(calls[0]).toContain("'hentaiteca'");
    expect(calls[0]).toContain("'imperiodabritannia'");
    expect(calls[0]).toContain('ON CONFLICT (id) DO NOTHING');
    expect(calls[1]).toContain("id = 'kuro'");
    expect(calls[1]).toContain("status = 'ACTIVE'");
    expect(calls[1]).toContain('sourceAuditRecovery');
    expect(calls[2]).toContain("'megahentai'");
    expect(calls[2]).toContain("'osakascan'");
    expect(calls[2]).toContain("status = 'DISABLED'");
    expect(query.mock.calls[0][1]).toEqual([SOURCE_AUDIT_RELEASE]);
    expect(query.mock.calls[1][1]).toEqual([SOURCE_AUDIT_RELEASE]);
    expect(query.mock.calls[2][1]).toEqual([SOURCE_AUDIT_RELEASE]);
  });
});
