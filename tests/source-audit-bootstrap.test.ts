import { describe, expect, it, vi } from 'vitest';
import {
  BLOCKED_SOURCE_RETIREMENT_RELEASE,
  KURO_RETIREMENT_RELEASE,
  reconcileAuditedSources,
  SOURCE_AUDIT_RELEASE,
} from '../src/core/source-audit-bootstrap.js';

describe('source audit bootstrap', () => {
  it('adds only validated providers, reactivates only healthy adapters, and keeps retirements durable', async () => {
    const query = vi.fn()
      .mockResolvedValueOnce({ rowCount: 3 })
      .mockResolvedValueOnce({ rowCount: 4 })
      .mockResolvedValueOnce({ rowCount: 1 })
      .mockResolvedValueOnce({ rowCount: 2 })
      .mockResolvedValueOnce({ rowCount: 11 });

    await reconcileAuditedSources({ query } as any);

    expect(query).toHaveBeenCalledTimes(5);
    const calls = query.mock.calls.map(([sql]) => String(sql));
    expect(calls[0]).toContain("'brscans'");
    expect(calls[0]).not.toContain("'mangastop'");
    expect(calls[0]).toContain("'hentaiteca'");
    expect(calls[0]).toContain("'imperiodabritannia'");
    expect(calls[0]).toContain('ON CONFLICT (id) DO NOTHING');
    expect(calls[1]).toContain('WITH recovered(id, name, base_url)');
    expect(calls[1]).toContain('sourceAuditActivation');
    expect(calls[1]).toContain("status = 'ACTIVE'");
    expect(calls[1]).toContain('catalog_discovery_enabled = false');
    expect(calls[2]).toContain("id = 'kuro'");
    expect(calls[2]).toContain("status = 'DISABLED'");
    expect(calls[2]).toContain('sourceRetirement');
    expect(calls[2]).toContain('KURO_EXTERNAL_CLOUDFLARE_LOGIN_CHALLENGE');
    expect(calls[3]).toContain("'megahentai'");
    expect(calls[3]).toContain("'osakascan'");
    expect(calls[3]).toContain("status = 'DISABLED'");
    expect(query.mock.calls[0][1]).toEqual([SOURCE_AUDIT_RELEASE]);
    expect(query.mock.calls[1][1]).toEqual([
      SOURCE_AUDIT_RELEASE,
      'manhastro', 'Manhastro', 'https://manhastro.net',
      'nexus', 'Nexus Mangas', 'https://www.nexusmangas.com',
      'nocturnesummer', 'Nocturne Summer', 'https://nocfsb.com',
      'tankouhentai', 'Tankou Hentai', 'https://tankouhentai.com',
    ]);
    expect(query.mock.calls[2][1]).toEqual([KURO_RETIREMENT_RELEASE]);
    expect(query.mock.calls[3][1]).toEqual([SOURCE_AUDIT_RELEASE]);
    expect(calls[4]).toContain('WITH retired(id, reason)');
    expect(calls[4]).toContain("status = 'DISABLED'");
    expect(calls[4]).toContain('sourceRetirementReason');
    expect(query.mock.calls[4][1]).toEqual([
      BLOCKED_SOURCE_RETIREMENT_RELEASE,
      'acervohentai', 'AUDIT_2026_10_JS_CHALLENGE',
      'amuy', 'AUDIT_2026_10_JS_CHALLENGE',
      'arthurscan', 'AUDIT_2026_10_JS_CHALLENGE',
      'geasscomics', 'AUDIT_2026_10_JS_CHALLENGE',
      'inkapk', 'AUDIT_2026_10_JS_CHALLENGE',
      'mangaonline', 'AUDIT_2026_10_JS_CHALLENGE',
      'mangastop', 'AUDIT_2026_10_JS_CHALLENGE',
      'pointzerotoons', 'AUDIT_2026_10_JS_CHALLENGE',
      'vegitoons', 'AUDIT_2026_10_API_BLOCK',
      'yaoifanclub', 'AUDIT_2026_10_JS_CHALLENGE',
      'yuriverso', 'AUDIT_2026_10_JS_CHALLENGE',
    ]);
  });
});
