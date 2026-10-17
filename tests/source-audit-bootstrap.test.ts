import { describe, expect, it, vi } from 'vitest';
import { reconcileAuditedSources, SOURCE_AUDIT_RELEASE } from '../src/core/source-audit-bootstrap.js';

describe('source audit bootstrap', () => {
  it('adds only validated providers, reactivates the audited adapters once, restores Kuro, and retires only the two proven dead adapters', async () => {
    const query = vi.fn()
      .mockResolvedValueOnce({ rowCount: 4 })
      .mockResolvedValueOnce({ rowCount: 13 })
      .mockResolvedValueOnce({ rowCount: 1 })
      .mockResolvedValueOnce({ rowCount: 2 });

    await reconcileAuditedSources({ query } as any);

    expect(query).toHaveBeenCalledTimes(4);
    const calls = query.mock.calls.map(([sql]) => String(sql));
    expect(calls[0]).toContain("'brscans'");
    expect(calls[0]).toContain("'mangastop'");
    expect(calls[0]).toContain("'hentaiteca'");
    expect(calls[0]).toContain("'imperiodabritannia'");
    expect(calls[0]).toContain('ON CONFLICT (id) DO NOTHING');
    expect(calls[1]).toContain('WITH recovered(id, name, base_url)');
    expect(calls[1]).toContain('sourceAuditActivation');
    expect(calls[1]).toContain("status = 'ACTIVE'");
    expect(calls[1]).toContain('catalog_discovery_enabled = false');
    expect(calls[2]).toContain("id = 'kuro'");
    expect(calls[2]).toContain("status = 'ACTIVE'");
    expect(calls[2]).toContain('sourceAuditRecovery');
    expect(calls[3]).toContain("'megahentai'");
    expect(calls[3]).toContain("'osakascan'");
    expect(calls[3]).toContain("status = 'DISABLED'");
    expect(query.mock.calls[0][1]).toEqual([SOURCE_AUDIT_RELEASE]);
    expect(query.mock.calls[1][1]).toEqual([
      SOURCE_AUDIT_RELEASE,
      'acervohentai', 'Acervo Hentai', 'https://acervohentai.com',
      'amuy', 'AMUY Scan', 'https://www.apenasmaisumyaoi.com',
      'arthurscan', 'Arthur Scan', 'https://arthurscan.xyz',
      'geasscomics', 'Geass Comics', 'https://geasscomics.xyz',
      'inkapk', 'Inkapk', 'https://inkapk.net',
      'mangaonline', 'Manga Online', 'https://mangaonline.love',
      'manhastro', 'Manhastro', 'https://manhastro.net',
      'nexus', 'Nexus Mangas', 'https://www.nexusmangas.com',
      'nocturnesummer', 'Nocturne Summer', 'https://nocfsb.com',
      'pointzerotoons', 'Kitsune Yako', 'https://kitsuneyako.com',
      'tankouhentai', 'Tankou Hentai', 'https://tankouhentai.com',
      'yaoifanclub', 'Yaoi Fan Club', 'https://yaoifanclub.com',
      'yuriverso', 'Yuri on Air', 'https://yurionair.top',
    ]);
    expect(query.mock.calls[2][1]).toEqual([SOURCE_AUDIT_RELEASE]);
    expect(query.mock.calls[3][1]).toEqual([SOURCE_AUDIT_RELEASE]);
  });
});
