import { describe, expect, it, vi } from 'vitest';
import { HostRateLimiter } from '../src/core/rate-limiter.js';
import { WpGalleryAdapter } from '../src/sources/common/wpgallery-adapter.js';
import { ZeistMangaAdapter } from '../src/sources/common/zeistmanga-adapter.js';
import { GreenShitAdapter } from '../src/sources/common/greenshit-adapter.js';
import { MadaraAdapter } from '../src/sources/common/madara-adapter.js';
import { NexusAdapter } from '../src/sources/nexus/nexus-adapter.js';
import { MangaFlixAdapter } from '../src/sources/mangaflix/mangaflix-adapter.js';
import { MangoToonsAdapter } from '../src/sources/mangotoons/mangotoons-adapter.js';
import { ManhastroAdapter } from '../src/sources/manhastro/manhastro-adapter.js';
import {
  buildDiscoveryQueryCandidates,
  classifyExplicitSourceMetadata,
} from '../src/lite/mapping-metadata.js';
import { cleanAlternativeTitle, mergeAlternativeTitles } from '../src/core/alternative-titles.js';
import {
  verifySourceIndependently,
  auditWork,
  extractRawAlternativeTitlesFromAdapterDetails,
} from '../scripts/audit_full_catalog_daemon.js';

describe('V7 Metadata & Lockstep Auditor Definitive Suite', () => {
  const limiter = new HostRateLimiter(100);

  it('1. Adapter returning timeout -> auditor classifies as DEFERRED_TIMEOUT, never NOT_FOUND', async () => {
    const slowAdapter: any = {
      id: 'manhuaus',
      name: 'ManhuaUS',
      baseUrl: 'https://manhuaus.com',
      searchWorks: async () => {
        throw new Error('Request timeout after 5000ms');
      },
      fetchWorkDetails: async () => {
        throw new Error('Request timeout after 5000ms');
      },
    };

    const searchCheck = await verifySourceIndependently(slowAdapter, 'Solo Leveling', null, []);
    expect(searchCheck.status).toBe('DEFERRED_TIMEOUT');
    expect(searchCheck.status).not.toBe('NOT_FOUND');

    const detailsCheck = await verifySourceIndependently(
      slowAdapter,
      'Solo Leveling',
      { source: 'manhuaus', source_work_id: 'solo-leveling' },
      []
    );
    expect(detailsCheck.status).toBe('DEFERRED_TIMEOUT');
  });

  it('2. Adapter returning HTTP 429 -> auditor classifies as DEFERRED_ERROR, never NOT_FOUND', async () => {
    const rateLimitedAdapter: any = {
      id: 'mangadex',
      name: 'MangaDex',
      baseUrl: 'https://api.mangadex.org',
      searchWorks: async () => {
        const err: any = new Error('HTTP 429 Too Many Requests');
        err.status = 429;
        throw err;
      },
      fetchWorkDetails: async () => {
        const err: any = new Error('HTTP 429 Too Many Requests');
        err.status = 429;
        throw err;
      },
    };

    const searchCheck = await verifySourceIndependently(rateLimitedAdapter, 'Solo Leveling', null, []);
    expect(searchCheck.status).toBe('DEFERRED_ERROR');

    const detailsCheck = await verifySourceIndependently(
      rateLimitedAdapter,
      'Solo Leveling',
      { source: 'mangadex', source_work_id: 'id-1' },
      []
    );
    expect(detailsCheck.status).toBe('DEFERRED_ERROR');
  });

  it('3. Adapter returning HTTP 500/502/503 -> auditor classifies as DEFERRED_ERROR, never NOT_FOUND', async () => {
    const serverErrorAdapter: any = {
      id: 'anilist',
      name: 'AniList',
      baseUrl: 'https://graphql.anilist.co',
      searchWorks: async () => {
        throw new Error('HTTP 502 Bad Gateway');
      },
      fetchWorkDetails: async () => {
        throw new Error('HTTP 503 Service Unavailable');
      },
    };

    const searchCheck = await verifySourceIndependently(serverErrorAdapter, 'Solo Leveling', null, []);
    expect(searchCheck.status).toBe('DEFERRED_ERROR');

    const detailsCheck = await verifySourceIndependently(
      serverErrorAdapter,
      'Solo Leveling',
      { source: 'anilist', source_work_id: '101' },
      []
    );
    expect(detailsCheck.status).toBe('DEFERRED_ERROR');
  });

  it('4. Adapter returning Cloudflare 403 / invalid HTML challenge -> auditor classifies as DEFERRED_ERROR', async () => {
    const cloudflareAdapter: any = {
      id: 'harimanga',
      name: 'Harimanga',
      baseUrl: 'https://harimanga.me',
      searchWorks: async () => {
        throw new Error('HTTP 403 Forbidden (Cloudflare challenge)');
      },
      fetchWorkDetails: async () => {
        throw new Error('Unexpected token < in JSON at position 0');
      },
    };

    const searchCheck = await verifySourceIndependently(cloudflareAdapter, 'Solo Leveling', null, []);
    expect(searchCheck.status).toBe('DEFERRED_ERROR');

    const detailsCheck = await verifySourceIndependently(
      cloudflareAdapter,
      'Solo Leveling',
      { source: 'harimanga', source_work_id: 'solo' },
      []
    );
    expect(detailsCheck.status).toBe('DEFERRED_ERROR');
  });

  it('5. Adapter returning legitimate empty results -> auditor classifies as NOT_FOUND', async () => {
    const emptyAdapter: any = {
      id: 'egotoons',
      name: 'EgoToons',
      baseUrl: 'https://egotoons.com',
      searchWorks: async () => [],
      fetchWorkDetails: async () => null,
    };

    const searchCheck = await verifySourceIndependently(emptyAdapter, 'NonExistentWorkXYZ', null, []);
    expect(searchCheck.status).toBe('NOT_FOUND');
  });

  it('6. Source with work without alternative titles -> auditor classifies as FOUND_NO_ALIASES', async () => {
    const noAliasAdapter: any = {
      id: 'borutoexplorer',
      name: 'BorutoExplorer',
      baseUrl: 'https://borutoexplorer.com',
      searchWorks: async () => [
        { sourceWorkId: 'solo-leveling', title: 'Solo Leveling', slug: 'solo-leveling' },
      ],
      fetchWorkDetails: async () => ({
        sourceWorkId: 'solo-leveling',
        title: 'Solo Leveling',
        slug: 'solo-leveling',
        alternativeTitles: [],
        raw: {},
      }),
    };

    const res = await verifySourceIndependently(noAliasAdapter, 'Solo Leveling', null, []);
    expect(res.status).toBe('FOUND_NO_ALIASES');
    expect(res.aliasesFound).toEqual([]);
  });

  it('7. Source with work and legitimate alternative titles -> auditor classifies as FOUND_WITH_ALIASES', async () => {
    const withAliasAdapter: any = {
      id: 'mangadex',
      name: 'MangaDex',
      baseUrl: 'https://api.mangadex.org',
      searchWorks: async () => [
        { sourceWorkId: 'md-1', title: 'Solo Leveling', slug: 'solo-leveling' },
      ],
      fetchWorkDetails: async () => ({
        sourceWorkId: 'md-1',
        title: 'Solo Leveling',
        slug: 'solo-leveling',
        alternativeTitles: ['Ore dake Level Up na Ken', '나 혼자만 레벨업'],
        raw: {},
      }),
    };

    const res = await verifySourceIndependently(withAliasAdapter, 'Solo Leveling', null, []);
    expect(res.status).toBe('FOUND_WITH_ALIASES');
    expect(res.aliasesFound).toContain('Ore dake Level Up na Ken');
    expect(res.aliasesFound).toContain('나 혼자만 레벨업');
  });

  it('8. Composite alias splitting preserves titles with internal interjection commas while splitting real lists', () => {
    const extracted = classifyExplicitSourceMetadata(
      {
        alternativeTitles: [
          'Hello, Fluffy Griffin!',
          'Solo Leveling, Ore dake Level Up na Ken, Only I Level Up',
        ],
      },
      'Fluffy Griffin'
    );
    expect(extracted.aliases).toContain('Hello, Fluffy Griffin!');
    expect(extracted.aliases).toContain('Solo Leveling');
    expect(extracted.aliases).toContain('Ore dake Level Up na Ken');
    expect(extracted.aliases).toContain('Only I Level Up');

    const queries = buildDiscoveryQueryCandidates('Hello, Fluffy Griffin!', ['Griffin!']);
    expect(queries[0]).toBe('Hello, Fluffy Griffin!');
    expect(queries).not.toContain('Hello');
  });

  it('9. Alias in JSON string array is decoded properly by both importer and auditor', () => {
    const rawMeta = {
      alternativeTitles: '["The Fist King\'s Rebirth", "Rebirth of the Fist King"]',
    };
    const importerExtracted = classifyExplicitSourceMetadata(rawMeta, 'O Renascimento do Rei do Punho');
    expect(importerExtracted.aliases).toEqual([
      "The Fist King's Rebirth",
      'Rebirth of the Fist King',
    ]);

    const auditorExtracted = extractRawAlternativeTitlesFromAdapterDetails(
      { title: 'O Renascimento do Rei do Punho', raw: rawMeta },
      'O Renascimento do Rei do Punho'
    );
    expect(auditorExtracted).toContain("The Fist King's Rebirth");
    expect(auditorExtracted).toContain('Rebirth of the Fist King');
  });

  it('10. Noise (CSS, JS, ratings, taxonomy words) is rejected without affecting legitimate aliases', () => {
    expect(cleanAlternativeTitle('.wp-post-title { font-size: 14px; color: rgb(0,0,0); }')).toBeNull();
    expect(cleanAlternativeTitle('4.5 de 5 estrelas')).toBeNull();
    expect(cleanAlternativeTitle('Manhwa')).toBeNull();
    expect(cleanAlternativeTitle('Ore dake Level Up na Ken')).toBe('Ore dake Level Up na Ken');
  });

  it('11. MadaraAdapter.searchWorks ignores RSS feed links and extracts real title when thumbnail link comes first', async () => {
    const adapter = new MadaraAdapter(
      { id: 'astratoons', name: 'AstraToons', baseUrl: 'https://astratoons.com' },
      limiter
    );
    vi.spyOn(adapter as any, 'fetchHtml').mockResolvedValue(`
      <div class="row c-tabs-item__content">
        <div class="tab-thumb c-image-hover">
          <a href="https://astratoons.com/manga/the-fist-kings-rebirth/" title="O Renascimento do Rei do Punho">
            <img src="cover.jpg" alt="O Renascimento do Rei do Punho" />
          </a>
        </div>
        <div class="tab-summary">
          <div class="post-title">
            <h3><a href="https://astratoons.com/manga/the-fist-kings-rebirth/">O Renascimento do Rei do Punho</a></h3>
          </div>
          <a href="https://astratoons.com/manga/the-fist-kings-rebirth/feed/">feed</a>
        </div>
      </div>
    `);

    const results = await adapter.searchWorks('The Fist King');
    expect(results).toHaveLength(1);
    expect(results[0].sourceWorkId).toBe('the-fist-kings-rebirth');
    expect(results[0].title).toBe('O Renascimento do Rei do Punho');
  });

  it('12. WpGallery, ZeistManga, GreenShit, Nexus, MangaFlix, MangoToons, and Manhastro rethrow non-404 errors in searchWorks', async () => {
    const wp = new WpGalleryAdapter({ id: 'hentaitokyo', name: 'HT', baseUrl: 'https://hentaitokyo.net' }, limiter);
    vi.spyOn(wp as any, 'fetchJson').mockRejectedValue(new Error('HTTP 503 Service Unavailable'));
    await expect(wp.searchWorks('test')).rejects.toThrow('HTTP 503');

    const zeist = new ZeistMangaAdapter({ id: 'ler999', name: 'Ler999', baseUrl: 'https://ler999.blogspot.com' }, limiter);
    vi.spyOn(zeist as any, 'fetchJson').mockRejectedValue(new Error('Request timeout'));
    await expect(zeist.searchWorks('test')).rejects.toThrow('Request timeout');

    const gs = new GreenShitAdapter(
      {
        id: 'maidscan',
        name: 'MaidScan',
        baseUrl: 'https://maidscan.com',
        apiUrl: 'https://api.maidscan.com',
        cdnUrl: 'https://cdn.maidscan.com',
        scanId: 1,
      },
      limiter
    );
    vi.spyOn(gs as any, 'fetchJson').mockRejectedValue(new Error('HTTP 502 Bad Gateway'));
    await expect(gs.searchWorks('test')).rejects.toThrow('HTTP 502');

    const nexus = new NexusAdapter(limiter);
    vi.spyOn(nexus as any, 'ensureCatalogLoaded').mockReturnValue(false);
    vi.spyOn(nexus as any, 'request').mockRejectedValue(new Error('fetch failed'));
    await expect(nexus.searchWorks('test')).rejects.toThrow('fetch failed');

    const flix = new MangaFlixAdapter(limiter);
    vi.spyOn(flix as any, 'request').mockRejectedValue(new Error('HTTP 403 Forbidden'));
    await expect(flix.searchWorks('test')).rejects.toThrow('HTTP 403');

    const mango = new MangoToonsAdapter(limiter);
    vi.spyOn(mango as any, 'requestApi').mockRejectedValue(new Error('AbortError: timed out'));
    await expect(mango.searchWorks('test')).rejects.toThrow('AbortError');

    const manhastro = new ManhastroAdapter(limiter);
    vi.spyOn(manhastro as any, 'ensureCatalogLoaded').mockReturnValue(false);
    vi.spyOn(manhastro as any, 'request').mockRejectedValue(new Error('TimeoutError'));
    await expect(manhastro.searchWorks('test')).rejects.toThrow('TimeoutError');
  });

  it('13 & 14. Auditor V7 verdicts: DEFERRED_TIMEOUT -> AUDIT_INCONCLUSIVE; missing alias -> AUDIT_FAIL; all verified -> AUDIT_PASS', async () => {
    const fetchSpy = vi.spyOn(globalThis, 'fetch').mockResolvedValue({
      text: async () => 'alternativeTitles: ["Ore dake Level Up na Ken", "Only I Level Up"]',
    } as any);
    try {
      const work = {
        id: '00000000-0000-4000-8000-000000000001',
        title: 'Solo Leveling',
        slug: 'solo-leveling',
        aliases: ['Ore dake Level Up na Ken'],
      };
      const taskInfo = {
        id: '00000000-0000-4000-8000-000000000099',
        state: 'DONE',
        updated_at: new Date().toISOString(),
        checkpoint: { coverage: 'CROSS_SOURCE_PARTIAL_EXTERNAL_DEFERRED', deferredSources: ['brscans'] },
      };

      // Case A: One operational source times out -> AUDIT_INCONCLUSIVE (never AUDIT_PASS!)
      const timeoutSources: any[] = [
        {
          id: 'mangadex',
          searchWorks: async () => {
            throw new Error('SEARCH_TIMEOUT_6000MS');
          },
        },
        { id: 'brscans' },
      ];
      const resInconclusive = await auditWork(work, timeoutSources as any, [], work.aliases, taskInfo, null);
      expect(resInconclusive.verdict).toBe('AUDIT_INCONCLUSIVE');
      expect(resInconclusive.sourceChecks.find((c: any) => c.sourceId === 'brscans')?.status).toBe('EXTERNAL_UNAVAILABLE');

      // Case B: Source has legitimate alias missing from YSQL -> AUDIT_FAIL
      const missingAliasSources: any[] = [
        {
          id: 'mangadex',
          searchWorks: async () => [{ sourceWorkId: 'md-1', title: 'Solo Leveling', slug: 'solo-leveling' }],
          fetchWorkDetails: async () => ({
            sourceWorkId: 'md-1',
            title: 'Solo Leveling',
            slug: 'solo-leveling',
            alternativeTitles: ['Ore dake Level Up na Ken', 'Only I Level Up'],
            raw: {},
          }),
        },
        { id: 'brscans' },
      ];
      const resFail = await auditWork(work, missingAliasSources as any, [], work.aliases, taskInfo, null);
      expect(resFail.verdict).toBe('AUDIT_FAIL');
      expect(resFail.missingFromYsql).toContain('Only I Level Up');

      // Case C: All operational sources verified and all aliases present in YSQL -> AUDIT_PASS
      const resPass = await auditWork(
        work,
        missingAliasSources as any,
        [],
        ['Ore dake Level Up na Ken', 'Only I Level Up'],
        taskInfo,
        null
      );
      expect(resPass.verdict).toBe('AUDIT_PASS');
      expect(resPass.missingFromYsql).toEqual([]);
    } finally {
      fetchSpy.mockRestore();
    }
  });

  it('15. Additive alias merging preserves all existing legitimate aliases across multiple sources', () => {
    const existing = ['Ore dake Level Up na Ken', '나 혼자만 레벨업'];
    const incomingFromSourceA = ['Only I Level Up', 'Ore dake Level Up na Ken'];
    const incomingFromSourceB = ['I Alone Level Up'];

    const afterA = mergeAlternativeTitles(existing, incomingFromSourceA, 'Solo Leveling');
    const afterB = mergeAlternativeTitles(afterA, incomingFromSourceB, 'Solo Leveling');

    expect(afterB).toEqual([
      'Ore dake Level Up na Ken',
      '나 혼자만 레벨업',
      'Only I Level Up',
      'I Alone Level Up',
    ]);
  });
});
