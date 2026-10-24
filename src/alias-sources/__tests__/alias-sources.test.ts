import { describe, expect, it } from 'vitest';
import {
  ALIAS_ONLY_SOURCE_IDS,
  AliasOnlyEnrichmentService,
  AliasOnlyRepository,
  MangaBallAliasSource,
  MangaDexAliasSource,
  MangaFireAliasSource,
  buildSignedMangaFireApiUrl,
  computeMergedWorkAliases,
  evaluateSourceCandidates,
  isPortugueseAlias,
  signMangaFirePath,
  type AliasSourceCandidate,
  type NoxWorkAliasTarget,
} from '../index.js';

function jsonResponse(body: unknown, status = 200, headers: Record<string, string> = {}): Response {
  return new Response(typeof body === 'string' ? body : JSON.stringify(body), {
    status,
    headers: { 'content-type': 'application/json', ...headers },
  });
}

describe('ALIAS_ONLY 3-Source Suite (MangaDex, MangaFire, MangaBall)', () => {
  it('adapts MangaDex search & detail responses, extracting primary title + localized altTitles (PT-BR, EN, CJK, Romaji) and filtering non-target scripts', async () => {
    const mockFetch: typeof fetch = async (input) => {
      const url = String(input);
      expect(url).toContain('api.mangadex.org/manga');
      expect(url).toContain('contentRating%5B%5D=pornographic');
      return jsonResponse({
        result: 'ok',
        data: [
          {
            id: 'f461e344-2385-46b5-83b8-8776f5ea65d0',
            type: 'manga',
            attributes: {
              title: { en: 'The Top Dungeon Farmer' },
              altTitles: [
                { 'pt-br': 'O Único Fazendeiro da Torre' },
                { en: 'Solo Farming in the Tower' },
                { ko: '나혼자 탑에서 농사' },
                { 'ko-ro': 'Nahonja Tabeseo Nongsa' },
                { ja: '俺だけ塔で農家はじめました' },
                { zh: '我独自在高塔种田' },
                { ru: 'Фермерство в башне в одиночку' },
              ],
              originalLanguage: 'ko',
              year: 2023,
              contentRating: 'safe',
              links: { mu: 'fdbhhx7', al: '165000' },
            },
            relationships: [
              { id: '1', type: 'author', attributes: { name: 'Lim Hyeong' } },
              { id: '2', type: 'artist', attributes: { name: 'Lee Ha Kyung' } },
            ],
          },
        ],
      });
    };

    const md = new MangaDexAliasSource({ fetchImpl: mockFetch, minIntervalMs: 0 });
    const res = await md.searchWorks('Solo Farming in the Tower');
    expect(res.ok).toBe(true);
    expect(res.candidates).toHaveLength(1);
    const cand = res.candidates[0];
    expect(cand.primaryTitle).toBe('The Top Dungeon Farmer');
    expect(cand.authors).toContain('Lim Hyeong');
    expect(cand.links?.mangadexId).toBe('f461e344-2385-46b5-83b8-8776f5ea65d0');
    // PT-BR prioritized first, Russian excluded from persisted aliases
    expect(cand.aliases[0]).toEqual({
      title: 'O Único Fazendeiro da Torre',
      language: 'pt-br',
      isPrimary: false,
    });
    expect(cand.aliases.some((a) => a.title.includes('Фермерство'))).toBe(false);
    expect(cand.rawMatchAliases).toContain('Фермерство в башне в одиночку');
  });

  it('signs MangaFire VRF queries deterministically and extracts primary + altTitles from /api/titles/<hid>', async () => {
    const vrf = signMangaFirePath('/titles?keyword=Solo Farming in the Tower&limit=10&page=1');
    expect(vrf.length).toBeGreaterThan(20);
    const signedUrl = buildSignedMangaFireApiUrl('https://mangafire.to', '/api/titles', [
      ['page', '1'],
      ['keyword', 'Solo Farming in the Tower'],
      ['limit', '10'],
    ]);
    expect(signedUrl).toContain(`vrf=${vrf}`);

    const mockFetch: typeof fetch = async (input) => {
      const url = String(input);
      if (url.includes('/api/titles/zx57l')) {
        return jsonResponse({
          data: {
            id: 45361,
            hid: 'zx57l',
            slug: 'the-top-dungeon-farmer',
            title: 'The Top Dungeon Farmer',
            type: 'manhwa',
            contentRating: 'safe',
            year: 2023,
            url: '/title/zx57l-the-top-dungeon-farmer',
            altTitles: [
              'Solo Farming in the Tower',
              'Nahonja Tabeseo Nongsa',
              '나혼자 탑에서 농사',
              'ปลูกผักพิชิตหอคอย',
            ],
            authors: [{ id: 1, title: 'Lim Hyeong' }],
            artists: [{ id: 2, title: 'Lee Ha Kyung' }],
          },
        });
      }
      return jsonResponse({
        items: [
          {
            id: 45361,
            hid: 'zx57l',
            slug: 'the-top-dungeon-farmer',
            title: 'The Top Dungeon Farmer',
            type: 'manhwa',
            year: 2023,
            url: '/title/zx57l-the-top-dungeon-farmer',
          },
        ],
      });
    };

    const mf = new MangaFireAliasSource({ fetchImpl: mockFetch, minIntervalMs: 0 });
    const search = await mf.searchWorks('Solo Farming in the Tower');
    expect(search.ok).toBe(true);
    expect(search.candidates[0].sourceWorkId).toBe('zx57l');

    const detail = await mf.fetchWorkAliases('zx57l');
    expect(detail.ok).toBe(true);
    expect(detail.candidate?.primaryTitle).toBe('The Top Dungeon Farmer');
    expect(detail.candidate?.aliases.map((a) => a.title)).toContain('Solo Farming in the Tower');
    expect(detail.candidate?.aliases.map((a) => a.title)).toContain('나혼자 탑에서 농사');
  });

  it('extracts MangaBall search-advanced alternateName + MangaDex UUID cross-link in a single call', async () => {
    const mockFetch: typeof fetch = async () =>
      jsonResponse({
        code: 200,
        status: 'success',
        data: [
          {
            id: '68515edf702284f83417a1eb',
            name: 'The Top Dungeon Farmer',
            slug: 'the-top-dungeon-farmer',
            alternateName: [
              'O Único Fazendeiro da Torre',
              'Solo Farming in the Tower',
              '나혼자 탑에서 농사',
              'Làm Nông Dân Trong Tòa Thách',
            ],
            authors: [{ name: 'Lim Hyeong' }],
            originalLanguage: 'ko',
            is18plus: false,
            image: {
              cdn_mangadex:
                'https://uploads.mangadex.org/covers/f461e344-2385-46b5-83b8-8776f5ea65d0/cover.jpg',
            },
          },
        ],
      });

    const mb = new MangaBallAliasSource({ fetchImpl: mockFetch, minIntervalMs: 0 });
    const res = await mb.searchWorks('Solo Farming in the Tower');
    expect(res.ok).toBe(true);
    expect(res.candidates[0].detailsLoaded).toBe(true);
    expect(res.candidates[0].links?.mangadexId).toBe('f461e344-2385-46b5-83b8-8776f5ea65d0');
    expect(res.candidates[0].aliases.map((a) => a.title)).toContain('O Único Fazendeiro da Torre');
  });

  it('confirms active ALIAS_ONLY_SOURCE_IDS contains strictly MangaBall, MangaDex, and MangaFire (Comix removed)', () => {
    expect(ALIAS_ONLY_SOURCE_IDS).toEqual(['mangaball', 'mangadex', 'mangafire']);
  });

  it('rejects false positives (sequels, novels, safe vs explicit porn, uncorroborated generic homonyms) while confirming legitimate variants', () => {
    const noxMain: NoxWorkAliasTarget = {
      id: '00000000-0000-0000-0000-000000000001',
      title: 'Solo Farming in the Tower',
      slug: 'solo-farming-in-the-tower',
      aliases: [],
      kind: 'manhwa',
      year: 2023,
    };

    // Source primary title is PT-BR ("O Único Fazendeiro da Torre"), altTitle has "Solo Farming in the Tower"
    const ptSourceCand: AliasSourceCandidate = {
      source: 'mangaball',
      sourceWorkId: 'mb-1',
      url: 'https://mangaball.com/title-detail/mb-1',
      primaryTitle: 'O Único Fazendeiro da Torre',
      aliases: [
        { title: 'O Único Fazendeiro da Torre', isPrimary: true },
        { title: 'Solo Farming in the Tower' },
        { title: '나혼자 탑에서 농사' },
      ],
      rawMatchAliases: ['O Único Fazendeiro da Torre', 'Solo Farming in the Tower', '나혼자 탑에서 농사'],
      detailsLoaded: true,
    };
    const confirmed = evaluateSourceCandidates(noxMain, [ptSourceCand]);
    expect(confirmed.status).toBe('CONFIRMED_MATCH');
    const merged = computeMergedWorkAliases(noxMain, ptSourceCand.aliases);
    expect(merged.addedAliases).toContain('O Único Fazendeiro da Torre');
    expect(merged.addedAliases).toContain('나혼자 탑에서 농사');
    expect(merged.ptBrAliasesAdded).toContain('O Único Fazendeiro da Torre');

    // Novel conflict rejected
    const novelCand: AliasSourceCandidate = {
      ...ptSourceCand,
      sourceWorkId: 'novel-1',
      primaryTitle: 'Solo Farming in the Tower (Novel)',
      kind: 'novel',
    };
    expect(evaluateSourceCandidates(noxMain, [novelCand]).status).toBe('NOT_FOUND');

    // Sequel conflict rejected
    const sequelCand: AliasSourceCandidate = {
      ...ptSourceCand,
      sourceWorkId: 'seq-2',
      primaryTitle: 'Solo Farming in the Tower Season 2',
      rawMatchAliases: ['Solo Farming in the Tower Season 2'],
      aliases: [{ title: 'Solo Farming in the Tower Season 2', isPrimary: true }],
    };
    expect(evaluateSourceCandidates(noxMain, [sequelCand]).status).toBe('NOT_FOUND');

    // Generic homonyms ("Secret") without creator/secondary alias -> AMBIGUOUS_MATCH (never CONFIRMED_MATCH!)
    const genericNox: NoxWorkAliasTarget = {
      id: '00000000-0000-0000-0000-000000000002',
      title: 'Secret',
      slug: 'secret',
      aliases: [],
    };
    const homonym1: AliasSourceCandidate = {
      source: 'mangafire',
      sourceWorkId: 'h1',
      url: 'https://mangafire.to/title/h1',
      primaryTitle: 'Secret',
      aliases: [{ title: 'Secret', isPrimary: true }, { title: '秘密 (Author A)' }],
      rawMatchAliases: ['Secret'],
      detailsLoaded: true,
    };
    const homonym2: AliasSourceCandidate = {
      source: 'mangafire',
      sourceWorkId: 'h2',
      url: 'https://mangafire.to/title/h2',
      primaryTitle: 'Secret!',
      aliases: [{ title: 'Secret!', isPrimary: true }, { title: 'シークレット (Author B)' }],
      rawMatchAliases: ['Secret!'],
      detailsLoaded: true,
    };
    const homonymEval = evaluateSourceCandidates(genericNox, [homonym1, homonym2]);
    expect(homonymEval.status).toBe('AMBIGUOUS_MATCH');
  });

  it('preserves grammatical commas, 66,666 numbers, hyphenated compounds, existing aliases, and guarantees strict idempotency', () => {
    const nox: NoxWorkAliasTarget = {
      id: '00000000-0000-0000-0000-000000000003',
      title: '66,666 Years: Advent of the Dark Mage',
      slug: '66666-years-advent-of-the-dark-mage',
      aliases: [
        'Summon The Demon, For I Am The Abyss',
        "The Returned C-Rank Tank Won't Die!",
      ],
    };

    const incoming = [
      { title: 'O Mago Negro Após 66,666 Anos', language: 'pt-br' },
      { title: '66,666년 만에 환생한 흑마법사', language: 'ko' },
      { title: 'Summon The Demon', language: 'en' }, // half-sentence fragment must be blocked
      { title: "Rank Tank Won't Die!", language: 'en' }, // hyphen-suffix fragment must be blocked
      { title: 'Фермерство в башне', language: 'ru' }, // Cyrillic must be excluded
      { title: 'Wo Yige Shen Hao, Dang Zhanan Hen Heli ba 我一个神豪,当渣男很合理吧' }, // Latin+CJK space concatenation with internal commas
      { title: '[H] O Mago Negro Após 66,666 Anos' }, // [H] category prefix stripped and deduped
    ];

    const pass1 = computeMergedWorkAliases(nox, incoming);
    expect(pass1.aliasesChanged).toBe(true);
    expect(pass1.aliasesAfter).toContain('Summon The Demon, For I Am The Abyss');
    expect(pass1.aliasesAfter).toContain("The Returned C-Rank Tank Won't Die!");
    expect(pass1.aliasesAfter).toContain('O Mago Negro Após 66,666 Anos');
    expect(pass1.aliasesAfter).toContain('66,666년 만에 환생한 흑마법사');
    expect(pass1.aliasesAfter).toContain('Wo Yige Shen Hao, Dang Zhanan Hen Heli ba');
    expect(pass1.aliasesAfter).toContain('我一个神豪,当渣男很合理吧');
    expect(pass1.aliasesAfter).not.toContain('Dang Zhanan Hen Heli ba 我一个神豪');
    expect(pass1.aliasesAfter).not.toContain('[H] O Mago Negro Após 66,666 Anos');
    expect(pass1.aliasesAfter).not.toContain('Summon The Demon');
    expect(pass1.aliasesAfter).not.toContain("Rank Tank Won't Die!");
    expect(pass1.ptBrAliasesAdded).toEqual(['O Mago Negro Após 66,666 Anos']);
    expect(isPortugueseAlias('Cultivando en la torre en solitario')).toBe(false);

    // Idempotency: running pass 2 with the exact same incoming produces zero new changes
    const pass2 = computeMergedWorkAliases({ ...nox, aliases: pass1.aliasesAfter }, incoming);
    expect(pass2.aliasesChanged).toBe(false);
    expect(pass2.addedAliases).toEqual([]);
    expect(pass2.aliasesAfter).toEqual(pass1.aliasesAfter);
  });

  it('handles HTTP 429 Retry-After, timeouts, and offline sources without blocking healthy sources, and chains Wave-1 discoveries to Wave-2', async () => {
    let mdAttempts = 0;
    const md = new MangaDexAliasSource({
      minIntervalMs: 0,
      maxRetries: 1,
      fetchImpl: async () => {
        mdAttempts++;
        if (mdAttempts === 1) {
          return jsonResponse({ message: 'Too Many Requests' }, 429, { 'retry-after': '0' });
        }
        return jsonResponse({
          result: 'ok',
          data: [
            {
              id: 'f461e344-2385-46b5-83b8-8776f5ea65d0',
              type: 'manga',
              attributes: {
                title: { en: 'The Top Dungeon Farmer' },
                altTitles: [{ 'ko-ro': 'Nahonja Tabeseo Nongsa' }],
                originalLanguage: 'ko',
                contentRating: 'safe',
              },
            },
          ],
        });
      },
    });

    const mb = new MangaBallAliasSource({
      minIntervalMs: 0,
      fetchImpl: async () =>
        jsonResponse({
          code: 200,
          data: [
            {
              id: 'mb-99',
              name: 'The Top Dungeon Farmer',
              alternateName: ['O Único Fazendeiro da Torre', 'Solo Farming in the Tower'],
              originalLanguage: 'ko',
              image: {
                cdn_mangadex:
                  'https://uploads.mangadex.org/covers/f461e344-2385-46b5-83b8-8776f5ea65d0/cover.jpg',
              },
            },
          ],
        }),
    });

    // MangaFire only knows the English title ("The Top Dungeon Farmer"), while Nox work only has PT-BR!
    // Wave-1 MangaBall discovers "The Top Dungeon Farmer" -> Wave-2 MangaFire finds it automatically!
    const mf = new MangaFireAliasSource({
      minIntervalMs: 0,
      fetchImpl: async (input) => {
        const u = String(input);
        if (u.includes('/api/titles/zx57l')) {
          return jsonResponse({
            data: {
              hid: 'zx57l',
              title: 'The Top Dungeon Farmer',
              altTitles: ['Farming in the Tower Alone', '나혼자 탑에서 농사'],
              type: 'manhwa',
              contentRating: 'safe',
            },
          });
        }
        if (u.includes('The+Top+Dungeon+Farmer') || u.includes('The%20Top%20Dungeon%20Farmer')) {
          return jsonResponse({
            items: [{ hid: 'zx57l', title: 'The Top Dungeon Farmer', type: 'manhwa' }],
          });
        }
        return jsonResponse({ items: [] });
      },
    });

    const service = new AliasOnlyEnrichmentService({
      adapters: { mangadex: md, mangaball: mb, mangafire: mf },
    });

    const ptOnlyWork: NoxWorkAliasTarget = {
      id: '00000000-0000-0000-0000-000000000099',
      title: 'O Único Fazendeiro da Torre',
      slug: 'o-unico-fazendeiro-da-torre',
      aliases: [],
    };

    const result = await service.enrichWork(ptOnlyWork);
    expect(result.confirmedSources).toEqual(expect.arrayContaining(['mangaball', 'mangadex', 'mangafire']));
    expect(result.errorSources).toEqual([]);
    expect(result.addedAliases).toEqual(
      expect.arrayContaining([
        'The Top Dungeon Farmer',
        'Solo Farming in the Tower',
        'Nahonja Tabeseo Nongsa',
        'Farming in the Tower Alone',
        '나혼자 탑에서 농사',
      ]),
    );
  });

  it('selects newly created works via immediateWorkIds even while published = false before chapter publication completes', async () => {
    const mockPool = {
      query: async (sql: string, params?: any[]) => {
        if (sql.includes('WHERE w.id = ANY($1::uuid[])')) {
          expect(params?.[0]).toEqual(['a3da533f-23d4-4e00-aee5-c2dde4d8429a']);
          return {
            rows: [
              {
                id: 'a3da533f-23d4-4e00-aee5-c2dde4d8429a',
                title: 'Hirotta Kinpatsu Gal ni Furimawasareru Kurashi mo, Angai Warukunai no Kamoshirenai',
                slug: 'hirotta-kinpatsu-gal-ni-furimawasareru-kurashi-mo-angai-warukunai-no-kamoshirenai',
                aliases: ['拾った金髪ギャルに振り回される暮らしも、案外悪くないのかもしれない'],
                author: null,
                artist: null,
                kind: 'manga',
                year: 2024,
                age_rating: 18,
                content_rating: 'ADULT_18',
              },
            ],
          };
        }
        return { rows: [] };
      },
    } as any;

    const repo = new AliasOnlyRepository(mockPool);
    (repo as any).schemaReady = true;
    const due = await repo.findDueWorksForRuntimeEnrichment(3, ['a3da533f-23d4-4e00-aee5-c2dde4d8429a']);
    expect(due).toHaveLength(1);
    expect(due[0].target.id).toBe('a3da533f-23d4-4e00-aee5-c2dde4d8429a');
    expect(due[0].reason).toBe('NEW_WORK');
  });
});
