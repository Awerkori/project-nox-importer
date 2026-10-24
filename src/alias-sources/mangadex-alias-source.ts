import { SourceHttpClient, type SourceHttpClientOptions } from './http-client.js';
import type {
  AliasOnlySourceAdapter,
  AliasSourceCandidate,
  AliasSourceDetailOutcome,
  AliasSourceHealthSnapshot,
  AliasSourceSearchOutcome,
  ExternalCrossLinks,
  LocalizedAlias,
} from './types.js';

interface MdLocalizedMap {
  [lang: string]: string | undefined;
}

interface MdRelationship {
  id: string;
  type: string;
  attributes?: {
    name?: string;
  };
}

interface MdMangaData {
  id: string;
  type: string;
  attributes?: {
    title?: MdLocalizedMap;
    altTitles?: MdLocalizedMap[];
    links?: Record<string, string | null | undefined>;
    originalLanguage?: string | null;
    publicationDemographic?: string | null;
    status?: string | null;
    year?: number | null;
    contentRating?: string | null;
    tags?: Array<{
      id?: string;
      attributes?: {
        name?: MdLocalizedMap;
        group?: string;
      };
    }>;
  };
  relationships?: MdRelationship[];
}

interface MdListResponse {
  result?: string;
  data?: MdMangaData[];
}

interface MdSingleResponse {
  result?: string;
  data?: MdMangaData;
}

const PREFERRED_LANG_ORDER: Record<string, number> = {
  'pt-br': 1,
  pt: 2,
  en: 3,
  'ja-ro': 4,
  'ko-ro': 5,
  'zh-ro': 6,
  ja: 7,
  ko: 8,
  zh: 9,
  'zh-hk': 10,
  la: 11,
  es: 12,
  'es-la': 13,
  fr: 14,
};

const ALLOWED_PERSIST_LANGS = new Set([
  'pt-br',
  'pt',
  'en',
  'ja-ro',
  'ko-ro',
  'zh-ro',
  'ja',
  'ko',
  'zh',
  'zh-hk',
  'la',
]);

function extractExternalLinks(links?: Record<string, string | null | undefined>, id?: string): ExternalCrossLinks {
  const out: ExternalCrossLinks = {};
  if (id) out.mangadexId = id;
  if (!links) return out;
  if (links.al) out.anilistId = String(links.al).trim();
  if (links.mal) out.malId = String(links.mal).trim();
  if (links.mu) out.mangaUpdatesId = String(links.mu).trim();
  return out;
}

function mapMangaDexEntry(entry: MdMangaData): AliasSourceCandidate | null {
  const id = String(entry?.id || '').trim();
  const attrs = entry?.attributes;
  if (!id || !attrs) return null;

  const titleMap = attrs.title || {};
  const primaryTitle =
    titleMap['pt-br'] ||
    titleMap.pt ||
    titleMap.en ||
    titleMap['ja-ro'] ||
    titleMap['ko-ro'] ||
    titleMap['zh-ro'] ||
    Object.values(titleMap).find((v) => typeof v === 'string' && v.trim().length > 0) ||
    '';

  if (!primaryTitle.trim()) return null;

  const rawMatchAliases: string[] = [];
  const localizedCandidates: Array<LocalizedAlias & { rank: number }> = [];

  for (const [langRaw, val] of Object.entries(titleMap)) {
    const text = String(val || '').trim();
    if (!text) continue;
    const lang = langRaw.toLowerCase();
    rawMatchAliases.push(text);
    localizedCandidates.push({
      title: text,
      language: lang,
      isPrimary: text === primaryTitle,
      rank: PREFERRED_LANG_ORDER[lang] ?? 20,
    });
  }

  for (const altObj of attrs.altTitles || []) {
    if (!altObj || typeof altObj !== 'object') continue;
    for (const [langRaw, val] of Object.entries(altObj)) {
      const text = String(val || '').trim();
      if (!text) continue;
      const lang = langRaw.toLowerCase();
      rawMatchAliases.push(text);
      if (ALLOWED_PERSIST_LANGS.has(lang)) {
        localizedCandidates.push({
          title: text,
          language: lang,
          isPrimary: false,
          rank: PREFERRED_LANG_ORDER[lang] ?? 25,
        });
      }
    }
  }

  localizedCandidates.sort((a, b) => a.rank - b.rank);
  const aliases: LocalizedAlias[] = localizedCandidates.map(({ title, language, isPrimary }) => ({
    title,
    language,
    isPrimary,
  }));

  const authors: string[] = [];
  const artists: string[] = [];
  for (const rel of entry.relationships || []) {
    const name = String(rel?.attributes?.name || '').trim();
    if (!name) continue;
    if (rel.type === 'author' && !authors.includes(name)) authors.push(name);
    if (rel.type === 'artist' && !artists.includes(name)) artists.push(name);
  }

  const tags: string[] = [];
  for (const tag of attrs.tags || []) {
    const name = tag?.attributes?.name?.en?.trim();
    if (name) tags.push(name);
  }

  const origLang = attrs.originalLanguage?.toLowerCase() || undefined;
  const kind =
    origLang === 'ko'
      ? 'manhwa'
      : origLang === 'zh' || origLang === 'zh-hk'
        ? 'manhua'
        : origLang === 'ja'
          ? 'manga'
          : undefined;

  const contentRating = (attrs.contentRating || 'safe').toLowerCase();
  const isAdult = contentRating === 'pornographic' || contentRating === 'erotica';

  return {
    source: 'mangadex',
    sourceWorkId: id,
    url: `https://mangadex.org/title/${id}`,
    primaryTitle: primaryTitle.trim(),
    aliases,
    rawMatchAliases,
    authors,
    artists,
    year: typeof attrs.year === 'number' ? attrs.year : null,
    kind,
    originalLanguage: origLang,
    contentRating,
    isAdult,
    tags,
    links: extractExternalLinks(attrs.links, id),
    detailsLoaded: true,
  };
}

export class MangaDexAliasSource implements AliasOnlySourceAdapter {
  readonly id = 'mangadex' as const;
  readonly name = 'MangaDex';
  readonly baseUrl = 'https://api.mangadex.org';
  readonly category = 'ALIAS_ONLY' as const;
  private readonly http: SourceHttpClient;

  constructor(httpOptions?: Partial<SourceHttpClientOptions>) {
    this.http = new SourceHttpClient({
      source: 'mangadex',
      maxConcurrent: 4,
      minIntervalMs: 220, // MangaDex public API rate limit is 5 req/sec; 220ms (~4.5 rps) stays safely under limit
      timeoutMs: 12_000,
      maxRetries: 2,
      circuitBreakerThreshold: 8,
      circuitBreakerCooldownMs: 30_000,
      ...httpOptions,
    });
  }

  getHealthSnapshot(): AliasSourceHealthSnapshot {
    return this.http.getHealthSnapshot();
  }

  async searchWorks(query: string, limit = 15): Promise<AliasSourceSearchOutcome> {
    const cleaned = query.trim();
    if (!cleaned) {
      return { source: 'mangadex', ok: true, candidates: [], latencyMs: 0 };
    }

    const url = new URL(`${this.baseUrl}/manga`);
    url.searchParams.set('title', cleaned);
    url.searchParams.set('limit', String(Math.min(Math.max(1, limit), 50)));
    url.searchParams.append('includes[]', 'author');
    url.searchParams.append('includes[]', 'artist');
    for (const cr of ['safe', 'suggestive', 'erotica', 'pornographic']) {
      url.searchParams.append('contentRating[]', cr);
    }

    const res = await this.http.request<MdListResponse>(url.toString(), {
      headers: { Accept: 'application/json' },
    });

    if (!res.ok) {
      if (res.status === 404) {
        return { source: 'mangadex', ok: true, candidates: [], latencyMs: res.latencyMs, fromCache: res.fromCache };
      }
      return {
        source: 'mangadex',
        ok: false,
        candidates: [],
        errorCode: res.errorCode,
        errorMessage: res.errorMessage,
        statusCode: res.status,
        latencyMs: res.latencyMs,
      };
    }

    const candidates = (res.data?.data || [])
      .map(mapMangaDexEntry)
      .filter((c): c is AliasSourceCandidate => c !== null);

    return {
      source: 'mangadex',
      ok: true,
      candidates,
      latencyMs: res.latencyMs,
      fromCache: res.fromCache,
    };
  }

  async fetchWorkAliases(sourceWorkId: string): Promise<AliasSourceDetailOutcome> {
    const cleanId = sourceWorkId
      .trim()
      .replace(/^https?:\/\/mangadex\.org\/title\//i, '')
      .split('/')[0]
      .trim();

    if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(cleanId)) {
      return {
        source: 'mangadex',
        ok: false,
        candidate: null,
        errorCode: 'INVALID_MANGADEX_UUID',
        errorMessage: `Invalid MangaDex UUID: ${sourceWorkId}`,
        latencyMs: 0,
      };
    }

    const url = new URL(`${this.baseUrl}/manga/${cleanId}`);
    url.searchParams.append('includes[]', 'author');
    url.searchParams.append('includes[]', 'artist');

    const res = await this.http.request<MdSingleResponse>(url.toString(), {
      headers: { Accept: 'application/json' },
    });

    if (!res.ok) {
      if (res.status === 404) {
        return { source: 'mangadex', ok: true, candidate: null, latencyMs: res.latencyMs, fromCache: res.fromCache };
      }
      return {
        source: 'mangadex',
        ok: false,
        candidate: null,
        errorCode: res.errorCode,
        errorMessage: res.errorMessage,
        statusCode: res.status,
        latencyMs: res.latencyMs,
      };
    }

    const rawData = Array.isArray(res.data?.data) ? res.data?.data[0] : res.data?.data;
    const mapped = rawData ? mapMangaDexEntry(rawData) : null;
    return {
      source: 'mangadex',
      ok: true,
      candidate: mapped,
      latencyMs: res.latencyMs,
      fromCache: res.fromCache,
    };
  }

  async fetchWorksByIds(uuids: string[]): Promise<Map<string, AliasSourceCandidate>> {
    const valid = [...new Set(
      uuids
        .map((u) => u.trim().toLowerCase())
        .filter((u) => /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(u)),
    )];
    const out = new Map<string, AliasSourceCandidate>();
    if (!valid.length) return out;

    for (let i = 0; i < valid.length; i += 80) {
      const batch = valid.slice(i, i + 80);
      const url = new URL(`${this.baseUrl}/manga`);
      url.searchParams.set('limit', String(batch.length));
      for (const id of batch) url.searchParams.append('ids[]', id);
      url.searchParams.append('includes[]', 'author');
      url.searchParams.append('includes[]', 'artist');
      for (const cr of ['safe', 'suggestive', 'erotica', 'pornographic']) {
        url.searchParams.append('contentRating[]', cr);
      }
      const res = await this.http.request<MdListResponse>(url.toString(), {
        headers: { Accept: 'application/json' },
      });
      if (res.ok && res.data?.data) {
        for (const item of res.data.data) {
          const mapped = mapMangaDexEntry(item);
          if (mapped) out.set(mapped.sourceWorkId.toLowerCase(), mapped);
        }
      }
    }
    return out;
  }
}
