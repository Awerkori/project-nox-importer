import { SourceHttpClient, type SourceHttpClientOptions } from './http-client.js';
import type {
  AliasOnlySourceAdapter,
  AliasSourceCandidate,
  AliasSourceDetailOutcome,
  AliasSourceHealthSnapshot,
  AliasSourceSearchOutcome,
  ExternalCrossLinks,
} from './types.js';

interface MbPersonDto {
  name?: string | null;
}

interface MbTagDto {
  name?: string | null;
  slug?: string | null;
  group?: string | null;
}

interface MbItemDto {
  id?: string | number | null;
  _id?: string | number | null;
  name?: string | null;
  slug?: string | null;
  alternateName?: string[] | null;
  authors?: MbPersonDto[] | null;
  author?: MbPersonDto[] | null;
  tags?: MbTagDto[] | null;
  originalLanguage?: string | null;
  is18plus?: boolean | string | number | null;
  date_published?: string | number | null;
  image?: {
    cdn_mangadex?: string | null;
  } | null;
  links?: {
    mangadex?: string | null;
    mangaUpdate?: string | null;
    myanimelist?: string | null;
    anilist?: string | null;
  } | null;
}

interface MbSearchResponse {
  code?: number;
  status?: string;
  data?: MbItemDto[];
}

interface MbDetailResponse {
  code?: number;
  status?: string;
  data?: MbItemDto;
}

const MANGADEX_UUID_IN_URL = /([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})/i;

function parseMbLinks(item: MbItemDto): ExternalCrossLinks {
  const out: ExternalCrossLinks = {};
  const mdFromLink = item.links?.mangadex ? MANGADEX_UUID_IN_URL.exec(item.links.mangadex)?.[1] : undefined;
  const mdFromCover = item.image?.cdn_mangadex ? MANGADEX_UUID_IN_URL.exec(item.image.cdn_mangadex)?.[1] : undefined;
  const mdId = mdFromLink || mdFromCover;
  if (mdId) out.mangadexId = mdId.toLowerCase();

  if (item.links?.myanimelist) {
    const m = String(item.links.myanimelist).match(/(?:manga\/|^)(\d+)/i);
    if (m) out.malId = m[1];
  }
  if (item.links?.anilist) {
    const m = String(item.links.anilist).match(/(?:manga\/|^)(\d+)/i);
    if (m) out.anilistId = m[1];
  }
  if (item.links?.mangaUpdate) {
    const m = String(item.links.mangaUpdate).match(/series\/([a-z0-9]+)/i);
    if (m) out.mangaUpdatesId = m[1];
  }
  return out;
}

function parseMbYear(raw: unknown): number | null {
  if (typeof raw === 'number' && raw >= 1950 && raw <= 2100) return raw;
  if (typeof raw === 'string') {
    const m = raw.match(/\b(19\d{2}|20\d{2})\b/);
    if (m) return Number(m[1]);
  }
  return null;
}

const MB_CATEGORY_PREFIX_REGEX = /^(?:\[(?:H|A|MOTION|18\+|R-?18|NSFW|DJ|Doujin(?:shi)?)\]\s*)+/i;
const MB_CATEGORY_SUFFIX_REGEX = /\s*(?:\(\s*(?:Official|Raw|Decensored|Uncensored)\s*\)|\bRaw)\s*$/i;

function stripMbPrefix(raw: string): string {
  return raw
    .replace(MB_CATEGORY_PREFIX_REGEX, '')
    .replace(MB_CATEGORY_SUFFIX_REGEX, '')
    .trim();
}

function mapMbItem(baseUrl: string, item: MbItemDto): AliasSourceCandidate | null {
  const id = String(item.id ?? item._id ?? '').trim();
  const rawPrimary = String(item.name || '').trim();
  const primaryHadAdultBadge = /^\[(?:H|18\+|R-?18|NSFW)\]\s*/i.test(rawPrimary);
  const primaryTitle = stripMbPrefix(rawPrimary);
  if (!id || !primaryTitle) return null;

  const altNames = Array.isArray(item.alternateName)
    ? item.alternateName.map((s) => stripMbPrefix(String(s || ''))).filter(Boolean)
    : [];

  const authors = [
    ...new Set(
      [...(item.authors || []), ...(item.author || [])]
        .map((p) => String(p?.name || '').trim())
        .filter(Boolean),
    ),
  ];

  const tags = (item.tags || []).map((t) => String(t?.name || '').trim()).filter(Boolean);
  const origLang = item.originalLanguage?.toLowerCase() || undefined;
  const kind =
    origLang === 'ko'
      ? 'manhwa'
      : origLang === 'zh' || origLang === 'zh-hk' || origLang === 'cn'
        ? 'manhua'
        : origLang === 'ja'
          ? 'manga'
          : undefined;

  const isAdult =
    primaryHadAdultBadge ||
    item.is18plus === true ||
    item.is18plus === 1 ||
    item.is18plus === 'true' ||
    tags.some((t) => /^(?:hentai|smut|erotica|pornographic)$/i.test(t));

  return {
    source: 'mangaball',
    sourceWorkId: id,
    url: `${baseUrl}/title-detail/${id}`,
    slug: item.slug || undefined,
    primaryTitle,
    aliases: [
      { title: primaryTitle, isPrimary: true },
      ...altNames.map((a) => ({ title: a, isPrimary: false })),
    ],
    rawMatchAliases: [primaryTitle, ...altNames],
    authors,
    artists: [],
    year: parseMbYear(item.date_published),
    kind,
    originalLanguage: origLang,
    contentRating: isAdult ? 'erotica' : 'safe',
    isAdult,
    tags,
    links: parseMbLinks(item),
    detailsLoaded: true,
  };
}

export class MangaBallAliasSource implements AliasOnlySourceAdapter {
  readonly id = 'mangaball' as const;
  readonly name = 'MangaBall';
  readonly baseUrl = 'https://mangaball.com';
  readonly category = 'ALIAS_ONLY' as const;
  private readonly apiBase = 'https://mangaball.com/api/v1';
  private readonly fallbackApiBase = 'https://api.mangaball.com/api/v1';
  private readonly http: SourceHttpClient;

  constructor(httpOptions?: Partial<SourceHttpClientOptions>) {
    this.http = new SourceHttpClient({
      source: 'mangaball',
      maxConcurrent: 5,
      minIntervalMs: 140,
      timeoutMs: 12_000,
      maxRetries: 2,
      circuitBreakerThreshold: 8,
      circuitBreakerCooldownMs: 30_000,
      accessRestrictionDetector: (status, body) => {
        if ((status === 403 || status === 503) && /cf-chl|Just a moment|Attention Required/i.test(body)) {
          return 'CLOUDFLARE_CHALLENGE';
        }
        return null;
      },
      ...httpOptions,
    });
  }

  getHealthSnapshot(): AliasSourceHealthSnapshot {
    return this.http.getHealthSnapshot();
  }

  async searchWorks(query: string, limit = 15): Promise<AliasSourceSearchOutcome> {
    const cleaned = query.trim();
    if (!cleaned) {
      return { source: 'mangaball', ok: true, candidates: [], latencyMs: 0 };
    }

    const buildSearchUrl = (root: string) => {
      const u = new URL(`${root}/title/search-advanced`);
      u.searchParams.set('page', '1');
      u.searchParams.set('limit', String(Math.min(Math.max(1, limit), 50)));
      u.searchParams.set('tag_mode', 'AND');
      u.searchParams.set('adult_mode', 'all');
      u.searchParams.set('keyword', cleaned);
      return u.toString();
    };

    let res = await this.http.request<MbSearchResponse>(buildSearchUrl(this.apiBase), {
      headers: {
        Accept: 'application/json, text/plain, */*',
        Referer: `${this.baseUrl}/`,
      },
    });

    if (!res.ok && res.status >= 500) {
      res = await this.http.request<MbSearchResponse>(buildSearchUrl(this.fallbackApiBase), {
        headers: {
          Accept: 'application/json, text/plain, */*',
          Referer: `${this.baseUrl}/`,
        },
        bypassCircuit: true,
      });
    }

    if (!res.ok) {
      if (res.status === 404) {
        return { source: 'mangaball', ok: true, candidates: [], latencyMs: res.latencyMs, fromCache: res.fromCache };
      }
      return {
        source: 'mangaball',
        ok: false,
        candidates: [],
        errorCode: res.errorCode,
        errorMessage: res.errorMessage,
        statusCode: res.status,
        latencyMs: res.latencyMs,
      };
    }

    const candidates = (res.data?.data || [])
      .map((item) => mapMbItem(this.baseUrl, item))
      .filter((c): c is AliasSourceCandidate => c !== null);

    return {
      source: 'mangaball',
      ok: true,
      candidates,
      latencyMs: res.latencyMs,
      fromCache: res.fromCache,
    };
  }

  async fetchWorkAliases(sourceWorkId: string): Promise<AliasSourceDetailOutcome> {
    const cleanId = sourceWorkId
      .trim()
      .replace(/^https?:\/\/(?:www\.)?mangaball\.com\/title-detail\//i, '')
      .split('/')[0]
      .trim();

    if (!cleanId) {
      return {
        source: 'mangaball',
        ok: false,
        candidate: null,
        errorCode: 'INVALID_MANGABALL_ID',
        errorMessage: `Invalid MangaBall ID: ${sourceWorkId}`,
        latencyMs: 0,
      };
    }

    const res = await this.http.request<MbDetailResponse>(`${this.apiBase}/title/detail/${cleanId}`, {
      headers: {
        Accept: 'application/json, text/plain, */*',
        Referer: `${this.baseUrl}/`,
      },
    });

    if (!res.ok) {
      if (res.status === 404) {
        return { source: 'mangaball', ok: true, candidate: null, latencyMs: res.latencyMs, fromCache: res.fromCache };
      }
      return {
        source: 'mangaball',
        ok: false,
        candidate: null,
        errorCode: res.errorCode,
        errorMessage: res.errorMessage,
        statusCode: res.status,
        latencyMs: res.latencyMs,
      };
    }

    const mapped = res.data?.data ? mapMbItem(this.baseUrl, res.data.data) : null;
    return {
      source: 'mangaball',
      ok: true,
      candidate: mapped,
      latencyMs: res.latencyMs,
      fromCache: res.fromCache,
    };
  }
}
