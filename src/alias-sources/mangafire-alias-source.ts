import { SourceHttpClient, type SourceHttpClientOptions } from './http-client.js';
import type {
  AliasOnlySourceAdapter,
  AliasSourceCandidate,
  AliasSourceDetailOutcome,
  AliasSourceHealthSnapshot,
  AliasSourceSearchOutcome,
  ExternalCrossLinks,
} from './types.js';

/**
 * Pure TypeScript adaptation of Keiyoushi's MangaFire VrfSigner (Apache-2.0 / MIT compatible).
 * Signs canonical `/api/*` paths (with `/api` prefix stripped and sorted decoded query values).
 */
const VRF_STAGES: ReadonlyArray<readonly [Buffer, Buffer, number]> = [
  [
    Buffer.from(
      'yINlmUNho8VYJT+ibTIP+9ESiULpVEtMOoD6U6lRE0R/xwXo/Xp9NrUgC4cw/Lmo33vUyjUE40kUoEWIr/fxfNNcq2s79ShQ5NhNrFnJ4hXPwOu/SuXzIbuTQKGFvfm08E9jvCfqAtoDqvQq3dVWPQFmJjgvkISBeXY3BgANR+yVnjGbcxZ47d6kLNfZPIayTq3/YGySb1KuVZodWp/WGNAO5pfMcpaK53Hhs0allBszaMaxuouOwdxbwgxIw6YunSsXjI05Yi0j9j4eHKfSXR8Ifo/Od+8iamRfCXTyvm7NGRGYdcQ0ywcK/u6RXhrbcCm4t2eCtrDgQVecJGkQ+A==',
      'base64',
    ),
    Buffer.from('0Ec58JOY3uBzJK9m3zqIOpdlF7UFiax9DmA=', 'base64'),
    0x5a,
  ],
  [
    Buffer.from(
      'IUFltCxD3Oc2cwCgkJffthaOg9cgPUb0LgW6H/VtfcF0kc5F25t+aWj6JH9VOhOaY0rAFdUxlDnl5BLNvwEJvQtP5qcw7vdb/K+chnbwnspSHT8mz5lqwz41TezG0hkO06FTjJZhsyNuFLDpD2ZZxQj/QIRcF90zpmQ7Byu483WsQqUE0C342HL+JXngRB6fRzxRyVTaKu83h7UYTJ0QMt6ixFh6S3F8gqkKwrGTL3jHNBsD45UnifK8+RGtishQV2K3rujLKEkiZxpr2dYcudFW4oFsDKhad3CLBvuyTqsCo4B7mL5IKQ1vXo/MOOvq1I1d8ar9X6Ttu5KF4fZgiA==',
      'base64',
    ),
    Buffer.from('AAdjb1iPY8CiDmq9H34tKTBF8a3oDQ==', 'base64'),
    0x35,
  ],
  [
    Buffer.from(
      'NQHlu1/wVO5EmkwQymF810qqY2xG1k2obcas4Z9mCsPEIFl9pRIjFxbJ7ybMHbBckT5Ton85E0FOeHezbh/mjlEYpmpnlXOS8dgrqeq2KfxImTh1YK9y0PeMNhzA1OQzSY9brYOJq/l2QnE/hwOeZIhPixVSKIUlDb5vLcH6RWKxkIEMuP0bDwIqQ71AJJaEaMJL7A6YtyIwoRT+L5v4aZzodN/0+3nOGsfblFjgxSfPzVDjNFeNl5P26+kEC/8AHgdrpAbt3hHz3HrRN1Y6e+JHgF7ncFWnoF0y3THL1S71WgWGCa6KtSzTCCG58n68nTyj2T3Sshk7utqCtMi/ZQ==',
      'base64',
    ),
    Buffer.from('DELOJgPsVaCcblDtTGMdHzM=', 'base64'),
    0xba,
  ],
];

export function signMangaFirePath(canonicalPathWithDecodedQuery: string): string {
  let data: Buffer = Buffer.from(canonicalPathWithDecodedQuery, 'utf8');
  for (const [table, key, iv] of VRF_STAGES) {
    const out = Buffer.allocUnsafe(data.length);
    let prev = iv;
    const keyLen = key.length;
    for (let i = 0; i < data.length; i++) {
      prev = table[(data[i] ^ key[i % keyLen] ^ prev) & 0xff] & 0xff;
      out[i] = prev;
    }
    data = out;
  }
  return data.toString('base64url');
}

export function buildSignedMangaFireApiUrl(
  baseUrl: string,
  apiPath: string,
  params: Array<[string, string]> = [],
): string {
  const grouped = new Map<string, string[]>();
  for (const [k, v] of params) {
    const list = grouped.get(k) || [];
    list.push(v);
    grouped.set(k, list);
  }
  const sortedKeys = [...grouped.keys()].sort((a, b) => (a < b ? -1 : a > b ? 1 : 0));
  const sortedPairs: Array<[string, string]> = [];
  for (const k of sortedKeys) {
    for (const v of grouped.get(k)!) {
      sortedPairs.push([k, v]);
    }
  }

  const strippedPath = apiPath.startsWith('/api') ? apiPath.slice(4) : apiPath;
  let canonical = strippedPath;
  if (sortedPairs.length > 0) {
    let lastKey = '';
    let idx = 0;
    const parts = sortedPairs.map(([key, value]) => {
      let newKey = key;
      if (key.endsWith('[]')) {
        if (lastKey !== key) idx = 0;
        lastKey = key;
        newKey = `${key.slice(0, -2)}[${idx++}]`;
      }
      return `${newKey}=${value}`;
    });
    canonical += `?${parts.join('&')}`;
  }

  const url = new URL(apiPath, baseUrl);
  for (const [k, v] of sortedPairs) {
    url.searchParams.append(k, v);
  }
  url.searchParams.append('vrf', signMangaFirePath(canonical));
  return url.toString();
}

interface MfListItemDto {
  id?: number;
  hid?: string;
  slug?: string;
  title?: string;
  type?: string;
  status?: string;
  year?: number | null;
  url?: string;
}

interface MfListResponse {
  items?: MfListItemDto[];
  meta?: {
    hasNext?: boolean;
    lastPage?: number;
  };
}

interface MfTagItem {
  id?: number;
  title?: string;
}

interface MfDetailDto {
  id?: number;
  hid?: string;
  slug?: string;
  title?: string;
  altTitles?: string[];
  type?: string;
  status?: string;
  contentRating?: string;
  year?: number | null;
  url?: string;
  links?: Record<string, string | null | undefined>;
  malId?: string | number | null;
  anilistId?: string | number | null;
  genres?: MfTagItem[];
  themes?: MfTagItem[];
  demographics?: MfTagItem[];
  authors?: MfTagItem[];
  artists?: MfTagItem[];
}

interface MfDetailResponse {
  data?: MfDetailDto;
}

export function extractMangaFireHid(urlOrHid: string): string {
  const cleaned = urlOrHid.trim().replace(/\/+$/, '');
  const lastPart = cleaned.includes('/') ? cleaned.slice(cleaned.lastIndexOf('/') + 1) : cleaned;
  if (lastPart.includes('.')) return lastPart.slice(lastPart.lastIndexOf('.') + 1);
  if (lastPart.includes('-')) return lastPart.slice(0, lastPart.indexOf('-'));
  return lastPart;
}

function extractMfLinks(d: MfDetailDto): ExternalCrossLinks {
  const links: ExternalCrossLinks = {};
  if (d.malId !== undefined && d.malId !== null && String(d.malId).trim() !== '') {
    links.malId = String(d.malId).trim();
  }
  if (d.anilistId !== undefined && d.anilistId !== null && String(d.anilistId).trim() !== '') {
    links.anilistId = String(d.anilistId).trim();
  }
  const muUrl = d.links?.mu;
  if (muUrl) {
    const m = String(muUrl).match(/series\/([a-z0-9]+)/i);
    if (m) links.mangaUpdatesId = m[1];
  }
  return links;
}

export class MangaFireAliasSource implements AliasOnlySourceAdapter {
  readonly id = 'mangafire' as const;
  readonly name = 'MangaFire';
  readonly baseUrl = 'https://mangafire.to';
  readonly category = 'ALIAS_ONLY' as const;
  private readonly http: SourceHttpClient;

  constructor(httpOptions?: Partial<SourceHttpClientOptions>) {
    this.http = new SourceHttpClient({
      source: 'mangafire',
      maxConcurrent: 5,
      minIntervalMs: 140,
      timeoutMs: 10_000,
      maxRetries: 2,
      circuitBreakerThreshold: 8,
      circuitBreakerCooldownMs: 30_000,
      accessRestrictionDetector: (status, body) => {
        if (status === 403 && /Invalid token/i.test(body)) return 'MANGAFIRE_VRF_TOKEN_INVALID';
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
      return { source: 'mangafire', ok: true, candidates: [], latencyMs: 0 };
    }

    const signedUrl = buildSignedMangaFireApiUrl(this.baseUrl, '/api/titles', [
      ['keyword', cleaned],
      ['limit', String(Math.min(Math.max(1, limit), 50))],
      ['page', '1'],
    ]);

    const res = await this.http.request<MfListResponse>(signedUrl, {
      headers: {
        Accept: 'application/json',
        Referer: `${this.baseUrl}/`,
      },
    });

    if (!res.ok) {
      if (res.status === 404) {
        return { source: 'mangafire', ok: true, candidates: [], latencyMs: res.latencyMs, fromCache: res.fromCache };
      }
      return {
        source: 'mangafire',
        ok: false,
        candidates: [],
        errorCode: res.errorCode,
        errorMessage: res.errorMessage,
        statusCode: res.status,
        latencyMs: res.latencyMs,
      };
    }

    const candidates: AliasSourceCandidate[] = [];
    for (const item of res.data?.items || []) {
      const hid = String(item.hid || '').trim();
      const title = String(item.title || '').trim();
      if (!hid || !title) continue;
      const relUrl = item.url || (item.slug ? `/title/${hid}-${item.slug}` : `/title/${hid}`);
      candidates.push({
        source: 'mangafire',
        sourceWorkId: hid,
        url: `${this.baseUrl}${relUrl.startsWith('/') ? '' : '/'}${relUrl}`,
        slug: item.slug,
        primaryTitle: title,
        aliases: [{ title, isPrimary: true }],
        rawMatchAliases: [title],
        year: typeof item.year === 'number' ? item.year : null,
        kind: item.type?.toLowerCase(),
        detailsLoaded: false,
      });
    }

    return {
      source: 'mangafire',
      ok: true,
      candidates,
      latencyMs: res.latencyMs,
      fromCache: res.fromCache,
    };
  }

  async fetchWorkAliases(sourceWorkId: string): Promise<AliasSourceDetailOutcome> {
    const hid = extractMangaFireHid(sourceWorkId);
    if (!hid) {
      return {
        source: 'mangafire',
        ok: false,
        candidate: null,
        errorCode: 'INVALID_MANGAFIRE_HID',
        errorMessage: `Invalid MangaFire HID: ${sourceWorkId}`,
        latencyMs: 0,
      };
    }

    const signedUrl = buildSignedMangaFireApiUrl(this.baseUrl, `/api/titles/${hid}`);
    const res = await this.http.request<MfDetailResponse>(signedUrl, {
      headers: {
        Accept: 'application/json',
        Referer: `${this.baseUrl}/`,
      },
    });

    if (!res.ok) {
      if (res.status === 404) {
        return { source: 'mangafire', ok: true, candidate: null, latencyMs: res.latencyMs, fromCache: res.fromCache };
      }
      return {
        source: 'mangafire',
        ok: false,
        candidate: null,
        errorCode: res.errorCode,
        errorMessage: res.errorMessage,
        statusCode: res.status,
        latencyMs: res.latencyMs,
      };
    }

    const d = res.data?.data;
    const title = String(d?.title || '').trim();
    if (!d || !title) {
      return { source: 'mangafire', ok: true, candidate: null, latencyMs: res.latencyMs, fromCache: res.fromCache };
    }

    const altList = Array.isArray(d.altTitles) ? d.altTitles.map((s) => String(s || '').trim()).filter(Boolean) : [];
    const rawMatchAliases = [title, ...altList];
    const relUrl = d.url || (d.slug ? `/title/${hid}-${d.slug}` : `/title/${hid}`);
    const contentRating = (d.contentRating || 'safe').toLowerCase();
    const tags = [
      ...(d.genres || []).map((g) => String(g.title || '').trim()),
      ...(d.themes || []).map((t) => String(t.title || '').trim()),
      ...(d.demographics || []).map((dm) => String(dm.title || '').trim()),
    ].filter(Boolean);
    const isAdult =
      contentRating === 'pornographic' ||
      contentRating === 'erotica' ||
      tags.some((t) => /^(?:adult|hentai|smut|mature)$/i.test(t));

    return {
      source: 'mangafire',
      ok: true,
      candidate: {
        source: 'mangafire',
        sourceWorkId: String(d.hid || hid),
        url: `${this.baseUrl}${relUrl.startsWith('/') ? '' : '/'}${relUrl}`,
        slug: d.slug,
        primaryTitle: title,
        aliases: [
          { title, isPrimary: true },
          ...altList.map((a) => ({ title: a, isPrimary: false })),
        ],
        rawMatchAliases,
        authors: (d.authors || []).map((a) => String(a.title || '').trim()).filter(Boolean),
        artists: (d.artists || []).map((a) => String(a.title || '').trim()).filter(Boolean),
        year: typeof d.year === 'number' ? d.year : null,
        kind: d.type?.toLowerCase(),
        contentRating,
        isAdult,
        tags,
        links: extractMfLinks(d),
        detailsLoaded: true,
      },
      latencyMs: res.latencyMs,
      fromCache: res.fromCache,
    };
  }
}
