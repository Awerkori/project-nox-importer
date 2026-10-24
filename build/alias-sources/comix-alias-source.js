import { normalizeTitle } from '../core/matching.js';
import { SourceHttpClient } from './http-client.js';
const INITIAL_DATA_REGEX = /<script[^>]*id="initial-data"[^>]*>([\s\S]*?)<\/script>/i;
const MANGADEX_UUID_REGEX = /([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})/i;
function extractComixLinks(dto) {
    const out = {};
    if (dto.links?.md) {
        const m = MANGADEX_UUID_REGEX.exec(dto.links.md);
        if (m)
            out.mangadexId = m[1].toLowerCase();
    }
    if (dto.links?.al) {
        const m = String(dto.links.al).match(/(?:manga\/|^)(\d+)/i);
        if (m)
            out.anilistId = m[1];
    }
    if (dto.links?.mal) {
        const m = String(dto.links.mal).match(/(?:manga\/|^)(\d+)/i);
        if (m)
            out.malId = m[1];
    }
    if (dto.links?.mu) {
        const m = String(dto.links.mu).match(/series\/([a-z0-9]+)/i);
        if (m)
            out.mangaUpdatesId = m[1];
    }
    if (dto.links?.mb) {
        const m = String(dto.links.mb).match(/mangabaka\.org\/(\d+)/i);
        if (m)
            out.mangaBakaId = m[1];
    }
    return out;
}
function mapComixDto(baseUrl, dto, detailsLoaded) {
    const hid = String(dto.hid || '').trim();
    const primaryTitle = String(dto.title || '').trim();
    if (!hid || !primaryTitle)
        return null;
    const altList = [
        ...(Array.isArray(dto.altTitles) ? dto.altTitles : []),
        ...(Array.isArray(dto.alt_titles) ? dto.alt_titles : []),
    ]
        .map((s) => String(s || '').trim())
        .filter(Boolean);
    const relUrl = dto.url || `/title/${hid}`;
    const slug = relUrl.startsWith('/title/') ? relUrl.slice('/title/'.length) : undefined;
    const contentRating = (dto.contentRating || 'safe').toLowerCase();
    const tags = [
        ...(dto.genres || []).map((t) => String(t.title || '').trim()),
        ...(dto.formats || []).map((t) => String(t.title || '').trim()),
        ...(dto.tags || []).map((t) => String(t.title || '').trim()),
    ].filter(Boolean);
    const isAdult = contentRating === 'pornographic' ||
        contentRating === 'erotica' ||
        tags.some((t) => /^(?:adult|hentai|smut|mature)$/i.test(t));
    return {
        source: 'comix',
        sourceWorkId: hid,
        url: `${baseUrl}${relUrl.startsWith('/') ? '' : '/'}${relUrl}`,
        slug,
        primaryTitle,
        aliases: [
            { title: primaryTitle, isPrimary: true },
            ...altList.map((a) => ({ title: a, isPrimary: false })),
        ],
        rawMatchAliases: [primaryTitle, ...altList],
        authors: (dto.authors || []).map((a) => String(a.title || '').trim()).filter(Boolean),
        artists: (dto.artists || []).map((a) => String(a.title || '').trim()).filter(Boolean),
        year: typeof dto.year === 'number' ? dto.year : null,
        kind: dto.type?.toLowerCase(),
        originalLanguage: dto.originalLanguage?.toLowerCase(),
        contentRating,
        isAdult,
        tags,
        links: extractComixLinks(dto),
        detailsLoaded,
    };
}
export function extractComixInitialData(html) {
    const match = INITIAL_DATA_REGEX.exec(html);
    if (!match?.[1])
        return null;
    try {
        const parsed = JSON.parse(match[1]);
        return parsed && typeof parsed === 'object' ? parsed : null;
    }
    catch {
        return null;
    }
}
export function extractComixHid(urlOrHid) {
    const cleaned = urlOrHid
        .trim()
        .replace(/^https?:\/\/(?:www\.)?comix\.(?:to|ws)\//i, '')
        .replace(/^\/+/, '')
        .replace(/^title\//i, '')
        .replace(/\/+$/, '');
    const segment = cleaned.split('/')[0] || '';
    return segment.includes('-') ? segment.slice(0, segment.indexOf('-')) : segment;
}
/**
 * Comix (`https://comix.to`) ALIAS_ONLY adapter:
 * - Public SSR routes (`/` and `/title/<hid>`) expose unencrypted `<script id="initial-data">`
 *   with `title`, `altTitles`, `authors`, `artists`, `year`, and `links.{al, mal, mu, md, mb}`.
 * - Search API (`/api/v1/manga`) currently enforces a custom VM-obfuscated token + WAF gate
 *   (`HTTP 403 {"code":"missing_token"}`). Per project rules, we NEVER bypass access controls,
 *   WAF, or Cloudflare: the search API client trips its circuit breaker cleanly on `missing_token`
 *   while keeping public SSR discovery and direct `/title/<hid>` resolution operational.
 */
export class ComixAliasSource {
    id = 'comix';
    name = 'Comix';
    baseUrl = 'https://comix.to';
    category = 'ALIAS_ONLY';
    ssrHttp;
    apiHttp;
    ssrCatalogSeeded = false;
    ssrSeedPromise = null;
    ssrCandidatesByHid = new Map();
    ssrTitleIndex = new Map();
    constructor(httpOptions) {
        this.ssrHttp = new SourceHttpClient({
            source: 'comix',
            maxConcurrent: 3,
            minIntervalMs: 300,
            timeoutMs: 10_000,
            maxRetries: 1,
            circuitBreakerThreshold: 6,
            circuitBreakerCooldownMs: 60_000,
            accessRestrictionDetector: (status, body) => {
                if ((status === 403 || status === 503) && /Security check|id="stage"|cf-chl|Just a moment/i.test(body)) {
                    return 'COMIX_WAF_CHALLENGE';
                }
                return null;
            },
            ...httpOptions,
        });
        this.apiHttp = new SourceHttpClient({
            source: 'comix',
            maxConcurrent: 2,
            minIntervalMs: 300,
            timeoutMs: 8_000,
            maxRetries: 0,
            circuitBreakerThreshold: 1,
            circuitBreakerCooldownMs: 30 * 60_000, // Trip for 30m once missing_token is confirmed so 9,292 works never spam 403s
            accessRestrictionDetector: (status, body) => {
                if (status === 403 && /missing_token|Missing token|invalid_token/i.test(body)) {
                    return 'COMIX_SEARCH_API_TOKEN_RESTRICTED';
                }
                if ((status === 403 || status === 503) && /Security check|cf-chl|Just a moment/i.test(body)) {
                    return 'COMIX_WAF_CHALLENGE';
                }
                return null;
            },
            ...httpOptions,
        });
    }
    getHealthSnapshot() {
        const ssr = this.ssrHttp.getHealthSnapshot();
        const api = this.apiHttp.getHealthSnapshot();
        const totalLatencyCount = (ssr.successTotal + ssr.errorTotal) + (api.successTotal + api.errorTotal);
        const weightedLatency = totalLatencyCount > 0
            ? Math.round((ssr.avgLatencyMs * (ssr.successTotal + ssr.errorTotal) +
                api.avgLatencyMs * (api.successTotal + api.errorTotal)) /
                totalLatencyCount)
            : 0;
        return {
            source: 'comix',
            requestsTotal: ssr.requestsTotal + api.requestsTotal,
            cacheHits: ssr.cacheHits + api.cacheHits,
            successTotal: ssr.successTotal + api.successTotal,
            errorTotal: ssr.errorTotal + api.errorTotal,
            rateLimitedTotal: ssr.rateLimitedTotal + api.rateLimitedTotal,
            circuitOpen: api.circuitOpen || ssr.circuitOpen,
            circuitReason: api.circuitReason || ssr.circuitReason,
            circuitOpenUntil: api.circuitOpenUntil || ssr.circuitOpenUntil,
            avgLatencyMs: weightedLatency,
        };
    }
    indexSsrCandidate(candidate) {
        this.ssrCandidatesByHid.set(candidate.sourceWorkId, candidate);
        for (const raw of candidate.rawMatchAliases || [candidate.primaryTitle]) {
            const norm = normalizeTitle(raw);
            if (!norm)
                continue;
            let bucket = this.ssrTitleIndex.get(norm);
            if (!bucket) {
                bucket = new Set();
                this.ssrTitleIndex.set(norm, bucket);
            }
            bucket.add(candidate.sourceWorkId);
        }
    }
    async ensureSsrSeeded() {
        if (this.ssrCatalogSeeded)
            return;
        if (this.ssrSeedPromise)
            return this.ssrSeedPromise;
        this.ssrSeedPromise = (async () => {
            try {
                const res = await this.ssrHttp.request(`${this.baseUrl}/`, {
                    parseAs: 'text',
                    headers: { Accept: 'text/html', Referer: `${this.baseUrl}/` },
                });
                if (!res.ok || !res.data)
                    return;
                const data = extractComixInitialData(res.data);
                const queries = data?.queries;
                if (!queries || typeof queries !== 'object')
                    return;
                for (const val of Object.values(queries)) {
                    const items = Array.isArray(val)
                        ? val
                        : Array.isArray(val?.items)
                            ? val.items
                            : [];
                    for (const item of items) {
                        if (item && typeof item === 'object' && item.hid && item.title) {
                            const mapped = mapComixDto(this.baseUrl, item, Array.isArray(item.altTitles) || Array.isArray(item.alt_titles));
                            if (mapped)
                                this.indexSsrCandidate(mapped);
                        }
                    }
                }
            }
            finally {
                this.ssrCatalogSeeded = true;
            }
        })();
        return this.ssrSeedPromise;
    }
    async searchWorks(query, limit = 15) {
        const cleaned = query.trim();
        if (!cleaned) {
            return { source: 'comix', ok: true, candidates: [], latencyMs: 0 };
        }
        await this.ensureSsrSeeded();
        const normQuery = normalizeTitle(cleaned);
        if (normQuery) {
            const matchedHids = this.ssrTitleIndex.get(normQuery);
            if (matchedHids && matchedHids.size > 0) {
                const hits = [...matchedHids]
                    .map((hid) => this.ssrCandidatesByHid.get(hid))
                    .filter((c) => Boolean(c));
                if (hits.length > 0) {
                    return {
                        source: 'comix',
                        ok: true,
                        candidates: hits.slice(0, limit),
                        latencyMs: 0,
                        fromCache: true,
                    };
                }
            }
        }
        const url = new URL(`${this.baseUrl}/api/v1/manga`);
        url.searchParams.set('keyword', cleaned);
        url.searchParams.set('limit', String(Math.min(Math.max(1, limit), 28)));
        url.searchParams.set('page', '1');
        const apiRes = await this.apiHttp.request(url.toString(), {
            headers: {
                Accept: 'application/json',
                Referer: `${this.baseUrl}/browse`,
            },
        });
        if (!apiRes.ok) {
            if (apiRes.status === 404) {
                return { source: 'comix', ok: true, candidates: [], latencyMs: apiRes.latencyMs };
            }
            return {
                source: 'comix',
                ok: false,
                candidates: [],
                errorCode: apiRes.errorCode,
                errorMessage: apiRes.errorMessage,
                statusCode: apiRes.status,
                latencyMs: apiRes.latencyMs,
            };
        }
        const rawItems = apiRes.data?.result?.items || apiRes.data?.items || [];
        const candidates = rawItems
            .map((item) => mapComixDto(this.baseUrl, item, Array.isArray(item.altTitles) || Array.isArray(item.alt_titles)))
            .filter((c) => c !== null);
        return {
            source: 'comix',
            ok: true,
            candidates,
            latencyMs: apiRes.latencyMs,
            fromCache: apiRes.fromCache,
        };
    }
    async fetchWorkAliases(sourceWorkId) {
        const hid = extractComixHid(sourceWorkId);
        if (!hid) {
            return {
                source: 'comix',
                ok: false,
                candidate: null,
                errorCode: 'INVALID_COMIX_HID',
                errorMessage: `Invalid Comix HID: ${sourceWorkId}`,
                latencyMs: 0,
            };
        }
        const res = await this.ssrHttp.request(`${this.baseUrl}/title/${hid}`, {
            parseAs: 'text',
            headers: {
                Accept: 'text/html,application/xhtml+xml',
                Referer: `${this.baseUrl}/`,
            },
        });
        if (!res.ok) {
            if (res.status === 404) {
                return { source: 'comix', ok: true, candidate: null, latencyMs: res.latencyMs, fromCache: res.fromCache };
            }
            return {
                source: 'comix',
                ok: false,
                candidate: null,
                errorCode: res.errorCode,
                errorMessage: res.errorMessage,
                statusCode: res.status,
                latencyMs: res.latencyMs,
            };
        }
        const data = extractComixInitialData(res.data || '');
        const queries = data?.queries;
        if (!queries || typeof queries !== 'object') {
            return { source: 'comix', ok: true, candidate: null, latencyMs: res.latencyMs, fromCache: res.fromCache };
        }
        let detailDto = null;
        for (const [key, value] of Object.entries(queries)) {
            if (key.includes('"detail"') && value && typeof value === 'object') {
                detailDto = value;
                break;
            }
        }
        const candidate = detailDto ? mapComixDto(this.baseUrl, detailDto, true) : null;
        if (candidate)
            this.indexSsrCandidate(candidate);
        return {
            source: 'comix',
            ok: true,
            candidate,
            latencyMs: res.latencyMs,
            fromCache: res.fromCache,
        };
    }
}
