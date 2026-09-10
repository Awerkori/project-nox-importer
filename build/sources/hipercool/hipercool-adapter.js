import { HostRateLimiter } from '../../core/rate-limiter.js';
import { Logger } from '../../core/logger.js';
import { slugify } from '../common/html-utils.js';
export class HipercoolAdapter {
    rateLimiter;
    transport;
    id = 'hipercool';
    name = 'HipercooL';
    baseUrl = 'https://lerhentais.com';
    logger = new Logger('HipercoolAdapter');
    cookies = new Map();
    lastSessionFetch = 0;
    constructor(rateLimiter = new HostRateLimiter(2.0), transport = fetch) {
        this.rateLimiter = rateLimiter;
        this.transport = transport;
        this.rateLimiter.setHostRate('lerhentais.com', 2.0, 4, 4.0);
        this.rateLimiter.setHostRate('awshc.r2d2storage.com', 8.0, 16, 16.0);
    }
    get headers() {
        const cookieHeader = Array.from(this.cookies.entries())
            .map(([k, v]) => `${k}=${v}`)
            .join('; ');
        const h = {
            Accept: 'application/json',
            'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
            'x-flux-node': 'G2ZsDdWhUwdU82Vw',
            Referer: `${this.baseUrl}/`,
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
        };
        if (cookieHeader)
            h.Cookie = cookieHeader;
        return h;
    }
    storeCookies(res) {
        let rawCookies = [];
        if (typeof res.headers.getSetCookie === 'function') {
            rawCookies = res.headers.getSetCookie();
        }
        else {
            const single = res.headers.get('set-cookie');
            if (single)
                rawCookies = [single];
        }
        for (const c of rawCookies) {
            const pair = c.split(';')[0];
            const eqIdx = pair.indexOf('=');
            if (eqIdx !== -1) {
                const k = pair.slice(0, eqIdx).trim();
                const v = pair.slice(eqIdx + 1).trim();
                if (k && v)
                    this.cookies.set(k, v);
            }
        }
    }
    async ensureSession() {
        const now = Date.now();
        if (this.cookies.has('__st') && now - this.lastSessionFetch < 24 * 60 * 60 * 1000) {
            return;
        }
        try {
            await this.rateLimiter.acquire('lerhentais.com');
            const res = await this.transport(this.baseUrl, {
                headers: {
                    Accept: 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                    'x-flux-node': 'G2ZsDdWhUwdU82Vw',
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
                },
                signal: AbortSignal.timeout(15_000),
            });
            this.storeCookies(res);
            this.lastSessionFetch = Date.now();
        }
        catch (err) {
            this.logger.warn('Failed to establish Hipercool session', { error: err?.message });
        }
    }
    async fetchJson(url) {
        await this.ensureSession();
        const parsed = new URL(url);
        await this.rateLimiter.acquire(parsed.host);
        let attempts = 0;
        const maxAttempts = 3;
        while (attempts < maxAttempts) {
            attempts++;
            try {
                const res = await this.transport(url, {
                    headers: this.headers,
                    signal: AbortSignal.timeout(30_000),
                });
                this.storeCookies(res);
                if (res.status === 401) {
                    // Re-establish session
                    this.cookies.clear();
                    await this.ensureSession();
                    continue;
                }
                if (res.status === 429) {
                    const retryAfter = res.headers.get('Retry-After');
                    this.rateLimiter.handle429(parsed.host, retryAfter, attempts);
                    if (attempts >= maxAttempts)
                        throw new Error(`HTTP 429 rate limit on ${parsed.host}`);
                    continue;
                }
                if (!res.ok) {
                    throw new Error(`Hipercool request failed: HTTP ${res.status}`);
                }
                this.rateLimiter.recordSuccess(parsed.host);
                return (await res.json());
            }
            catch (err) {
                if (attempts >= maxAttempts)
                    throw err;
                await new Promise((r) => setTimeout(r, 1000 * attempts));
            }
        }
        throw new Error(`Failed after ${maxAttempts} attempts for ${url}`);
    }
    getImageHeaders(_url) {
        return {
            Referer: `${this.baseUrl}/`,
            Origin: this.baseUrl,
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
        };
    }
    async fetchUpdatedWorks(cursor, _options) {
        const page = cursor ? parseInt(cursor, 10) : 1;
        const limit = 30;
        const offset = (page - 1) * limit;
        const input = JSON.stringify({
            '0': {
                json: {
                    q: '',
                    sort: 'recent',
                    filters: {
                        genres: null,
                        type: null,
                        status: null,
                        contentRating: null,
                        author: null,
                        artist: null,
                        year: null,
                    },
                    limit,
                    offset,
                },
                meta: {
                    values: {
                        'filters.genres': ['undefined'],
                        'filters.type': ['undefined'],
                        'filters.status': ['undefined'],
                        'filters.contentRating': ['undefined'],
                        'filters.author': ['undefined'],
                        'filters.artist': ['undefined'],
                        'filters.year': ['undefined'],
                    },
                },
            },
        });
        const url = `${this.baseUrl}/api/trpc/search.query?batch=1&input=${encodeURIComponent(input)}`;
        const data = await this.fetchJson(url);
        const hits = data[0]?.result?.data?.json?.hits || [];
        const works = hits.map((hit) => ({
            sourceWorkId: hit.slug || String(hit.id),
            title: hit.title || hit.slug,
            slug: hit.slug || slugify(hit.title),
            coverUrl: hit.coverUrl || null,
        }));
        const hasNext = hits.length >= limit;
        const nextCursor = hasNext ? String(page + 1) : null;
        return { works, nextCursor };
    }
    async fetchWorkDetails(sourceWorkId) {
        const slug = sourceWorkId.replace(/^\//, '').replace(/\/$/, '');
        const input = JSON.stringify({
            '0': { json: null, meta: { values: ['undefined'] } },
            '1': { json: { slug } },
        });
        const url = `${this.baseUrl}/api/trpc/auth.me,series.bySlugWithGenres?batch=1&input=${encodeURIComponent(input)}`;
        const data = await this.fetchJson(url);
        const series = data[1]?.result?.data?.json;
        if (!series) {
            throw new Error(`Hipercool: series details not found for slug ${slug}`);
        }
        const title = series.title || slug;
        const coverUrl = series.coverUrl || null;
        const synopsis = series.synopsis || '';
        const author = series.author || undefined;
        const artist = series.artist || undefined;
        const genres = (series.genres || []).map((g) => (typeof g === 'string' ? g : g.name)).filter(Boolean);
        let kind = 'MANHWA';
        const rawType = (series.type || '').toUpperCase();
        if (rawType.includes('MANGA'))
            kind = 'MANGA';
        else if (rawType.includes('WEBTOON'))
            kind = 'WEBTOON';
        let status = 'ONGOING';
        const rawStatus = (series.status || '').toLowerCase();
        if (rawStatus.includes('complet') || rawStatus.includes('finish'))
            status = 'COMPLETED';
        else if (rawStatus.includes('hiat'))
            status = 'HIATUS';
        return {
            sourceWorkId: slug,
            title,
            slug,
            coverUrl,
            synopsis,
            author,
            artist,
            kind,
            status,
            ageRating: 18,
            genres,
            raw: series,
        };
    }
    async fetchChapters(sourceWorkId) {
        const slug = sourceWorkId.replace(/^\//, '').replace(/\/$/, '');
        const details = await this.fetchWorkDetails(slug);
        const seriesId = details.raw?.id;
        if (!seriesId) {
            throw new Error(`Hipercool series ID not found for ${slug}`);
        }
        const input = JSON.stringify({
            '0': { json: { values: ['undefined'] } },
            '1': {
                json: { seriesId, chapterId: null, sort: 'best', page: 1, limit: 500 },
                meta: { values: { chapterId: ['undefined'] } },
            },
            '2': { json: { seriesId } },
        });
        const url = `${this.baseUrl}/api/trpc/auth.me,series.chapters?batch=1&input=${encodeURIComponent(input)}`;
        const data = await this.fetchJson(url);
        const chapterList = data[data.length - 1]?.result?.data?.json || [];
        const chapters = [];
        const seen = new Set();
        for (const ch of chapterList) {
            const num = typeof ch.number === 'number' ? ch.number : parseFloat(ch.number);
            if (isNaN(num) || seen.has(num))
                continue;
            seen.add(num);
            chapters.push({
                sourceChapterId: `${slug}/${num}`,
                number: num,
                title: ch.title ? `Capítulo ${num} - ${ch.title}` : `Capítulo ${num}`,
            });
        }
        chapters.sort((a, b) => a.number - b.number);
        return chapters;
    }
    async fetchChapterPages(sourceChapterId, _chapterNumber) {
        const parts = sourceChapterId.split('/');
        const slug = parts[0];
        const chapterNumber = parseFloat(parts[1] || '1');
        const input = JSON.stringify({
            '0': { json: null, meta: { values: ['undefined'] } },
            '1': { json: { slug } },
            '2': { json: { seriesSlug: slug, chapterNumber } },
            '3': { json: { position: 'footer_bottom' } },
        });
        const url = `${this.baseUrl}/api/trpc/auth.me,series.bySlug,reader.chapterPages?batch=1&input=${encodeURIComponent(input)}`;
        const data = await this.fetchJson(url);
        const pagesResult = data[2]?.result?.data?.json || [];
        const urls = [];
        for (const p of pagesResult) {
            const u = p.webpUrl || p.avifUrl || p.url;
            if (typeof u === 'string' && u.startsWith('http')) {
                urls.push(u);
            }
        }
        return urls;
    }
    async searchWorks(query) {
        const input = JSON.stringify({
            '0': {
                json: {
                    q: query,
                    sort: 'popular',
                    filters: {
                        genres: null,
                        type: null,
                        status: null,
                        contentRating: null,
                        author: null,
                        artist: null,
                        year: null,
                    },
                    limit: 30,
                    offset: 0,
                },
                meta: {
                    values: {
                        'filters.genres': ['undefined'],
                        'filters.type': ['undefined'],
                        'filters.status': ['undefined'],
                        'filters.contentRating': ['undefined'],
                        'filters.author': ['undefined'],
                        'filters.artist': ['undefined'],
                        'filters.year': ['undefined'],
                    },
                },
            },
        });
        const url = `${this.baseUrl}/api/trpc/search.query?batch=1&input=${encodeURIComponent(input)}`;
        const data = await this.fetchJson(url);
        const hits = data[0]?.result?.data?.json?.hits || [];
        return hits.map((hit) => ({
            sourceWorkId: hit.slug || String(hit.id),
            title: hit.title || hit.slug,
            slug: hit.slug || slugify(hit.title),
            coverUrl: hit.coverUrl || null,
        }));
    }
}
