import { HostRateLimiter } from '../../core/rate-limiter.js';
import { Logger } from '../../core/logger.js';
export class GeassComicsAdapter {
    rateLimiter;
    transport;
    id = 'geasscomics';
    name = 'Geass Comics';
    baseUrl = 'https://geasscomics.xyz';
    apiUrl = 'https://api.geasscomics.xyz';
    logger = new Logger('GeassComicsAdapter');
    constructor(rateLimiter = new HostRateLimiter(2.0), transport = fetch) {
        this.rateLimiter = rateLimiter;
        this.transport = transport;
        this.rateLimiter.setHostRate('api.geasscomics.xyz', 2.0, 4, 4);
        this.rateLimiter.setHostRate('geasscomics.xyz', 2.0, 4, 4);
        this.rateLimiter.setHostRate('cdn.geasscomics.xyz', 5.0, 10, 10);
    }
    get headers() {
        return {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
            Accept: 'application/json, text/plain, */*',
            Referer: `${this.baseUrl}/`,
            Origin: this.baseUrl,
        };
    }
    async fetchJson(url) {
        const host = new URL(url).host;
        await this.rateLimiter.acquire(host);
        let attempts = 0;
        const maxAttempts = 3;
        while (attempts < maxAttempts) {
            attempts++;
            try {
                const res = await this.transport(url, { headers: this.headers });
                if (res.status === 429 || res.status >= 500) {
                    if (attempts < maxAttempts) {
                        await new Promise((resolve) => setTimeout(resolve, 1000 * attempts));
                        continue;
                    }
                }
                if (!res.ok) {
                    throw new Error(`HTTP ${res.status} from ${url}`);
                }
                return (await res.json());
            }
            catch (err) {
                if (attempts >= maxAttempts)
                    throw err;
                await new Promise((resolve) => setTimeout(resolve, 1000 * attempts));
            }
        }
        throw new Error(`Failed to fetch ${url} after ${maxAttempts} attempts`);
    }
    async fetchUpdatedWorks(cursor, options) {
        const page = cursor ? parseInt(cursor, 10) : 1;
        const isMaintenance = options?.mode === 'maintenance';
        const limit = isMaintenance ? 24 : 36;
        const url = `${this.apiUrl}/api/works?page=${page}&limit=${limit}&sortBy=recent&sortDir=desc`;
        try {
            const res = await this.fetchJson(url);
            const items = res.data?.items || [];
            const works = items.map((item) => ({
                sourceWorkId: item.slug,
                title: item.title,
                slug: item.slug,
                coverUrl: item.cover || null,
                updatedAt: item.updatedAt || undefined,
            }));
            const hasNext = page < (res.data?.pageCount || 1);
            return {
                works,
                nextCursor: hasNext && !isMaintenance ? (page + 1).toString() : null,
            };
        }
        catch (err) {
            this.logger.error(`Error fetching updated works: ${err.message}`);
            return { works: [], nextCursor: null };
        }
    }
    async fetchWorkDetails(sourceWorkId) {
        const url = `${this.apiUrl}/api/works/${sourceWorkId}`;
        const res = await this.fetchJson(url);
        const w = res.data;
        let kind = 'UNKNOWN';
        const rawKind = (w.kind || '').toUpperCase();
        if (rawKind === 'MANHWA')
            kind = 'MANHWA';
        else if (rawKind === 'MANHUA')
            kind = 'MANHUA';
        else if (rawKind === 'MANGA')
            kind = 'MANGA';
        else if (rawKind === 'WEBTOON')
            kind = 'WEBTOON';
        let status = 'ONGOING';
        const rawStatus = (w.status || '').toLowerCase();
        if (rawStatus === 'completed')
            status = 'COMPLETED';
        else if (rawStatus === 'hiatus')
            status = 'HIATUS';
        else if (rawStatus === 'cancelled')
            status = 'CANCELLED';
        return {
            sourceWorkId: w.slug || sourceWorkId,
            title: w.title || sourceWorkId,
            slug: w.slug || sourceWorkId,
            coverUrl: w.cover || null,
            synopsis: w.synopsis || undefined,
            author: w.author || undefined,
            genres: Array.isArray(w.tags) ? w.tags : [],
            kind,
            status,
        };
    }
    async fetchChapters(sourceWorkId) {
        const url = `${this.apiUrl}/api/works/${sourceWorkId}`;
        const res = await this.fetchJson(url);
        const chapters = res.data?.chapters || [];
        const summaries = chapters.map((c) => {
            const num = typeof c.number === 'number' ? c.number : parseFloat(c.number) || 0;
            return {
                sourceChapterId: `${sourceWorkId}/${num}`,
                number: num,
                title: c.title ? `Capítulo ${num} - ${c.title}` : `Capítulo ${num}`,
                createdAt: c.releasedAt || undefined,
                pageCount: typeof c.pageCount === 'number' ? c.pageCount : null,
            };
        });
        summaries.sort((a, b) => a.number - b.number);
        return summaries;
    }
    async fetchChapterPages(sourceChapterId, _chapterNumber) {
        const parts = sourceChapterId.split('/');
        const slug = parts[0];
        const chapNum = parts[1];
        const url = `${this.baseUrl}/api/read/${slug}/${chapNum}`;
        const res = await this.fetchJson(url);
        const pages = res.pages || [];
        return pages.filter((u) => typeof u === 'string' && u.startsWith('http'));
    }
    async searchWorks(query) {
        const url = `${this.apiUrl}/api/works?page=1&limit=24&q=${encodeURIComponent(query)}`;
        try {
            const res = await this.fetchJson(url);
            const items = res.data?.items || [];
            return items.map((item) => ({
                sourceWorkId: item.slug,
                title: item.title,
                slug: item.slug,
                coverUrl: item.cover || null,
            }));
        }
        catch {
            return [];
        }
    }
    getImageHeaders(_url) {
        return {
            Referer: `${this.baseUrl}/`,
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
        };
    }
}
