import { HostRateLimiter } from '../../core/rate-limiter.js';
import { Logger } from '../../core/logger.js';
import { slugify } from '../common/html-utils.js';
export class TaimuMangasAdapter {
    rateLimiter;
    transport;
    id = 'taimumangas';
    name = 'TaimuMangas';
    baseUrl = 'https://beta.taimumangas.com';
    apiUrl = 'https://apiv2.taimumangas.com/api/v1/reader';
    logger = new Logger('TaimuMangasAdapter');
    constructor(rateLimiter = new HostRateLimiter(2.0), transport = fetch) {
        this.rateLimiter = rateLimiter;
        this.transport = transport;
        this.rateLimiter.setHostRate('apiv2.taimumangas.com', 4.0, 8, 8.0);
        this.rateLimiter.setHostRate('cdn.taimumangas.com', 12.0, 24, 24.0);
    }
    get headers() {
        return {
            Accept: 'application/json',
            Referer: `${this.baseUrl}/`,
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
        };
    }
    async request(url, options = {}) {
        const host = new URL(url).host;
        await this.rateLimiter.acquire(host);
        let attempts = 0;
        const maxAttempts = 3;
        while (attempts < maxAttempts) {
            attempts++;
            try {
                const response = await this.transport(url, {
                    ...options,
                    headers: {
                        ...this.headers,
                        ...(options.headers || {}),
                    },
                    signal: AbortSignal.timeout(20_000),
                });
                if (response.status === 429) {
                    const retryAfter = response.headers.get('Retry-After');
                    this.rateLimiter.handle429(host, retryAfter, attempts);
                    if (attempts >= maxAttempts)
                        throw new Error(`Rate limited (429) for ${host}`);
                    await new Promise((r) => setTimeout(r, 1000 * attempts));
                    continue;
                }
                if (!response.ok) {
                    throw new Error(`Taimu request failed: HTTP ${response.status} from ${url}`);
                }
                this.rateLimiter.recordSuccess(host);
                return (await response.json());
            }
            catch (err) {
                if (attempts >= maxAttempts)
                    throw err;
                await new Promise((r) => setTimeout(r, 1000 * attempts));
            }
        }
        throw new Error(`Taimu request failed after ${maxAttempts} attempts`);
    }
    async fetchUpdatedWorks(cursor, options) {
        const page = cursor ? Math.max(1, parseInt(cursor, 10)) : 1;
        const url = `${this.apiUrl}/updates?page=${page}&per_page=24&adult_mode=true`;
        const data = await this.request(url);
        const seen = new Set();
        const works = [];
        for (const item of data.items || []) {
            if (seen.has(item.series_identifier))
                continue;
            seen.add(item.series_identifier);
            works.push({
                sourceWorkId: item.series_identifier,
                slug: slugify(item.series_title),
                title: item.series_title,
                coverUrl: item.series_cover || null,
                updatedAt: item.chapter_published_at || new Date().toISOString(),
            });
        }
        return {
            works,
            nextCursor: data.has_more ? String(page + 1) : null,
        };
    }
    async fetchWorkDetails(sourceWorkId) {
        const url = `${this.apiUrl}/series/${sourceWorkId}`;
        const data = await this.request(url);
        let status = 'ONGOING';
        const s = (data.status || '').toLowerCase();
        if (s === 'completed' || s === 'finalizado')
            status = 'COMPLETED';
        else if (s === 'hiatus' || s === 'hiato')
            status = 'HIATUS';
        else if (s === 'cancelled' || s === 'cancelado')
            status = 'CANCELLED';
        return {
            sourceWorkId: data.identifier,
            slug: slugify(data.title),
            title: data.title,
            coverUrl: data.cover || null,
            synopsis: data.synopsis || undefined,
            author: data.authors?.map((a) => a.name).join(', ') || undefined,
            artist: data.artists?.map((a) => a.name).join(', ') || undefined,
            genres: data.genres?.map((g) => g.name).filter(Boolean),
            status,
            ageRating: data.adult ? 18 : 0,
        };
    }
    async fetchChapters(sourceWorkId) {
        const chapters = [];
        let page = 1;
        let hasMore = true;
        while (hasMore && page <= 5) {
            const url = `${this.apiUrl}/series/${sourceWorkId}/chapters?page=${page}&per_page=100&order=asc`;
            const data = await this.request(url);
            for (const item of data.items || []) {
                const num = typeof item.number === 'number' ? item.number : parseFloat(String(item.number).replace(',', '.'));
                chapters.push({
                    sourceChapterId: item.identifier,
                    number: isNaN(num) ? 0 : num,
                    title: `Capítulo ${item.number}`,
                    createdAt: item.published_at,
                });
            }
            hasMore = data.has_more;
            page++;
        }
        chapters.sort((a, b) => a.number - b.number);
        return chapters;
    }
    async fetchChapterPages(sourceChapterId, _chapterNumber) {
        const url = `${this.apiUrl}/chapters/${sourceChapterId}?adult=true`;
        const data = await this.request(url);
        const sorted = (data.pages || []).sort((a, b) => a.number - b.number);
        return sorted.map((p) => p.url).filter(Boolean);
    }
    async searchWorks(query) {
        const url = `${this.apiUrl}/library?page=1&per_page=24&q=${encodeURIComponent(query)}&adult=true`;
        const data = await this.request(url);
        return (data.items || []).map((item) => ({
            sourceWorkId: item.identifier,
            slug: slugify(item.title),
            title: item.title,
            coverUrl: item.cover || null,
        }));
    }
    getImageHeaders(url) {
        return {
            Referer: `${this.baseUrl}/`,
            'User-Agent': this.headers['User-Agent'],
            Accept: 'image/avif,image/webp,image/apng,image/*,*/*;q=0.8',
        };
    }
}
