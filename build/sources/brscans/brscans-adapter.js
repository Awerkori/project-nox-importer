import { HostRateLimiter } from '../../core/rate-limiter.js';
import { extractChapterNumber, slugify } from '../common/html-utils.js';
/**
 * BR Scans exposes a small, version-independent JSON API backed by its own
 * catalogue. It is deliberately modelled on that API rather than the public
 * web UI, so a theme change cannot break discovery or the reader manifest.
 */
export class BRScansAdapter {
    rateLimiter;
    transport;
    id = 'brscans';
    name = 'BR Scans';
    baseUrl = 'https://brscans.vercel.app';
    apiUrl = 'https://e5oer7ngt8.execute-api.sa-east-1.amazonaws.com/dev';
    constructor(rateLimiter = new HostRateLimiter(2), transport = fetch) {
        this.rateLimiter = rateLimiter;
        this.transport = transport;
        this.rateLimiter.setHostRate(new URL(this.apiUrl).host, 2, 4, 4);
        this.rateLimiter.setHostRate('d47sf0ah3vbtc.cloudfront.net', 8, 16, 16);
    }
    async request(path) {
        const url = `${this.apiUrl}${path}`;
        const host = new URL(url).host;
        await this.rateLimiter.acquire(host);
        const response = await this.transport(url, {
            headers: { Accept: 'application/json', Referer: `${this.baseUrl}/` },
            signal: AbortSignal.timeout(15_000),
        });
        if (!response.ok)
            throw new Error(`BRScans HTTP ${response.status} for ${path}`);
        this.rateLimiter.recordSuccess(host);
        return response.json();
    }
    workSummary(work) {
        const title = String(work.title || work.original_title || `Obra ${work.id}`);
        return {
            sourceWorkId: String(work.id),
            title,
            slug: slugify(title),
            coverUrl: work.thumbnail?.translated || work.thumbnail?.original || null,
            updatedAt: work.updated_at || work.created_at || work.chapters?.[0]?.created_at,
        };
    }
    workDetails(work) {
        const title = String(work.title || work.original_title || `Obra ${work.id}`);
        const originalTitle = typeof work.original_title === 'string' ? work.original_title.trim() : '';
        const statusText = String(work.status || '').toLocaleLowerCase();
        const status = /conclu|complete|finished/.test(statusText)
            ? 'COMPLETED'
            : /hiat|paus/.test(statusText)
                ? 'HIATUS'
                : statusText ? 'ONGOING' : 'UNKNOWN';
        const genres = (Array.isArray(work.genres) ? work.genres : [])
            .map((genre) => typeof genre === 'string' ? genre : genre?.name)
            .filter((genre) => Boolean(genre));
        return {
            sourceWorkId: String(work.id),
            title,
            slug: slugify(title),
            coverUrl: work.thumbnail?.translated || work.thumbnail?.original || null,
            synopsis: work.description || undefined,
            author: work.author || undefined,
            status,
            ageRating: work.is_nsfw === true ? 18 : undefined,
            genres: genres.length ? [...new Set(genres)] : undefined,
            alternativeTitles: originalTitle && originalTitle.localeCompare(title, undefined, { sensitivity: 'accent' }) !== 0
                ? [originalTitle]
                : undefined,
            raw: work,
        };
    }
    chapters(workId, work) {
        const sourceChapters = Array.isArray(work.chapters) ? work.chapters : [];
        return sourceChapters.map((chapter) => {
            const displayNumber = String(chapter.slug || chapter.number || chapter.title || chapter.id);
            const parsed = Number.parseFloat(displayNumber.replace(',', '.'));
            return {
                sourceChapterId: `${workId}:${chapter.id}`,
                displayNumber,
                number: Number.isFinite(parsed) ? parsed : extractChapterNumber(String(chapter.title || displayNumber)),
                title: chapter.title || undefined,
                createdAt: chapter.release_date || chapter.created_at || undefined,
                pageCount: chapter.quantity_pages ?? null,
            };
        }).sort((a, b) => a.number - b.number);
    }
    async fetchUpdatedWorks(cursor) {
        const page = Math.max(1, Number.parseInt(cursor || '1', 10) || 1);
        const data = await this.request(`/manhwas/?page=${page}`);
        return { works: (data.results || []).map((work) => this.workSummary(work)), nextCursor: data.next ? String(page + 1) : null };
    }
    async fetchWorkDetails(sourceWorkId) {
        return this.workDetails(await this.request(`/manhwas/${encodeURIComponent(sourceWorkId)}/`));
    }
    async fetchWorkSnapshot(sourceWorkId) {
        const work = await this.request(`/manhwas/${encodeURIComponent(sourceWorkId)}/`);
        return { details: this.workDetails(work), chapters: this.chapters(sourceWorkId, work) };
    }
    async fetchChapters(sourceWorkId) {
        const work = await this.request(`/manhwas/${encodeURIComponent(sourceWorkId)}/`);
        return this.chapters(sourceWorkId, work);
    }
    async fetchChapterPages(sourceChapterId) {
        const chapterId = sourceChapterId.slice(sourceChapterId.lastIndexOf(':') + 1);
        const chapter = await this.request(`/chapters/${encodeURIComponent(chapterId)}/`);
        return [...(chapter.pages || [])]
            .sort((a, b) => (a.order ?? 0) - (b.order ?? 0))
            .map((page) => page.images?.translated || page.images?.original || page.images?.raw)
            .filter((url) => Boolean(url));
    }
    async searchWorks(query) {
        const data = await this.request(`/manhwas/search/?query=${encodeURIComponent(query)}`);
        return (data || []).map((work) => this.workSummary(work));
    }
    getImageHeaders() {
        return { Referer: `${this.baseUrl}/`, Accept: 'image/avif,image/webp,image/*,*/*;q=0.8' };
    }
}
