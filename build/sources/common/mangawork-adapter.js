import { HostRateLimiter } from '../../core/rate-limiter.js';
import { Logger } from '../../core/logger.js';
import { decodeHtmlEntities, stripHtml, extractChapterNumber } from './html-utils.js';
export class MangaWorkAdapter {
    rateLimiter;
    transport;
    id;
    name;
    baseUrl;
    seriesPath;
    logger;
    constructor(options, rateLimiter = new HostRateLimiter(2.0), transport = fetch) {
        this.rateLimiter = rateLimiter;
        this.transport = transport;
        this.id = options.id;
        this.name = options.name;
        this.baseUrl = options.baseUrl.replace(/\/$/, '');
        this.seriesPath = options.seriesPath || 'series';
        this.logger = new Logger(`MangaWorkAdapter:${this.id}`);
        const host = new URL(this.baseUrl).host;
        const rps = options.rateLimitRps || 2.0;
        this.rateLimiter.setHostRate(host, rps, Math.ceil(rps * 2), rps * 2);
    }
    get headers() {
        return {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
            Accept: 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
            'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
            Referer: `${this.baseUrl}/`,
        };
    }
    async fetchHtml(url, options = {}) {
        const host = new URL(url).host;
        await this.rateLimiter.acquire(host);
        let attempts = 0;
        const maxAttempts = 3;
        while (attempts < maxAttempts) {
            attempts++;
            try {
                const res = await this.transport(url, {
                    ...options,
                    headers: {
                        ...this.headers,
                        ...(options.headers || {}),
                    },
                });
                if (res.status === 429 || res.status >= 500) {
                    if (attempts < maxAttempts) {
                        await new Promise((resolve) => setTimeout(resolve, 1000 * attempts));
                        continue;
                    }
                }
                if (!res.ok) {
                    throw new Error(`HTTP ${res.status} from ${url}`);
                }
                return await res.text();
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
        const url = page === 1
            ? `${this.baseUrl}/${this.seriesPath}/`
            : `${this.baseUrl}/${this.seriesPath}/page/${page}/`;
        let html;
        try {
            html = await this.fetchHtml(url);
        }
        catch (err) {
            this.logger.error(`Error fetching updated works: ${err.message}`);
            return { works: [], nextCursor: null };
        }
        const mangaLinkRegex = /href="([^"]*\/manga\/([^"\/]+)\/?)"[^>]*>([\s\S]*?)<\/a>/gi;
        const matches = Array.from(html.matchAll(mangaLinkRegex));
        const works = [];
        const seen = new Set();
        for (const m of matches) {
            const fullUrl = m[1].trim();
            const slug = m[2].trim();
            const innerHtml = m[3];
            if (seen.has(slug) || slug === 'page' || slug === 'feed')
                continue;
            seen.add(slug);
            // Title
            const titleMatch = innerHtml.match(/<h[1-6][^>]*>([\s\S]*?)<\/h[1-6]>/i) ||
                innerHtml.match(/title="([^"]+)"/i);
            const title = titleMatch ? stripHtml(decodeHtmlEntities(titleMatch[1])).trim() : slug;
            // Cover
            const imgMatch = innerHtml.match(/src="([^"]+)"/i) || innerHtml.match(/data-src="([^"]+)"/i);
            const coverUrl = imgMatch ? imgMatch[1].trim() : null;
            works.push({
                sourceWorkId: slug,
                title: title || slug,
                slug,
                coverUrl: coverUrl?.startsWith('http') ? coverUrl : (coverUrl ? `${this.baseUrl}${coverUrl}` : null),
            });
            if (isMaintenance && works.length >= 24)
                break;
        }
        const hasNext = html.includes(`page/${page + 1}/`);
        return {
            works,
            nextCursor: hasNext && !isMaintenance ? (page + 1).toString() : null,
        };
    }
    async fetchWorkDetails(sourceWorkId) {
        const workUrl = sourceWorkId.startsWith('http')
            ? sourceWorkId
            : `${this.baseUrl}/manga/${sourceWorkId}/`;
        const html = await this.fetchHtml(workUrl);
        // Title
        const titleMatch = html.match(/<h1[^>]*>([\s\S]*?)<\/h1>/i);
        const title = titleMatch ? stripHtml(decodeHtmlEntities(titleMatch[1])).trim() : sourceWorkId;
        // Cover
        const coverMatch = html.match(/itemprop="image"[^>]*src="([^"]+)"/i) ||
            html.match(/class="[^"]*thumb[^"]*"[^>]*src="([^"]+)"/i);
        const coverUrl = coverMatch ? coverMatch[1].trim() : null;
        // Synopsis
        const synMatch = html.match(/itemprop="description"[^>]*>([\s\S]*?)<\/div>/i) ||
            html.match(/class="[^"]*description[^"]*"[^>]*>([\s\S]*?)<\/div>/i);
        const synopsis = synMatch ? stripHtml(decodeHtmlEntities(synMatch[1])).trim() : undefined;
        // Genres
        const genreMatches = Array.from(html.matchAll(/itemprop="genre"[^>]*>([^<]+)<\/a>/gi));
        const genres = Array.from(new Set(genreMatches.map(m => decodeHtmlEntities(m[1].trim()))));
        let kind = 'MANGA';
        const lowerGenres = genres.map(g => g.toLowerCase());
        if (lowerGenres.includes('manhwa'))
            kind = 'MANHWA';
        else if (lowerGenres.includes('manhua'))
            kind = 'MANHUA';
        else if (lowerGenres.includes('webtoon'))
            kind = 'WEBTOON';
        return {
            sourceWorkId,
            title,
            slug: sourceWorkId,
            coverUrl,
            synopsis,
            genres,
            kind,
            status: 'ONGOING',
        };
    }
    async fetchChapters(sourceWorkId) {
        const workUrl = sourceWorkId.startsWith('http')
            ? sourceWorkId
            : `${this.baseUrl}/manga/${sourceWorkId}/`;
        const html = await this.fetchHtml(workUrl);
        const chapterMatches = Array.from(html.matchAll(/href="([^"]*(?:capitulo|chapter)[^"]*)"/gi));
        const seenUrls = new Set();
        const chapters = [];
        for (const m of chapterMatches) {
            const chapUrl = m[1].trim();
            if (!chapUrl || seenUrls.has(chapUrl))
                continue;
            seenUrls.add(chapUrl);
            const num = extractChapterNumber(chapUrl);
            chapters.push({
                sourceChapterId: chapUrl.startsWith('http') ? chapUrl : `${this.baseUrl}${chapUrl.startsWith('/') ? '' : '/'}${chapUrl}`,
                number: num,
                title: `Capítulo ${num}`,
            });
        }
        chapters.sort((a, b) => a.number - b.number);
        return chapters;
    }
    async fetchChapterPages(sourceChapterId, _chapterNumber) {
        const chapterUrl = sourceChapterId.startsWith('http')
            ? sourceChapterId
            : `${this.baseUrl}${sourceChapterId.startsWith('/') ? '' : '/'}${sourceChapterId}`;
        const html = await this.fetchHtml(chapterUrl);
        const pages = [];
        const seen = new Set();
        const readerareaMatch = html.match(/class="reader-area"[^>]*>([\s\S]*?)<\/div>/i);
        const containerHtml = readerareaMatch ? readerareaMatch[1] : html;
        const imgs = Array.from(containerHtml.matchAll(/<img[^>]+(?:src|data-src)="([^"]+)"[^>]*>/gi));
        for (const m of imgs) {
            const url = m[1].trim();
            if (url.startsWith('data:') || /discord|logo|banner|advert/i.test(url))
                continue;
            if (!seen.has(url)) {
                seen.add(url);
                pages.push(url);
            }
        }
        return pages;
    }
    async searchWorks(query) {
        const url = `${this.baseUrl}/?s=${encodeURIComponent(query)}`;
        const html = await this.fetchHtml(url);
        const mangaLinkRegex = /href="([^"]*\/manga\/([^"\/]+)\/?)"[^>]*>([\s\S]*?)<\/a>/gi;
        const matches = Array.from(html.matchAll(mangaLinkRegex));
        const works = [];
        const seen = new Set();
        for (const m of matches) {
            const slug = m[2].trim();
            const innerHtml = m[3];
            if (seen.has(slug) || slug === 'page' || slug === 'feed')
                continue;
            seen.add(slug);
            const titleMatch = innerHtml.match(/<h[1-6][^>]*>([\s\S]*?)<\/h[1-6]>/i) ||
                innerHtml.match(/title="([^"]+)"/i);
            const title = titleMatch ? stripHtml(decodeHtmlEntities(titleMatch[1])).trim() : slug;
            const imgMatch = innerHtml.match(/src="([^"]+)"/i) || innerHtml.match(/data-src="([^"]+)"/i);
            const coverUrl = imgMatch ? imgMatch[1].trim() : null;
            works.push({
                sourceWorkId: slug,
                title: title || slug,
                slug,
                coverUrl: coverUrl?.startsWith('http') ? coverUrl : (coverUrl ? `${this.baseUrl}${coverUrl}` : null),
            });
        }
        return works;
    }
    getImageHeaders(_url) {
        return {
            Referer: `${this.baseUrl}/`,
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
        };
    }
}
