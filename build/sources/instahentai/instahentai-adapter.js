import { HostRateLimiter } from '../../core/rate-limiter.js';
import { Logger } from '../../core/logger.js';
import { decodeHtmlEntities, stripHtml, extractChapterNumber } from '../common/html-utils.js';
export class InstaHentaiAdapter {
    rateLimiter;
    transport;
    id = 'instahentai';
    name = 'InstaHentai';
    baseUrl = 'https://instahentai.com';
    logger = new Logger('InstaHentaiAdapter');
    constructor(rateLimiter = new HostRateLimiter(2.0), transport = fetch) {
        this.rateLimiter = rateLimiter;
        this.transport = transport;
        this.rateLimiter.setHostRate('instahentai.com', 2.0, 4, 4.0);
        this.rateLimiter.setHostRate('cdn.instahentai.com', 8.0, 16, 16.0);
    }
    get headers() {
        return {
            Accept: 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
            Referer: `${this.baseUrl}/`,
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
        };
    }
    async fetchHtml(url, options = {}) {
        const parsed = new URL(url);
        await this.rateLimiter.acquire(parsed.host);
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
                    signal: AbortSignal.timeout(30_000),
                });
                if (res.status === 429) {
                    const retryAfter = res.headers.get('Retry-After');
                    this.rateLimiter.handle429(parsed.host, retryAfter, attempts);
                    if (attempts >= maxAttempts)
                        throw new Error(`HTTP 429 rate limit on ${parsed.host}`);
                    continue;
                }
                if (!res.ok) {
                    throw new Error(`InstaHentai request failed: HTTP ${res.status}`);
                }
                this.rateLimiter.recordSuccess(parsed.host);
                return await res.text();
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
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
        };
    }
    async fetchUpdatedWorks(cursor, _options) {
        const page = cursor ? parseInt(cursor, 10) : 1;
        const url = page === 1 ? `${this.baseUrl}/` : `${this.baseUrl}/page/${page}/`;
        const html = await this.fetchHtml(url);
        const works = [];
        const itemRegex = /<article class="[^"]*card_item[^"]*"[\s\S]*?<\/article>/gi;
        const matches = html.match(itemRegex) || [];
        for (const block of matches) {
            const linkMatch = block.match(/href="([^"]*\/serie\/([^"/]+)\/?)"/i);
            if (!linkMatch)
                continue;
            const slug = linkMatch[2];
            const titleMatch = block.match(/aria-label="([^"]+)"/i) || block.match(/<a[^>]*>([\s\S]*?)<\/a>/i);
            const title = titleMatch ? decodeHtmlEntities(stripHtml(titleMatch[1])) : slug.replace(/[-_]+/g, ' ');
            const imgMatch = block.match(/<img[^>]+(?:data-src|src)="([^"]+)"/i);
            const coverUrl = imgMatch ? imgMatch[1].trim() : null;
            works.push({
                sourceWorkId: slug,
                title,
                slug,
                coverUrl,
            });
        }
        const hasNext = html.includes(`/page/${page + 1}/`) || html.includes('nextpostslink');
        const nextCursor = hasNext && works.length > 0 ? String(page + 1) : null;
        return { works, nextCursor };
    }
    async fetchWorkDetails(sourceWorkId) {
        const slug = sourceWorkId.replace(/^\//, '').replace(/\/$/, '').replace(/^serie\//, '');
        const url = `${this.baseUrl}/serie/${slug}/`;
        const html = await this.fetchHtml(url);
        const titleMatch = html.match(/<h1[^>]*>([\s\S]*?)<\/h1>/i);
        const title = titleMatch ? decodeHtmlEntities(stripHtml(titleMatch[1])) : slug.replace(/[-_]+/g, ' ');
        const coverMatch = html.match(/<img[^>]+itemprop="image"[^>]+src="([^"]+)"/i) ||
            html.match(/<img[^>]+src="([^"]+)"[^>]+itemprop="image"/i) ||
            html.match(/property="og:image"\s+content="([^"]+)"/i);
        const coverUrl = coverMatch ? coverMatch[1].trim() : null;
        const descMatch = html.match(/class="[^"]*(?:description|sinopse)[^"]*"[^>]*>([\s\S]*?)<\/div>/i) ||
            html.match(/<p>([\s\S]*?)<\/p>/i);
        const synopsis = descMatch ? decodeHtmlEntities(stripHtml(descMatch[1])) : '';
        const authorMatch = html.match(/href="[^"]*\/autor\/[^"]*"[^>]*>([\s\S]*?)<\/a>/i);
        const author = authorMatch ? decodeHtmlEntities(stripHtml(authorMatch[1])) : undefined;
        const artistMatch = html.match(/href="[^"]*\/artista\/[^"]*"[^>]*>([\s\S]*?)<\/a>/i);
        const artist = artistMatch ? decodeHtmlEntities(stripHtml(artistMatch[1])) : undefined;
        const genres = [];
        const genreMatches = [
            ...html.matchAll(/href="[^"]*\/(?:genero|categoria)\/[^"]*"[^>]*>([\s\S]*?)<\/a>/gi),
        ];
        for (const gm of genreMatches) {
            const g = decodeHtmlEntities(stripHtml(gm[1]));
            if (g && !genres.includes(g))
                genres.push(g);
        }
        return {
            sourceWorkId: slug,
            title,
            slug,
            coverUrl,
            synopsis,
            author,
            artist,
            kind: 'MANHWA',
            status: 'ONGOING',
            ageRating: 18,
            genres,
            raw: { sourceWorkId: slug },
        };
    }
    async fetchChapters(sourceWorkId) {
        const slug = sourceWorkId.replace(/^\//, '').replace(/\/$/, '').replace(/^serie\//, '');
        const url = `${this.baseUrl}/serie/${slug}/`;
        const html = await this.fetchHtml(url);
        const chapters = [];
        const seenNumbers = new Set();
        const linkMatches = [...html.matchAll(/<a[^>]+href="([^"]*\/ler\/([^"/]+)\/?)"[^>]*>([\s\S]*?)<\/a>/gi)];
        for (const m of linkMatches) {
            const chSlug = m[2];
            const linkText = decodeHtmlEntities(stripHtml(m[3]));
            const capNum = extractChapterNumber(linkText || chSlug);
            if (seenNumbers.has(capNum))
                continue;
            seenNumbers.add(capNum);
            chapters.push({
                sourceChapterId: chSlug,
                number: capNum,
                title: linkText || `Capítulo ${capNum}`,
            });
        }
        chapters.sort((a, b) => a.number - b.number);
        return chapters;
    }
    async fetchChapterPages(sourceChapterId, _chapterNumber) {
        const chSlug = sourceChapterId.replace(/^\//, '').replace(/\/$/, '').replace(/^ler\//, '');
        const url = `${this.baseUrl}/ler/${chSlug}/?echo=true`;
        const html = await this.fetchHtml(url);
        const urls = [];
        const imgMatches = [
            ...html.matchAll(/class="[^"]*cap[^"]*"[\s\S]*?<img[^>]+(?:data-src|src)="([^"]+)"/gi),
            ...html.matchAll(/(?:data-src|src)="([^"]+cdn\.instahentai\.com\/static\/[^"]+)"/gi),
        ];
        for (const m of imgMatches) {
            const u = m[1].replace(/[\r\n\t\s]+/g, '').trim();
            if (u && !urls.includes(u)) {
                urls.push(u);
            }
        }
        return urls;
    }
    async searchWorks(query) {
        const url = `${this.baseUrl}/?s=${encodeURIComponent(query)}`;
        const html = await this.fetchHtml(url);
        const works = [];
        const itemRegex = /<article class="[^"]*card_item[^"]*"[\s\S]*?<\/article>/gi;
        const matches = html.match(itemRegex) || [];
        for (const block of matches) {
            const linkMatch = block.match(/href="([^"]*\/serie\/([^"/]+)\/?)"/i);
            if (!linkMatch)
                continue;
            const slug = linkMatch[2];
            const titleMatch = block.match(/aria-label="([^"]+)"/i) || block.match(/<a[^>]*>([\s\S]*?)<\/a>/i);
            const title = titleMatch ? decodeHtmlEntities(stripHtml(titleMatch[1])) : slug.replace(/[-_]+/g, ' ');
            const imgMatch = block.match(/<img[^>]+(?:data-src|src)="([^"]+)"/i);
            const coverUrl = imgMatch ? imgMatch[1].trim() : null;
            works.push({
                sourceWorkId: slug,
                title,
                slug,
                coverUrl,
            });
        }
        return works;
    }
}
