import { HostRateLimiter } from '../../core/rate-limiter.js';
import { Logger } from '../../core/logger.js';
import { decodeHtmlEntities, stripHtml, extractChapterNumber } from '../common/html-utils.js';
export class MegaHentaiAdapter {
    rateLimiter;
    transport;
    id = 'megahentai';
    name = 'MegaHentai';
    baseUrl = 'https://megahentai.biz';
    logger = new Logger('MegaHentaiAdapter');
    constructor(rateLimiter = new HostRateLimiter(2.0), transport = fetch) {
        this.rateLimiter = rateLimiter;
        this.transport = transport;
        this.rateLimiter.setHostRate('megahentai.biz', 2.0, 4, 4.0);
        this.rateLimiter.setHostRate('gall2.megahentai.biz', 8.0, 16, 16.0);
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
                    throw new Error(`MegaHentai request failed: HTTP ${res.status}`);
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
        const url = page === 1 ? `${this.baseUrl}/capitulos-recentes/` : `${this.baseUrl}/capitulos-recentes/page/${page}/`;
        const html = await this.fetchHtml(url);
        const works = [];
        const linkMatches = [...html.matchAll(/href="(https:\/\/megahentai\.biz\/ler-online\/([^"/]+)\/?)"/g)];
        const seenSlugs = new Set();
        for (const m of linkMatches) {
            const slug = m[2];
            if (seenSlugs.has(slug) || slug === 'feed' || slug === 'page')
                continue;
            seenSlugs.add(slug);
            const idx = m.index || 0;
            const snippet = html.slice(Math.max(0, idx - 200), idx + 400);
            const titleMatch = snippet.match(/class="[^"]*title[^"]*"[^>]*>([\s\S]*?)<\//i) ||
                snippet.match(/title="([^"]+)"/i);
            const rawTitle = titleMatch ? decodeHtmlEntities(stripHtml(titleMatch[1])) : slug.replace(/[-_]+/g, ' ');
            const title = rawTitle.replace(/^Todos\s+os\s+Cap[ií]tulos\s+de\s+/i, '').replace(/\s+Todos\s+os\s+Cap[ií]tulos$/i, '').trim();
            const imgMatch = snippet.match(/<img[^>]+(?:src|data-src|data-lazy-src)="([^"]+)"/i);
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
        const slug = sourceWorkId.replace(/^\//, '').replace(/\/$/, '').replace(/^ler-online\//, '');
        const url = `${this.baseUrl}/ler-online/${slug}/`;
        const html = await this.fetchHtml(url);
        const titleMatch = html.match(/class="[^"]*data_main[^"]*"[\s\S]*?<h1[^>]*>([\s\S]*?)<\/h1>/i) ||
            html.match(/<h1[^>]*>([\s\S]*?)<\/h1>/i);
        let title = titleMatch ? decodeHtmlEntities(stripHtml(titleMatch[1])) : slug.replace(/[-_]+/g, ' ');
        title = title.replace(/^Todos\s+os\s+Cap[ií]tulos\s+de\s+/i, '').replace(/\s+Todos\s+os\s+Cap[ií]tulos$/i, '').trim();
        const coverMatch = html.match(/class="[^"]*poster[^"]*"[\s\S]*?<img[^>]+(?:src|data-src|data-lazy-src)="([^"]+)"/i) ||
            html.match(/property="og:image"\s+content="([^"]+)"/i);
        const coverUrl = coverMatch ? coverMatch[1].trim() : null;
        const descMatch = html.match(/class="[^"]*sinopse[^"]*"[\s\S]*?class="texto"[^>]*>([\s\S]*?)<\/div>/i) ||
            html.match(/<p>([\s\S]*?)<\/p>/i);
        const synopsis = descMatch ? decodeHtmlEntities(stripHtml(descMatch[1])) : '';
        const authorMatch = html.match(/Autor[\s\S]*?<span>([\s\S]*?)<\/span>/i);
        const author = authorMatch ? decodeHtmlEntities(stripHtml(authorMatch[1])) : undefined;
        const artistMatch = html.match(/Artista[\s\S]*?<span>([\s\S]*?)<\/span>/i);
        const artist = artistMatch ? decodeHtmlEntities(stripHtml(artistMatch[1])) : undefined;
        const genres = [];
        const genreMatches = [...html.matchAll(/class="[^"]*gen_flex[^"]*"[\s\S]*?<a[^>]*>([\s\S]*?)<\/a>/gi)];
        for (const gm of genreMatches) {
            const g = decodeHtmlEntities(stripHtml(gm[1]));
            if (g && !genres.includes(g))
                genres.push(g);
        }
        let status = 'UNKNOWN';
        if (/status[\s\S]*?complet/i.test(html) || /conclu[ií]d/i.test(html)) {
            status = 'COMPLETED';
        }
        else if (/hiat/i.test(html)) {
            status = 'HIATUS';
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
            status,
            ageRating: 18,
            genres,
            raw: { sourceWorkId: slug },
        };
    }
    async fetchChapters(sourceWorkId) {
        const slug = sourceWorkId.replace(/^\//, '').replace(/\/$/, '').replace(/^ler-online\//, '');
        const url = `${this.baseUrl}/ler-online/${slug}/`;
        const html = await this.fetchHtml(url);
        const chapters = [];
        const seenNumbers = new Set();
        const linkMatches = [...html.matchAll(/href="(https:\/\/megahentai\.biz\/ler\/([^"/]+)\/?)"/g)];
        for (const m of linkMatches) {
            const chSlug = m[2];
            const capNum = extractChapterNumber(chSlug);
            if (seenNumbers.has(capNum))
                continue;
            seenNumbers.add(capNum);
            chapters.push({
                sourceChapterId: chSlug,
                number: capNum,
                title: `Capítulo ${capNum}`,
            });
        }
        chapters.sort((a, b) => a.number - b.number);
        return chapters;
    }
    async fetchChapterPages(sourceChapterId, _chapterNumber) {
        const chSlug = sourceChapterId.replace(/^\//, '').replace(/\/$/, '').replace(/^ler\//, '');
        const url = `${this.baseUrl}/ler/${chSlug}/`;
        const html = await this.fetchHtml(url);
        const urls = [];
        const imgMatches = [
            ...html.matchAll(/src="([^"]*megahentai\.biz\/static\/[^"]+)"/gi),
            ...html.matchAll(/id="content"[^>]*class="[^"]*cap[^"]*"[\s\S]*?<img[^>]+src="([^"]+)"/gi),
        ];
        for (const m of imgMatches) {
            const u = m[1].replace(/[\r\n\t\s]+/g, '').trim();
            if (u && !urls.includes(u) && u.includes('/static/')) {
                urls.push(u);
            }
        }
        return urls;
    }
    async searchWorks(query) {
        const url = `${this.baseUrl}/?s=${encodeURIComponent(query)}`;
        const html = await this.fetchHtml(url);
        const works = [];
        const linkMatches = [...html.matchAll(/href="(https:\/\/megahentai\.biz\/ler-online\/([^"/]+)\/?)"/g)];
        const seenSlugs = new Set();
        for (const m of linkMatches) {
            const slug = m[2];
            if (seenSlugs.has(slug) || slug === 'feed' || slug === 'page')
                continue;
            seenSlugs.add(slug);
            const idx = m.index || 0;
            const snippet = html.slice(Math.max(0, idx - 200), idx + 400);
            const titleMatch = snippet.match(/class="[^"]*title[^"]*"[^>]*>([\s\S]*?)<\//i) ||
                snippet.match(/title="([^"]+)"/i);
            const rawTitle = titleMatch ? decodeHtmlEntities(stripHtml(titleMatch[1])) : slug.replace(/[-_]+/g, ' ');
            const title = rawTitle.replace(/^Todos\s+os\s+Cap[ií]tulos\s+de\s+/i, '').replace(/\s+Todos\s+os\s+Cap[ií]tulos$/i, '').trim();
            const imgMatch = snippet.match(/<img[^>]+(?:src|data-src|data-lazy-src)="([^"]+)"/i);
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
