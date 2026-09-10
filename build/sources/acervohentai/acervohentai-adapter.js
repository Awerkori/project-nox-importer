import { HostRateLimiter } from '../../core/rate-limiter.js';
import { Logger } from '../../core/logger.js';
import { decodeHtmlEntities, stripHtml, extractChapterNumber } from '../common/html-utils.js';
export class AcervoHentaiAdapter {
    rateLimiter;
    transport;
    id = 'acervohentai';
    name = 'Acervo Hentai';
    baseUrl = 'https://acervohentai.com';
    logger = new Logger('AcervoHentaiAdapter');
    cookies = new Map();
    constructor(rateLimiter = new HostRateLimiter(2.0), transport = fetch) {
        this.rateLimiter = rateLimiter;
        this.transport = transport;
        this.rateLimiter.setHostRate('acervohentai.com', 2.0, 4, 4.0);
    }
    get headers() {
        const cookieHeader = Array.from(this.cookies.entries())
            .map(([k, v]) => `${k}=${v}`)
            .join('; ');
        const h = {
            Accept: 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
            'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
            Referer: `${this.baseUrl}/`,
            'Sec-Ch-Ua': '"Chromium";v="130", "Google Chrome";v="130", "Not?A_Brand";v="99"',
            'Sec-Ch-Ua-Mobile': '?0',
            'Sec-Ch-Ua-Platform': '"Windows"',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-Fetch-User': '?1',
            'Upgrade-Insecure-Requests': '1',
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
                this.storeCookies(res);
                if (res.status === 429) {
                    const retryAfter = res.headers.get('Retry-After');
                    this.rateLimiter.handle429(parsed.host, retryAfter, attempts);
                    if (attempts >= maxAttempts)
                        throw new Error(`HTTP 429 rate limit on ${parsed.host}`);
                    continue;
                }
                if (!res.ok) {
                    if (res.status === 403) {
                        throw new Error(`Acervo Hentai bloqueado por Cloudflare (HTTP 403): Just a moment... / Datacenter WAF`);
                    }
                    throw new Error(`Acervo Hentai request failed: HTTP ${res.status}`);
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
    async fetchUpdatedWorks(cursor, _options) {
        const page = cursor ? parseInt(cursor, 10) : 1;
        const url = page === 1 ? `${this.baseUrl}/manhwa/` : `${this.baseUrl}/manhwa/page/${page}/`;
        const html = await this.fetchHtml(url);
        const works = [];
        const itemRegex = /<div class="[^"]*page-item-detail[^"]*"[\s\S]*?<\/div>\s*<\/div>/gi;
        const matches = html.match(itemRegex) || [];
        for (const block of matches) {
            const linkMatch = block.match(/href="([^"]*\/manhwa\/([^"/]+)\/?)"/i);
            if (!linkMatch)
                continue;
            const fullUrl = linkMatch[1];
            const slug = linkMatch[2];
            if (slug === 'feed' || slug === 'page')
                continue;
            const titleMatch = block.match(/<h[3-5][^>]*>\s*<a[^>]*>([\s\S]*?)<\/a>/i);
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
        const slug = sourceWorkId.replace(/^\//, '').replace(/\/$/, '').replace(/^manhwa\//, '');
        const url = `${this.baseUrl}/manhwa/${slug}/`;
        const html = await this.fetchHtml(url);
        const titleMatch = html.match(/<div class="post-title"[^>]*>[\s\S]*?<h1[^>]*>([\s\S]*?)<\/h1>/i) ||
            html.match(/<h1[^>]*>([\s\S]*?)<\/h1>/i);
        const title = titleMatch ? decodeHtmlEntities(stripHtml(titleMatch[1])) : slug.replace(/[-_]+/g, ' ');
        const coverMatch = html.match(/<div class="summary_image"[^>]*>[\s\S]*?<img[^>]+(?:data-src|src)="([^"]+)"/i);
        const coverUrl = coverMatch ? coverMatch[1].trim() : null;
        const descMatch = html.match(/<div class="description-summary"[\s\S]*?<p>([\s\S]*?)<\/p>/i) ||
            html.match(/<div class="summary__content[^"]*"[\s\S]*?<p>([\s\S]*?)<\/p>/i);
        const synopsis = descMatch ? decodeHtmlEntities(stripHtml(descMatch[1])) : '';
        const authorMatch = html.match(/class="author-content"[\s\S]*?<a[^>]*>([\s\S]*?)<\/a>/i);
        const author = authorMatch ? decodeHtmlEntities(stripHtml(authorMatch[1])) : undefined;
        const artistMatch = html.match(/class="artist-content"[\s\S]*?<a[^>]*>([\s\S]*?)<\/a>/i);
        const artist = artistMatch ? decodeHtmlEntities(stripHtml(artistMatch[1])) : undefined;
        const genres = [];
        const genreMatches = [...html.matchAll(/class="genres-content"[\s\S]*?<a[^>]*>([\s\S]*?)<\/a>/gi)];
        for (const gm of genreMatches) {
            const g = decodeHtmlEntities(stripHtml(gm[1]));
            if (g && !genres.includes(g))
                genres.push(g);
        }
        let status = 'ONGOING';
        if (/status[\s\S]*?complet/i.test(html) || /conclu[ií]d/i.test(html)) {
            status = 'COMPLETED';
        }
        else if (/hiat/i.test(html)) {
            status = 'HIATUS';
        }
        else if (/cancel/i.test(html)) {
            status = 'CANCELLED';
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
        const slug = sourceWorkId.replace(/^\//, '').replace(/\/$/, '').replace(/^manhwa\//, '');
        const url = `${this.baseUrl}/manhwa/${slug}/`;
        let html = await this.fetchHtml(url);
        let chapterBlocks = [...html.matchAll(/<li class="[^"]*wp-manga-chapter[^"]*"[\s\S]*?<\/li>/gi)].map((m) => m[0]);
        if (chapterBlocks.length === 0) {
            // Try ajax endpoint
            try {
                const ajaxHtml = await this.fetchHtml(`${this.baseUrl}/manhwa/${slug}/ajax/chapters/`, {
                    method: 'POST',
                });
                chapterBlocks = [...ajaxHtml.matchAll(/<li class="[^"]*wp-manga-chapter[^"]*"[\s\S]*?<\/li>/gi)].map((m) => m[0]);
            }
            catch {
                // Fallback
            }
        }
        const chapters = [];
        const seenNumbers = new Set();
        for (const block of chapterBlocks) {
            const linkMatch = block.match(/href="([^"]*\/manhwa\/[^"/]+\/([^"/]+)\/?)"/i);
            if (!linkMatch)
                continue;
            const chSlug = linkMatch[2];
            const chNum = extractChapterNumber(chSlug);
            if (seenNumbers.has(chNum))
                continue;
            seenNumbers.add(chNum);
            const titleMatch = block.match(/<a[^>]*>([\s\S]*?)<\/a>/i);
            const title = titleMatch ? decodeHtmlEntities(stripHtml(titleMatch[1])) : `Capítulo ${chNum}`;
            chapters.push({
                sourceChapterId: `${slug}/${chSlug}`,
                number: chNum,
                title,
            });
        }
        chapters.sort((a, b) => a.number - b.number);
        return chapters;
    }
    async fetchChapterPages(sourceChapterId, _chapterNumber) {
        const chPath = sourceChapterId.replace(/^\//, '').replace(/\/$/, '');
        const url = `${this.baseUrl}/manhwa/${chPath}/?style=list`;
        const html = await this.fetchHtml(url);
        const urls = [];
        const imgMatches = [
            ...html.matchAll(/class="[^"]*wp-manga-chapter-img[^"]*"[^>]+(?:data-src|src)="([^"]+)"/gi),
            ...html.matchAll(/(?:data-src|src)="([^"]+)"[^>]+class="[^"]*wp-manga-chapter-img[^"]*"/gi),
        ];
        for (const m of imgMatches) {
            const u = m[1].trim();
            if (u && !urls.includes(u) && !u.endsWith('logo-PNG.png')) {
                urls.push(u);
            }
        }
        if (urls.length === 0) {
            // Fallback to data-src inside reading-content
            const fallbackMatches = [...html.matchAll(/<img[^>]+(?:data-src|src)="([^"]+)"/gi)];
            for (const m of fallbackMatches) {
                const u = m[1].trim();
                if (u.includes('/WP-manga/data/') && !urls.includes(u)) {
                    urls.push(u);
                }
            }
        }
        return urls;
    }
    async searchWorks(query) {
        try {
            const url = `${this.baseUrl}/?s=${encodeURIComponent(query)}&post_type=wp-manga`;
            const html = await this.fetchHtml(url);
            const works = [];
            const itemRegex = /<div class="[^"]*c-tabs-item__content[^"]*"[\s\S]*?<\/div>\s*<\/div>\s*<\/div>/gi;
            const matches = html.match(itemRegex) || html.match(/<div class="[^"]*page-item-detail[^"]*"[\s\S]*?<\/div>\s*<\/div>/gi) || [];
            for (const block of matches) {
                const linkMatch = block.match(/href="([^"]*\/manhwa\/([^"/]+)\/?)"/i);
                if (!linkMatch)
                    continue;
                const slug = linkMatch[2];
                if (slug === 'feed' || slug === 'page')
                    continue;
                const titleMatch = block.match(/<h[3-5][^>]*>\s*<a[^>]*>([\s\S]*?)<\/a>/i);
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
        catch (err) {
            this.logger.debug(`Acervo Hentai search not available for "${query}": ${err?.message}`);
            return [];
        }
    }
}
