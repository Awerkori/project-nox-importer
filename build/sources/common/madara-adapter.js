import { HostRateLimiter } from '../../core/rate-limiter.js';
import { Logger } from '../../core/logger.js';
import { slugify, decodeHtmlEntities, stripHtml, extractChapterNumber } from './html-utils.js';
export class MadaraAdapter {
    rateLimiter;
    transport;
    id;
    name;
    baseUrl;
    mangaSubString;
    logger;
    constructor(options, rateLimiter = new HostRateLimiter(2.0), transport = fetch) {
        this.rateLimiter = rateLimiter;
        this.transport = transport;
        this.id = options.id;
        this.name = options.name;
        this.baseUrl = options.baseUrl.replace(/\/$/, '');
        this.mangaSubString = options.mangaSubString || 'manga';
        this.logger = new Logger(`MadaraAdapter:${this.id}`);
        const host = new URL(this.baseUrl).host;
        const rps = options.rateLimitRps || 3.0;
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
                    signal: AbortSignal.timeout(20_000),
                });
                if (res.status === 429) {
                    const retryAfter = res.headers.get('Retry-After');
                    this.rateLimiter.handle429(host, retryAfter, attempts);
                    if (attempts >= maxAttempts)
                        throw new Error(`Rate limited (429) for ${host}`);
                    await new Promise((r) => setTimeout(r, 1000 * attempts));
                    continue;
                }
                if (!res.ok) {
                    throw new Error(`HTTP ${res.status} from ${url}`);
                }
                this.rateLimiter.recordSuccess(host);
                return await res.text();
            }
            catch (err) {
                if (attempts >= maxAttempts)
                    throw err;
                await new Promise((r) => setTimeout(r, 1000 * attempts));
            }
        }
        throw new Error(`Failed to fetch ${url} after ${maxAttempts} attempts`);
    }
    async fetchUpdatedWorks(cursor, options) {
        const page = cursor ? Math.max(1, parseInt(cursor, 10)) : 1;
        let html = '';
        try {
            // 1. Primary: Madara AJAX load more
            html = await this.fetchHtml(`${this.baseUrl}/wp-admin/admin-ajax.php`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
                    'X-Requested-With': 'XMLHttpRequest',
                    Referer: `${this.baseUrl}/${this.mangaSubString}/`,
                },
                body: `action=madara_load_more&page=${page - 1}&template=madara-core%2Fcontent%2Fcontent-archive&vars%5Bpaged%5D=1&vars%5Btemplate%5D=archive&vars%5Bposts_per_page%5D=24&vars%5Bpost_type%5D=wp-manga&vars%5Bpost_status%5D=publish`,
            });
        }
        catch { }
        if (!html || html.trim() === '0' || html.trim().length < 50) {
            // 2. Fallback: standard pagination
            const pageUrl = page === 1 ? `${this.baseUrl}/${this.mangaSubString}/` : `${this.baseUrl}/${this.mangaSubString}/page/${page}/`;
            html = await this.fetchHtml(pageUrl);
        }
        const works = [];
        const seenSlugs = new Set();
        const regex = new RegExp(`href="([^"]*(?:\\/${this.mangaSubString}\\/|\\/obra\\/|\\/projeto\\/)([^"\\/?#]+)\\/?)"[^>]*>([^<]+)?`, 'gi');
        let match;
        while ((match = regex.exec(html)) !== null) {
            const fullLink = match[1];
            const rawSlug = match[2].trim();
            if (!rawSlug || rawSlug === this.mangaSubString || rawSlug === 'feed' || rawSlug === 'page' || rawSlug === 'order' || seenSlugs.has(rawSlug))
                continue;
            seenSlugs.add(rawSlug);
            const chunkStart = Math.max(0, match.index - 500);
            const chunkEnd = Math.min(html.length, match.index + 500);
            const chunk = html.slice(chunkStart, chunkEnd);
            const titleMatch = chunk.match(/class="[^"]*post-title[^"]*"[^>]*>[\s\S]*?<a[^>]*>([^<]+)<\/a>/i) ||
                chunk.match(/<h[345][^>]*>[\s\S]*?<a[^>]*>([^<]+)<\/a>/i) ||
                chunk.match(/alt="([^"]+)"/i);
            const title = decodeHtmlEntities(titleMatch ? titleMatch[1].trim() : rawSlug);
            const imgMatch = chunk.match(/<img[^>]+(?:data-src|data-full-url|src)="([^"]+)"/i);
            let coverUrl = imgMatch ? imgMatch[1].trim() : null;
            if (coverUrl && coverUrl.startsWith('//'))
                coverUrl = `https:${coverUrl}`;
            works.push({
                sourceWorkId: rawSlug,
                slug: slugify(rawSlug),
                title,
                coverUrl,
                updatedAt: new Date().toISOString(),
            });
        }
        const hasNextPage = works.length >= 10;
        return {
            works,
            nextCursor: hasNextPage ? String(page + 1) : null,
        };
    }
    async fetchWorkDetails(sourceWorkId) {
        const workUrl = `${this.baseUrl}/${this.mangaSubString}/${sourceWorkId}/`;
        const html = await this.fetchHtml(workUrl);
        // Title
        const titleMatch = html.match(/<h1[^>]*>([\s\S]*?)<\/h1>/i) ||
            html.match(/<meta\s+property="og:title"\s+content="([^"]+)"/i);
        let title = sourceWorkId;
        if (titleMatch) {
            title = stripHtml(titleMatch[1]).replace(/^Manga\s*-\s*/i, '').trim();
        }
        // Cover
        const coverMatch = html.match(/class="[^"]*summary_image[^"]*"[\s\S]*?<img[^>]+(?:data-src|data-full-url|src)="([^"]+)"/i) ||
            html.match(/<meta\s+property="og:image"\s+content="([^"]+)"/i);
        let coverUrl = coverMatch ? coverMatch[1].trim() : null;
        if (coverUrl && coverUrl.startsWith('//'))
            coverUrl = `https:${coverUrl}`;
        // Synopsis
        const synopsisMatch = html.match(/class="[^"]*(?:summary__content|description-summary|manga-excerpt)[^"]*"[\s\S]*?<p>([\s\S]*?)<\/p>/i) ||
            html.match(/<meta\s+property="og:description"\s+content="([^"]+)"/i);
        const synopsis = synopsisMatch ? stripHtml(synopsisMatch[1]) : undefined;
        // Author
        const authorMatch = html.match(/class="[^"]*author-content[^"]*"[\s\S]*?<a[^>]*>([^<]+)<\/a>/i);
        const author = authorMatch ? stripHtml(authorMatch[1]) : undefined;
        // Artist
        const artistMatch = html.match(/class="[^"]*artist-content[^"]*"[\s\S]*?<a[^>]*>([^<]+)<\/a>/i);
        const artist = artistMatch ? stripHtml(artistMatch[1]) : undefined;
        // Status
        let status = 'ONGOING';
        if (/completo|completed|finalizado/i.test(html)) {
            status = 'COMPLETED';
        }
        else if (/hiato|hiatus/i.test(html)) {
            status = 'HIATUS';
        }
        else if (/cancelado|cancelled/i.test(html)) {
            status = 'CANCELLED';
        }
        // Genres
        const genreMatches = Array.from(html.matchAll(/href="[^"]*(?:\/manga-genre\/|\/genero\/)[^"]*"[^>]*>([^<]+)<\/a>/gi));
        const genres = genreMatches.map((m) => stripHtml(m[1])).filter(Boolean);
        return {
            sourceWorkId,
            slug: slugify(sourceWorkId),
            title: decodeHtmlEntities(title),
            coverUrl,
            synopsis,
            author,
            artist,
            status,
            genres: genres.length > 0 ? Array.from(new Set(genres)) : undefined,
        };
    }
    async fetchChapters(sourceWorkId) {
        const workUrl = `${this.baseUrl}/${this.mangaSubString}/${sourceWorkId}/`;
        let html = await this.fetchHtml(workUrl);
        // 1. First extract chapters from standard wp-manga-chapter list items
        const wpMangaChapterRegex = /<li[^>]*class="[^"]*wp-manga-chapter[^"]*"[\s\S]*?<a[^>]+href="([^"]+)"/gi;
        let chapterMatches = Array.from(html.matchAll(wpMangaChapterRegex)).map((m) => m[1]);
        // 2. Fallback to general chapter path matching
        if (chapterMatches.length === 0) {
            chapterMatches = Array.from(html.matchAll(/href="([^"]*(?:\/capitulo|\/cap-|\/ch-|\/chapter)[^"]*)"/gi)).map((m) => m[1]);
        }
        // 3. Modern Madara AJAX endpoint: /ajax/chapters/
        if (chapterMatches.length === 0) {
            try {
                const ajaxHtml = await this.fetchHtml(`${workUrl}ajax/chapters/`, {
                    method: 'POST',
                    headers: {
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
                        Referer: workUrl,
                    },
                });
                chapterMatches = Array.from(ajaxHtml.matchAll(wpMangaChapterRegex)).map((m) => m[1]);
                if (chapterMatches.length === 0) {
                    chapterMatches = Array.from(ajaxHtml.matchAll(/href="([^"]*(?:\/capitulo|\/cap-|\/ch-|\/chapter)[^"]*)"/gi)).map((m) => m[1]);
                }
            }
            catch { }
        }
        // 4. Legacy Madara AJAX endpoint: /wp-admin/admin-ajax.php
        if (chapterMatches.length === 0) {
            const postIdMatch = html.match(/id="manga-chapters-holder"\s+data-id="(\d+)"/i) ||
                html.match(/class="[^"]*wp-manga-action-button[^"]*"[^>]*data-post="(\d+)"/i) ||
                html.match(/data-id="(\d+)"/i);
            if (postIdMatch) {
                try {
                    const ajaxHtml = await this.fetchHtml(`${this.baseUrl}/wp-admin/admin-ajax.php`, {
                        method: 'POST',
                        headers: {
                            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
                            'X-Requested-With': 'XMLHttpRequest',
                            Referer: workUrl,
                        },
                        body: `action=manga_get_chapters&manga=${postIdMatch[1]}`,
                    });
                    chapterMatches = Array.from(ajaxHtml.matchAll(wpMangaChapterRegex)).map((m) => m[1]);
                    if (chapterMatches.length === 0) {
                        chapterMatches = Array.from(ajaxHtml.matchAll(/href="([^"]*(?:\/capitulo|\/cap-|\/ch-|\/chapter)[^"]*)"/gi)).map((m) => m[1]);
                    }
                }
                catch { }
            }
        }
        const seenUrls = new Set();
        const chapters = [];
        for (let chapUrl of chapterMatches) {
            chapUrl = chapUrl.trim();
            if (!chapUrl || seenUrls.has(chapUrl))
                continue;
            seenUrls.add(chapUrl);
            const num = extractChapterNumber(chapUrl);
            const relativeOrFull = chapUrl.startsWith('http') ? chapUrl : `${this.baseUrl}${chapUrl.startsWith('/') ? '' : '/'}${chapUrl}`;
            chapters.push({
                sourceChapterId: relativeOrFull,
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
        // Extract all img tags
        const imgTags = html.match(/<img[^>]+>/gi) || [];
        const seen = new Set();
        const pages = [];
        for (const tag of imgTags) {
            if (!/wp-manga-chapter-img|page-break|reading-content|read-container/i.test(tag) &&
                !/wp-content\/uploads\/(?:WP-manga|\d{4}\/\d{2})/i.test(tag)) {
                continue;
            }
            // Priority: data-src -> data-full-url -> src
            const dataSrc = (tag.match(/data-src=["']([^"']+)["']/i) || [])[1];
            const dataFull = (tag.match(/data-full-url=["']([^"']+)["']/i) || [])[1];
            const rawSrc = (tag.match(/src=["']([^"']+)["']/i) || [])[1];
            let candidate = (dataSrc || dataFull || rawSrc || '').trim();
            if (!candidate)
                continue;
            if (/dflazy|placeholder|loading/i.test(candidate)) {
                if (dataSrc && !/dflazy|placeholder/i.test(dataSrc))
                    candidate = dataSrc.trim();
                else if (dataFull && !/dflazy|placeholder/i.test(dataFull))
                    candidate = dataFull.trim();
                else
                    continue;
            }
            const filename = candidate.split('/').pop()?.split('?')[0] || '';
            if (/(?:^|[_\-.])(logo|avatar|icon|banner|ads|advert|discord|telegram)(?:[_\-.]|$)/i.test(filename))
                continue;
            if (candidate.startsWith('//'))
                candidate = `https:${candidate}`;
            else if (candidate.startsWith('/'))
                candidate = `${this.baseUrl}${candidate}`;
            if (seen.has(candidate))
                continue;
            seen.add(candidate);
            pages.push(candidate);
        }
        return pages;
    }
    async searchWorks(query) {
        const url = `${this.baseUrl}/?s=${encodeURIComponent(query)}&post_type=wp-manga`;
        const html = await this.fetchHtml(url);
        const works = [];
        const seenSlugs = new Set();
        const regex = new RegExp(`href="([^"]*(?:\\/${this.mangaSubString}\\/|\\/obra\\/|\\/projeto\\/)([^"\\/?#]+)\\/?)"[^>]*>([^<]+)?`, 'gi');
        let match;
        while ((match = regex.exec(html)) !== null) {
            const rawSlug = match[2].trim();
            if (!rawSlug || rawSlug === this.mangaSubString || seenSlugs.has(rawSlug))
                continue;
            seenSlugs.add(rawSlug);
            works.push({
                sourceWorkId: rawSlug,
                slug: slugify(rawSlug),
                title: decodeHtmlEntities(match[3]?.trim() || rawSlug),
            });
        }
        return works;
    }
    getImageHeaders(url) {
        return {
            Referer: `${this.baseUrl}/`,
            'User-Agent': this.headers['User-Agent'],
            Accept: 'image/avif,image/webp,image/apng,image/*,*/*;q=0.8',
        };
    }
}
