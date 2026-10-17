import { HostRateLimiter } from '../../core/rate-limiter.js';
import { Logger } from '../../core/logger.js';
import { decodeHtmlEntities, extractChapterNumber, slugify, stripHtml } from '../common/html-utils.js';
function escapeRegex(value) {
    return value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}
/**
 * Manga Online moved from its old Madara installation to a bespoke WordPress
 * theme. Keep this separate from Madara: chapter URLs now live at
 * \`/capitulo/<work>-capitulo-<n>/\`, which is deliberately not compatible with
 * Madara's \`/manga/<work>/capitulo-<n>/\` ownership invariant.
 */
export class MangaOnlineAdapter {
    rateLimiter;
    transport;
    id = 'mangaonline';
    name = 'Manga Online';
    baseUrl = 'https://mangaonline.love';
    probeUrl = 'https://mangaonline.love/?atualizacoes=1';
    get admissionProbe() {
        return {
            url: this.probeUrl,
            headers: this.headers,
            expectedType: 'html',
            allowInlineJsDetection: true,
            usableHtmlMarker: /\bhome-manga-card\b/i,
        };
    }
    logger = new Logger('MangaOnlineAdapter');
    constructor(rateLimiter = new HostRateLimiter(2.0), transport = fetch) {
        this.rateLimiter = rateLimiter;
        this.transport = transport;
        this.rateLimiter.setHostRate('mangaonline.love', 2.0, 4, 4);
    }
    get headers() {
        return {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
            Accept: 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
            'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
            Referer: `${this.baseUrl}/`,
        };
    }
    absolute(raw, context = `${this.baseUrl}/`) {
        return new URL(decodeHtmlEntities(raw.trim()), context).href;
    }
    async fetchHtml(url) {
        const host = new URL(url).host;
        await this.rateLimiter.acquire(host);
        let lastError;
        for (let attempt = 1; attempt <= 3; attempt += 1) {
            try {
                const response = await this.transport(url, { headers: this.headers, signal: AbortSignal.timeout(20_000) });
                if (response.status === 429 || response.status >= 500) {
                    if (response.status === 429)
                        this.rateLimiter.handle429(host, response.headers.get('Retry-After'), attempt);
                    throw new Error(`HTTP ${response.status} from ${url}`);
                }
                if (!response.ok)
                    throw new Error(`HTTP ${response.status} from ${url}`);
                this.rateLimiter.recordSuccess(host);
                return response.text();
            }
            catch (error) {
                lastError = error;
                if (attempt < 3)
                    await new Promise((resolve) => setTimeout(resolve, 500 * attempt));
            }
        }
        throw lastError instanceof Error ? lastError : new Error(`Failed to fetch ${url}`);
    }
    parseCards(html) {
        const works = [];
        const seen = new Set();
        const cards = html.split(/<article\b/gi).slice(1);
        for (const card of cards) {
            const fragment = `<article${card.split(/<\/article>/i)[0]}`;
            if (!/home-manga-card/i.test(fragment))
                continue;
            const link = fragment.match(/<a[^>]+(?:class=["'][^"']*manga-card-link[^"']*["'][^>]*)?href=["']([^"']+)["']/i)
                || fragment.match(/<a[^>]+href=["']([^"']+)["'][^>]*>[\s\S]{0,800}?<h3/i);
            const titleMatch = fragment.match(/<h3[^>]*(?:class=["'][^"']*manga-card-title[^"']*["'])?[^>]*>([\s\S]*?)<\/h3>/i)
                || fragment.match(/<img[^>]+alt=["']([^"']+)["']/i);
            if (!link || !titleMatch)
                continue;
            const workUrl = this.absolute(link[1]);
            const slug = new URL(workUrl).pathname.replace(/\/+$/, '').split('/').pop()?.trim() || '';
            if (!slug || seen.has(slug))
                continue;
            seen.add(slug);
            const image = fragment.match(/<img[^>]+(?:data-src|src)=["']([^"']+)["']/i);
            works.push({
                sourceWorkId: slug,
                slug: slugify(slug),
                title: decodeHtmlEntities(stripHtml(titleMatch[1])).trim() || slug,
                coverUrl: image && !image[1].startsWith('data:') ? this.absolute(image[1], workUrl) : null,
            });
        }
        return works;
    }
    async fetchUpdatedWorks(cursor, options) {
        const page = Math.max(1, Number.parseInt(cursor || '1', 10) || 1);
        const url = page === 1
            ? `${this.baseUrl}/?atualizacoes=1`
            : `${this.baseUrl}/page/${page}/?atualizacoes=1`;
        const works = this.parseCards(await this.fetchHtml(url));
        const hasNext = works.length > 0 && options?.mode !== 'maintenance';
        return { works, nextCursor: hasNext ? String(page + 1) : null };
    }
    workUrl(sourceWorkId) {
        return `${this.baseUrl}/manga/${encodeURIComponent(sourceWorkId.replace(/^\/+|\/+$/g, ''))}/`;
    }
    parseDetails(sourceWorkId, html) {
        const titleMatch = html.match(/<h1[^>]*class=["'][^"']*manga-title[^"']*["'][^>]*>([\s\S]*?)<\/h1>/i)
            || html.match(/<h1[^>]*>([\s\S]*?)<\/h1>/i);
        const coverMatch = html.match(/class=["'][^"']*manga-cover[^"']*["'][\s\S]*?<img[^>]+(?:data-src|src)=["']([^"']+)["']/i);
        const synopsisMatch = html.match(/class=["'][^"']*synopsis-content[^"']*["'][^>]*>([\s\S]*?)<\/(?:div|p)>/i);
        const tagMatches = Array.from(html.matchAll(/class=["'][^"']*manga-tags[^"']*["'][\s\S]*?<\/div>/gi))
            .flatMap((match) => Array.from(match[0].matchAll(/<a[^>]*>([\s\S]*?)<\/a>/gi)))
            .map((match) => decodeHtmlEntities(stripHtml(match[1])).trim())
            .filter(Boolean);
        const fields = Array.from(html.matchAll(/class=["'][^"']*manga-meta-item[^"']*["'][\s\S]*?<\/div>/gi));
        let author;
        let artist;
        let status = 'UNKNOWN';
        for (const field of fields) {
            const label = decodeHtmlEntities(stripHtml(field[0].match(/class=["'][^"']*meta-label[^"']*["'][^>]*>([\s\S]*?)<\//i)?.[1] || '')).toLocaleLowerCase();
            const value = decodeHtmlEntities(stripHtml(field[0].match(/class=["'][^"']*meta-value[^"']*["'][^>]*>([\s\S]*?)<\//i)?.[1] || '')).trim();
            if (/autor/.test(label))
                author = value || undefined;
            if (/artista/.test(label))
                artist = value || undefined;
            if (/status/.test(label)) {
                const normalized = value.toLocaleLowerCase();
                if (/completo/.test(normalized))
                    status = 'COMPLETED';
                else if (/hiato|pausado/.test(normalized))
                    status = 'HIATUS';
                else if (/cancelado/.test(normalized))
                    status = 'CANCELLED';
                else if (/lançamento|lancamento|andamento/.test(normalized))
                    status = 'ONGOING';
            }
        }
        return {
            sourceWorkId,
            slug: slugify(sourceWorkId),
            title: decodeHtmlEntities(stripHtml(titleMatch?.[1] || sourceWorkId)).trim(),
            coverUrl: coverMatch ? this.absolute(coverMatch[1], this.workUrl(sourceWorkId)) : null,
            synopsis: synopsisMatch ? decodeHtmlEntities(stripHtml(synopsisMatch[1])).trim() : undefined,
            author,
            artist,
            status,
            genres: Array.from(new Set(tagMatches)),
            language: 'pt-BR',
        };
    }
    parseChapters(sourceWorkId, html) {
        const chapters = [];
        const seen = new Set();
        const blocks = html.split(/<li\b/gi).slice(1)
            .map((fragment) => `<li${fragment.split(/<\/li>/i)[0]}`)
            .filter((fragment) => /chapter-item/i.test(fragment));
        for (const block of blocks) {
            const link = block.match(/(?:class=["'][^"']*chapter-link[^"']*["'][^>]*href|href=["']([^"']+)["'][^>]*class=["'][^"']*chapter-link[^"']*["'])=["']([^"']+)["']/i)
                || block.match(/<a[^>]+href=["']([^"']+)["']/i);
            const label = block.match(/class=["'][^"']*chapter-number[^"']*["'][^>]*>([\s\S]*?)<\//i);
            if (!link)
                continue;
            const chapterUrl = this.absolute(link[2] || link[1], this.workUrl(sourceWorkId));
            if (seen.has(chapterUrl) || !this.isChapterOwnedByWork(sourceWorkId, chapterUrl))
                continue;
            seen.add(chapterUrl);
            const displayNumber = decodeHtmlEntities(stripHtml(label?.[1] || '')).trim();
            chapters.push({
                sourceChapterId: chapterUrl,
                displayNumber: displayNumber || undefined,
                number: extractChapterNumber(displayNumber || chapterUrl),
                title: displayNumber || undefined,
            });
        }
        return chapters.sort((left, right) => left.number - right.number);
    }
    async fetchWorkDetails(sourceWorkId) {
        return this.parseDetails(sourceWorkId, await this.fetchHtml(this.workUrl(sourceWorkId)));
    }
    async fetchChapters(sourceWorkId) {
        return this.parseChapters(sourceWorkId, await this.fetchHtml(this.workUrl(sourceWorkId)));
    }
    isChapterOwnedByWork(sourceWorkId, sourceChapterId) {
        try {
            const chapter = new URL(sourceChapterId, this.baseUrl);
            if (chapter.origin !== new URL(this.baseUrl).origin)
                return false;
            const slug = decodeURIComponent(sourceWorkId).trim().toLocaleLowerCase();
            const path = decodeURIComponent(chapter.pathname).toLocaleLowerCase();
            return Boolean(slug) && new RegExp(`^/capitulo/${escapeRegex(slug)}-capitulo-(?:[^/]+)/?$`).test(path);
        }
        catch {
            return false;
        }
    }
    async fetchChapterPages(sourceChapterId) {
        const chapterUrl = this.absolute(sourceChapterId);
        const html = await this.fetchHtml(chapterUrl);
        const pageBlocks = html.split(/<div\b/gi).slice(1)
            .map((fragment) => `<div${fragment.split(/<\/div>/i)[0]}`)
            .filter((fragment) => /chapter-image-container/i.test(fragment));
        const pages = pageBlocks
            .map((block) => block.match(/<img[^>]+(?:data-src|src)=["']([^"']+)["']/i)?.[1])
            .filter((url) => typeof url === 'string' && !url.startsWith('data:') && !/logo|banner|avatar|icon/i.test(url))
            .map((url) => this.absolute(url, chapterUrl));
        const unique = Array.from(new Set(pages));
        if (unique.length === 0)
            this.logger.warn('Reader contained no chapter image', { sourceChapterId });
        return unique;
    }
    async searchWorks(query) {
        const clean = query.trim();
        if (!clean)
            return [];
        return this.parseCards(await this.fetchHtml(`${this.baseUrl}/?s=${encodeURIComponent(clean)}`));
    }
    getImageHeaders() {
        return this.headers;
    }
}
