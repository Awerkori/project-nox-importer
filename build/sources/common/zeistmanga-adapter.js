import { HostRateLimiter } from '../../core/rate-limiter.js';
import { Logger } from '../../core/logger.js';
import { cleanAlternativeTitle } from '../../core/alternative-titles.js';
import { slugify, decodeHtmlEntities, stripHtml, extractChapterNumber } from './html-utils.js';
function htmlAttribute(tag, name) {
    const match = tag.match(new RegExp(`\\b${name}\\s*=\\s*(["'])([\\s\\S]*?)\\1`, 'i'));
    return match?.[2];
}
function metaContent(html, names) {
    const wanted = new Set(names.map((name) => name.toLowerCase()));
    for (const match of html.matchAll(/<meta\b[^>]*>/gi)) {
        const tag = match[0];
        const key = htmlAttribute(tag, 'property') || htmlAttribute(tag, 'name');
        const content = htmlAttribute(tag, 'content');
        if (key && content && wanted.has(key.toLowerCase()))
            return decodeHtmlEntities(content).trim();
    }
    return undefined;
}
export class ZeistMangaAdapter {
    rateLimiter;
    transport;
    id;
    name;
    baseUrl;
    seriesCategory;
    logger;
    constructor(options, rateLimiter = new HostRateLimiter(2.0), transport = fetch) {
        this.rateLimiter = rateLimiter;
        this.transport = transport;
        this.id = options.id;
        this.name = options.name;
        this.baseUrl = options.baseUrl.replace(/\/$/, '');
        this.seriesCategory = options.seriesCategory || 'Series';
        this.logger = new Logger(`ZeistMangaAdapter:${this.id}`);
        const host = new URL(this.baseUrl).host;
        const rps = options.rateLimitRps || 2.0;
        this.rateLimiter.setHostRate(host, rps, Math.ceil(rps * 2), rps * 2);
    }
    get headers() {
        return {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
            Accept: 'application/json,text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
            Referer: `${this.baseUrl}/`,
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
                const res = await this.transport(url, {
                    headers: this.headers,
                    signal: AbortSignal.timeout(10000),
                });
                if (res.status === 429 || res.status >= 500) {
                    if (attempts < maxAttempts) {
                        await new Promise((resolve) => setTimeout(resolve, 1000 * attempts));
                        continue;
                    }
                }
                if (!res.ok) {
                    const err = new Error(`HTTP ${res.status} from ${url}`);
                    err.status = res.status;
                    throw err;
                }
                return (await res.json());
            }
            catch (err) {
                if (err?.status && err.status >= 400 && err.status < 500 && err.status !== 429) {
                    throw err;
                }
                if (attempts >= maxAttempts)
                    throw err;
                await new Promise((resolve) => setTimeout(resolve, 1000 * attempts));
            }
        }
        throw new Error(`Failed to fetch JSON from ${url}`);
    }
    async fetchHtml(url) {
        const host = new URL(url).host;
        await this.rateLimiter.acquire(host);
        const res = await this.transport(url, {
            headers: this.headers,
            signal: AbortSignal.timeout(10000),
        });
        if (!res.ok) {
            throw new Error(`HTTP ${res.status} from ${url}`);
        }
        return await res.text();
    }
    extractSeriesLabel(categories, title) {
        const generic = new Set([
            'a-z', 'ação', 'aventura', 'comédia', 'drama', 'ecchi', 'fantasia', 'harém', 'hot', 'isekai',
            'lançando', 'magia', 'manga', 'mangá', 'manhwa', 'webtoon', 'comic', 'nsfw', 'recentes',
            'romance', 'series', 'shounen', 'shoujo', 'seinen', 'slice of life', 'vida escolar',
            'update', 'project', 'ongoing', 'completo', 'dropado', 'destaques', 'pt', 'new', 'novel',
            'medicinal', 'cultivo', 'martial arts', 'harem', 'action', 'fantasy', 'school life',
            'supernatural', 'dropped', 'em lançamento', 'em andamento', 'finalizado', 'hiato',
            '+18', '+16', '+14', '+12', '18+', '16+', '14+', '12+', 'adulto', 'hentai', 'mature', 'smut',
            'doujinshi', 'coreano', 'japones', 'chines', 'mangas', 'manhwas', 'webtoons'
        ]);
        // Priority 1: Match title directly if present in categories
        const lowerTitle = title.trim().toLowerCase();
        for (const c of categories) {
            const lower = c.trim().toLowerCase();
            if (lower === lowerTitle) {
                return c.trim();
            }
        }
        // Priority 2: Return first non-generic, non-numeric, non-age-rating category
        for (const c of categories) {
            const lower = c.trim().toLowerCase();
            if (!generic.has(lower) &&
                !/^[+~#]|\d+(\.\d+)?$/.test(lower) &&
                lower.length > 1) {
                return c.trim();
            }
        }
        return title;
    }
    async fetchUpdatedWorks(cursor, options) {
        const page = cursor ? parseInt(cursor, 10) : 1;
        const maxResults = 25;
        const startIndex = (page - 1) * maxResults + 1;
        const feedUrl = `${this.baseUrl}/feeds/posts/default/-/${encodeURIComponent(this.seriesCategory)}?alt=json&start-index=${startIndex}&max-results=${maxResults}`;
        try {
            const data = await this.fetchJson(feedUrl);
            const entries = data?.feed?.entry || [];
            const works = [];
            for (const entry of entries) {
                const title = decodeHtmlEntities(entry.title?.['$t'] || '').trim();
                if (!title)
                    continue;
                const altLink = (entry.link || []).find((l) => l.rel === 'alternate')?.href || '';
                const coverUrl = entry.media$thumbnail?.url?.replace(/\/s72-c\//, '/s600/') ||
                    this.extractCoverFromContent(entry.content?.['$t'] || entry.summary?.['$t'] || '');
                const categories = (entry.category || []).map((c) => c.term);
                const specificLabel = this.extractSeriesLabel(categories, title);
                const workSlug = altLink
                    ? altLink.replace(this.baseUrl, '').replace(/\.html$/, '').replace(/^\/+/, '').replace(/\//g, '-')
                    : slugify(title);
                works.push({
                    sourceWorkId: `${altLink}|${specificLabel}`,
                    title,
                    slug: workSlug,
                    coverUrl: coverUrl || null,
                    updatedAt: entry.updated?.['$t'] || entry.published?.['$t'] || new Date().toISOString(),
                });
            }
            const totalResults = parseInt(data?.feed?.openSearch$totalResults?.['$t'] || '0', 10);
            const hasNext = startIndex + entries.length <= totalResults && entries.length === maxResults;
            return {
                works,
                nextCursor: hasNext ? String(page + 1) : null,
            };
        }
        catch (err) {
            this.logger.error(`fetchUpdatedWorks failed: ${err.message}`);
            return { works: [], nextCursor: null };
        }
    }
    async fetchWorkDetails(sourceWorkId) {
        const [workUrl] = sourceWorkId.split('|');
        const targetUrl = workUrl.startsWith('http') ? workUrl : `${this.baseUrl}/${workUrl}.html`;
        try {
            const html = await this.fetchHtml(targetUrl);
            // Blogger templates frequently put the site name in their first <h1>.
            // Prefer explicit document metadata/data attributes, which are stable
            // across theme layout changes and represent the actual series title.
            const dataTitleMatch = html.match(/data-title=["']([^"']+)["']/i);
            const titleMatch = dataTitleMatch ||
                html.match(/itemprop=["']name["'][^>]*>([\s\S]*?)<\/h1>/i) ||
                html.match(/<h1[^>]*class=["'][^"']*entry-title[^"']*["'][^>]*>([\s\S]*?)<\/h1>/i) ||
                html.match(/<h1[^>]*>([\s\S]*?)<\/h1>/i) ||
                html.match(/<title>([^<-]+)/i);
            const title = (dataTitleMatch ? decodeHtmlEntities(dataTitleMatch[1]).trim() : undefined) ||
                metaContent(html, ['og:title', 'twitter:title', 'title']) ||
                decodeHtmlEntities(stripHtml(titleMatch ? titleMatch[1] : 'Sem Título')).trim();
            const descMatch = html.match(/id=["']synopsis["'][^>]*>([\s\S]*?)<\/div>/i) ||
                html.match(/class=["'][^"']*synopsis[^"']*["'][^>]*>([\s\S]*?)<\/div>/i) ||
                html.match(/<meta[^>]+property=["']og:description["'][^>]+content=["']([^"']+)["']/i);
            const synopsis = descMatch ? decodeHtmlEntities(stripHtml(descMatch[1])).trim() : undefined;
            const coverMatch = html.match(/data-image=["']([^"']+)["']/i) ||
                html.match(/class=["'][^"']*entry-content[^"']*["'][\s\S]*?<img[^>]+src=["']([^"']+)["']/i);
            const coverUrl = metaContent(html, ['og:image', 'twitter:image']) || (coverMatch ? coverMatch[1] : undefined);
            const genres = [];
            const genreMatches = html.matchAll(/rel=["']tag["'][^>]*>([^<]+)<\/a>/gi);
            for (const m of genreMatches) {
                const g = decodeHtmlEntities(m[1]).trim();
                if (g && !genres.includes(g) && g.toLowerCase() !== 'series') {
                    genres.push(g);
                }
            }
            // Alternative Titles
            let alternativeTitles = [];
            const cleanHtml = html
                .replace(/<style[^>]*>[\s\S]*?<\/style>/gi, ' ')
                .replace(/<script[^>]*>[\s\S]*?<\/script>/gi, ' ');
            const altMatch = cleanHtml.match(/(?:alter(?:native)?|outros?\s+nomes?|sin[oô]nimos?|t[ií]tulos?\s*alternativos?)[^<:]*[:<\/][^>]*>([\s\S]*?)<\/(?:span|div|p|dd|li)>/i)
                || cleanHtml.match(/class=["'][^"']*(?:alter|alternative)[^"']*["'][^>]*>([\s\S]*?)<\//i);
            if (altMatch) {
                const rawAlt = stripHtml(altMatch[1]);
                const normalizedRaw = rawAlt.replace(/<br\s*\/?>/gi, '\n');
                const lines = normalizedRaw.includes('\n')
                    ? normalizedRaw.split(/[\r\n]+/)
                    : normalizedRaw.includes(';')
                        ? rawAlt.split(/;+/)
                        : normalizedRaw.includes(' / ')
                            ? rawAlt.split(/\s+\/\s+/)
                            : rawAlt.split(/[,;\/\n\r]+/);
                alternativeTitles = lines
                    .map((s) => cleanAlternativeTitle(s))
                    .filter((s) => typeof s === 'string' && s.length > 0 && !/^(?:n\/a|none|sem\s+t[ií]tulo|-)$/i.test(s));
            }
            return {
                sourceWorkId,
                title,
                slug: slugify(title),
                coverUrl: coverUrl || null,
                synopsis,
                genres: genres.length > 0 ? genres : undefined,
                alternativeTitles: alternativeTitles.length > 0 ? alternativeTitles : undefined,
                status: 'ONGOING',
            };
        }
        catch (err) {
            this.logger.warn(`fetchWorkDetails fallback for ${sourceWorkId}: ${err.message}`);
            // A placeholder is not source evidence for a legitimate empty tag or
            // alias list. Preserve the distinction between parser output and a
            // failed upstream read.
            throw err;
        }
    }
    async fetchChapters(sourceWorkId) {
        const [workUrl, labelHint] = sourceWorkId.split('|');
        const targetUrl = workUrl.startsWith('http') ? workUrl : `${this.baseUrl}/${workUrl}.html`;
        try {
            let label = labelHint;
            // 1. Try to get cleaner label from the work page if available
            try {
                const html = await this.fetchHtml(targetUrl);
                const m = html.match(/data-labelchapter=["']([^"']+)["']/i);
                if (m && m[1] && !m[1].toLowerCase().includes('pesquisar') && !m[1].toLowerCase().includes('search')) {
                    label = m[1].trim();
                }
            }
            catch (_) {
                // use labelHint
            }
            if (!label) {
                return [];
            }
            // Query Blogger feed for this label
            const feedUrl = `${this.baseUrl}/feeds/posts/default/-/${encodeURIComponent(label)}?alt=json&max-results=500`;
            const data = await this.fetchJson(feedUrl);
            const entries = data?.feed?.entry || [];
            const chapters = [];
            for (const entry of entries) {
                const title = decodeHtmlEntities(entry.title?.['$t'] || '').trim();
                const altLink = (entry.link || []).find((l) => l.rel === 'alternate')?.href || '';
                // Skip the series overview post itself
                if (altLink === targetUrl || title.toLowerCase() === label.toLowerCase()) {
                    continue;
                }
                const num = extractChapterNumber(title);
                if (num !== null && !chapters.some((c) => c.number === num)) {
                    chapters.push({
                        sourceChapterId: altLink,
                        number: num,
                        title,
                        createdAt: entry.published?.['$t'] || entry.updated?.['$t'],
                    });
                }
            }
            chapters.sort((a, b) => b.number - a.number);
            return chapters;
        }
        catch (err) {
            this.logger.error(`fetchChapters failed for ${sourceWorkId}: ${err.message}`);
            return [];
        }
    }
    async fetchChapterPages(sourceChapterId) {
        const chapterUrl = sourceChapterId.startsWith('http')
            ? sourceChapterId
            : `${this.baseUrl}/${sourceChapterId}.html`;
        try {
            // Blogger sites render images via JS lightbox — not in static HTML <img> tags.
            // Use the Blogger JSON API to get full post content including all image URLs.
            const urlObj = new URL(chapterUrl);
            const pathParts = urlObj.pathname.replace(/\.html$/, '').split('/').filter(Boolean);
            const slug = pathParts[pathParts.length - 1] ?? '';
            // Blogger sites render images via JS lightbox — not in static HTML <img> tags.
            // Use the Blogger JSON API with exact path parameter first:
            try {
                const pathUrl = `${urlObj.origin}/feeds/posts/default?alt=json&path=${encodeURIComponent(urlObj.pathname)}`;
                const data = await this.fetchJson(pathUrl);
                const entry = data?.entry || data?.feed?.entry?.[0];
                if (entry) {
                    const content = entry.content?.['$t'] ?? entry.summary?.['$t'] ?? '';
                    const pages = this._extractBloggerImages(content);
                    if (pages.length > 0)
                        return pages;
                }
            }
            catch (_pathErr) {
                // Fall back to slug search
            }
            if (slug) {
                const apiUrl = `${urlObj.origin}/feeds/posts/default?alt=json&q=${encodeURIComponent(slug)}&max-results=5`;
                try {
                    const data = await this.fetchJson(apiUrl);
                    const entries = data?.feed?.entry ?? [];
                    // Find the entry matching this exact URL
                    const entry = entries.find((e) => {
                        const alt = (e.link ?? []).find((l) => l.rel === 'alternate');
                        return alt?.href === chapterUrl;
                    }) ?? entries[0];
                    if (entry) {
                        const content = entry.content?.['$t'] ?? entry.summary?.['$t'] ?? '';
                        const pages = this._extractBloggerImages(content);
                        if (pages.length > 0)
                            return pages;
                    }
                }
                catch (_apiErr) {
                    this.logger.warn(`Blogger API fallback for ${chapterUrl}: ${_apiErr.message}`);
                }
            }
            // Fallback: parse static HTML (works for sites that embed <img> tags)
            const html = await this.fetchHtml(chapterUrl);
            return this._extractBloggerImages(html);
        }
        catch (err) {
            this.logger.error(`fetchChapterPages failed for ${sourceChapterId}: ${err.message}`);
            return [];
        }
    }
    /** Extract unique full-size Blogger/standard images from HTML or JSON content string. */
    _extractBloggerImages(content) {
        const seen = new Set();
        const pages = [];
        // Blogger image pattern: base URL + /(sNNN|wNNN|s0|...)/ size param + filename
        // We normalise to /s0/ (maximum size) and deduplicate by base path.
        const bloggerRe = /(https:\/\/(?:\d+\.bp\.blogspot\.com|blogger\.googleusercontent\.com)\/[^\s"'<>]+?)\/(?:s\d+|s0|w\d+|w\d+-[^/]+)\/([^\s"'<>]+?\.(?:jpe?g|png|webp|avif|gif))/gi;
        let m;
        while ((m = bloggerRe.exec(content)) !== null) {
            const base = m[1];
            const filename = m[2];
            if (seen.has(base))
                continue;
            // Skip cover/thumbnail-only images, banners and blog mascots by filename
            if (/(?:capa|cover|thumbnail|banner|logo|icon|avatar|Blog|doacao|recrutamento|discord|telegram)/i.test(filename))
                continue;
            seen.add(base);
            pages.push(`${base}/s0/${filename}`);
        }
        if (pages.length > 0)
            return pages;
        // Fallback: standard <img src|data-src> parsing for non-Blogger hosts
        const imgRe = /<img[^>]+(?:src|data-src)=["']([^"']+)["'][^>]*>/gi;
        while ((m = imgRe.exec(content)) !== null) {
            const src = m[1];
            if (!src.includes('capa-oculta') &&
                !src.includes('banner') &&
                !src.includes('logo') &&
                !src.includes('icon') &&
                !src.includes('avatar') &&
                !src.includes('Blog') &&
                !src.includes('doacao') &&
                !src.includes('recrutamento') &&
                !src.includes('discord') &&
                !src.includes('telegram') &&
                (src.includes('blogger.googleusercontent.com') ||
                    src.includes('.bp.blogspot.com') ||
                    src.includes('imgur.com') ||
                    src.match(/\.(jpe?g|png|webp|avif)/i))) {
                if (!seen.has(src)) {
                    seen.add(src);
                    pages.push(src);
                }
            }
        }
        return pages;
    }
    async searchWorks(query) {
        const searchUrl = `${this.baseUrl}/feeds/posts/default/-/${encodeURIComponent(this.seriesCategory)}?alt=json&q=${encodeURIComponent(query)}&max-results=20`;
        try {
            const data = await this.fetchJson(searchUrl);
            const entries = data?.feed?.entry || [];
            return entries.map((entry) => {
                const title = decodeHtmlEntities(entry.title?.['$t'] || '').trim();
                const altLink = (entry.link || []).find((l) => l.rel === 'alternate')?.href || '';
                return {
                    sourceWorkId: altLink || slugify(title),
                    title,
                    slug: slugify(title),
                    coverUrl: entry.media$thumbnail?.url || null,
                };
            });
        }
        catch (err) {
            if (err?.status === 404 || err?.status === 400) {
                return [];
            }
            this.logger.error(`searchWorks failed for query "${query}": ${err.message}`);
            throw err;
        }
    }
    getImageHeaders() {
        return {
            Referer: `${this.baseUrl}/`,
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
        };
    }
    extractCoverFromContent(content) {
        const m = content.match(/<img[^>]+src=["']([^"']+)["']/i);
        return m ? m[1] : null;
    }
}
