import { HostRateLimiter } from '../../core/rate-limiter.js';
import { Logger } from '../../core/logger.js';
import { slugify, decodeHtmlEntities, stripHtml } from './html-utils.js';
export class WpGalleryAdapter {
    rateLimiter;
    transport;
    id;
    name;
    baseUrl;
    logger;
    constructor(options, rateLimiter = new HostRateLimiter(2.0), transport = fetch) {
        this.rateLimiter = rateLimiter;
        this.transport = transport;
        this.id = options.id;
        this.name = options.name;
        this.baseUrl = options.baseUrl.replace(/\/$/, '');
        this.logger = new Logger(`WpGalleryAdapter:${this.id}`);
        const host = new URL(this.baseUrl).host;
        const rps = options.rateLimitRps || 2.0;
        this.rateLimiter.setHostRate(host, rps, Math.ceil(rps * 2), rps * 2);
    }
    get headers() {
        return {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
            Accept: 'application/json, text/html, */*',
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
    async fetchUpdatedWorks(cursor, options) {
        const page = cursor ? parseInt(cursor, 10) : 1;
        const perPage = 20;
        const url = `${this.baseUrl}/wp-json/wp/v2/posts?page=${page}&per_page=${perPage}&_embed=wp:featuredmedia`;
        try {
            const posts = await this.fetchJson(url);
            const works = [];
            for (const p of posts) {
                const title = decodeHtmlEntities(stripHtml(p.title?.rendered || '')).trim();
                const id = String(p.id);
                const link = p.link || '';
                const coverUrl = p._embedded?.['wp:featuredmedia']?.[0]?.source_url ||
                    p.yoast_head_json?.og_image?.[0]?.url ||
                    null;
                works.push({
                    sourceWorkId: id,
                    title,
                    slug: p.slug || slugify(title),
                    coverUrl,
                    updatedAt: p.modified || p.date || new Date().toISOString(),
                });
            }
            const hasNext = posts.length === perPage;
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
        const url = `${this.baseUrl}/wp-json/wp/v2/posts/${sourceWorkId}?_embed=wp:featuredmedia`;
        try {
            const p = await this.fetchJson(url);
            const title = decodeHtmlEntities(stripHtml(p.title?.rendered || '')).trim();
            const synopsis = p.excerpt?.rendered ? decodeHtmlEntities(stripHtml(p.excerpt.rendered)).trim() : undefined;
            const coverUrl = p._embedded?.['wp:featuredmedia']?.[0]?.source_url ||
                p.yoast_head_json?.og_image?.[0]?.url ||
                null;
            return {
                sourceWorkId,
                title,
                slug: p.slug || slugify(title),
                coverUrl,
                synopsis,
                status: 'UNKNOWN',
            };
        }
        catch (err) {
            this.logger.error(`fetchWorkDetails failed for ${sourceWorkId}: ${err.message}`);
            return {
                sourceWorkId,
                title: sourceWorkId,
                slug: slugify(sourceWorkId),
            };
        }
    }
    async fetchChapters(sourceWorkId) {
        // Each post in WP gallery is a single complete chapter / oneshot
        const url = `${this.baseUrl}/wp-json/wp/v2/posts/${sourceWorkId}`;
        try {
            const p = await this.fetchJson(url);
            return [
                {
                    sourceChapterId: `${sourceWorkId}|${p.link || ''}`,
                    number: 1,
                    title: 'Capítulo Único',
                    createdAt: p.date,
                },
            ];
        }
        catch (err) {
            this.logger.error(`fetchChapters failed for ${sourceWorkId}: ${err.message}`);
            return [];
        }
    }
    async fetchChapterPages(sourceChapterId) {
        const [postId, link] = sourceChapterId.split('|');
        try {
            // Fetch post content from REST API or direct link
            const postUrl = `${this.baseUrl}/wp-json/wp/v2/posts/${postId}`;
            const p = await this.fetchJson(postUrl);
            const contentHtml = p.content?.rendered || '';
            const pages = [];
            // Extract images from rendered content
            const imgRegex = /<img[^>]+(?:src|data-src)=["']([^"']+)["'][^>]*>/gi;
            for (const m of contentHtml.matchAll(imgRegex)) {
                const src = m[1];
                if (!src.includes('avatar') &&
                    !src.includes('logo') &&
                    !src.includes('banner') &&
                    (src.includes('wp-content/uploads') || src.match(/\.(jpe?g|png|webp|avif)/i))) {
                    if (!pages.includes(src)) {
                        pages.push(src);
                    }
                }
            }
            // If no images in content, try scraping the link
            if (pages.length === 0 && link) {
                let targetLink = link;
                let html = await this.fetchHtml(targetLink);
                // Check for dedicated gallery link (e.g. Universo Hentai: a[title="Abrir galeria"] or .btn-ver-galeria)
                const gallerySubMatch = html.match(/href=["']([^"']+)["'][^>]*title=["']Abrir galeria["']/i) ||
                    html.match(/class=["'][^"']*btn-ver-galeria[^"']*["'][^>]*href=["']([^"']+)["']/i);
                if (gallerySubMatch) {
                    targetLink = gallerySubMatch[1];
                    html = await this.fetchHtml(targetLink);
                }
                const fancyboxMatches = html.matchAll(/class=["'][^"']*fancybox[^"']*["'][^>]+href=["']([^"']+)["']/gi);
                for (const m of fancyboxMatches) {
                    const href = m[1];
                    if (!pages.includes(href))
                        pages.push(href);
                }
                if (pages.length === 0) {
                    // Try div.listaImagens ul.post-fotos img or galeria img or all content imgs
                    const galleryImgs = html.matchAll(/(?:class=["'][^"']*(?:post-fotos|listaImagens|galeria|galeria-foto|entry-content)[^"']*["'][\s\S]*?)?<img[^>]+(?:data-src|src)=["']([^"']+)["']/gi);
                    for (const gm of galleryImgs) {
                        let src = gm[1];
                        // Strip WP thumbnail dimension suffix (-300x400.jpg -> .jpg)
                        src = src.replace(/-\d+x\d+(?=\.\w+$)/, '');
                        if (!src.toLowerCase().includes('avatar') &&
                            !src.toLowerCase().includes('logo') &&
                            !src.toLowerCase().includes('banner') &&
                            !src.toLowerCase().includes('selo') &&
                            !src.toLowerCase().endsWith('.gif') &&
                            !src.toLowerCase().includes('live-action') &&
                            (src.includes('wp-content/uploads') || src.match(/\.(jpe?g|png|webp|avif)/i))) {
                            if (!pages.includes(src))
                                pages.push(src);
                        }
                    }
                }
            }
            return pages;
        }
        catch (err) {
            this.logger.error(`fetchChapterPages failed for ${sourceChapterId}: ${err.message}`);
            return [];
        }
    }
    async searchWorks(query) {
        const url = `${this.baseUrl}/wp-json/wp/v2/posts?search=${encodeURIComponent(query)}&per_page=20`;
        try {
            const posts = await this.fetchJson(url);
            return posts.map((p) => ({
                sourceWorkId: String(p.id),
                title: decodeHtmlEntities(stripHtml(p.title?.rendered || '')).trim(),
                slug: p.slug || slugify(p.title?.rendered || ''),
            }));
        }
        catch (err) {
            this.logger.error(`searchWorks failed for query "${query}": ${err.message}`);
            return [];
        }
    }
    getImageHeaders(url) {
        return {
            Referer: `${this.baseUrl}/`,
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
        };
    }
}
