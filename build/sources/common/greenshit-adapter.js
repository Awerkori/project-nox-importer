import { HostRateLimiter } from '../../core/rate-limiter.js';
import { Logger } from '../../core/logger.js';
import { slugify, decodeHtmlEntities, stripHtml } from './html-utils.js';
export class GreenShitAdapter {
    rateLimiter;
    transport;
    id;
    name;
    baseUrl;
    apiUrl;
    cdnUrl;
    scanId;
    defaultGenreId;
    logger;
    constructor(options, rateLimiter = new HostRateLimiter(2.0), transport = fetch) {
        this.rateLimiter = rateLimiter;
        this.transport = transport;
        this.id = options.id;
        this.name = options.name;
        this.baseUrl = options.baseUrl.replace(/\/$/, '');
        this.apiUrl = options.apiUrl.replace(/\/$/, '');
        this.cdnUrl = options.cdnUrl.replace(/\/$/, '');
        this.scanId = options.scanId;
        this.defaultGenreId = options.defaultGenreId || '1';
        this.logger = new Logger(`GreenShitAdapter:${this.id}`);
        const host = new URL(this.apiUrl).host;
        const rps = options.rateLimitRps || 2.0;
        this.rateLimiter.setHostRate(host, rps, Math.ceil(rps * 2), rps * 2);
    }
    get headers() {
        return {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
            Accept: 'application/json, text/plain, */*',
            'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
            'scan-id': this.scanId,
            Referer: `${this.baseUrl}/`,
            Origin: this.baseUrl,
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
    async fetchUpdatedWorks(cursor, options) {
        const page = cursor ? parseInt(cursor, 10) : 1;
        const limit = 24;
        const url = `${this.apiUrl}/obras/atualizacoes?pagina=${page}&limite=${limit}&gen_id=${this.defaultGenreId}`;
        try {
            const data = await this.fetchJson(url);
            const obras = data?.obras || data?.data || [];
            const works = [];
            for (const item of obras) {
                const title = decodeHtmlEntities(item.obr_nome || '').trim();
                const id = String(item.obr_id);
                const slug = item.obr_slug || slugify(title);
                const coverImg = item.obr_imagem;
                const coverUrl = coverImg ? `${this.cdnUrl}/scans/${this.scanId}/obras/${id}/${coverImg}` : null;
                works.push({
                    sourceWorkId: id,
                    title,
                    slug,
                    coverUrl,
                    updatedAt: item.obr_data_ultimo_capitulo || new Date().toISOString(),
                });
            }
            const totalPaginas = data?.totalPaginas || 1;
            const hasNext = page < totalPaginas;
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
        const url = `${this.apiUrl}/obras/${sourceWorkId}`;
        try {
            const data = await this.fetchJson(url);
            const obra = data?.obra || data;
            const title = decodeHtmlEntities(obra.obr_nome || '').trim();
            const synopsis = obra.obr_descricao ? decodeHtmlEntities(stripHtml(obra.obr_descricao)).trim() : undefined;
            const coverImg = obra.obr_imagem;
            const coverUrl = coverImg ? `${this.cdnUrl}/scans/${this.scanId}/obras/${sourceWorkId}/${coverImg}` : null;
            const genres = [];
            if (obra.genero?.gen_nome)
                genres.push(obra.genero.gen_nome);
            if (Array.isArray(obra.tags)) {
                for (const t of obra.tags) {
                    if (t.tag_nome)
                        genres.push(t.tag_nome);
                }
            }
            return {
                sourceWorkId,
                title,
                slug: obra.obr_slug || slugify(title),
                coverUrl,
                synopsis,
                genres: genres.length > 0 ? genres : undefined,
                status: obra.obr_status === 12 ? 'COMPLETED' : 'ONGOING',
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
        const url = `${this.apiUrl}/obras/${sourceWorkId}`;
        try {
            const data = await this.fetchJson(url);
            const obra = data?.obra || data;
            const rawChapters = obra?.capitulos || data?.capitulos || [];
            const chapters = [];
            for (const ch of rawChapters) {
                const id = String(ch.cap_id);
                const num = parseFloat(String(ch.cap_numero)) || 0;
                const title = ch.cap_nome || `Capítulo ${num}`;
                chapters.push({
                    sourceChapterId: id,
                    number: num,
                    title,
                    createdAt: ch.cap_criado_em,
                });
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
        const url = `${this.apiUrl}/capitulos/${sourceChapterId}`;
        try {
            const data = await this.fetchJson(url);
            const rawPages = data?.cap_paginas || data?.paginas || [];
            const pages = [];
            for (const p of rawPages) {
                const pPath = typeof p === 'string' ? p : p.path || p.src;
                if (pPath) {
                    const fullUrl = pPath.startsWith('http') ? pPath : `${this.cdnUrl}/${pPath}`;
                    pages.push(fullUrl);
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
        const url = `${this.apiUrl}/obras/pesquisa?q=${encodeURIComponent(query)}`;
        try {
            const data = await this.fetchJson(url);
            const obras = data?.obras || data?.data || [];
            return obras.map((item) => ({
                sourceWorkId: String(item.obr_id),
                title: decodeHtmlEntities(item.obr_nome || '').trim(),
                slug: item.obr_slug || slugify(item.obr_nome),
                coverUrl: item.obr_imagem ? `${this.cdnUrl}/scans/${this.scanId}/obras/${item.obr_id}/${item.obr_imagem}` : null,
            }));
        }
        catch (err) {
            this.logger.error(`searchWorks failed for query "${query}": ${err.message}`);
            return [];
        }
    }
    getImageHeaders() {
        return {
            Referer: `${this.baseUrl}/`,
            Origin: this.baseUrl,
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
        };
    }
}
