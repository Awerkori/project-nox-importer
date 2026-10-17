import { HostRateLimiter } from '../../core/rate-limiter.js';
import { extractChapterNumber, slugify, stripHtml } from '../common/html-utils.js';
/** Public first-party WordPress API; no browser token, cookie or DOM selector. */
export class MangaStopAdapter {
    rateLimiter;
    transport;
    id = 'mangastop';
    name = 'Manga Stop';
    baseUrl = 'https://mangastop.net';
    apiUrl = `${this.baseUrl}/wp-json/mangastop/v1`;
    // The marketing page is behind a browser challenge, while the documented
    // first-party API used by this adapter remains publicly available. Probe
    // the actual execution surface rather than a page the importer never uses.
    probeUrl = `${this.baseUrl}/wp-json/mangastop/v1/recentes?pagina=1&por_pagina=1`;
    constructor(rateLimiter = new HostRateLimiter(2), transport = fetch) {
        this.rateLimiter = rateLimiter;
        this.transport = transport;
        this.rateLimiter.setHostRate(new URL(this.baseUrl).host, 2, 4, 4);
        this.rateLimiter.setHostRate('images.mangastop.wtf', 8, 16, 16);
    }
    async request(path) {
        const url = `${this.apiUrl}${path}`;
        const host = new URL(url).host;
        await this.rateLimiter.acquire(host);
        const response = await this.transport(url, {
            headers: { Accept: 'application/json', Origin: this.baseUrl, Referer: `${this.baseUrl}/` },
            signal: AbortSignal.timeout(15_000),
        });
        if (!response.ok)
            throw new Error(`MangaStop HTTP ${response.status} for ${path}`);
        this.rateLimiter.recordSuccess(host);
        return response.json();
    }
    summary(work) {
        const title = String(work.titulo || work.title || `Obra ${work.id}`);
        return {
            sourceWorkId: String(work.id), title, slug: work.slug || slugify(title),
            coverUrl: work.thumbnail || work.capa_url || null,
            updatedAt: work.capitulos?.[0]?.data || work.updated_at || undefined,
        };
    }
    details(work) {
        const title = String(work.titulo || work.title || `Obra ${work.id}`);
        const type = String(work.tipo || '').toLocaleLowerCase();
        const kind = type.includes('manhua') ? 'MANHUA'
            : type.includes('webtoon') ? 'WEBTOON'
                : type.includes('pornhwa') ? 'PORNHWA'
                    : type.includes('manhwa') ? 'MANHWA'
                        : type.includes('manga') ? 'MANGA' : 'UNKNOWN';
        const statusText = String(work.status || '').toLocaleLowerCase();
        const status = statusText.includes('complete') || statusText.includes('conclu') ? 'COMPLETED'
            : statusText.includes('hiat') ? 'HIATUS'
                : statusText.includes('ongoing') || statusText.includes('andamento') ? 'ONGOING' : 'UNKNOWN';
        const alternate = typeof work.alternativo === 'string' ? work.alternativo.trim() : '';
        const genres = (Array.isArray(work.generos) ? work.generos : [])
            .map((genre) => typeof genre === 'string' ? genre : genre?.nome || genre?.name)
            .filter((genre) => Boolean(genre));
        return {
            sourceWorkId: String(work.id), title, slug: work.slug || slugify(title),
            coverUrl: work.capa_url || work.thumbnail || null,
            synopsis: typeof work.sinopse === 'string' ? stripHtml(work.sinopse).trim() || undefined : undefined,
            author: work.autor || work.manga_autor?.map((author) => author.nome).filter(Boolean).join(', ') || undefined,
            artist: work.artista || work.manga_artista?.map((artist) => artist.nome).filter(Boolean).join(', ') || undefined,
            kind, status, genres: genres.length ? [...new Set(genres)] : undefined,
            alternativeTitles: alternate && alternate.localeCompare(title, undefined, { sensitivity: 'accent' }) !== 0 ? [alternate] : undefined,
            raw: work,
        };
    }
    chapters(work) {
        const sourceChapters = Array.isArray(work.capitulos) ? work.capitulos : [];
        return sourceChapters.map((chapter) => {
            const displayNumber = String(chapter.numero || chapter.id);
            const parsed = Number.parseFloat(displayNumber.replace(',', '.'));
            return {
                sourceChapterId: String(chapter.id), displayNumber,
                number: Number.isFinite(parsed) ? parsed : extractChapterNumber(String(chapter.titulo_post || chapter.titulo || displayNumber)),
                title: chapter.titulo_post || chapter.titulo || `Capítulo ${displayNumber}`,
                createdAt: chapter.data_publicacao || chapter.data || undefined,
            };
        }).sort((a, b) => a.number - b.number);
    }
    async fetchUpdatedWorks(cursor) {
        const page = Math.max(1, Number.parseInt(cursor || '1', 10) || 1);
        const data = await this.request(`/recentes?pagina=${page}&por_pagina=24`);
        return { works: (data.mangas || []).map((work) => this.summary(work)), nextCursor: data.paginacao?.tem_proxima ? String(page + 1) : null };
    }
    async fetchWorkDetails(sourceWorkId) {
        return this.details(await this.request(`/obra/${encodeURIComponent(sourceWorkId)}`));
    }
    async fetchWorkSnapshot(sourceWorkId) {
        const work = await this.request(`/obra/${encodeURIComponent(sourceWorkId)}`);
        return { details: this.details(work), chapters: this.chapters(work) };
    }
    async fetchChapters(sourceWorkId) {
        return this.chapters(await this.request(`/obra/${encodeURIComponent(sourceWorkId)}`));
    }
    async fetchChapterPages(sourceChapterId) {
        const data = await this.request(`/leitor/${encodeURIComponent(sourceChapterId)}`);
        return (data.imagens || []).map((image) => image.url).filter((url) => Boolean(url));
    }
    async searchWorks(query) {
        const data = await this.request(`/busca?pagina=1&q=${encodeURIComponent(query)}`);
        return (data.obras?.lista || []).map((work) => this.summary(work));
    }
    getImageHeaders() {
        return { Referer: `${this.baseUrl}/`, Accept: 'image/avif,image/webp,image/*,*/*;q=0.8' };
    }
}
