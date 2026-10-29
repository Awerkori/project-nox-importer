import { HostRateLimiter } from '../../core/rate-limiter.js';
import { Logger } from '../../core/logger.js';
import { decryptMangoPayload, DEFAULT_MANGOTOONS_ENC_KEY, DEFAULT_MANGOTOONS_SALT } from './mangotoons-decryptor.js';
function slugify(text) {
    return text
        .normalize('NFD')
        .replace(/[\u0300-\u036f]/g, '')
        .toLowerCase()
        .replace(/[^a-z0-9]+/g, '-')
        .replace(/^-+|-+$/g, '');
}
/**
 * MangoToons declares alternate names under `titulo_alternativo`.  Preserve
 * only explicit scalar/list values from that API field; a description or an
 * inferred translation must never enter the alias pipeline.
 */
function explicitAlternativeTitles(value, primaryTitle) {
    const values = Array.isArray(value) ? value : [value];
    const aliases = values
        .filter((entry) => typeof entry === 'string')
        .flatMap((entry) => entry.split(/[\r\n;]+|\s+\/\s+/))
        .map((entry) => entry.normalize('NFKC').replace(/\s+/g, ' ').trim())
        .filter((entry) => entry.length >= 1 && entry !== primaryTitle);
    return [...new Map(aliases.map((entry) => [entry.toLocaleLowerCase(), entry])).values()];
}
export class MangoToonsAdapter {
    rateLimiter;
    transport;
    id = 'mangotoons';
    name = 'Mango Toons';
    baseUrl = 'https://mangotoons.com';
    // The public reader moved from /api to the versioned API.  Keep this
    // separate from the browser base URL: pages are served by the CDN while
    // catalogue, metadata and manifests come from this stable API surface.
    apiUrl = 'https://api.mangotoons.com/api/v1';
    cdnUrl = 'https://cdn.mangotoons.com';
    logger = new Logger('MangoToonsAdapter');
    encKey = DEFAULT_MANGOTOONS_ENC_KEY;
    salt = DEFAULT_MANGOTOONS_SALT;
    token = null;
    constructor(rateLimiter = new HostRateLimiter(2.0), transport = fetch) {
        this.rateLimiter = rateLimiter;
        this.transport = transport;
        this.rateLimiter.setHostRate('api.mangotoons.com', 2.0, 4, 4.0);
        this.rateLimiter.setHostRate('cdn.mangotoons.com', 8.0, 16, 16.0);
        if (process.env.MANGOTOONS_TOKEN) {
            this.token = process.env.MANGOTOONS_TOKEN;
        }
    }
    getImageHeaders(_url) {
        return {
            Referer: `${this.baseUrl}/`,
            Origin: this.baseUrl,
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
            Accept: 'image/avif,image/webp,image/apng,image/*,*/*;q=0.8',
            'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
        };
    }
    async login() {
        const email = process.env.MANGOTOONS_EMAIL;
        const password = process.env.MANGOTOONS_PASSWORD;
        if (!email || !password) {
            return false;
        }
        try {
            await this.rateLimiter.acquire('api.mangotoons.com');
            const res = await this.transport(`${this.apiUrl}/auth/login`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    Accept: 'application/json',
                    'User-Agent': '-',
                    'sec-fetch-mode': 'none',
                    Referer: `${this.baseUrl}/`,
                },
                body: JSON.stringify({ email, senha: password }),
            });
            if (res.ok) {
                const data = await res.json();
                if (data.token || data.access_token) {
                    this.token = data.token || data.access_token;
                    this.logger.info('Successfully authenticated with MangoToons');
                    return true;
                }
            }
            this.logger.warn('Failed to authenticate with MangoToons', { status: res.status });
            return false;
        }
        catch (err) {
            this.logger.warn('Error during MangoToons login', { error: err?.message });
            return false;
        }
    }
    async requestApi(path, options = {}) {
        const url = path.startsWith('http') ? path : `${this.apiUrl}${path}`;
        const parsed = new URL(url);
        await this.rateLimiter.acquire(parsed.host);
        const headers = {
            'User-Agent': '-',
            'sec-fetch-mode': 'none',
            Referer: `${this.baseUrl}/`,
            Accept: 'application/json',
            ...(options.headers || {}),
        };
        if (this.token) {
            headers['Authorization'] = `Bearer ${this.token}`;
        }
        let res = await this.transport(url, { ...options, headers });
        // Auto-retry once on 401 if credentials are configured
        if (res.status === 401 && (process.env.MANGOTOONS_EMAIL && process.env.MANGOTOONS_PASSWORD)) {
            this.logger.info('Received 401 from MangoToons, re-authenticating...');
            this.token = null;
            const loggedIn = await this.login();
            if (loggedIn && this.token) {
                headers['Authorization'] = `Bearer ${this.token}`;
                res = await this.transport(url, { ...options, headers });
            }
        }
        if (!res.ok) {
            const err = new Error(`MangoToons HTTP ${res.status} for ${url}`);
            err.status = res.status;
            err.headers = res.headers;
            throw err;
        }
        this.rateLimiter.recordSuccess(parsed.host);
        const isEncrypted = res.headers.get('x-encrypted') === 'true';
        const text = await res.text();
        if (isEncrypted || (!text.trim().startsWith('{') && !text.trim().startsWith('['))) {
            return decryptMangoPayload(text, this.encKey, this.salt);
        }
        return JSON.parse(text);
    }
    async fetchUpdatedWorks(cursor, options) {
        // The releases endpoint is the provider's current public update feed.
        // It gives us real published chapters and, unlike walking the full work
        // catalogue, keeps routine maintenance bounded. Cursor pairs are issued
        // by the provider and preserve a durable, deterministic traversal.
        const params = new URLSearchParams({ limite: options?.mode === 'maintenance' ? '24' : '48' });
        if (cursor) {
            const separator = cursor.lastIndexOf('|');
            if (separator > 0) {
                params.set('cursor_data', cursor.slice(0, separator));
                params.set('cursor_id', cursor.slice(separator + 1));
            }
        }
        const rawReleases = await this.requestApi(`/obras/lancamentos?${params.toString()}`);
        const releases = Array.isArray(rawReleases) ? rawReleases : rawReleases.items || rawReleases.dados || [];
        const seen = new Set();
        const works = [];
        for (const release of releases) {
            const sourceWorkId = String(release.obra_id || release.work_id || release.id_obra || '');
            if (!sourceWorkId || seen.has(sourceWorkId))
                continue;
            seen.add(sourceWorkId);
            const title = release.obra_nome || release.work_title || release.nome || release.title;
            works.push({
                sourceWorkId,
                title: title || sourceWorkId,
                slug: release.obra_slug || release.slug || slugify(title || sourceWorkId),
                coverUrl: this.resolveCoverUrl(release.imagem || release.coverImage || release.banner_imagem),
                updatedAt: release.published_at || release.updated_at || undefined,
            });
        }
        const last = releases[releases.length - 1];
        const nextCursor = last?.published_at && last?.id ? `${last.published_at}|${last.id}` : null;
        return { works, nextCursor };
    }
    async fetchWorkDetails(sourceWorkId) {
        const data = await this.requestApi(`/obras/${sourceWorkId}`);
        const w = data.obra || data.dados || data;
        const title = w.nome || w.title || `Work ${sourceWorkId}`;
        const slug = w.slug || w.nome_url || slugify(title);
        let kind = 'UNKNOWN';
        const fmt = (w.formato_nome || w.formato || '').toLowerCase();
        if (fmt.includes('manga'))
            kind = 'UNKNOWN';
        else if (fmt.includes('manhua'))
            kind = 'MANHUA';
        else if (fmt.includes('webtoon') || fmt.includes('comic'))
            kind = 'WEBTOON';
        let status = 'UNKNOWN';
        const st = (w.status_nome || w.status || '').toLowerCase();
        if (st.includes('conclu'))
            status = 'COMPLETED';
        else if (st.includes('hiat') || st.includes('paus'))
            status = 'HIATUS';
        else if (st.includes('cancel'))
            status = 'CANCELLED';
        else if (st.includes('ongo') || st.includes('andamento') || st.includes('releasing') || st.includes('ativo') || st.includes('lanç') || st.includes('lanc'))
            status = 'ONGOING';
        const genres = (w.tags || []).map((t) => typeof t === 'string' ? t : t.nome || t.name).filter(Boolean);
        const alternativeTitles = explicitAlternativeTitles(w.titulo_alternativo ?? w.titulos_alternativos ?? w.titulo_alternative ?? w.nome_alternativo ?? w.outros_nomes ?? w.obr_outros_nomes ?? w.alternative_titles ?? w.alternativeTitles, title);
        return {
            sourceWorkId: String(w.id || sourceWorkId),
            title,
            slug,
            coverUrl: this.resolveCoverUrl(w.imagem || w.coverImage || w.banner_imagem),
            synopsis: w.descricao || undefined,
            kind,
            status,
            genres,
            alternativeTitles: alternativeTitles.length ? alternativeTitles : undefined,
            raw: w,
        };
    }
    async fetchChapters(sourceWorkId) {
        const data = await this.requestApi(`/obras/${sourceWorkId}/capitulos?pagina=1&limite=500`);
        const chaptersList = Array.isArray(data) ? data : data.capitulos || data.items || data.dados || [];
        const summaries = chaptersList.map((ch) => {
            const num = typeof ch.numero === 'number' ? ch.numero : parseFloat(ch.numero);
            return {
                // The public manifest is keyed by its displayed chapter value, not
                // by the internal database ID. Preserve its exact source spelling so
                // fractional/special chapters are not collapsed.
                sourceChapterId: `${sourceWorkId}:${encodeURIComponent(String(ch.numero ?? ch.id))}`,
                number: isNaN(num) ? 0 : num,
                title: ch.nome || ch.title || undefined,
                createdAt: ch.published_at || ch.criado_em || ch.atualizado_em || undefined,
                pageCount: ch.total_paginas || undefined,
            };
        });
        // Return chapters sorted by chapter number ascending for clarity
        return summaries.sort((a, b) => a.number - b.number);
    }
    async fetchChapterPages(sourceChapterId, chapterNumber) {
        // sourceChapterId is formatted as "${sourceWorkId}:${chapterIdOrNumber}"
        let workId = '';
        let chapterValue = '';
        if (sourceChapterId.includes(':')) {
            const [wId, cId] = sourceChapterId.split(':', 2);
            workId = wId;
            chapterValue = decodeURIComponent(cId || (chapterNumber !== undefined ? String(chapterNumber) : '1'));
        }
        else {
            workId = sourceChapterId;
            chapterValue = chapterNumber !== undefined ? String(chapterNumber) : '1';
        }
        const data = await this.requestApi(`/leitor/obras/${encodeURIComponent(workId)}/capitulos/${encodeURIComponent(chapterValue)}/manifest`);
        const pages = data.pages || [];
        const sortedPages = [...pages].sort((a, b) => (a.index ?? a.numero ?? 0) - (b.index ?? b.numero ?? 0));
        return sortedPages
            .map((p) => {
            const url = p.url || p.cdn_id || p.imagem || p.image || p.src || p.link || p.path;
            if (!url)
                return null;
            if (url.startsWith('http'))
                return url;
            return `${this.cdnUrl}/${url.replace(/^\//, '')}`;
        })
            .filter((url) => Boolean(url));
    }
    resolveCoverUrl(url) {
        if (!url)
            return null;
        if (url.startsWith('http'))
            return url;
        return `${this.cdnUrl}/${url.replace(/^\//, '')}`;
    }
    async searchWorks(query) {
        const clean = query.trim();
        if (!clean)
            return [];
        try {
            const data = await this.requestApi(`/obras?pagina=1&limite=20&busca=${encodeURIComponent(clean)}`);
            const rawWorks = data.obras || data.dados || data.items || [];
            return rawWorks.map((w) => ({
                sourceWorkId: String(w.id),
                title: w.nome || w.title,
                slug: w.slug || w.nome_url || slugify(w.nome || w.title || String(w.id)),
                coverUrl: this.resolveCoverUrl(w.imagem || w.coverImage || w.banner_imagem),
                updatedAt: w.atualizada_em || w.criada_em || undefined,
            }));
        }
        catch (err) {
            if (err?.status === 404 || err?.status === 400 || err?.message?.includes('HTTP 404') || err?.message?.includes('HTTP 400')) {
                return [];
            }
            this.logger.warn(`Search failed on MangoToons for query "${query}"`, { error: err?.message });
            throw err;
        }
    }
}
