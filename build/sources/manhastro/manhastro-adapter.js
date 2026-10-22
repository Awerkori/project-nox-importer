import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { HostRateLimiter } from '../../core/rate-limiter.js';
import { Logger } from '../../core/logger.js';
function normalizeText(text) {
    return text
        .normalize('NFKD')
        .replace(/[\u0300-\u036f]/g, '')
        .normalize('NFC')
        .toLowerCase()
        .replace(/[^\p{L}\p{N}]+/gu, ' ')
        .trim();
}
function slugify(text) {
    return text
        .normalize('NFD')
        .replace(/[\u0300-\u036f]/g, '')
        .toLowerCase()
        .replace(/[^a-z0-9]+/g, '-')
        .replace(/^-+|-+$/g, '');
}
function cleanJsonResponse(body) {
    return body
        .replace(/^\uFEFF/, '')
        .replace(/^\)\]\}'/, '')
        .replace(/^,/, '')
        .replace(/^_/, '')
        .trim();
}
function extractChapterNumber(name) {
    const match = name.match(/(\d+(?:\.\d+)?)/);
    return match ? parseFloat(match[1]) : 0;
}
export class ManhastroAdapter {
    rateLimiter;
    transport;
    id = 'manhastro';
    name = 'Manhastro';
    baseUrl = 'https://manhastro.net';
    probeUrl = 'https://api.manhastro.net/lancamentos?p=1';
    apiUrl = 'https://api.manhastro.net';
    logger = new Logger('ManhastroAdapter');
    catalogItems = [];
    catalogByNormalizedTitle = new Map();
    catalogBySlug = new Map();
    catalogById = new Map();
    catalogLoaded = false;
    constructor(rateLimiter = new HostRateLimiter(2.0), transport = fetch) {
        this.rateLimiter = rateLimiter;
        this.transport = transport;
        this.rateLimiter.setHostRate('api.manhastro.net', 2.0, 4, 4.0);
        this.rateLimiter.setHostRate('albums.manhastro.net', 8.0, 16, 16.0);
    }
    ensureCatalogLoaded() {
        if (this.catalogLoaded)
            return true;
        try {
            const currentDir = typeof __dirname !== 'undefined'
                ? __dirname
                : path.dirname(fileURLToPath(import.meta.url));
            const catalogPath = path.resolve(currentDir, 'manhastro-catalog.json');
            if (fs.existsSync(catalogPath)) {
                const raw = fs.readFileSync(catalogPath, 'utf8');
                this.catalogItems = JSON.parse(raw);
                for (const item of this.catalogItems) {
                    this.catalogById.set(item.id, item);
                    if (item.slug)
                        this.catalogBySlug.set(item.slug, item);
                    const titles = [item.t, item.tb, ...(item.alt || [])].filter(Boolean);
                    for (const title of titles) {
                        const norm = normalizeText(title);
                        if (norm) {
                            const list = this.catalogByNormalizedTitle.get(norm) || [];
                            list.push(item);
                            this.catalogByNormalizedTitle.set(norm, list);
                        }
                    }
                }
                this.catalogLoaded = true;
                this.logger.info(`Loaded Manhastro local catalog index with ${this.catalogItems.length} works`);
                return true;
            }
        }
        catch (err) {
            this.logger.warn('Failed to load local Manhastro catalog cache', { error: err?.message });
        }
        return false;
    }
    get headers() {
        return {
            Accept: 'application/json, text/plain, */*',
            Origin: this.baseUrl,
            Referer: `${this.baseUrl}/`,
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
        };
    }
    async request(url, options = {}) {
        const parsedUrl = new URL(url);
        await this.rateLimiter.acquire(parsedUrl.host);
        let attempts = 0;
        const maxAttempts = 3;
        while (attempts < maxAttempts) {
            attempts++;
            try {
                const response = await this.transport(url, {
                    ...options,
                    headers: {
                        ...this.headers,
                        ...(options.headers || {}),
                    },
                    signal: AbortSignal.timeout(30_000),
                });
                if (response.status === 429) {
                    const retryAfter = response.headers.get('Retry-After');
                    // A source-level 429 is already a durable Lite retry boundary. Do
                    // not issue the remaining adapter retries immediately: doing so
                    // ignores the limiter's just-recorded cooldown, burns provider
                    // budget and holds a chapter slot while every peer waits. Propagate
                    // the exact bounded cooldown to the executor instead.
                    const retryAfterSeconds = this.rateLimiter.handle429(parsedUrl.host, retryAfter, attempts);
                    const error = new Error(`Rate limit reached (HTTP 429) for ${parsedUrl.host}`);
                    error.status = 429;
                    error.retryAfter = retryAfterSeconds;
                    throw error;
                }
                if (!response.ok) {
                    const errText = await response.text().catch(() => '');
                    throw new Error(`Manhastro request failed: HTTP ${response.status} - ${errText.slice(0, 200)}`);
                }
                this.rateLimiter.recordSuccess(parsedUrl.host);
                const rawText = await response.text();
                const cleaned = cleanJsonResponse(rawText);
                return JSON.parse(cleaned);
            }
            catch (err) {
                if (err?.status === 429)
                    throw err;
                if (err?.name === 'AbortError' || err?.name === 'TimeoutError' || err?.message?.includes('aborted'))
                    throw err;
                if (err?.status && err.status >= 400 && err.status < 500)
                    throw err;
                if (attempts >= maxAttempts)
                    throw err;
                await new Promise((r) => setTimeout(r, 1000 * attempts));
            }
        }
        throw new Error(`Manhastro request failed after ${maxAttempts} attempts`);
    }
    async fetchUpdatedWorks(cursor, options) {
        const mode = options?.mode || 'maintenance';
        if (mode === 'bootstrap') {
            const page = cursor ? parseInt(cursor, 10) : 1;
            const url = `${this.apiUrl}/dados?sort=views&order=desc&limit=100&page=${page}`;
            const response = await this.request(url);
            const items = response.data || [];
            const works = items.map((item) => {
                const title = item.titulo_brasil?.trim() || item.titulo.trim();
                const cover = item.imagem
                    ? item.imagem.startsWith('http')
                        ? item.imagem
                        : `https://${item.imagem}`
                    : null;
                return {
                    sourceWorkId: String(item.manga_id),
                    title,
                    slug: slugify(title),
                    coverUrl: cover,
                    updatedAt: item.ultimo_capitulo || new Date().toISOString(),
                };
            });
            const hasNext = response.meta?.has_more ?? (items.length >= 20);
            return {
                works,
                nextCursor: hasNext ? String(page + 1) : null,
            };
        }
        else {
            // Maintenance: fetch latest updates
            const page = cursor ? parseInt(cursor, 10) : 1;
            const url = `${this.apiUrl}/lancamentos?p=${page}`;
            const response = await this.request(url);
            const items = response.data || [];
            const works = items.map((item) => {
                const title = item.titulo_brasil?.trim() || item.titulo.trim();
                const cover = item.imagem
                    ? item.imagem.startsWith('http')
                        ? item.imagem
                        : `https://${item.imagem}`
                    : null;
                return {
                    sourceWorkId: String(item.manga_id),
                    title,
                    slug: slugify(title),
                    coverUrl: cover,
                    updatedAt: item.ultimo_capitulo || new Date().toISOString(),
                };
            });
            return {
                works,
                nextCursor: items[0]?.ultimo_capitulo || new Date().toISOString(),
            };
        }
    }
    async fetchWorkDetails(sourceWorkId) {
        if (!sourceWorkId || sourceWorkId === 'undefined' || sourceWorkId === 'null') {
            throw new Error(`Invalid Manhastro sourceWorkId: ${sourceWorkId}`);
        }
        if (this.ensureCatalogLoaded()) {
            const cached = this.catalogById.get(String(sourceWorkId)) || this.catalogBySlug.get(String(sourceWorkId));
            if (cached) {
                const title = cached.tb?.trim() || cached.t.trim();
                const alternativeTitles = Array.from(new Set([
                    cached.t.trim(),
                    cached.tb?.trim(),
                    ...(cached.alt || [])
                ].filter((c) => Boolean(c && c !== title))));
                let kind = 'UNKNOWN';
                const cat = (cached.cat || '').toLowerCase();
                if (cat.includes('manhwa'))
                    kind = 'MANHWA';
                else if (cat.includes('manhua'))
                    kind = 'MANHUA';
                else if (cat.includes('webtoon'))
                    kind = 'WEBTOON';
                let status = 'UNKNOWN';
                const st = (cached.st || '').toLowerCase().replace(/[^a-z0-9]/g, '');
                if (st.includes('completed') || st.includes('completo'))
                    status = 'COMPLETED';
                else if (st.includes('hiat'))
                    status = 'HIATUS';
                else if (st.includes('cancel'))
                    status = 'CANCELLED';
                else if (st.includes('ongo') || st.includes('andamento') || st.includes('releasing') || st.includes('ativo') || st.includes('lanc'))
                    status = 'ONGOING';
                return {
                    sourceWorkId: cached.id,
                    title,
                    slug: cached.slug || slugify(title),
                    coverUrl: cached.img ? (cached.img.startsWith('http') ? cached.img : `https://${cached.img}`) : null,
                    synopsis: cached.desc || '',
                    kind,
                    status,
                    language: 'pt-BR',
                    genres: cached.gen || [],
                    alternativeTitles,
                    raw: cached,
                };
            }
        }
        const url = `${this.apiUrl}/dados?manga_id=${sourceWorkId}`;
        const response = await this.request(url);
        const item = (response.data || []).find((entry) => String(entry.manga_id) === String(sourceWorkId));
        if (!item)
            throw new Error(`Work not found on Manhastro: ${sourceWorkId}`);
        const title = item.titulo_brasil?.trim() || item.titulo.trim();
        const alternativeTitles = Array.from(new Set([
            item.titulo.trim(),
            item.titulo_brasil?.trim(),
            ...(Array.isArray(item.titulo_alternative) ? item.titulo_alternative : []),
        ]
            .map((candidate) => candidate?.trim())
            .filter((candidate) => Boolean(candidate) && candidate !== title)));
        const cover = item.imagem
            ? item.imagem.startsWith('http')
                ? item.imagem
                : `https://${item.imagem}`
            : null;
        let kind = 'UNKNOWN';
        const cat = (item.categoria || '').toLowerCase();
        if (cat.includes('manhwa'))
            kind = 'MANHWA';
        else if (cat.includes('manhua'))
            kind = 'MANHUA';
        else if (cat.includes('webtoon'))
            kind = 'WEBTOON';
        let status = 'UNKNOWN';
        const st = (item.status || '').toLowerCase().replace(/[^a-z0-9]/g, '');
        if (st.includes('completed') || st.includes('completo'))
            status = 'COMPLETED';
        else if (st.includes('hiat'))
            status = 'HIATUS';
        else if (st.includes('cancel'))
            status = 'CANCELLED';
        else if (st.includes('ongo') || st.includes('andamento') || st.includes('releasing') || st.includes('ativo') || st.includes('lanc'))
            status = 'ONGOING';
        return {
            sourceWorkId: String(item.manga_id),
            title,
            slug: slugify(title),
            coverUrl: cover,
            synopsis: item.descricao_brasil?.trim() || item.descricao?.trim() || '',
            kind,
            status,
            language: 'pt-BR',
            genres: item.generos || [],
            alternativeTitles,
            raw: item,
        };
    }
    async fetchChapters(sourceWorkId) {
        const url = `${this.apiUrl}/dados/${sourceWorkId}`;
        const response = await this.request(url);
        const rawChapters = response.data || [];
        const chapters = rawChapters.map((ch) => {
            const num = extractChapterNumber(ch.capitulo_nome);
            const displayNumber = ch.capitulo_nome?.match(/(\d+(?:\.\d+)?)/)?.[1] || String(num);
            return {
                sourceChapterId: String(ch.capitulo_id),
                displayNumber,
                number: num,
                title: ch.capitulo_nome?.trim() || `Capítulo ${num}`,
                createdAt: ch.capitulo_data || new Date().toISOString(),
                pageCount: null,
            };
        });
        return chapters.sort((a, b) => a.number - b.number);
    }
    async fetchChapterPages(sourceChapterId, _chapterNumber) {
        const url = `${this.apiUrl}/paginas/${sourceChapterId}`;
        const response = await this.request(url);
        const chapter = response.data?.chapter;
        if (!chapter || !chapter.data || chapter.data.length === 0) {
            throw new Error(`Manhastro returned 0 pages for chapter ${sourceChapterId}`);
        }
        const { baseUrl, hash, data } = chapter;
        const cleanBase = baseUrl.endsWith('/') ? baseUrl.slice(0, -1) : baseUrl;
        return data
            .map((filename) => `${cleanBase}/${hash}/${filename}`)
            .filter((u) => u.startsWith('http://') || u.startsWith('https://'));
    }
    searchCatalog(query) {
        const norm = normalizeText(query);
        const slug = slugify(query);
        const matches = [];
        const seen = new Set();
        // 1. Exact slug match
        const bySlug = this.catalogBySlug.get(slug);
        if (bySlug && !seen.has(bySlug.id)) {
            matches.push(bySlug);
            seen.add(bySlug.id);
        }
        // 2. Exact normalized title or alias match
        const exact = this.catalogByNormalizedTitle.get(norm) || [];
        for (const item of exact) {
            if (!seen.has(item.id)) {
                matches.push(item);
                seen.add(item.id);
            }
        }
        // 3. Substring matching if query is specific enough (>= 4 chars) and few exact matches
        if (matches.length < 5 && norm.length >= 4) {
            for (const item of this.catalogItems) {
                if (seen.has(item.id))
                    continue;
                const itemNorm = normalizeText(item.t);
                const itemNormBr = item.tb ? normalizeText(item.tb) : '';
                let matched = (itemNorm.length >= 4 && (itemNorm.includes(norm) || norm.includes(itemNorm))) ||
                    (itemNormBr.length >= 4 && (itemNormBr.includes(norm) || norm.includes(itemNormBr)));
                if (!matched && item.alt && item.alt.length > 0) {
                    for (const altTitle of item.alt) {
                        const altNorm = normalizeText(altTitle);
                        if (altNorm.length >= 4 && (altNorm.includes(norm) || norm.includes(altNorm))) {
                            matched = true;
                            break;
                        }
                    }
                }
                if (matched) {
                    matches.push(item);
                    seen.add(item.id);
                    if (matches.length >= 10)
                        break;
                }
            }
        }
        return matches.map((item) => {
            const title = item.tb || item.t;
            const cover = item.img
                ? item.img.startsWith('http')
                    ? item.img
                    : `https://${item.img}`
                : null;
            return {
                sourceWorkId: item.id,
                title,
                slug: item.slug || slugify(title),
                coverUrl: cover,
                updatedAt: new Date().toISOString(),
            };
        });
    }
    async searchWorks(query) {
        const clean = query.trim();
        if (!clean)
            return [];
        // 1. High-speed zero-network in-memory search over the complete catalog
        if (this.ensureCatalogLoaded()) {
            return this.searchCatalog(clean);
        }
        // 2. Fallback: live API only if local catalog failed to load
        const url = `${this.apiUrl}/dados?nome=${encodeURIComponent(clean)}&limit=10&sort=views&order=desc`;
        try {
            const response = await this.request(url, { signal: AbortSignal.timeout(5_000) });
            const items = response.data || [];
            return items.map((item) => {
                const title = item.titulo_brasil?.trim() || item.titulo.trim();
                const cover = item.imagem
                    ? item.imagem.startsWith('http')
                        ? item.imagem
                        : `https://${item.imagem}`
                    : null;
                return {
                    sourceWorkId: String(item.manga_id),
                    title,
                    slug: slugify(title),
                    coverUrl: cover,
                    updatedAt: item.ultimo_capitulo || new Date().toISOString(),
                };
            });
        }
        catch (err) {
            if (err?.status === 404 || err?.status === 400 || err?.message?.includes('HTTP 404') || err?.message?.includes('HTTP 400')) {
                return [];
            }
            this.logger.warn(`Search failed or timed out on Manhastro for query "${query}"`, { error: err?.message });
            throw err;
        }
    }
}
