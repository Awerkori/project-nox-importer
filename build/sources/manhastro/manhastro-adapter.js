import { HostRateLimiter } from '../../core/rate-limiter.js';
import { Logger } from '../../core/logger.js';
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
    apiUrl = 'https://api2.manhastro.net';
    logger = new Logger('ManhastroAdapter');
    constructor(rateLimiter = new HostRateLimiter(2.0), transport = fetch) {
        this.rateLimiter = rateLimiter;
        this.transport = transport;
        this.rateLimiter.setHostRate('api2.manhastro.net', 2.0, 4, 4.0);
        this.rateLimiter.setHostRate('albums.manhastro.net', 8.0, 16, 16.0);
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
                    this.rateLimiter.handle429(parsedUrl.host, retryAfter, attempts);
                    const error = new Error(`Rate limit reached (HTTP 429) for ${parsedUrl.host}`);
                    error.status = 429;
                    error.retryAfter = retryAfter;
                    if (attempts >= maxAttempts)
                        throw error;
                    continue;
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
        const url = `${this.apiUrl}/dados?manga_id=${sourceWorkId}`;
        const response = await this.request(url);
        const item = response.data?.[0];
        if (!item)
            throw new Error(`Work not found on Manhastro: ${sourceWorkId}`);
        const title = item.titulo_brasil?.trim() || item.titulo.trim();
        const cover = item.imagem
            ? item.imagem.startsWith('http')
                ? item.imagem
                : `https://${item.imagem}`
            : null;
        let kind = 'MANGA';
        const cat = (item.categoria || '').toLowerCase();
        if (cat.includes('manhwa'))
            kind = 'MANHWA';
        else if (cat.includes('manhua'))
            kind = 'MANHUA';
        else if (cat.includes('webtoon'))
            kind = 'WEBTOON';
        let status = 'ONGOING';
        const st = (item.status || '').toLowerCase();
        if (st.includes('completed') || st.includes('completo'))
            status = 'COMPLETED';
        else if (st.includes('hiat'))
            status = 'HIATUS';
        else if (st.includes('cancel'))
            status = 'CANCELLED';
        return {
            sourceWorkId: String(item.manga_id),
            title,
            slug: slugify(title),
            coverUrl: cover,
            synopsis: item.descricao_brasil?.trim() || item.descricao?.trim() || '',
            kind,
            status,
            genres: item.generos || [],
            raw: item,
        };
    }
    async fetchChapters(sourceWorkId) {
        const url = `${this.apiUrl}/dados/${sourceWorkId}`;
        const response = await this.request(url);
        const rawChapters = response.data || [];
        const chapters = rawChapters.map((ch) => {
            const num = extractChapterNumber(ch.capitulo_nome);
            return {
                sourceChapterId: String(ch.capitulo_id),
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
    async searchWorks(query) {
        const clean = query.trim();
        if (!clean)
            return [];
        const url = `${this.apiUrl}/dados?nome=${encodeURIComponent(clean)}`;
        try {
            const response = await this.request(url);
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
            this.logger.warn(`Search failed on Manhastro for query "${query}"`, { error: err?.message });
            return [];
        }
    }
}
