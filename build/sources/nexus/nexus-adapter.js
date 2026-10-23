import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { HostRateLimiter } from '../../core/rate-limiter.js';
import { Logger } from '../../core/logger.js';
import { extractStoredMappingMetadata } from '../../lite/mapping-metadata.js';
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
const ANON_KEY = 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJzdXBhYmFzZSIsImlhdCI6MTc4NzgwMjAwMCwiZXhwIjo0OTQzNDc1NjAwLCJyb2xlIjoiYW5vbiJ9.Cnl8Jw2DeKe84OAkmJYfO33xlcZsw0TC2Nw_il0tpRs';
export class NexusAdapter {
    rateLimiter;
    transport;
    id = 'nexus';
    name = 'Nexus Mangas';
    baseUrl = 'https://www.nexusmangas.com';
    admissionProbe = {
        url: 'https://supabase.nexusmangas.com/rest/v1/chapters?select=work_id&limit=1',
        headers: {
            apikey: ANON_KEY,
            Authorization: `Bearer ${ANON_KEY}`,
            Accept: 'application/json',
        },
        expectedType: 'json',
    };
    apiUrl = 'https://supabase.nexusmangas.com/rest/v1';
    functionsUrl = 'https://supabase.nexusmangas.com/functions/v1';
    logger = new Logger('NexusAdapter');
    catalogItems = [];
    catalogByNormalizedTitle = new Map();
    catalogBySlug = new Map();
    catalogById = new Map();
    catalogLoaded = false;
    constructor(rateLimiter = new HostRateLimiter(4.0), transport = fetch) {
        this.rateLimiter = rateLimiter;
        this.transport = transport;
        this.rateLimiter.setHostRate('supabase.nexusmangas.com', 4.0, 8, 8.0);
        this.rateLimiter.setHostRate('cdn.nexusmangas.com', 8.0, 16, 16.0);
    }
    ensureCatalogLoaded() {
        if (this.catalogLoaded)
            return true;
        try {
            const currentDir = typeof __dirname !== 'undefined'
                ? __dirname
                : path.dirname(fileURLToPath(import.meta.url));
            const catalogPath = path.resolve(currentDir, 'nexus-catalog.json');
            if (fs.existsSync(catalogPath)) {
                const raw = fs.readFileSync(catalogPath, 'utf8');
                this.catalogItems = JSON.parse(raw);
                for (const item of this.catalogItems) {
                    this.catalogById.set(String(item.id), item);
                    if (item.slug)
                        this.catalogBySlug.set(String(item.slug), item);
                    const parsedAlts = item.alternative_title
                        ? extractStoredMappingMetadata({ alternative_title: item.alternative_title }, item.title || '').aliases
                        : [];
                    const titles = [item.title, ...parsedAlts].filter(Boolean);
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
                this.logger.info(`Loaded Nexus local catalog index with ${this.catalogItems.length} works`);
                return true;
            }
        }
        catch (err) {
            this.logger.warn('Failed to load local Nexus catalog cache', { error: err?.message });
        }
        return false;
    }
    get headers() {
        return {
            apikey: ANON_KEY,
            Authorization: `Bearer ${ANON_KEY}`,
            Accept: 'application/json',
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
                    continue;
                }
                if (!response.ok) {
                    const errText = await response.text().catch(() => '');
                    const error = new Error(`Nexus request failed: HTTP ${response.status} - ${errText.slice(0, 200)}`);
                    error.status = response.status;
                    throw error;
                }
                this.rateLimiter.recordSuccess(parsedUrl.host);
                return (await response.json());
            }
            catch (err) {
                if (err?.status && err.status >= 400 && err.status < 500 && err.status !== 429) {
                    throw err;
                }
                if (attempts >= maxAttempts)
                    throw err;
                await new Promise((r) => setTimeout(r, 1000 * attempts));
            }
        }
        throw new Error(`Nexus request failed after ${maxAttempts} attempts`);
    }
    async fetchUpdatedWorks(cursor, options) {
        const mode = options?.mode || (cursor ? 'bootstrap' : 'maintenance');
        const limit = 24;
        let url = `${this.apiUrl}/chapters?select=work_id,created_at&order=created_at.desc&limit=${limit}`;
        if (mode === 'bootstrap' && cursor) {
            url += `&created_at=lt.${encodeURIComponent(cursor)}`;
        }
        else if (mode === 'maintenance' && cursor) {
            url += `&created_at=gt.${encodeURIComponent(cursor)}`;
        }
        const latest = await this.request(url);
        if (!latest || latest.length === 0) {
            return { works: [], nextCursor: mode === 'maintenance' ? cursor ?? null : null };
        }
        const nextCursor = mode === 'bootstrap'
            ? latest[latest.length - 1].created_at
            : latest[0].created_at;
        const workIds = Array.from(new Set(latest.map((item) => item.work_id))).filter(Boolean);
        if (workIds.length === 0) {
            return { works: [], nextCursor };
        }
        const worksUrl = `${this.apiUrl}/works?id=in.(${workIds.join(',')})&select=id,title,slug,cover_url,updated_at`;
        const workRows = await this.request(worksUrl);
        const summaries = (workRows || []).map((w) => ({
            sourceWorkId: w.id,
            title: w.title,
            slug: w.slug,
            coverUrl: w.cover_url || null,
            updatedAt: w.updated_at,
        }));
        return { works: summaries, nextCursor };
    }
    async fetchWorkDetails(sourceWorkId) {
        let row;
        if (this.transport === fetch && this.ensureCatalogLoaded()) {
            row = this.catalogById.get(String(sourceWorkId)) || this.catalogBySlug.get(String(sourceWorkId));
        }
        if (!row) {
            const url = `${this.apiUrl}/works?id=eq.${sourceWorkId}&select=id,title,slug,cover_url,description,alternative_title,status,type,content_rating,release_year,author,artist,work_genres(genre:genres(name))`;
            const rows = await this.request(url);
            row = rows?.[0];
        }
        if (!row) {
            throw new Error(`Work not found on Nexus: ${sourceWorkId}`);
        }
        // Normalize kind
        let kind = 'UNKNOWN';
        const rawType = (row.type || '').toUpperCase();
        if (rawType.includes('MANHWA'))
            kind = 'MANHWA';
        else if (rawType.includes('MANHUA'))
            kind = 'MANHUA';
        else if (rawType.includes('WEBTOON'))
            kind = 'WEBTOON';
        // Normalize status
        let status = 'UNKNOWN';
        const rawStatus = (row.status || '').toUpperCase().replace(/[^A-Z0-9]/g, '');
        if (rawStatus.includes('COMPLET') || rawStatus.includes('CONCLU'))
            status = 'COMPLETED';
        else if (rawStatus.includes('HIAT'))
            status = 'HIATUS';
        else if (rawStatus.includes('CANCEL'))
            status = 'CANCELLED';
        else if (rawStatus.includes('ONGO') || rawStatus.includes('RELEAS') || rawStatus.includes('ANDAM') || rawStatus.includes('LANC'))
            status = 'ONGOING';
        // Extract genres
        const genres = [];
        if (Array.isArray(row.work_genres)) {
            for (const wg of row.work_genres) {
                if (wg.genre?.name)
                    genres.push(wg.genre.name);
            }
        }
        const alternativeTitles = [];
        if (row.alternative_title && typeof row.alternative_title === 'string') {
            const trimmed = row.alternative_title.trim();
            if (trimmed.length >= 1) {
                if (/[\r\n]+/.test(trimmed)) {
                    alternativeTitles.push(...trimmed.split(/[\r\n]+/).map((s) => s.trim()).filter(Boolean));
                }
                else if (/;+/.test(trimmed)) {
                    alternativeTitles.push(...trimmed.split(/;+/).map((s) => s.trim()).filter(Boolean));
                }
                else if (/\s+\/\s+/.test(trimmed)) {
                    alternativeTitles.push(...trimmed.split(/\s+\/\s+/).map((s) => s.trim()).filter(Boolean));
                }
                else if (/,\s+/.test(trimmed)) {
                    const parts = trimmed.split(/,\s+/).map((s) => s.trim()).filter(Boolean);
                    const firstWord = (parts[0] || '').toLowerCase().replace(/[^a-z\u00C0-\u017F]/g, '');
                    const isInterjection = ['hello', 'oh', 'ah', 'hey', 'dear', 'bye', 'ola', 'olá', 'yes', 'no'].includes(firstWord);
                    const isContinuation = parts.some((p, idx) => idx > 0 &&
                        (/^(?:for|and|or|so|because|but|yet|too|also|with|without|nor|mas|e|ou|pois|porque|com|sem|para|por)\b/i.test(p) ||
                            /^[a-z]/.test(p)));
                    if (parts.length > 1 && !isInterjection && !isContinuation) {
                        alternativeTitles.push(...parts);
                    }
                    else {
                        alternativeTitles.push(trimmed);
                    }
                }
                else {
                    alternativeTitles.push(trimmed);
                }
            }
        }
        const year = row.release_year ? parseInt(String(row.release_year), 10) : undefined;
        return {
            sourceWorkId: row.id,
            title: row.title,
            slug: row.slug,
            coverUrl: row.cover_url || null,
            synopsis: row.description || '',
            author: row.author || '',
            artist: row.artist || '',
            kind,
            status,
            year: isNaN(year) ? undefined : year,
            genres,
            alternativeTitles,
            raw: row,
        };
    }
    async fetchChapters(sourceWorkId) {
        const url = `${this.apiUrl}/chapters?work_id=eq.${sourceWorkId}&select=id,number,title,created_at,page_count&order=number.asc&limit=1000`;
        const rows = await this.request(url);
        return (rows || []).map((ch) => ({
            sourceChapterId: ch.id,
            displayNumber: String(ch.number ?? ''),
            number: typeof ch.number === 'number' ? ch.number : parseFloat(ch.number || '0'),
            title: ch.title || undefined,
            createdAt: ch.created_at,
            pageCount: ch.page_count ?? undefined,
        }));
    }
    async fetchChapterPages(sourceChapterId) {
        const url = `${this.functionsUrl}/read-chapter`;
        const res = await this.request(url, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'x-nexus-client': 'reader-v3',
            },
            body: JSON.stringify({ chapterId: sourceChapterId }),
        });
        if (!res.success || !res.chapter?.pages || res.chapter.pages.length === 0) {
            if (res.access && res.access.allowed === false) {
                throw new Error(`Nexus chapter ${sourceChapterId} access denied (${res.access.reason || 'LICENSED'})`);
            }
            throw new Error(`Failed to fetch pages from Nexus for chapter ${sourceChapterId}`);
        }
        return res.chapter.pages;
    }
    searchCatalog(query) {
        const norm = normalizeText(query);
        const slug = slugify(query);
        const matches = [];
        const seen = new Set();
        const bySlug = this.catalogBySlug.get(slug);
        if (bySlug && !seen.has(bySlug.id)) {
            matches.push(bySlug);
            seen.add(bySlug.id);
        }
        const exact = this.catalogByNormalizedTitle.get(norm) || [];
        for (const item of exact) {
            if (!seen.has(item.id)) {
                matches.push(item);
                seen.add(item.id);
            }
        }
        if (matches.length < 5 && norm.length >= 4) {
            for (const item of this.catalogItems) {
                if (seen.has(item.id))
                    continue;
                const itemNorm = normalizeText(item.title || '');
                const altNorm = item.alternative_title ? normalizeText(item.alternative_title) : '';
                const matched = (itemNorm.length >= 4 && (itemNorm.includes(norm) || norm.includes(itemNorm))) ||
                    (altNorm.length >= 4 && (altNorm.includes(norm) || norm.includes(altNorm)));
                if (matched) {
                    matches.push(item);
                    seen.add(item.id);
                    if (matches.length >= 10)
                        break;
                }
            }
        }
        return matches.map((w) => ({
            sourceWorkId: w.id,
            title: w.title,
            slug: w.slug,
            coverUrl: w.cover_url || null,
            updatedAt: w.updated_at,
        }));
    }
    async searchWorks(query) {
        const clean = query.replace(/[?,()\*"]/g, ' ').replace(/\s+/g, ' ').trim();
        if (!clean)
            return [];
        if (this.transport === fetch && this.ensureCatalogLoaded()) {
            return this.searchCatalog(clean);
        }
        const words = clean.split(/\s+/).filter(Boolean).map(encodeURIComponent).join('*');
        const url = `${this.apiUrl}/works?or=(title.ilike.*${words}*,slug.ilike.*${words}*,alternative_title.ilike.*${words}*)&select=id,title,slug,cover_url,updated_at&limit=20`;
        try {
            const rows = await this.request(url);
            return (rows || []).map((w) => ({
                sourceWorkId: w.id,
                title: w.title,
                slug: w.slug,
                coverUrl: w.cover_url || null,
                updatedAt: w.updated_at,
            }));
        }
        catch (err) {
            if (err?.status === 404 || err?.status === 400 || err?.message?.includes('HTTP 404') || err?.message?.includes('HTTP 400')) {
                return [];
            }
            this.logger.warn(`Search failed on Nexus for query "${query}"`, { error: err?.message });
            throw err;
        }
    }
}
