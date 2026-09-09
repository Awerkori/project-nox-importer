import { HostRateLimiter } from '../../core/rate-limiter.js';
import { Logger } from '../../core/logger.js';
const ANON_KEY = 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJzdXBhYmFzZSIsImlhdCI6MTc4NzgwMjAwMCwiZXhwIjo0OTQzNDc1NjAwLCJyb2xlIjoiYW5vbiJ9.Cnl8Jw2DeKe84OAkmJYfO33xlcZsw0TC2Nw_il0tpRs';
export class NexusAdapter {
    rateLimiter;
    transport;
    id = 'nexus';
    name = 'Nexus Mangas';
    baseUrl = 'https://www.nexusmangas.com';
    apiUrl = 'https://supabase.nexusmangas.com/rest/v1';
    functionsUrl = 'https://supabase.nexusmangas.com/functions/v1';
    logger = new Logger('NexusAdapter');
    constructor(rateLimiter = new HostRateLimiter(2.0), transport = fetch) {
        this.rateLimiter = rateLimiter;
        this.transport = transport;
        this.rateLimiter.setHostRate('supabase.nexusmangas.com', 2.0, 4, 4.0);
        this.rateLimiter.setHostRate('cdn.nexusmangas.com', 8.0, 16, 16.0);
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
                    throw new Error(`Nexus request failed: HTTP ${response.status} - ${errText.slice(0, 200)}`);
                }
                this.rateLimiter.recordSuccess(parsedUrl.host);
                return (await response.json());
            }
            catch (err) {
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
        const url = `${this.apiUrl}/works?id=eq.${sourceWorkId}&select=id,title,slug,cover_url,description,alternative_title,status,type,content_rating,release_year,author,artist,work_genres(genre:genres(name))`;
        const rows = await this.request(url);
        const row = rows?.[0];
        if (!row) {
            throw new Error(`Work not found on Nexus: ${sourceWorkId}`);
        }
        // Normalize kind
        let kind = 'MANGA';
        const rawType = (row.type || '').toUpperCase();
        if (rawType.includes('MANHWA'))
            kind = 'MANHWA';
        else if (rawType.includes('MANHUA'))
            kind = 'MANHUA';
        else if (rawType.includes('WEBTOON'))
            kind = 'WEBTOON';
        // Normalize status
        let status = 'ONGOING';
        const rawStatus = (row.status || '').toUpperCase();
        if (rawStatus.includes('COMPLET') || rawStatus.includes('CONCLU'))
            status = 'COMPLETED';
        else if (rawStatus.includes('HIAT'))
            status = 'HIATUS';
        else if (rawStatus.includes('CANCEL'))
            status = 'CANCELLED';
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
            alternativeTitles.push(...row.alternative_title
                .split(/[,;\n]/)
                .map((s) => s.trim())
                .filter(Boolean));
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
            throw new Error(`Failed to fetch pages from Nexus for chapter ${sourceChapterId}`);
        }
        return res.chapter.pages;
    }
}
