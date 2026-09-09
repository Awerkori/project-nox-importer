import { HostRateLimiter } from '../../core/rate-limiter.js';
import { Logger } from '../../core/logger.js';
import { decryptVSecure, DEFAULT_ENC_KEY } from './kuro-decryptor.js';
function slugify(text) {
    return text
        .normalize('NFD')
        .replace(/[\u0300-\u036f]/g, '')
        .toLowerCase()
        .replace(/[^a-z0-9]+/g, '-')
        .replace(/^-+|-+$/g, '');
}
function buildThumbnailUrl(cdnUrl, path) {
    const cleanPath = path.replace(/^\//, '').replace(/^uploads\//, '');
    return `${cdnUrl}/${cleanPath}`;
}
export class KuroAdapter {
    rateLimiter;
    transport;
    id = 'kuro';
    name = 'Kuro Mangas';
    baseUrl = 'https://kuromangas.com';
    apiUrl = 'https://kuromangas.com/api';
    cdnUrl = 'https://cdn.kuromangas.com';
    logger = new Logger('KuroAdapter');
    encKey = DEFAULT_ENC_KEY;
    // Cloudflare Workers internal bridge for zero-403 bypass
    bridgeUrl = null;
    bridgeToken = null;
    // In-memory session cached during process lifetime (never stored in database or printed)
    sessionCookie = null;
    clientToken = null;
    cfClearance = null;
    constructor(rateLimiter = new HostRateLimiter(2.0), transport = fetch) {
        this.rateLimiter = rateLimiter;
        this.transport = transport;
        this.rateLimiter.setHostRate('kuromangas.com', 2.0, 4, 4.0);
        this.rateLimiter.setHostRate('cdn.kuromangas.com', 8.0, 16, 16.0);
        const baseUrl = process.env.NOX_MANGA_URL || 'https://manga.project-nox-awerkori.workers.dev';
        this.bridgeToken = process.env.NOX_STORAGE_BRIDGE_TOKEN || null;
        if (this.bridgeToken) {
            this.bridgeUrl = `${baseUrl.replace(/\/$/, '')}/api/internal/importer/kuro-bridge`;
        }
        // Initialize from safe environment variables if present
        if (process.env.KURO_COOKIE) {
            const matchSession = process.env.KURO_COOKIE.match(/kuro_session=([^;]+)/);
            const matchKn = process.env.KURO_COOKIE.match(/_kn=([^;]+)/);
            const matchCf = process.env.KURO_COOKIE.match(/cf_clearance=([^;]+)/);
            if (matchSession)
                this.sessionCookie = matchSession[1];
            if (matchKn)
                this.clientToken = matchKn[1];
            if (matchCf)
                this.cfClearance = matchCf[1];
        }
        if (process.env.KURO_SESSION) {
            this.sessionCookie = process.env.KURO_SESSION;
        }
        if (process.env.KURO_CLIENT_TOKEN) {
            this.clientToken = process.env.KURO_CLIENT_TOKEN;
        }
        if (process.env.KURO_CF_CLEARANCE) {
            this.cfClearance = process.env.KURO_CF_CLEARANCE;
        }
    }
    hasValidSession() {
        return Boolean(this.sessionCookie && this.clientToken);
    }
    clearSession() {
        this.sessionCookie = null;
        this.clientToken = null;
    }
    async login(force = false) {
        if (!force && this.hasValidSession()) {
            return true;
        }
        const email = process.env.KURO_EMAIL;
        const password = process.env.KURO_PASSWORD;
        if (!email || !password) {
            return false;
        }
        // 1. Try login via internal Cloudflare Workers bridge first
        if (this.bridgeUrl && this.bridgeToken) {
            try {
                const res = await this.transport(this.bridgeUrl, {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                        Authorization: `Bearer ${this.bridgeToken}`,
                    },
                    body: JSON.stringify({
                        url: `${this.apiUrl}/auth/login`,
                        method: 'POST',
                        headers: {
                            'Content-Type': 'application/json',
                            Accept: 'application/json',
                            Origin: this.baseUrl,
                            Referer: `${this.baseUrl}/login`,
                        },
                        body: { email, password, rememberMe: true },
                    }),
                    signal: AbortSignal.timeout(30_000),
                });
                if (res.ok) {
                    const bridgeData = (await res.json());
                    if (bridgeData.status === 200 && Array.isArray(bridgeData.cookies)) {
                        const combinedCookies = bridgeData.cookies.join('; ');
                        const matchSession = combinedCookies.match(/kuro_session=([^;]+)/);
                        const matchKn = combinedCookies.match(/_kn=([^;]+)/);
                        const matchCf = combinedCookies.match(/cf_clearance=([^;]+)/);
                        if (matchSession && matchKn) {
                            this.sessionCookie = matchSession[1];
                            this.clientToken = matchKn[1];
                            if (matchCf)
                                this.cfClearance = matchCf[1];
                            this.logger.info('Kuro authentication successful (via Cloudflare Workers bridge)');
                            return true;
                        }
                    }
                }
            }
            catch (bridgeErr) {
                this.logger.warn('Kuro bridge login encountered error, falling back to direct login', {
                    error: bridgeErr?.message,
                });
            }
        }
        // 2. Direct login fallback
        try {
            const loginUrl = `${this.apiUrl}/auth/login`;
            const headers = {
                'Content-Type': 'application/json',
                Accept: 'application/json',
                Referer: `${this.baseUrl}/login`,
                Origin: this.baseUrl,
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
            };
            if (this.cfClearance) {
                headers['Cookie'] = `cf_clearance=${this.cfClearance}`;
            }
            const res = await this.transport(loginUrl, {
                method: 'POST',
                headers,
                body: JSON.stringify({ email, password, rememberMe: true }),
            });
            if (res.ok) {
                let cookieHeaders = [];
                if (typeof res.headers.getSetCookie === 'function') {
                    cookieHeaders = res.headers.getSetCookie();
                }
                else {
                    const raw = res.headers.get('set-cookie');
                    if (raw)
                        cookieHeaders = [raw];
                }
                const combinedCookies = cookieHeaders.join('; ');
                const matchSession = combinedCookies.match(/kuro_session=([^;]+)/);
                const matchKn = combinedCookies.match(/_kn=([^;]+)/);
                const matchCf = combinedCookies.match(/cf_clearance=([^;]+)/);
                if (matchSession && matchKn) {
                    this.sessionCookie = matchSession[1];
                    this.clientToken = matchKn[1];
                    if (matchCf)
                        this.cfClearance = matchCf[1];
                    this.logger.info('Kuro authentication successful (direct session established in memory)');
                    return true;
                }
            }
            else {
                const bodySnippet = await res.text().catch(() => '');
                this.logger.warn(`Kuro direct login failed: HTTP ${res.status} - ${bodySnippet.slice(0, 150)}`);
            }
        }
        catch (err) {
            this.logger.warn('Failed to login with Kuro credentials from environment', {
                error: err?.message,
            });
        }
        return false;
    }
    async getAuthHeaders() {
        const buildCookieHeader = () => {
            const cookieParts = [
                `kuro_session=${this.sessionCookie}`,
                `_kn=${this.clientToken}`,
            ];
            if (this.cfClearance) {
                cookieParts.push(`cf_clearance=${this.cfClearance}`);
            }
            return cookieParts.join('; ');
        };
        // If already have session, return cookies
        if (this.sessionCookie && this.clientToken) {
            return {
                Cookie: buildCookieHeader(),
                'X-Client-Token': this.clientToken,
            };
        }
        // Try authenticating with email/password if available
        const loggedIn = await this.login();
        if (loggedIn && this.sessionCookie && this.clientToken) {
            return {
                Cookie: buildCookieHeader(),
                'X-Client-Token': this.clientToken,
            };
        }
        return {};
    }
    async request(url, options = {}) {
        const parsedUrl = new URL(url);
        await this.rateLimiter.acquire(parsedUrl.host);
        let attempts = 0;
        const maxAttempts = 3;
        while (attempts < maxAttempts) {
            attempts++;
            try {
                const authHeaders = await this.getAuthHeaders();
                // If Cloudflare Workers internal bridge is configured, route API requests through it
                if (this.bridgeUrl && this.bridgeToken) {
                    try {
                        const bridgeRes = await this.transport(this.bridgeUrl, {
                            method: 'POST',
                            headers: {
                                'Content-Type': 'application/json',
                                Authorization: `Bearer ${this.bridgeToken}`,
                            },
                            body: JSON.stringify({
                                url,
                                method: options.method || 'GET',
                                headers: {
                                    Accept: 'application/json, text/plain, */*',
                                    Origin: this.baseUrl,
                                    Referer: `${this.baseUrl}/catalogo`,
                                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
                                    ...authHeaders,
                                    ...(options.headers || {}),
                                },
                                body: options.body,
                            }),
                            signal: AbortSignal.timeout(30_000),
                        });
                        if (bridgeRes.ok) {
                            const bridgePayload = (await bridgeRes.json());
                            if (bridgePayload.status === 401 || bridgePayload.status === 403) {
                                this.clearSession();
                                if (attempts < maxAttempts && Boolean(process.env.KURO_EMAIL && process.env.KURO_PASSWORD)) {
                                    this.logger.info('Kuro session expired via bridge. Auto-renewing session...');
                                    const renewed = await this.login(true);
                                    if (renewed)
                                        continue;
                                }
                                throw new Error('Kuro requires authentication: session expired or invalid credentials');
                            }
                            if (bridgePayload.status === 429) {
                                const retryAfter = bridgePayload.headers?.['retry-after'];
                                this.rateLimiter.handle429(parsedUrl.host, retryAfter, attempts);
                                if (attempts >= maxAttempts)
                                    throw new Error(`Rate limit reached (HTTP 429) for ${parsedUrl.host}`);
                                continue;
                            }
                            if (bridgePayload.status >= 400) {
                                throw new Error(`Kuro bridge upstream failed: HTTP ${bridgePayload.status}`);
                            }
                            this.rateLimiter.recordSuccess(parsedUrl.host);
                            const dataKey = bridgePayload.headers?.['x-kuro-datakey'];
                            let json = bridgePayload.data;
                            if (!json && bridgePayload.text) {
                                try {
                                    json = JSON.parse(bridgePayload.text);
                                }
                                catch { }
                            }
                            if (json && typeof json === 'object' && '_v_secure' in json) {
                                return decryptVSecure(json._v_secure, dataKey || undefined, this.encKey);
                            }
                            return (json || bridgePayload.text);
                        }
                    }
                    catch (bridgeErr) {
                        if (bridgeErr.message?.includes('requires authentication') || bridgeErr.message?.includes('429')) {
                            throw bridgeErr;
                        }
                        this.logger.warn('Kuro bridge request encountered transient error, falling back to direct request', {
                            error: bridgeErr?.message,
                        });
                    }
                }
                // Direct request fallback
                const response = await this.transport(url, {
                    ...options,
                    headers: {
                        Accept: 'application/json, text/plain, */*',
                        Referer: `${this.baseUrl}/catalogo`,
                        Origin: this.baseUrl,
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
                        ...authHeaders,
                        ...(options.headers || {}),
                    },
                    signal: AbortSignal.timeout(30_000),
                });
                if (response.status === 401 || response.status === 403) {
                    const errText = await response.text().catch(() => '');
                    const isCloudflare = errText.includes('Just a moment') ||
                        errText.includes('Attention Required') ||
                        errText.includes('Cloudflare') ||
                        errText.includes('error code: 1033');
                    if (isCloudflare) {
                        throw new Error(`Kuro blocked by Cloudflare (HTTP ${response.status}): ${errText.slice(0, 150)}`);
                    }
                    this.clearSession();
                    if (attempts < maxAttempts && Boolean(process.env.KURO_EMAIL && process.env.KURO_PASSWORD)) {
                        this.logger.info('Kuro session expired or unauthorized. Auto-renewing session...');
                        const renewed = await this.login(true);
                        if (renewed) {
                            continue;
                        }
                    }
                    throw new Error('Kuro requires authentication: session expired or invalid credentials');
                }
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
                    throw new Error(`Kuro request failed: HTTP ${response.status} - ${errText.slice(0, 200)}`);
                }
                this.rateLimiter.recordSuccess(parsedUrl.host);
                const dataKey = response.headers.get('x-kuro-datakey');
                const json = await response.json();
                // Check if payload is encrypted with _v_secure
                if (json && typeof json === 'object' && '_v_secure' in json) {
                    return decryptVSecure(json._v_secure, dataKey || undefined, this.encKey);
                }
                return json;
            }
            catch (err) {
                if (err.message?.includes('requires authentication') || attempts >= maxAttempts) {
                    throw err;
                }
                await new Promise((r) => setTimeout(r, 1000 * attempts));
            }
        }
        throw new Error(`Kuro request failed after ${maxAttempts} attempts`);
    }
    async fetchUpdatedWorks(cursor, options) {
        const mode = options?.mode || 'maintenance';
        const page = cursor ? parseInt(cursor, 10) : 1;
        const limit = 24;
        if (mode === 'bootstrap') {
            const url = `${this.apiUrl}/mangas?page=${page}&limit=${limit}&sort=view_count&order=DESC`;
            const response = await this.request(url);
            const items = response.data || [];
            const works = items.map((item) => ({
                sourceWorkId: String(item.id),
                title: item.title,
                slug: slugify(item.title),
                coverUrl: item.cover_image ? buildThumbnailUrl(this.cdnUrl, item.cover_image) : null,
                updatedAt: new Date().toISOString(),
            }));
            const hasNext = response.pagination?.hasNext ?? (items.length >= limit);
            return {
                works,
                nextCursor: hasNext ? String(page + 1) : null,
            };
        }
        else {
            // Maintenance: fetch recently updated chapters
            const url = `${this.apiUrl}/chapters/recent?page=${page}&limit=${limit}&days=30`;
            const response = await this.request(url);
            const items = response.data || [];
            const works = items.map((item) => ({
                sourceWorkId: String(item.manga_id),
                title: item.manga_title,
                slug: slugify(item.manga_title),
                coverUrl: item.manga_cover ? buildThumbnailUrl(this.cdnUrl, item.manga_cover) : null,
                updatedAt: new Date().toISOString(),
            }));
            return {
                works,
                nextCursor: items.length > 0 ? String(page + 1) : null,
            };
        }
    }
    async fetchWorkDetails(sourceWorkId) {
        const url = `${this.apiUrl}/mangas/${sourceWorkId}`;
        const response = await this.request(url);
        const manga = response.manga;
        if (!manga)
            throw new Error(`Work not found on Kuro: ${sourceWorkId}`);
        let status = 'ONGOING';
        const s = (manga.status || '').toLowerCase();
        if (s.includes('complet'))
            status = 'COMPLETED';
        else if (s.includes('hiat'))
            status = 'HIATUS';
        else if (s.includes('cancel'))
            status = 'CANCELLED';
        return {
            sourceWorkId: String(manga.id),
            title: manga.title,
            slug: slugify(manga.title),
            coverUrl: manga.cover_image ? buildThumbnailUrl(this.cdnUrl, manga.cover_image) : null,
            synopsis: manga.description?.trim() || '',
            author: manga.author || undefined,
            artist: manga.artist || undefined,
            kind: 'MANGA',
            status,
            genres: manga.genres || [],
            alternativeTitles: manga.alternative_titles || [],
            raw: manga,
        };
    }
    async fetchChapters(sourceWorkId) {
        const url = `${this.apiUrl}/mangas/${sourceWorkId}`;
        const response = await this.request(url);
        const rawChapters = response.chapters || [];
        const chapters = rawChapters.map((ch) => {
            const num = ch.chapter_number ? parseFloat(ch.chapter_number) : 0;
            const title = ch.title
                ? `Capítulo ${ch.chapter_number} - ${ch.title}`
                : `Capítulo ${ch.chapter_number || ch.id}`;
            return {
                sourceChapterId: String(ch.id),
                number: isNaN(num) ? 0 : num,
                title,
                createdAt: ch.upload_date || new Date().toISOString(),
                pageCount: 0,
            };
        });
        return chapters.sort((a, b) => a.number - b.number);
    }
    async fetchChapterPages(sourceChapterId, _chapterNumber) {
        const url = `${this.apiUrl}/chapters/${sourceChapterId}`;
        const response = await this.request(url);
        const pages = response.pages || [];
        if (pages.length === 0) {
            throw new Error(`Kuro returned 0 pages for chapter ${sourceChapterId}`);
        }
        return pages.map((pageUrl) => {
            const clean = pageUrl.replace(/^\/uploads\//, '/');
            return clean.startsWith('http') ? clean : `${this.cdnUrl}${clean}`;
        });
    }
    async searchWorks(query) {
        const clean = query.trim();
        if (!clean)
            return [];
        const url = `${this.apiUrl}/mangas?search=${encodeURIComponent(clean)}&page=1&limit=20`;
        try {
            const response = await this.request(url);
            const items = response.data || [];
            return items.map((item) => ({
                sourceWorkId: String(item.id),
                title: item.title,
                slug: slugify(item.title),
                coverUrl: item.cover_image ? buildThumbnailUrl(this.cdnUrl, item.cover_image) : null,
                updatedAt: new Date().toISOString(),
            }));
        }
        catch (err) {
            this.logger.warn(`Search failed or unauthorized on Kuro for query "${query}"`, {
                error: err?.message,
            });
            return [];
        }
    }
}
