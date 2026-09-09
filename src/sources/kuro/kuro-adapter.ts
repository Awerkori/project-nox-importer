import { SourceAdapter, SourceWorkSummary, SourceWorkDetails, SourceChapterSummary } from '../types.js';
import { HostRateLimiter } from '../../core/rate-limiter.js';
import { Logger } from '../../core/logger.js';
import { decryptVSecure, DEFAULT_ENC_KEY } from './kuro-decryptor.js';

function slugify(text: string): string {
  return text
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '');
}

function buildThumbnailUrl(cdnUrl: string, path: string): string {
  const cleanPath = path.replace(/^\//, '').replace(/^uploads\//, '');
  return `${cdnUrl}/${cleanPath}`;
}

export class KuroAdapter implements SourceAdapter {
  readonly id = 'kuro';
  readonly name = 'Kuro Mangas';
  readonly baseUrl = 'https://kuromangas.com';

  private apiUrl = 'https://kuromangas.com/api';
  private cdnUrl = 'https://cdn.kuromangas.com';
  private logger = new Logger('KuroAdapter');
  private encKey = DEFAULT_ENC_KEY;

  // In-memory session cached during process lifetime (never stored in database or printed)
  private sessionCookie: string | null = null;
  private clientToken: string | null = null;
  private cfClearance: string | null = null;

  constructor(
    private rateLimiter: HostRateLimiter = new HostRateLimiter(2.0),
    private transport: typeof fetch = fetch
  ) {
    this.rateLimiter.setHostRate('kuromangas.com', 2.0, 4, 4.0);
    this.rateLimiter.setHostRate('cdn.kuromangas.com', 8.0, 16, 16.0);

    // Initialize from safe environment variables if present
    if (process.env.KURO_COOKIE) {
      const matchSession = process.env.KURO_COOKIE.match(/kuro_session=([^;]+)/);
      const matchKn = process.env.KURO_COOKIE.match(/_kn=([^;]+)/);
      const matchCf = process.env.KURO_COOKIE.match(/cf_clearance=([^;]+)/);
      if (matchSession) this.sessionCookie = matchSession[1];
      if (matchKn) this.clientToken = matchKn[1];
      if (matchCf) this.cfClearance = matchCf[1];
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

  hasValidSession(): boolean {
    return Boolean(this.sessionCookie && this.clientToken);
  }

  clearSession(): void {
    this.sessionCookie = null;
    this.clientToken = null;
  }

  async login(force = false): Promise<boolean> {
    if (!force && this.hasValidSession()) {
      return true;
    }

    const email = process.env.KURO_EMAIL;
    const password = process.env.KURO_PASSWORD;

    if (!email || !password) {
      return false;
    }

    try {
      const loginUrl = `${this.apiUrl}/auth/login`;
      const headers: Record<string, string> = {
        'Content-Type': 'application/json',
        Accept: 'application/json',
        Referer: `${this.baseUrl}/login`,
        Origin: this.baseUrl,
        'User-Agent':
          'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
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
        // Collect cookies supporting both getSetCookie array and standard header string
        let cookieHeaders: string[] = [];
        if (typeof (res.headers as any).getSetCookie === 'function') {
          cookieHeaders = (res.headers as any).getSetCookie();
        } else {
          const raw = res.headers.get('set-cookie');
          if (raw) cookieHeaders = [raw];
        }

        const combinedCookies = cookieHeaders.join('; ');
        const matchSession = combinedCookies.match(/kuro_session=([^;]+)/);
        const matchKn = combinedCookies.match(/_kn=([^;]+)/);
        const matchCf = combinedCookies.match(/cf_clearance=([^;]+)/);

        if (matchSession && matchKn) {
          this.sessionCookie = matchSession[1];
          this.clientToken = matchKn[1];
          if (matchCf) this.cfClearance = matchCf[1];
          this.logger.info('Kuro authentication successful (session established in memory)');
          return true;
        }
      } else {
        const bodySnippet = await res.text().catch(() => '');
        this.logger.warn(`Kuro login failed: HTTP ${res.status} - ${bodySnippet.slice(0, 150)}`);
      }
    } catch (err: any) {
      this.logger.warn('Failed to login with Kuro credentials from environment', {
        error: err?.message,
      });
    }

    return false;
  }

  private async getAuthHeaders(): Promise<Record<string, string>> {
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

  private async request<T>(url: string, options: RequestInit = {}): Promise<T> {
    const parsedUrl = new URL(url);
    await this.rateLimiter.acquire(parsedUrl.host);

    let attempts = 0;
    const maxAttempts = 3;

    while (attempts < maxAttempts) {
      attempts++;
      try {
        const authHeaders = await this.getAuthHeaders();
        const response = await this.transport(url, {
          ...options,
          headers: {
            Accept: 'application/json, text/plain, */*',
            Referer: `${this.baseUrl}/catalogo`,
            Origin: this.baseUrl,
            'User-Agent':
              'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
            ...authHeaders,
            ...(options.headers || {}),
          },
          signal: AbortSignal.timeout(30_000),
        });

        if (response.status === 401 || response.status === 403) {
          this.clearSession();
          if (attempts < maxAttempts && Boolean(process.env.KURO_EMAIL && process.env.KURO_PASSWORD)) {
            this.logger.info('Kuro session expired or unauthorized. Auto-renewing session...');
            const renewed = await this.login(true);
            if (renewed) {
              continue;
            }
          }
          throw new Error(
            'Kuro requires authentication: session expired or invalid credentials'
          );
        }

        if (response.status === 429) {
          const retryAfter = response.headers.get('Retry-After');
          this.rateLimiter.handle429(parsedUrl.host, retryAfter, attempts);
          const error: any = new Error(`Rate limit reached (HTTP 429) for ${parsedUrl.host}`);
          error.status = 429;
          error.retryAfter = retryAfter;
          if (attempts >= maxAttempts) throw error;
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
        if (dataKey && json && typeof json === 'object' && '_v_secure' in json) {
          return decryptVSecure(json._v_secure, dataKey, this.encKey) as T;
        }

        return json as T;
      } catch (err: any) {
        if (err.message.includes('requires authentication') || attempts >= maxAttempts) {
          throw err;
        }
        await new Promise((r) => setTimeout(r, 1000 * attempts));
      }
    }
    throw new Error(`Kuro request failed after ${maxAttempts} attempts`);
  }

  async fetchUpdatedWorks(
    cursor?: string | null,
    options?: { mode?: 'bootstrap' | 'maintenance' }
  ): Promise<{
    works: SourceWorkSummary[];
    nextCursor: string | null;
  }> {
    const mode = options?.mode || 'maintenance';
    const page = cursor ? parseInt(cursor, 10) : 1;
    const limit = 24;

    if (mode === 'bootstrap') {
      const url = `${this.apiUrl}/mangas?page=${page}&limit=${limit}&sort=view_count&order=DESC`;
      const response = await this.request<{
        data: Array<{
          id: number;
          title: string;
          cover_image?: string | null;
        }>;
        pagination?: { hasNext?: boolean; total_pages?: number };
      }>(url);

      const items = response.data || [];
      const works: SourceWorkSummary[] = items.map((item) => ({
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
    } else {
      // Maintenance: fetch recently updated chapters
      const url = `${this.apiUrl}/chapters/recent?page=${page}&limit=${limit}&days=30`;
      const response = await this.request<{
        data: Array<{
          manga_id: number;
          manga_title: string;
          manga_cover?: string | null;
        }>;
        pagination?: { hasNext?: boolean };
      }>(url);

      const items = response.data || [];
      const works: SourceWorkSummary[] = items.map((item) => ({
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

  async fetchWorkDetails(sourceWorkId: string): Promise<SourceWorkDetails> {
    const url = `${this.apiUrl}/mangas/${sourceWorkId}`;
    const response = await this.request<{
      manga: {
        id: number;
        title: string;
        description?: string | null;
        status?: string | null;
        cover_image?: string | null;
        author?: string | null;
        artist?: string | null;
        genres?: string[];
        alternative_titles?: string[];
      };
    }>(url);

    const manga = response.manga;
    if (!manga) throw new Error(`Work not found on Kuro: ${sourceWorkId}`);

    let status: 'ONGOING' | 'COMPLETED' | 'HIATUS' | 'CANCELLED' = 'ONGOING';
    const s = (manga.status || '').toLowerCase();
    if (s.includes('complet')) status = 'COMPLETED';
    else if (s.includes('hiat')) status = 'HIATUS';
    else if (s.includes('cancel')) status = 'CANCELLED';

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

  async fetchChapters(sourceWorkId: string): Promise<SourceChapterSummary[]> {
    const url = `${this.apiUrl}/mangas/${sourceWorkId}`;
    const response = await this.request<{
      manga: { id: number };
      chapters?: Array<{
        id: number;
        title?: string | null;
        chapter_number?: string | null;
        upload_date?: string | null;
      }>;
    }>(url);

    const rawChapters = response.chapters || [];

    const chapters: SourceChapterSummary[] = rawChapters.map((ch) => {
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

  async fetchChapterPages(sourceChapterId: string, _chapterNumber?: number): Promise<string[]> {
    const url = `${this.apiUrl}/chapters/${sourceChapterId}`;
    const response = await this.request<{
      id: number;
      pages?: string[];
    }>(url);

    const pages = response.pages || [];
    if (pages.length === 0) {
      throw new Error(`Kuro returned 0 pages for chapter ${sourceChapterId}`);
    }

    return pages.map((pageUrl) => {
      const clean = pageUrl.replace(/^\/uploads\//, '/');
      return clean.startsWith('http') ? clean : `${this.cdnUrl}${clean}`;
    });
  }

  async searchWorks(query: string): Promise<SourceWorkSummary[]> {
    const clean = query.trim();
    if (!clean) return [];

    const url = `${this.apiUrl}/mangas?search=${encodeURIComponent(clean)}&page=1&limit=20`;

    try {
      const response = await this.request<{
        data: Array<{
          id: number;
          title: string;
          cover_image?: string | null;
        }>;
      }>(url);

      const items = response.data || [];
      return items.map((item) => ({
        sourceWorkId: String(item.id),
        title: item.title,
        slug: slugify(item.title),
        coverUrl: item.cover_image ? buildThumbnailUrl(this.cdnUrl, item.cover_image) : null,
        updatedAt: new Date().toISOString(),
      }));
    } catch (err: any) {
      this.logger.warn(`Search failed or unauthorized on Kuro for query "${query}"`, {
        error: err?.message,
      });
      return [];
    }
  }
}
