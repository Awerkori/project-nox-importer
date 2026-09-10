import { SourceAdapter, SourceWorkSummary, SourceWorkDetails, SourceChapterSummary } from '../types.js';
import { HostRateLimiter } from '../../core/rate-limiter.js';
import { Logger } from '../../core/logger.js';
import { decryptNexusToonsPayload, isEncryptedNexusToons } from './nexustoons-decryptor.js';

export class NexusToonsAdapter implements SourceAdapter {
  readonly id = 'nexus_toons';
  readonly name = 'Nexus Toons';
  readonly baseUrl = 'https://nexustoons.com';

  private apiUrl = 'https://nexustoons.com/api';
  private logger = new Logger('NexusToonsAdapter');

  constructor(
    private rateLimiter: HostRateLimiter = new HostRateLimiter(2.0),
    private transport: typeof fetch = fetch
  ) {
    this.rateLimiter.setHostRate('nexustoons.com', 2.0, 4, 4.0);
    this.rateLimiter.setHostRate('nx-toons.xyz', 2.0, 4, 4.0);
    this.rateLimiter.setHostRate('img.nx-toons.xyz', 8.0, 16, 16.0);
  }

  private get headers(): HeadersInit {
    return {
      Accept: 'application/json',
      Referer: `${this.baseUrl}/`,
      'User-Agent':
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
    };
  }

  private async request<T>(path: string, options: RequestInit = {}): Promise<T> {
    const url = path.startsWith('http') ? path : `${this.apiUrl}${path}`;
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
          throw new Error(`Nexus Toons request failed: HTTP ${response.status} - ${errText.slice(0, 200)}`);
        }

        this.rateLimiter.recordSuccess(parsedUrl.host);

        const rawData = await response.json();
        if (isEncryptedNexusToons(rawData)) {
          return decryptNexusToonsPayload<T>(rawData);
        }
        return rawData as T;
      } catch (err: any) {
        if (attempts >= maxAttempts) throw err;
        await new Promise((r) => setTimeout(r, 1000 * attempts));
      }
    }
    throw new Error(`Nexus Toons request failed after ${maxAttempts} attempts: ${path}`);
  }

  async fetchUpdatedWorks(
    cursor?: string | null,
    options?: { mode?: 'bootstrap' | 'maintenance' }
  ): Promise<{
    works: SourceWorkSummary[];
    nextCursor: string | null;
  }> {
    const page = cursor ? parseInt(cursor, 10) : 1;
    const sortBy = options?.mode === 'bootstrap' ? 'views' : 'lastChapterAt';
    const path = `/mangas?page=${page}&limit=50&sortBy=${sortBy}&includeNsfw=true`;

    const res = await this.request<{
      data?: Array<{
        id: number;
        slug: string;
        title: string;
        coverImage?: string | null;
        lastChapterAt?: string | null;
        updatedAt?: string | null;
      }>;
      page: number;
      pages: number;
    }>(path);

    const items = res?.data || [];
    const works: SourceWorkSummary[] = items.map((m) => ({
      sourceWorkId: m.slug,
      title: m.title,
      slug: m.slug,
      coverUrl: m.coverImage || null,
      updatedAt: m.lastChapterAt || m.updatedAt || undefined,
    }));

    const hasNext = res.page < res.pages;
    const nextCursor = hasNext ? String(page + 1) : null;

    return { works, nextCursor };
  }

  async fetchWorkDetails(sourceWorkId: string): Promise<SourceWorkDetails> {
    let cleanSlug = sourceWorkId.trim();

    // If sourceWorkId is a full URL or numeric, extract or resolve slug
    if (cleanSlug.includes('/manga/')) {
      cleanSlug = cleanSlug.substring(cleanSlug.indexOf('/manga/') + 7).replace(/\/.*$/, '');
    }

    let raw: any;
    try {
      raw = await this.request<any>(`/manga/${encodeURIComponent(cleanSlug)}`);
    } catch (err: any) {
      // Fallback: if slug lookup failed, search for it
      const searchRes = await this.searchWorks(cleanSlug);
      if (searchRes.length > 0) {
        cleanSlug = searchRes[0].slug;
        raw = await this.request<any>(`/manga/${encodeURIComponent(cleanSlug)}`);
      } else {
        throw err;
      }
    }

    if (!raw || (!raw.title && !raw.slug)) {
      throw new Error(`Work not found on Nexus Toons: ${sourceWorkId}`);
    }

    // Normalize kind
    let kind: SourceWorkDetails['kind'] = 'MANGA';
    const rawType = (raw.type || '').toUpperCase();
    if (rawType.includes('MANHWA') || rawType.includes('PORNHWA')) kind = 'MANHWA';
    else if (rawType.includes('MANHUA')) kind = 'MANHUA';
    else if (rawType.includes('WEBTOON') || rawType.includes('COMIC')) kind = 'WEBTOON';

    // Normalize status
    let status: SourceWorkDetails['status'] = 'ONGOING';
    const rawStatus = (raw.status || '').toUpperCase();
    if (rawStatus.includes('COMPLET') || rawStatus.includes('CONCLU')) status = 'COMPLETED';
    else if (rawStatus.includes('HIAT')) status = 'HIATUS';
    else if (rawStatus.includes('CANCEL')) status = 'CANCELLED';

    // Extract genres from categories
    const genres: string[] = [];
    if (Array.isArray(raw.categories)) {
      for (const cat of raw.categories) {
        const name = cat.name || cat.category?.name;
        if (name && typeof name === 'string') {
          genres.push(name.trim());
        }
      }
    }

    const alternativeTitles: string[] = [];
    if (raw.alternativeTitles && typeof raw.alternativeTitles === 'string') {
      alternativeTitles.push(
        ...raw.alternativeTitles
          .split(/[,;\n]/)
          .map((s: string) => s.trim())
          .filter(Boolean)
      );
    }

    const year = raw.releaseYear ? parseInt(String(raw.releaseYear), 10) : undefined;

    return {
      sourceWorkId: raw.slug || cleanSlug,
      title: raw.title,
      slug: raw.slug || cleanSlug,
      coverUrl: raw.coverImage || null,
      synopsis: raw.description || '',
      author: raw.author || '',
      artist: raw.artist || '',
      kind,
      status,
      year: isNaN(year!) ? undefined : year,
      genres,
      alternativeTitles,
      raw,
    };
  }

  async fetchChapters(sourceWorkId: string): Promise<SourceChapterSummary[]> {
    let cleanSlug = sourceWorkId.trim();
    if (cleanSlug.includes('/manga/')) {
      cleanSlug = cleanSlug.substring(cleanSlug.indexOf('/manga/') + 7).replace(/\/.*$/, '');
    }

    const raw = await this.request<any>(`/manga/${encodeURIComponent(cleanSlug)}`);
    const chaptersList = raw?.chapters || [];

    const summaries: SourceChapterSummary[] = chaptersList.map((ch: any) => ({
      sourceChapterId: String(ch.id),
      number: typeof ch.number === 'number' ? ch.number : parseFloat(String(ch.number || '0')),
      title: ch.title && String(ch.title).trim() !== '' ? String(ch.title).trim() : undefined,
      createdAt: ch.createdAt || undefined,
      pageCount: null,
    }));

    // Sort ascending by chapter number
    return summaries.sort((a, b) => a.number - b.number);
  }

  async fetchChapterPages(sourceChapterId: string): Promise<string[]> {
    const raw = await this.request<{
      pages?: Array<{ imageUrl?: string; pageNumber?: number }>;
      pageToken?: string;
    }>(`/read/${encodeURIComponent(sourceChapterId)}`);

    const pages = raw?.pages || [];
    if (pages.length === 0) {
      throw new Error(`Failed to fetch pages from Nexus Toons for chapter ${sourceChapterId}: empty pages list`);
    }

    // Case 1: pages contain direct imageUrl
    if (pages[0]?.imageUrl) {
      return pages.map((p) => p.imageUrl!).filter(Boolean);
    }

    // Case 2: pages use pageToken
    if (raw.pageToken) {
      return pages.map((p, idx) => `${this.baseUrl}/api/p/${raw.pageToken}/${p.pageNumber ?? idx}`);
    }

    throw new Error(`Failed to resolve image URLs from Nexus Toons for chapter ${sourceChapterId}`);
  }

  async searchWorks(query: string): Promise<SourceWorkSummary[]> {
    const clean = query.trim();
    if (!clean) return [];

    try {
      const res = await this.request<{
        data?: Array<{
          id: number;
          slug: string;
          title: string;
          coverImage?: string | null;
          lastChapterAt?: string | null;
          updatedAt?: string | null;
        }>;
      }>(`/mangas?search=${encodeURIComponent(clean)}&limit=20&includeNsfw=true`);

      const items = res?.data || [];
      return items.map((m) => ({
        sourceWorkId: m.slug,
        title: m.title,
        slug: m.slug,
        coverUrl: m.coverImage || null,
        updatedAt: m.lastChapterAt || m.updatedAt || undefined,
      }));
    } catch (err: any) {
      this.logger.warn(`Search failed on Nexus Toons for query "${query}"`, { error: err?.message });
      return [];
    }
  }
}
