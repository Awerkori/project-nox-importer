import { SourceAdapter, SourceWorkSummary, SourceWorkDetails, SourceChapterSummary } from '../types.js';
import { HostRateLimiter } from '../../core/rate-limiter.js';
import { Logger } from '../../core/logger.js';

function slugify(text: string): string {
  return text
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '');
}

export class MangaFlixAdapter implements SourceAdapter {
  readonly id = 'mangaflix';
  readonly name = 'MangaFlix';
  readonly baseUrl = 'https://mangaflix.net';

  private apiUrl = 'https://api.mangaflix.net/v1';
  private logger = new Logger('MangaFlixAdapter');

  constructor(
    private rateLimiter: HostRateLimiter = new HostRateLimiter(2.0),
    private transport: typeof fetch = fetch
  ) {
    this.rateLimiter.setHostRate('api.mangaflix.net', 2.0, 4, 4.0);
    this.rateLimiter.setHostRate('static.mangaflix.net', 8.0, 16, 16.0);
  }

  private get headers(): HeadersInit {
    return {
      Accept: 'application/json, text/plain, */*',
      Origin: this.baseUrl,
      Referer: `${this.baseUrl}/`,
      'User-Agent':
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
    };
  }

  private async request<T>(url: string, options: RequestInit = {}): Promise<T> {
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
          const error: any = new Error(`Rate limit reached (HTTP 429) for ${parsedUrl.host}`);
          error.status = 429;
          error.retryAfter = retryAfter;
          if (attempts >= maxAttempts) throw error;
          continue;
        }

        if (!response.ok) {
          const errText = await response.text().catch(() => '');
          throw new Error(`MangaFlix request failed: HTTP ${response.status} - ${errText.slice(0, 200)}`);
        }

        this.rateLimiter.recordSuccess(parsedUrl.host);

        return (await response.json()) as T;
      } catch (err: any) {
        if (attempts >= maxAttempts) throw err;
        await new Promise((r) => setTimeout(r, 1000 * attempts));
      }
    }
    throw new Error(`MangaFlix request failed after ${maxAttempts} attempts`);
  }

  async fetchUpdatedWorks(
    cursor?: string | null,
    options?: { mode?: 'bootstrap' | 'maintenance' }
  ): Promise<{
    works: SourceWorkSummary[];
    nextCursor: string | null;
  }> {
    const mode = options?.mode || 'maintenance';

    if (mode === 'bootstrap') {
      const offset = cursor ? parseInt(cursor, 10) : 0;
      const limit = 24;
      // Default to Shounen or primary genre for full catalog backlog crawling
      const genreId = '6511eb5eae08773cd4189ec5';
      const url = `${this.apiUrl}/genres/${genreId}/mangas/?offset=${offset}&limit=${limit}&include_adult=true`;

      const response = await this.request<{
        data: Array<{
          _id: string;
          name: string;
          description?: string;
          poster?: { default_url?: string };
        }>;
        metadata?: { total: number };
      }>(url);

      const items = response.data || [];
      const total = response.metadata?.total || 0;

      const works: SourceWorkSummary[] = items.map((item) => ({
        sourceWorkId: item._id,
        title: item.name,
        slug: slugify(item.name),
        coverUrl: item.poster?.default_url || null,
        updatedAt: new Date().toISOString(),
      }));

      const nextOffset = offset + items.length;
      const hasMore = items.length === limit && nextOffset < total;

      return {
        works,
        nextCursor: hasMore ? nextOffset.toString() : null,
      };
    } else {
      // Maintenance mode: query latest releases
      const url = `${this.apiUrl}/latest-releases?selected_language=pt-br`;
      const response = await this.request<{
        data: Array<{
          _id: string;
          name: string;
          description?: string;
          poster?: { default_url?: string };
          chapters?: Array<{ created_at?: string; iso_date?: string }>;
        }>;
      }>(url);

      const items = response.data || [];
      const works: SourceWorkSummary[] = items.map((item) => ({
        sourceWorkId: item._id,
        title: item.name,
        slug: slugify(item.name),
        coverUrl: item.poster?.default_url || null,
        updatedAt: item.chapters?.[0]?.created_at || new Date().toISOString(),
      }));

      const newestTimestamp = items[0]?.chapters?.[0]?.created_at || new Date().toISOString();

      return {
        works,
        nextCursor: newestTimestamp,
      };
    }
  }

  async fetchWorkDetails(sourceWorkId: string): Promise<SourceWorkDetails> {
    const url = `${this.apiUrl}/mangas/${sourceWorkId}`;
    const response = await this.request<{
      data: {
        _id: string;
        name: string;
        description?: string;
        poster?: { default_url?: string };
        genres?: Array<{ name: string }>;
        content_type?: string;
        chapters?: Array<{
          _id: string;
          number: string;
          owners?: Array<{ name: string }>;
        }>;
      };
    }>(url);

    const data = response.data;
    if (!data) throw new Error(`Manga not found on MangaFlix: ${sourceWorkId}`);

    let kind: 'MANGA' | 'MANHWA' | 'MANHUA' | 'WEBTOON' = 'MANGA';
    const ct = (data.content_type || '').toLowerCase();
    if (ct.includes('manhwa')) kind = 'MANHWA';
    else if (ct.includes('manhua')) kind = 'MANHUA';
    else if (ct.includes('webtoon')) kind = 'WEBTOON';

    const author = data.chapters?.[0]?.owners?.[0]?.name;

    return {
      sourceWorkId: data._id,
      title: data.name,
      slug: slugify(data.name),
      coverUrl: data.poster?.default_url || null,
      synopsis: data.description || '',
      author,
      kind,
      status: 'ONGOING',
      genres: data.genres?.map((g) => g.name) || [],
      raw: data,
    };
  }

  async fetchChapters(sourceWorkId: string): Promise<SourceChapterSummary[]> {
    const url = `${this.apiUrl}/mangas/${sourceWorkId}`;
    const response = await this.request<{
      data: {
        chapters?: Array<{
          _id: string;
          name?: string;
          number: string;
          created_at?: string;
          iso_date?: string;
          number_of_complete_pages?: number;
          number_of_pages?: number;
        }>;
      };
    }>(url);

    const rawChapters = response.data?.chapters || [];

    const chapters: SourceChapterSummary[] = rawChapters.map((ch) => {
      const num = parseFloat(ch.number) || 0;
      return {
        sourceChapterId: ch._id,
        number: num,
        title: ch.name?.trim() || `Capítulo ${ch.number}`,
        createdAt: ch.iso_date || ch.created_at || new Date().toISOString(),
        pageCount: ch.number_of_complete_pages || ch.number_of_pages || 0,
      };
    });

    // Sort ascending by chapter number
    return chapters.sort((a, b) => a.number - b.number);
  }

  async fetchChapterPages(sourceChapterId: string, _chapterNumber?: number): Promise<string[]> {
    const url = `${this.apiUrl}/chapters/${sourceChapterId}?selected_language=pt-br`;
    const response = await this.request<{
      data: {
        images?: Array<{
          default_url: string;
          order?: number;
        }>;
      };
    }>(url);

    const images = response.data?.images || [];
    if (images.length === 0) {
      throw new Error(`MangaFlix returned 0 images for chapter ${sourceChapterId}`);
    }

    return images
      .sort((a, b) => (a.order || 0) - (b.order || 0))
      .map((img) => img.default_url)
      .filter((u) => Boolean(u) && (u.startsWith('http://') || u.startsWith('https://')));
  }

  async searchWorks(query: string): Promise<SourceWorkSummary[]> {
    const clean = query.trim();
    if (!clean) return [];

    const url = `${this.apiUrl}/search/mangas?query=${encodeURIComponent(clean)}&selected_language=pt-br`;

    try {
      const response = await this.request<{
        data:
          | Array<{ _id: string; name: string; poster?: { default_url?: string } }>
          | {
              works?: Array<{ _id: string; name: string; poster?: { default_url?: string } }>;
            };
      }>(url);

      const items = Array.isArray(response.data)
        ? response.data
        : response.data?.works || [];

      return items.map((item) => ({
        sourceWorkId: item._id,
        title: item.name,
        slug: slugify(item.name),
        coverUrl: item.poster?.default_url || null,
        updatedAt: new Date().toISOString(),
      }));
    } catch (err: any) {
      this.logger.warn(`Search failed on MangaFlix for query "${query}"`, { error: err?.message });
      return [];
    }
  }
}
