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

export class ToonLivreAdapter implements SourceAdapter {
  readonly id = 'toonlivre';
  readonly name = 'Toon Livre';
  readonly baseUrl = 'https://toonlivre.net';

  private apiUrl = 'https://toonlivre.net/api';
  private logger = new Logger('ToonLivreAdapter');

  constructor(
    private rateLimiter: HostRateLimiter = new HostRateLimiter(2.0),
    private transport: typeof fetch = fetch
  ) {
    this.rateLimiter.setHostRate('toonlivre.net', 2.0);
    this.rateLimiter.setHostRate('cdn.toonlivre.net', 4.0);
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
          throw new Error(`Toon Livre request failed: HTTP ${response.status} - ${errText.slice(0, 200)}`);
        }

        return (await response.json()) as T;
      } catch (err: any) {
        if (attempts >= maxAttempts) throw err;
        await new Promise((r) => setTimeout(r, 1000 * attempts));
      }
    }
    throw new Error(`Toon Livre request failed after ${maxAttempts} attempts`);
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
    const sortBy = mode === 'maintenance' ? 'updated' : 'popular';
    const url = `${this.apiUrl}/mangas/search?page=${page}&limit=24&sortBy=${sortBy}&sortOrder=desc`;

    const response = await this.request<{
      mangas: Array<{
        id: string;
        title: string;
        coverUrl?: string | null;
        uploadSlug?: string;
        recentChapters?: Array<{ timestamp?: number }>;
      }>;
      pagination?: { hasNextPage: boolean };
    }>(url);

    const items = response.mangas || [];
    const works: SourceWorkSummary[] = items.map((item) => ({
      sourceWorkId: item.id,
      title: item.title,
      slug: item.uploadSlug || slugify(item.title),
      coverUrl: item.coverUrl || null,
      updatedAt: item.recentChapters?.[0]?.timestamp
        ? new Date(item.recentChapters[0].timestamp).toISOString()
        : new Date().toISOString(),
    }));

    const hasNext = response.pagination?.hasNextPage ?? (items.length >= 24);
    return {
      works,
      nextCursor: hasNext ? String(page + 1) : null,
    };
  }

  async fetchWorkDetails(sourceWorkId: string): Promise<SourceWorkDetails> {
    const url = `${this.apiUrl}/manga-by-slug/${sourceWorkId}`;
    const response = await this.request<{
      id: string;
      title: string;
      coverUrl?: string | null;
      type?: string | null;
      authors?: string[];
      artists?: string[];
      genres?: string[];
      description?: string | null;
      alternativeTitle?: string | null;
      status?: string | null;
      uploadSlug?: string;
    }>(url);

    if (!response || !response.title) {
      throw new Error(`Work not found on Toon Livre: ${sourceWorkId}`);
    }

    let kind: 'MANGA' | 'MANHWA' | 'MANHUA' | 'WEBTOON' = 'MANGA';
    const t = (response.type || '').toLowerCase();
    if (t.includes('manhwa')) kind = 'MANHWA';
    else if (t.includes('manhua')) kind = 'MANHUA';
    else if (t.includes('webtoon')) kind = 'WEBTOON';

    let status: 'ONGOING' | 'COMPLETED' | 'HIATUS' | 'CANCELLED' = 'ONGOING';
    const s = (response.status || '').toLowerCase();
    if (s.includes('completed') || s.includes('completo')) status = 'COMPLETED';
    else if (s.includes('hiat')) status = 'HIATUS';
    else if (s.includes('cancel')) status = 'CANCELLED';

    return {
      sourceWorkId: response.id,
      title: response.title,
      slug: response.uploadSlug || slugify(response.title),
      coverUrl: response.coverUrl || null,
      synopsis: response.description?.trim() || '',
      author: response.authors?.join(', '),
      artist: response.artists?.join(', '),
      kind,
      status,
      genres: response.genres || [],
      alternativeTitles: response.alternativeTitle ? [response.alternativeTitle] : [],
      raw: response,
    };
  }

  async fetchChapters(sourceWorkId: string): Promise<SourceChapterSummary[]> {
    const url = `${this.apiUrl}/manga-by-slug/${sourceWorkId}`;
    const response = await this.request<{
      chapters?: Array<{
        id: string;
        number: string;
        title?: string;
        timestamp?: number;
        pageCount?: number;
      }>;
    }>(url);

    const rawChapters = response.chapters || [];

    const chapters: SourceChapterSummary[] = rawChapters.map((ch) => {
      const num = parseFloat(ch.number) || 0;
      return {
        sourceChapterId: ch.id,
        number: num,
        title: ch.title?.trim() || `Capítulo ${ch.number}`,
        createdAt: ch.timestamp ? new Date(ch.timestamp).toISOString() : new Date().toISOString(),
        pageCount: ch.pageCount || 0,
      };
    });

    return chapters.sort((a, b) => a.number - b.number);
  }

  async fetchChapterPages(sourceChapterId: string, _chapterNumber?: number): Promise<string[]> {
    // Cloudflare Turnstile and challenge-platform protect Toon Livre chapter reader HTML.
    // In strict compliance with guidelines: do not attempt fragile or unsafe bypasses.
    // Fail cleanly with VERIFICATION_FAILED to protect catalog consistency.
    throw new Error(
      `Cloudflare Turnstile verification challenge required for Toon Livre chapter reader (${sourceChapterId}). Marked as VERIFICATION_FAILED.`
    );
  }
}
