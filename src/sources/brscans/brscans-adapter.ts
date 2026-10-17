import { HostRateLimiter } from '../../core/rate-limiter.js';
import { extractChapterNumber, slugify } from '../common/html-utils.js';
import { SourceAdapter, SourceChapterSummary, SourceWorkDetails, SourceWorkSnapshot, SourceWorkSummary } from '../types.js';

type BrWork = Record<string, any>;

/**
 * BR Scans exposes a small, version-independent JSON API backed by its own
 * catalogue. It is deliberately modelled on that API rather than the public
 * web UI, so a theme change cannot break discovery or the reader manifest.
 */
export class BRScansAdapter implements SourceAdapter {
  readonly id = 'brscans';
  readonly name = 'BR Scans';
  readonly baseUrl = 'https://brscans.vercel.app';
  private readonly apiUrl = 'https://e5oer7ngt8.execute-api.sa-east-1.amazonaws.com/dev';

  constructor(
    private readonly rateLimiter: HostRateLimiter = new HostRateLimiter(2),
    private readonly transport: typeof fetch = fetch,
  ) {
    this.rateLimiter.setHostRate(new URL(this.apiUrl).host, 2, 4, 4);
    this.rateLimiter.setHostRate('d47sf0ah3vbtc.cloudfront.net', 8, 16, 16);
  }

  private async request<T>(path: string): Promise<T> {
    const url = `${this.apiUrl}${path}`;
    const host = new URL(url).host;
    await this.rateLimiter.acquire(host);
    const response = await this.transport(url, {
      headers: { Accept: 'application/json', Referer: `${this.baseUrl}/` },
      signal: AbortSignal.timeout(15_000),
    });
    if (!response.ok) throw new Error(`BRScans HTTP ${response.status} for ${path}`);
    this.rateLimiter.recordSuccess(host);
    return response.json() as Promise<T>;
  }

  private workSummary(work: BrWork): SourceWorkSummary {
    const title = String(work.title || work.original_title || `Obra ${work.id}`);
    return {
      sourceWorkId: String(work.id),
      title,
      slug: slugify(title),
      coverUrl: work.thumbnail?.translated || work.thumbnail?.original || null,
      updatedAt: work.updated_at || work.created_at || work.chapters?.[0]?.created_at,
    };
  }

  private workDetails(work: BrWork): SourceWorkDetails {
    const title = String(work.title || work.original_title || `Obra ${work.id}`);
    const originalTitle = typeof work.original_title === 'string' ? work.original_title.trim() : '';
    const statusText = String(work.status || '').toLocaleLowerCase();
    const status: SourceWorkDetails['status'] = /conclu|complete|finished/.test(statusText)
      ? 'COMPLETED'
      : /hiat|paus/.test(statusText)
        ? 'HIATUS'
        : statusText ? 'ONGOING' : 'UNKNOWN';
    const genres: string[] = (Array.isArray(work.genres) ? work.genres : [])
      .map((genre: any): string | undefined => typeof genre === 'string' ? genre : genre?.name)
      .filter((genre: string | undefined): genre is string => Boolean(genre)) as string[];
    return {
      sourceWorkId: String(work.id),
      title,
      slug: slugify(title),
      coverUrl: work.thumbnail?.translated || work.thumbnail?.original || null,
      synopsis: work.description || undefined,
      author: work.author || undefined,
      status,
      ageRating: work.is_nsfw === true ? 18 : undefined,
      genres: genres.length ? [...new Set(genres)] : undefined,
      alternativeTitles: originalTitle && originalTitle.localeCompare(title, undefined, { sensitivity: 'accent' }) !== 0
        ? [originalTitle]
        : undefined,
      raw: work,
    };
  }

  private chapters(workId: string, work: BrWork): SourceChapterSummary[] {
    const sourceChapters: any[] = Array.isArray(work.chapters) ? work.chapters : [];
    return sourceChapters.map((chapter: any): SourceChapterSummary => {
      const displayNumber = String(chapter.slug || chapter.number || chapter.title || chapter.id);
      const parsed = Number.parseFloat(displayNumber.replace(',', '.'));
      return {
        sourceChapterId: `${workId}:${chapter.id}`,
        displayNumber,
        number: Number.isFinite(parsed) ? parsed : extractChapterNumber(String(chapter.title || displayNumber)),
        title: chapter.title || undefined,
        createdAt: chapter.release_date || chapter.created_at || undefined,
        pageCount: chapter.quantity_pages ?? null,
      };
    }).sort((a, b) => a.number - b.number);
  }

  async fetchUpdatedWorks(cursor?: string | null): Promise<{ works: SourceWorkSummary[]; nextCursor: string | null }> {
    const page = Math.max(1, Number.parseInt(cursor || '1', 10) || 1);
    const data = await this.request<{ results?: BrWork[]; next?: string | null }>(`/manhwas/?page=${page}`);
    return { works: (data.results || []).map((work) => this.workSummary(work)), nextCursor: data.next ? String(page + 1) : null };
  }

  async fetchWorkDetails(sourceWorkId: string): Promise<SourceWorkDetails> {
    return this.workDetails(await this.request<BrWork>(`/manhwas/${encodeURIComponent(sourceWorkId)}/`));
  }

  async fetchWorkSnapshot(sourceWorkId: string): Promise<SourceWorkSnapshot> {
    const work = await this.request<BrWork>(`/manhwas/${encodeURIComponent(sourceWorkId)}/`);
    return { details: this.workDetails(work), chapters: this.chapters(sourceWorkId, work) };
  }

  async fetchChapters(sourceWorkId: string): Promise<SourceChapterSummary[]> {
    const work = await this.request<BrWork>(`/manhwas/${encodeURIComponent(sourceWorkId)}/`);
    return this.chapters(sourceWorkId, work);
  }

  async fetchChapterPages(sourceChapterId: string): Promise<string[]> {
    const chapterId = sourceChapterId.slice(sourceChapterId.lastIndexOf(':') + 1);
    const chapter = await this.request<{ pages?: Array<{ images?: Record<string, string | null>; order?: number }> }>(`/chapters/${encodeURIComponent(chapterId)}/`);
    return [...(chapter.pages || [])]
      .sort((a, b) => (a.order ?? 0) - (b.order ?? 0))
      .map((page) => page.images?.translated || page.images?.original || page.images?.raw)
      .filter((url): url is string => Boolean(url));
  }

  async searchWorks(query: string): Promise<SourceWorkSummary[]> {
    const data = await this.request<BrWork[]>(`/manhwas/search/?query=${encodeURIComponent(query)}`);
    return (data || []).map((work) => this.workSummary(work));
  }

  getImageHeaders(): Record<string, string> {
    return { Referer: `${this.baseUrl}/`, Accept: 'image/avif,image/webp,image/*,*/*;q=0.8' };
  }
}
