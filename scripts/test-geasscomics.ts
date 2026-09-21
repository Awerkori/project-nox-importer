import { SourceAdapter, SourceWorkSummary, SourceWorkDetails, SourceChapterSummary } from '../src/sources/types.js';
import { HostRateLimiter } from '../src/core/rate-limiter.js';
import { Logger } from '../src/core/logger.js';

export class GeassComicsAdapter implements SourceAdapter {
  readonly id = 'geasscomics';
  readonly name = 'Geass Comics';
  readonly baseUrl = 'https://geasscomics.xyz';
  private readonly apiUrl = 'https://api.geasscomics.xyz';
  private logger = new Logger('GeassComicsAdapter');

  constructor(
    private rateLimiter: HostRateLimiter = new HostRateLimiter(2.0),
    private transport: typeof fetch = fetch
  ) {
    this.rateLimiter.setHostRate('api.geasscomics.xyz', 2.0, 4, 4);
    this.rateLimiter.setHostRate('geasscomics.xyz', 2.0, 4, 4);
    this.rateLimiter.setHostRate('cdn.geasscomics.xyz', 5.0, 10, 10);
  }

  private get headers(): Record<string, string> {
    return {
      'User-Agent':
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
      Accept: 'application/json, text/plain, */*',
      Referer: `${this.baseUrl}/`,
      Origin: this.baseUrl,
    };
  }

  private async fetchJson<T>(url: string): Promise<T> {
    const host = new URL(url).host;
    await this.rateLimiter.acquire(host);

    let attempts = 0;
    const maxAttempts = 3;

    while (attempts < maxAttempts) {
      attempts++;
      try {
        const res = await this.transport(url, { headers: this.headers });
        if (res.status === 429 || res.status >= 500) {
          if (attempts < maxAttempts) {
            await new Promise((resolve) => setTimeout(resolve, 1000 * attempts));
            continue;
          }
        }
        if (!res.ok) {
          throw new Error(`HTTP ${res.status} from ${url}`);
        }
        return (await res.json()) as T;
      } catch (err: any) {
        if (attempts >= maxAttempts) throw err;
        await new Promise((resolve) => setTimeout(resolve, 1000 * attempts));
      }
    }
    throw new Error(`Failed to fetch ${url} after ${maxAttempts} attempts`);
  }

  async fetchUpdatedWorks(
    cursor?: string | null,
    options?: { mode?: 'bootstrap' | 'maintenance' }
  ): Promise<{ works: SourceWorkSummary[]; nextCursor: string | null }> {
    const page = cursor ? parseInt(cursor, 10) : 1;
    const isMaintenance = options?.mode === 'maintenance';
    const limit = isMaintenance ? 24 : 36;

    const url = `${this.apiUrl}/api/works?page=${page}&limit=${limit}&sortBy=recent&sortDir=desc`;

    try {
      const res = await this.fetchJson<{ data: { items: any[]; pageCount: number } }>(url);
      const items = res.data?.items || [];

      const works: SourceWorkSummary[] = items.map((item) => ({
        sourceWorkId: item.slug,
        title: item.title,
        slug: item.slug,
        coverUrl: item.cover || null,
        updatedAt: item.updatedAt || undefined,
      }));

      const hasNext = page < (res.data?.pageCount || 1);

      return {
        works,
        nextCursor: hasNext && !isMaintenance ? (page + 1).toString() : null,
      };
    } catch (err: any) {
      this.logger.error(`Error fetching updated works: ${err.message}`);
      return { works: [], nextCursor: null };
    }
  }

  async fetchWorkDetails(sourceWorkId: string): Promise<SourceWorkDetails> {
    const url = `${this.apiUrl}/api/works/${sourceWorkId}`;
    const res = await this.fetchJson<{ data: any }>(url);
    const w = res.data;

    let kind: SourceWorkDetails['kind'] = 'UNKNOWN';
    const rawKind = (w.kind || '').toUpperCase();
    if (rawKind === 'MANHWA') kind = 'MANHWA';
    else if (rawKind === 'MANHUA') kind = 'MANHUA';
    else if (rawKind === 'MANGA') kind = 'MANGA';
    else if (rawKind === 'WEBTOON') kind = 'WEBTOON';

    let status: SourceWorkDetails['status'] = 'ONGOING';
    const rawStatus = (w.status || '').toLowerCase();
    if (rawStatus === 'completed') status = 'COMPLETED';
    else if (rawStatus === 'hiatus') status = 'HIATUS';
    else if (rawStatus === 'cancelled') status = 'CANCELLED';

    return {
      sourceWorkId: w.slug || sourceWorkId,
      title: w.title || sourceWorkId,
      slug: w.slug || sourceWorkId,
      coverUrl: w.cover || null,
      synopsis: w.synopsis || undefined,
      author: w.author || undefined,
      genres: Array.isArray(w.tags) ? w.tags : [],
      kind,
      status,
    };
  }

  async fetchChapters(sourceWorkId: string): Promise<SourceChapterSummary[]> {
    const url = `${this.apiUrl}/api/works/${sourceWorkId}`;
    const res = await this.fetchJson<{ data: { chapters: any[] } }>(url);
    const chapters = res.data?.chapters || [];

    const summaries: SourceChapterSummary[] = chapters.map((c) => {
      const num = typeof c.number === 'number' ? c.number : parseFloat(c.number) || 0;
      return {
        sourceChapterId: `${sourceWorkId}/${num}`,
        number: num,
        title: c.title ? `Capítulo ${num} - ${c.title}` : `Capítulo ${num}`,
        createdAt: c.releasedAt || undefined,
        pageCount: typeof c.pageCount === 'number' ? c.pageCount : null,
      };
    });

    summaries.sort((a, b) => a.number - b.number);
    return summaries;
  }

  async fetchChapterPages(sourceChapterId: string, _chapterNumber?: number): Promise<string[]> {
    const parts = sourceChapterId.split('/');
    const slug = parts[0];
    const chapNum = parts[1];
    const url = `${this.baseUrl}/api/read/${slug}/${chapNum}`;

    const res = await this.fetchJson<{ pages?: string[] }>(url);
    const pages = res.pages || [];

    return pages.filter((u) => typeof u === 'string' && u.startsWith('http'));
  }

  async searchWorks(query: string): Promise<SourceWorkSummary[]> {
    const url = `${this.apiUrl}/api/works?page=1&limit=24&q=${encodeURIComponent(query)}`;
    try {
      const res = await this.fetchJson<{ data: { items: any[] } }>(url);
      const items = res.data?.items || [];
      return items.map((item) => ({
        sourceWorkId: item.slug,
        title: item.title,
        slug: item.slug,
        coverUrl: item.cover || null,
      }));
    } catch {
      return [];
    }
  }

  getImageHeaders(_url: string): Record<string, string> {
    return {
      Referer: `${this.baseUrl}/`,
      'User-Agent':
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
    };
  }
}

async function test() {
  const ad = new GeassComicsAdapter();
  console.log('Testing GeassComicsAdapter...');
  const { works } = await ad.fetchUpdatedWorks(null, { mode: 'maintenance' });
  console.log('Works count:', works.length);
  if (works.length > 0) {
    console.log('Sample work:', works[0]);
    const details = await ad.fetchWorkDetails(works[0].sourceWorkId);
    console.log('Details title:', details.title, 'kind:', details.kind, 'genres:', details.genres.slice(0, 3));
    const chaps = await ad.fetchChapters(works[0].sourceWorkId);
    console.log('Chapters count:', chaps.length);
    if (chaps.length > 0) {
      console.log('Sample chap:', chaps[0]);
      const pages = await ad.fetchChapterPages(chaps[0].sourceChapterId);
      console.log('Pages count:', pages.length);
      if (pages.length > 0) {
        console.log('First page URL:', pages[0]);
        const imgRes = await fetch(pages[0], { headers: ad.getImageHeaders(pages[0]) });
        const buf = await imgRes.arrayBuffer();
        console.log('Image download:', imgRes.status, imgRes.headers.get('content-type'), buf.byteLength, 'bytes');
        if (imgRes.status === 200 && buf.byteLength > 500) {
          console.log('🌟 GeassComicsAdapter is 100% CERTIFIED ACTIVE!');
        }
      }
    }
  }
}

test().catch(console.error);
