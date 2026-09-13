import { SourceAdapter, SourceWorkSummary, SourceWorkDetails, SourceChapterSummary } from '../types.js';
import { HostRateLimiter } from '../../core/rate-limiter.js';
import { Logger } from '../../core/logger.js';
import { slugify, decodeHtmlEntities, stripHtml, extractChapterNumber } from './html-utils.js';

export interface MangaThemesiaOptions {
  id: string;
  name: string;
  baseUrl: string;
  mangaSubString?: string; // default 'manga'
  rateLimitRps?: number;
}

export class MangaThemesiaAdapter implements SourceAdapter {
  readonly id: string;
  readonly name: string;
  readonly baseUrl: string;
  readonly mangaSubString: string;

  protected logger: Logger;

  constructor(
    options: MangaThemesiaOptions,
    protected rateLimiter: HostRateLimiter = new HostRateLimiter(2.0),
    protected transport: typeof fetch = fetch
  ) {
    this.id = options.id;
    this.name = options.name;
    this.baseUrl = options.baseUrl.replace(/\/$/, '');
    this.mangaSubString = options.mangaSubString || 'manga';
    this.logger = new Logger(`MangaThemesiaAdapter:${this.id}`);

    const host = new URL(this.baseUrl).host;
    const rps = options.rateLimitRps || 2.0;
    this.rateLimiter.setHostRate(host, rps, Math.ceil(rps * 2), rps * 2);
  }

  protected get headers(): Record<string, string> {
    return {
      'User-Agent':
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
      Accept: 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
      'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
      Referer: `${this.baseUrl}/`,
    };
  }

  protected async fetchHtml(url: string, options: RequestInit = {}): Promise<string> {
    const host = new URL(url).host;
    await this.rateLimiter.acquire(host);

    let attempts = 0;
    const maxAttempts = 3;

    while (attempts < maxAttempts) {
      attempts++;
      try {
        const res = await this.transport(url, {
          ...options,
          headers: {
            ...this.headers,
            ...(options.headers || {}),
          },
        });

        if (res.status === 429 || res.status >= 500) {
          if (attempts < maxAttempts) {
            await new Promise((resolve) => setTimeout(resolve, 1000 * attempts));
            continue;
          }
        }

        if (!res.ok) {
          throw new Error(`HTTP ${res.status} from ${url}`);
        }

        return await res.text();
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

    const url = page === 1
      ? `${this.baseUrl}/${this.mangaSubString}/?order=update`
      : `${this.baseUrl}/${this.mangaSubString}/?page=${page}&order=update`;

    let html: string;
    try {
      html = await this.fetchHtml(url);
    } catch (err: any) {
      this.logger.error(`Error fetching updated works: ${err.message}`);
      return { works: [], nextCursor: null };
    }

    const bsxRegex = /<div class="bsx">\s*<a href="([^"]+)" title="([^"]+)">[\s\S]*?(?:data-src|src)="([^"]+)"[\s\S]*?<\/a>/gi;
    const matches = Array.from(html.matchAll(bsxRegex));
    const works: SourceWorkSummary[] = [];
    const seen = new Set<string>();

    for (const m of matches) {
      const workUrl = m[1].trim();
      const rawTitle = m[2].trim();
      let coverUrl = m[3].trim();

      if (coverUrl.startsWith('data:')) {
        const dataSrcMatch = m[0].match(/data-src=["']([^"']+)["']/i);
        if (dataSrcMatch && !dataSrcMatch[1].startsWith('data:')) {
          coverUrl = dataSrcMatch[1].trim();
        }
      }

      const cleanSlug = workUrl
        .replace(this.baseUrl, '')
        .replace(/^\/+|\/+$/g, '')
        .split('/')
        .pop() || slugify(rawTitle);

      if (seen.has(cleanSlug) || cleanSlug === 'feed' || cleanSlug === 'order') continue;
      seen.add(cleanSlug);

      works.push({
        sourceWorkId: cleanSlug,
        title: decodeHtmlEntities(rawTitle),
        slug: cleanSlug,
        coverUrl: coverUrl.startsWith('http') ? coverUrl : `${this.baseUrl}${coverUrl.startsWith('/') ? '' : '/'}${coverUrl}`,
      });

      if (isMaintenance && works.length >= 24) break;
    }

    const hasNext = html.includes(`page/${page + 1}/`) || html.includes(`page=${page + 1}`);
    return {
      works,
      nextCursor: hasNext && !isMaintenance ? (page + 1).toString() : null,
    };
  }

  async fetchWorkDetails(sourceWorkId: string): Promise<SourceWorkDetails> {
    const workUrl = sourceWorkId.startsWith('http')
      ? sourceWorkId
      : `${this.baseUrl}/${sourceWorkId}/`;

    const html = await this.fetchHtml(workUrl);

    // Title
    const titleMatch = html.match(/<h1[^>]*class="[^"]*entry-title[^"]*"[^>]*>([\s\S]*?)<\/h1>/i) ||
                       html.match(/<h1[^>]*>([\s\S]*?)<\/h1>/i);
    const title = titleMatch ? stripHtml(decodeHtmlEntities(titleMatch[1])).trim() : sourceWorkId;

    // Cover
    const coverMatch = html.match(/<div class="thumb"[^>]*>[\s\S]*?<img[^>]+(?:data-src|src)="([^"]+)"/i);
    let coverUrl = coverMatch ? coverMatch[1].trim() : null;
    if (coverUrl?.startsWith('data:')) {
      const dataSrc = html.match(/class="thumb"[\s\S]*?data-src="([^"]+)"/i);
      coverUrl = dataSrc ? dataSrc[1].trim() : null;
    }

    // Synopsis
    const synMatch = html.match(/<div class="entry-content entry-content-single"[^>]*>([\s\S]*?)<\/div>/i) ||
                     html.match(/id="synopsis"[^>]*>([\s\S]*?)<\/div>/i);
    const synopsis = synMatch ? stripHtml(decodeHtmlEntities(synMatch[1])).trim() : undefined;

    // Genres
    const genreMatches = Array.from(html.matchAll(/rel="tag">([^<]+)<\/a>/gi));
    const genres = Array.from(new Set(genreMatches.map(m => decodeHtmlEntities(m[1].trim()))));

    // Kind detection
    let kind: 'MANGA' | 'MANHWA' | 'MANHUA' | 'WEBTOON' = 'MANGA';
    const lowerGenres = genres.map(g => g.toLowerCase());
    if (lowerGenres.includes('manhwa')) kind = 'MANHWA';
    else if (lowerGenres.includes('manhua')) kind = 'MANHUA';
    else if (lowerGenres.includes('webtoon')) kind = 'WEBTOON';

    return {
      sourceWorkId,
      title,
      slug: sourceWorkId,
      coverUrl,
      synopsis,
      genres,
      kind,
      status: 'ONGOING',
    };
  }

  async fetchChapters(sourceWorkId: string): Promise<SourceChapterSummary[]> {
    const workUrl = sourceWorkId.startsWith('http')
      ? sourceWorkId
      : `${this.baseUrl}/${sourceWorkId}/`;

    const html = await this.fetchHtml(workUrl);

    // Look for chapters inside chapterlist
    const chapterMatches = Array.from(html.matchAll(/href="([^"]+)"\s+class="chapternum"/gi));
    const seenUrls = new Set<string>();
    const chapters: SourceChapterSummary[] = [];

    for (const m of chapterMatches) {
      const chapUrl = m[1].trim();
      if (!chapUrl || seenUrls.has(chapUrl)) continue;
      seenUrls.add(chapUrl);

      const num = extractChapterNumber(chapUrl);
      chapters.push({
        sourceChapterId: chapUrl,
        number: num,
        title: `Capítulo ${num}`,
      });
    }

    chapters.sort((a, b) => a.number - b.number);
    return chapters;
  }

  async fetchChapterPages(sourceChapterId: string, _chapterNumber?: number): Promise<string[]> {
    const chapterUrl = sourceChapterId.startsWith('http')
      ? sourceChapterId
      : `${this.baseUrl}${sourceChapterId.startsWith('/') ? '' : '/'}${sourceChapterId}`;

    const html = await this.fetchHtml(chapterUrl);

    const pages: string[] = [];
    const seen = new Set<string>();

    // Check readerarea
    const readerareaMatch = html.match(/<div id="readerarea"[^>]*>([\s\S]*?)<\/div>/i);
    if (readerareaMatch) {
      const imgs = Array.from(readerareaMatch[1].matchAll(/<img[^>]+(?:data-src|src)="([^"]+)"[^>]*>/gi));
      for (const m of imgs) {
        let url = m[1].trim();
        if (url.startsWith('data:')) {
          const dsMatch = m[0].match(/data-src=["']([^"']+)["']/i);
          if (dsMatch && !dsMatch[1].startsWith('data:')) url = dsMatch[1].trim();
          else continue;
        }
        if (/logo|banner|advert|discord/i.test(url)) continue;
        if (!seen.has(url)) {
          seen.add(url);
          pages.push(url);
        }
      }
    }

    // Fallback: check ts_reader.run
    if (pages.length === 0) {
      const tsMatch = html.match(/ts_reader\.run\(([\s\S]*?)\);/i);
      if (tsMatch) {
        try {
          const parsed = JSON.parse(tsMatch[1]);
          const images = parsed?.sources?.[0]?.images;
          if (Array.isArray(images)) {
            for (const img of images) {
              if (typeof img === 'string' && !seen.has(img)) {
                seen.add(img);
                pages.push(img);
              }
            }
          }
        } catch {}
      }
    }

    return pages;
  }

  async searchWorks(query: string): Promise<SourceWorkSummary[]> {
    const url = `${this.baseUrl}/?s=${encodeURIComponent(query)}`;
    const html = await this.fetchHtml(url);

    const bsxRegex = /<div class="bsx">\s*<a href="([^"]+)" title="([^"]+)">[\s\S]*?(?:data-src|src)="([^"]+)"[\s\S]*?<\/a>/gi;
    const matches = Array.from(html.matchAll(bsxRegex));
    const works: SourceWorkSummary[] = [];
    const seen = new Set<string>();

    for (const m of matches) {
      const workUrl = m[1].trim();
      const rawTitle = m[2].trim();
      const coverUrl = m[3].trim();
      const cleanSlug = workUrl
        .replace(this.baseUrl, '')
        .replace(/^\/+|\/+$/g, '')
        .split('/')
        .pop() || slugify(rawTitle);

      if (seen.has(cleanSlug)) continue;
      seen.add(cleanSlug);

      works.push({
        sourceWorkId: cleanSlug,
        title: decodeHtmlEntities(rawTitle),
        slug: cleanSlug,
        coverUrl: coverUrl.startsWith('http') ? coverUrl : `${this.baseUrl}${coverUrl.startsWith('/') ? '' : '/'}${coverUrl}`,
      });
    }

    return works;
  }

  getImageHeaders(_url?: string): Record<string, string> {
    return {
      Referer: `${this.baseUrl}/`,
      'User-Agent':
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
    };
  }
}
