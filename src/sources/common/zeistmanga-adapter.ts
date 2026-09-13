import { SourceAdapter, SourceWorkSummary, SourceWorkDetails, SourceChapterSummary } from '../types.js';
import { HostRateLimiter } from '../../core/rate-limiter.js';
import { Logger } from '../../core/logger.js';
import { slugify, decodeHtmlEntities, stripHtml, extractChapterNumber } from './html-utils.js';

export interface ZeistMangaOptions {
  id: string;
  name: string;
  baseUrl: string;
  seriesCategory?: string; // default 'Series'
  rateLimitRps?: number;
}

export class ZeistMangaAdapter implements SourceAdapter {
  readonly id: string;
  readonly name: string;
  readonly baseUrl: string;
  readonly seriesCategory: string;

  protected logger: Logger;

  constructor(
    options: ZeistMangaOptions,
    protected rateLimiter: HostRateLimiter = new HostRateLimiter(2.0),
    protected transport: typeof fetch = fetch
  ) {
    this.id = options.id;
    this.name = options.name;
    this.baseUrl = options.baseUrl.replace(/\/$/, '');
    this.seriesCategory = options.seriesCategory || 'Series';
    this.logger = new Logger(`ZeistMangaAdapter:${this.id}`);

    const host = new URL(this.baseUrl).host;
    const rps = options.rateLimitRps || 2.0;
    this.rateLimiter.setHostRate(host, rps, Math.ceil(rps * 2), rps * 2);
  }

  protected get headers(): Record<string, string> {
    return {
      'User-Agent':
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
      Accept: 'application/json,text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
      'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
      Referer: `${this.baseUrl}/`,
    };
  }

  protected async fetchJson<T = any>(url: string): Promise<T> {
    const host = new URL(url).host;
    await this.rateLimiter.acquire(host);

    let attempts = 0;
    const maxAttempts = 3;

    while (attempts < maxAttempts) {
      attempts++;
      try {
        const res = await this.transport(url, {
          headers: this.headers,
          signal: AbortSignal.timeout(10000),
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

        return (await res.json()) as T;
      } catch (err: any) {
        if (attempts >= maxAttempts) throw err;
        await new Promise((resolve) => setTimeout(resolve, 1000 * attempts));
      }
    }
    throw new Error(`Failed to fetch JSON from ${url}`);
  }

  protected async fetchHtml(url: string): Promise<string> {
    const host = new URL(url).host;
    await this.rateLimiter.acquire(host);

    const res = await this.transport(url, {
      headers: this.headers,
      signal: AbortSignal.timeout(10000),
    });
    if (!res.ok) {
      throw new Error(`HTTP ${res.status} from ${url}`);
    }
    return await res.text();
  }

  private extractSeriesLabel(categories: string[], title: string): string {
    const generic = new Set([
      'a-z', 'ação', 'aventura', 'comédia', 'drama', 'ecchi', 'fantasia', 'harém', 'hot', 'isekai',
      'lançando', 'magia', 'manga', 'mangá', 'manhwa', 'webtoon', 'comic', 'nsfw', 'recentes',
      'romance', 'series', 'shounen', 'shoujo', 'seinen', 'slice of life', 'vida escolar',
      'update', 'project', 'ongoing', 'completo', 'dropado', 'destaques', 'pt', 'new', 'novel',
      'medicinal', 'cultivo', 'martial arts', 'harem', 'action', 'fantasy', 'school life',
      'supernatural', 'dropped', 'em lançamento', 'em andamento', 'finalizado', 'hiato'
    ]);

    for (const c of categories) {
      const lower = c.trim().toLowerCase();
      if (!generic.has(lower) && !/^\d+(\.\d+)?$/.test(lower) && lower.length > 1) {
        return c.trim();
      }
    }
    return title;
  }

  async fetchUpdatedWorks(
    cursor?: string | null,
    options?: { mode?: 'bootstrap' | 'maintenance' }
  ): Promise<{ works: SourceWorkSummary[]; nextCursor: string | null }> {
    const page = cursor ? parseInt(cursor, 10) : 1;
    const maxResults = 25;
    const startIndex = (page - 1) * maxResults + 1;

    const feedUrl = `${this.baseUrl}/feeds/posts/default/-/${encodeURIComponent(
      this.seriesCategory
    )}?alt=json&start-index=${startIndex}&max-results=${maxResults}`;

    try {
      const data = await this.fetchJson(feedUrl);
      const entries = data?.feed?.entry || [];
      const works: SourceWorkSummary[] = [];

      for (const entry of entries) {
        const title = decodeHtmlEntities(entry.title?.['$t'] || '').trim();
        if (!title) continue;

        const altLink = (entry.link || []).find((l: any) => l.rel === 'alternate')?.href || '';
        const coverUrl =
          entry.media$thumbnail?.url?.replace(/\/s72-c\//, '/s600/') ||
          this.extractCoverFromContent(entry.content?.['$t'] || entry.summary?.['$t'] || '');

        const categories = (entry.category || []).map((c: any) => c.term);
        const specificLabel = this.extractSeriesLabel(categories, title);

        const workSlug = altLink
          ? altLink.replace(this.baseUrl, '').replace(/\.html$/, '').replace(/^\/+/, '').replace(/\//g, '-')
          : slugify(title);

        works.push({
          sourceWorkId: `${altLink}|${specificLabel}`,
          title,
          slug: workSlug,
          coverUrl: coverUrl || null,
          updatedAt: entry.updated?.['$t'] || entry.published?.['$t'] || new Date().toISOString(),
        });
      }

      const totalResults = parseInt(data?.feed?.openSearch$totalResults?.['$t'] || '0', 10);
      const hasNext = startIndex + entries.length <= totalResults && entries.length === maxResults;

      return {
        works,
        nextCursor: hasNext ? String(page + 1) : null,
      };
    } catch (err: any) {
      this.logger.error(`fetchUpdatedWorks failed: ${err.message}`);
      return { works: [], nextCursor: null };
    }
  }

  async fetchWorkDetails(sourceWorkId: string): Promise<SourceWorkDetails> {
    const [workUrl] = sourceWorkId.split('|');
    const targetUrl = workUrl.startsWith('http') ? workUrl : `${this.baseUrl}/${workUrl}.html`;

    try {
      const html = await this.fetchHtml(targetUrl);

      const titleMatch = html.match(/<h1[^>]*class=["'][^"']*entry-title[^"']*["'][^>]*>([\s\S]*?)<\/h1>/i) ||
        html.match(/<h1[^>]*>([\s\S]*?)<\/h1>/i) ||
        html.match(/<title>([^<-]+)/i);
      const title = decodeHtmlEntities(stripHtml(titleMatch ? titleMatch[1] : 'Sem Título')).trim();

      const descMatch = html.match(/id=["']synopsis["'][^>]*>([\s\S]*?)<\/div>/i) ||
        html.match(/class=["'][^"']*synopsis[^"']*["'][^>]*>([\s\S]*?)<\/div>/i) ||
        html.match(/<meta[^>]+property=["']og:description["'][^>]+content=["']([^"']+)["']/i);
      const synopsis = descMatch ? decodeHtmlEntities(stripHtml(descMatch[1])).trim() : undefined;

      const coverMatch = html.match(/<meta[^>]+property=["']og:image["'][^>]+content=["']([^"']+)["']/i) ||
        html.match(/class=["'][^"']*entry-content[^"']*["'][\s\S]*?<img[^>]+src=["']([^"']+)["']/i);
      const coverUrl = coverMatch ? coverMatch[1] : undefined;

      const genres: string[] = [];
      const genreMatches = html.matchAll(/rel=["']tag["'][^>]*>([^<]+)<\/a>/gi);
      for (const m of genreMatches) {
        const g = decodeHtmlEntities(m[1]).trim();
        if (g && !genres.includes(g) && g.toLowerCase() !== 'series') {
          genres.push(g);
        }
      }

      return {
        sourceWorkId,
        title,
        slug: slugify(title),
        coverUrl: coverUrl || null,
        synopsis,
        genres: genres.length > 0 ? genres : undefined,
        status: 'ONGOING',
      };
    } catch (err: any) {
      this.logger.warn(`fetchWorkDetails fallback for ${sourceWorkId}: ${err.message}`);
      return {
        sourceWorkId,
        title: sourceWorkId,
        slug: slugify(sourceWorkId),
      };
    }
  }

  async fetchChapters(sourceWorkId: string): Promise<SourceChapterSummary[]> {
    const [workUrl, labelHint] = sourceWorkId.split('|');
    const targetUrl = workUrl.startsWith('http') ? workUrl : `${this.baseUrl}/${workUrl}.html`;

    try {
      let label = labelHint;

      // 1. Try to get cleaner label from the work page if available
      try {
        const html = await this.fetchHtml(targetUrl);
        const m = html.match(/data-labelchapter=["']([^"']+)["']/i);
        if (m && m[1] && !m[1].toLowerCase().includes('pesquisar') && !m[1].toLowerCase().includes('search')) {
          label = m[1].trim();
        }
      } catch (_) {
        // use labelHint
      }

      if (!label) {
        return [];
      }

      // Query Blogger feed for this label
      const feedUrl = `${this.baseUrl}/feeds/posts/default/-/${encodeURIComponent(
        label
      )}?alt=json&max-results=500`;

      const data = await this.fetchJson(feedUrl);
      const entries = data?.feed?.entry || [];
      const chapters: SourceChapterSummary[] = [];

      for (const entry of entries) {
        const title = decodeHtmlEntities(entry.title?.['$t'] || '').trim();
        const altLink = (entry.link || []).find((l: any) => l.rel === 'alternate')?.href || '';

        // Skip the series overview post itself
        if (altLink === targetUrl || title.toLowerCase() === label.toLowerCase()) {
          continue;
        }

        const num = extractChapterNumber(title);
        if (num !== null && !chapters.some((c) => c.number === num)) {
          chapters.push({
            sourceChapterId: altLink,
            number: num,
            title,
            createdAt: entry.published?.['$t'] || entry.updated?.['$t'],
          });
        }
      }

      chapters.sort((a, b) => b.number - a.number);
      return chapters;
    } catch (err: any) {
      this.logger.error(`fetchChapters failed for ${sourceWorkId}: ${err.message}`);
      return [];
    }
  }

  async fetchChapterPages(sourceChapterId: string): Promise<string[]> {
    const chapterUrl = sourceChapterId.startsWith('http')
      ? sourceChapterId
      : `${this.baseUrl}/${sourceChapterId}.html`;

    try {
      const html = await this.fetchHtml(chapterUrl);

      // Look for reader images in check-box, separator, or entry-content
      const imgRegex = /<img[^>]+(?:src|data-src)=["']([^"']+)["'][^>]*>/gi;
      const pages: string[] = [];

      for (const m of html.matchAll(imgRegex)) {
        const src = m[1];
        if (
          !src.includes('capa-oculta') &&
          !src.includes('banner') &&
          !src.includes('logo') &&
          !src.includes('icon') &&
          !src.includes('avatar') &&
          (src.includes('blogger.googleusercontent.com') ||
            src.includes('.bp.blogspot.com') ||
            src.includes('imgur.com') ||
            src.match(/\.(jpe?g|png|webp|avif)/i))
        ) {
          if (!pages.includes(src)) {
            pages.push(src);
          }
        }
      }

      return pages;
    } catch (err: any) {
      this.logger.error(`fetchChapterPages failed for ${sourceChapterId}: ${err.message}`);
      return [];
    }
  }

  async searchWorks(query: string): Promise<SourceWorkSummary[]> {
    const searchUrl = `${this.baseUrl}/feeds/posts/default/-/${encodeURIComponent(
      this.seriesCategory
    )}?alt=json&q=${encodeURIComponent(query)}&max-results=20`;

    try {
      const data = await this.fetchJson(searchUrl);
      const entries = data?.feed?.entry || [];
      return entries.map((entry: any) => {
        const title = decodeHtmlEntities(entry.title?.['$t'] || '').trim();
        const altLink = (entry.link || []).find((l: any) => l.rel === 'alternate')?.href || '';
        return {
          sourceWorkId: altLink || slugify(title),
          title,
          slug: slugify(title),
          coverUrl: entry.media$thumbnail?.url || null,
        };
      });
    } catch (err: any) {
      this.logger.error(`searchWorks failed for query "${query}": ${err.message}`);
      return [];
    }
  }

  getImageHeaders(): Record<string, string> {
    return {
      Referer: `${this.baseUrl}/`,
      'User-Agent':
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
    };
  }

  private extractCoverFromContent(content: string): string | null {
    const m = content.match(/<img[^>]+src=["']([^"']+)["']/i);
    return m ? m[1] : null;
  }
}
