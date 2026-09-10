import { SourceAdapter, SourceWorkSummary, SourceWorkDetails, SourceChapterSummary } from '../types.js';
import { HostRateLimiter } from '../../core/rate-limiter.js';
import { Logger } from '../../core/logger.js';
import { slugify, decodeHtmlEntities, stripHtml, extractChapterNumber } from '../common/html-utils.js';

export class TiaManhwaAdapter implements SourceAdapter {
  readonly id = 'tiamanhwa';
  readonly name = 'Tia Manhwa';
  readonly baseUrl = 'https://tiamanhwa.com';

  private logger = new Logger('TiaManhwaAdapter');
  private cookies: Map<string, string> = new Map();

  constructor(
    private rateLimiter: HostRateLimiter = new HostRateLimiter(2.0),
    private transport: typeof fetch = fetch
  ) {
    this.rateLimiter.setHostRate('tiamanhwa.com', 2.0, 4, 4.0);
  }

  private get headers(): Record<string, string> {
    const cookieHeader = Array.from(this.cookies.entries())
      .map(([k, v]) => `${k}=${v}`)
      .join('; ');

    const h: Record<string, string> = {
      Accept: 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
      'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
      Referer: `${this.baseUrl}/`,
      'Sec-Ch-Ua': '"Chromium";v="130", "Google Chrome";v="130", "Not?A_Brand";v="99"',
      'Sec-Ch-Ua-Mobile': '?0',
      'Sec-Ch-Ua-Platform': '"Windows"',
      'Sec-Fetch-Dest': 'document',
      'Sec-Fetch-Mode': 'navigate',
      'Sec-Fetch-Site': 'same-origin',
      'Sec-Fetch-User': '?1',
      'Upgrade-Insecure-Requests': '1',
      'User-Agent':
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
    };
    if (cookieHeader) h.Cookie = cookieHeader;
    return h;
  }

  private storeCookies(res: Response): void {
    let rawCookies: string[] = [];
    if (typeof (res.headers as any).getSetCookie === 'function') {
      rawCookies = (res.headers as any).getSetCookie();
    } else {
      const single = res.headers.get('set-cookie');
      if (single) rawCookies = [single];
    }
    for (const c of rawCookies) {
      const pair = c.split(';')[0];
      const eqIdx = pair.indexOf('=');
      if (eqIdx !== -1) {
        const k = pair.slice(0, eqIdx).trim();
        const v = pair.slice(eqIdx + 1).trim();
        if (k && v) this.cookies.set(k, v);
      }
    }
  }

  private async fetchHtml(url: string, options: RequestInit = {}): Promise<string> {
    const parsed = new URL(url);
    await this.rateLimiter.acquire(parsed.host);

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
          signal: AbortSignal.timeout(30_000),
        });

        this.storeCookies(res);

        if (res.status === 429) {
          const retryAfter = res.headers.get('Retry-After');
          this.rateLimiter.handle429(parsed.host, retryAfter, attempts);
          if (attempts >= maxAttempts) throw new Error(`HTTP 429 rate limit on ${parsed.host}`);
          continue;
        }

        if (!res.ok) {
          if (res.status === 403) {
            throw new Error(`Tia Manhwa bloqueado por Cloudflare (HTTP 403): Just a moment... / Datacenter WAF`);
          }
          throw new Error(`Tia Manhwa request failed: HTTP ${res.status}`);
        }

        this.rateLimiter.recordSuccess(parsed.host);
        return await res.text();
      } catch (err: any) {
        if (attempts >= maxAttempts) throw err;
        await new Promise((r) => setTimeout(r, 1000 * attempts));
      }
    }
    throw new Error(`Failed after ${maxAttempts} attempts for ${url}`);
  }

  async fetchUpdatedWorks(
    cursor?: string | null,
    _options?: { mode?: 'bootstrap' | 'maintenance' }
  ): Promise<{ works: SourceWorkSummary[]; nextCursor: string | null }> {
    const page = cursor ? parseInt(cursor, 10) : 1;
    const url = page === 1 ? `${this.baseUrl}/manhwa/` : `${this.baseUrl}/manhwa/page/${page}/`;

    const html = await this.fetchHtml(url);
    const works: SourceWorkSummary[] = [];

    const linkMatches = [...html.matchAll(/href="(https:\/\/tiamanhwa\.com\/manhwa\/([^"/]+)\/?)"/g)];
    const seenSlugs = new Set<string>();

    for (const m of linkMatches) {
      const slug = m[2];
      if (seenSlugs.has(slug) || slug === 'feed' || slug === 'page') continue;
      seenSlugs.add(slug);

      const idx = m.index || 0;
      const snippet = html.slice(Math.max(0, idx - 100), idx + 500);

      const titleMatch = snippet.match(/<h[3-5][^>]*>\s*<a[^>]*>([\s\S]*?)<\/a>/i) ||
                         snippet.match(/title="([^"]+)"/i);
      const title = titleMatch ? decodeHtmlEntities(stripHtml(titleMatch[1])) : slug.replace(/[-_]+/g, ' ');

      const imgMatch = snippet.match(/<img[^>]+(?:src|data-src)="([^"]+)"/i);
      const coverUrl = imgMatch ? imgMatch[1].trim() : null;

      works.push({
        sourceWorkId: slug,
        title,
        slug,
        coverUrl,
      });
    }

    const hasNext = html.includes(`/page/${page + 1}/`) || html.includes('nextpostslink');
    const nextCursor = hasNext && works.length > 0 ? String(page + 1) : null;

    return { works, nextCursor };
  }

  async fetchWorkDetails(sourceWorkId: string): Promise<SourceWorkDetails> {
    const slug = sourceWorkId.replace(/^\//, '').replace(/\/$/, '').replace(/^manhwa\//, '');
    const url = `${this.baseUrl}/manhwa/${slug}/`;
    const html = await this.fetchHtml(url);

    const titleMatch = html.match(/<div class="[^"]*post-title[^"]*"[^>]*>[\s\S]*?<h1[^>]*>([\s\S]*?)<\/h1>/i) ||
                       html.match(/<h1[^>]*>([\s\S]*?)<\/h1>/i);
    const title = titleMatch ? decodeHtmlEntities(stripHtml(titleMatch[1])) : slug.replace(/[-_]+/g, ' ');

    const coverMatch = html.match(/<div class="[^"]*summary_image[^"]*"[^>]*>[\s\S]*?<img[^>]+(?:src|data-src)="([^"]+)"/i) ||
                       html.match(/property="og:image"\s+content="([^"]+)"/i);
    const coverUrl = coverMatch ? coverMatch[1].trim() : null;

    const descMatch = html.match(/class="[^"]*description-summary[^"]*"[\s\S]*?<p>([\s\S]*?)<\/p>/i) ||
                      html.match(/<p>([\s\S]*?)<\/p>/i);
    const synopsis = descMatch ? decodeHtmlEntities(stripHtml(descMatch[1])) : '';

    const authorMatch = html.match(/class="author-content"[\s\S]*?<a[^>]*>([\s\S]*?)<\/a>/i);
    const author = authorMatch ? decodeHtmlEntities(stripHtml(authorMatch[1])) : undefined;

    const artistMatch = html.match(/class="artist-content"[\s\S]*?<a[^>]*>([\s\S]*?)<\/a>/i);
    const artist = artistMatch ? decodeHtmlEntities(stripHtml(artistMatch[1])) : undefined;

    const genres: string[] = [];
    const genreMatches = [...html.matchAll(/class="genres-content"[\s\S]*?<a[^>]*>([\s\S]*?)<\/a>/gi)];
    for (const gm of genreMatches) {
      const g = decodeHtmlEntities(stripHtml(gm[1]));
      if (g && !genres.includes(g)) genres.push(g);
    }

    let status: SourceWorkDetails['status'] = 'ONGOING';
    if (/status[\s\S]*?complet/i.test(html) || /conclu[ií]d/i.test(html)) {
      status = 'COMPLETED';
    } else if (/hiat/i.test(html)) {
      status = 'HIATUS';
    }

    return {
      sourceWorkId: slug,
      title,
      slug,
      coverUrl,
      synopsis,
      author,
      artist,
      kind: 'MANHWA',
      status,
      ageRating: 18,
      genres,
      raw: { sourceWorkId: slug },
    };
  }

  async fetchChapters(sourceWorkId: string): Promise<SourceChapterSummary[]> {
    const slug = sourceWorkId.replace(/^\//, '').replace(/\/$/, '').replace(/^manhwa\//, '');
    const url = `${this.baseUrl}/manhwa/${slug}/`;
    let html = await this.fetchHtml(url);

    let chapterBlocks = [
      ...html.matchAll(/<li class="[^"]*(?:wp-manga-chapter|chapter-item)[^"]*"[\s\S]*?<\/li>/gi),
    ].map((m) => m[0]);

    if (chapterBlocks.length === 0) {
      try {
        const ajaxHtml = await this.fetchHtml(`${this.baseUrl}/manhwa/${slug}/ajax/chapters/`, {
          method: 'POST',
        });
        chapterBlocks = [...ajaxHtml.matchAll(/<li class="[^"]*wp-manga-chapter[^"]*"[\s\S]*?<\/li>/gi)].map((m) => m[0]);
      } catch {
        // Fallback
      }
    }

    const chapters: SourceChapterSummary[] = [];
    const seenNumbers = new Set<number>();

    for (const block of chapterBlocks) {
      const linkMatch = block.match(/href="([^"]*\/manhwa\/[^"/]+\/([^"/]+)\/?)"/i);
      if (!linkMatch) continue;

      const chSlug = linkMatch[2];
      const chNum = extractChapterNumber(chSlug);
      if (seenNumbers.has(chNum)) continue;
      seenNumbers.add(chNum);

      const titleMatch = block.match(/<a[^>]*>([\s\S]*?)<\/a>/i);
      const title = titleMatch ? decodeHtmlEntities(stripHtml(titleMatch[1])) : `Capítulo ${chNum}`;

      chapters.push({
        sourceChapterId: `${slug}/${chSlug}`,
        number: chNum,
        title,
      });
    }

    chapters.sort((a, b) => a.number - b.number);
    return chapters;
  }

  async fetchChapterPages(sourceChapterId: string, _chapterNumber?: number): Promise<string[]> {
    const chPath = sourceChapterId.replace(/^\//, '').replace(/\/$/, '');
    const url = `${this.baseUrl}/manhwa/${chPath}/?style=list`;
    const html = await this.fetchHtml(url);

    const urls: string[] = [];
    const imgMatches = [
      ...html.matchAll(/class="[^"]*wp-manga-chapter-img[^"]*"[^>]+(?:src|data-src)="([^"]+)"/gi),
      ...html.matchAll(/(?:src|data-src)="([^"]+)"[^>]+class="[^"]*wp-manga-chapter-img[^"]*"/gi),
    ];

    for (const m of imgMatches) {
      const u = m[1].replace(/[\r\n\t\s]+/g, '').trim();
      if (u && !urls.includes(u) && !u.endsWith('logo-PNG.png')) {
        urls.push(u);
      }
    }

    return urls;
  }

  async searchWorks(query: string): Promise<SourceWorkSummary[]> {
    try {
      const url = `${this.baseUrl}/page/1/?s=${encodeURIComponent(query)}&post_type=wp-manga`;
      const html = await this.fetchHtml(url);

      const works: SourceWorkSummary[] = [];
      const linkMatches = [...html.matchAll(/href="(https:\/\/tiamanhwa\.com\/manhwa\/([^"/]+)\/?)"/g)];
      const seenSlugs = new Set<string>();

      for (const m of linkMatches) {
        const slug = m[2];
        if (seenSlugs.has(slug) || slug === 'feed' || slug === 'page') continue;
        seenSlugs.add(slug);

        const idx = m.index || 0;
        const snippet = html.slice(Math.max(0, idx - 100), idx + 500);

        const titleMatch = snippet.match(/<h[3-5][^>]*>\s*<a[^>]*>([\s\S]*?)<\/a>/i) ||
                           snippet.match(/title="([^"]+)"/i);
        const title = titleMatch ? decodeHtmlEntities(stripHtml(titleMatch[1])) : slug.replace(/[-_]+/g, ' ');

        const imgMatch = snippet.match(/<img[^>]+(?:src|data-src)="([^"]+)"/i);
        const coverUrl = imgMatch ? imgMatch[1].trim() : null;

        works.push({
          sourceWorkId: slug,
          title,
          slug,
          coverUrl,
        });
      }

      return works;
    } catch (err: any) {
      this.logger.debug(`Tia Manhwa search not available for "${query}": ${err?.message}`);
      return [];
    }
  }
}
