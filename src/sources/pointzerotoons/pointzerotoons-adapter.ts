import { MangaThemesiaAdapter } from '../common/mangathemesia-adapter.js';
import { HostRateLimiter } from '../../core/rate-limiter.js';
import { SourceWorkSummary, SourceWorkDetails, SourceChapterSummary } from '../types.js';
import { stripHtml, decodeHtmlEntities, extractChapterNumber, slugify } from '../common/html-utils.js';

export class PointZeroToonsAdapter extends MangaThemesiaAdapter {
  constructor(rateLimiter?: HostRateLimiter, transport?: typeof fetch) {
    super(
      {
        id: 'pointzerotoons',
        name: 'Kitsune Yako',
        baseUrl: 'https://kitsuneyako.com',
        mangaSubString: 'manga',
        rateLimitRps: 2.5,
      },
      rateLimiter,
      transport
    );
  }

  override async fetchUpdatedWorks(
    cursor?: string | null,
    options?: { mode?: 'bootstrap' | 'maintenance' }
  ): Promise<{ works: SourceWorkSummary[]; nextCursor: string | null }> {
    const page = cursor ? parseInt(cursor, 10) : 1;
    const isMaintenance = options?.mode === 'maintenance';

    const url = page === 1
      ? `${this.baseUrl}/manga/?order=updated`
      : `${this.baseUrl}/manga/page/${page}/?order=updated`;

    let html: string;
    try {
      html = await this.fetchHtml(url);
    } catch (err: any) {
      this.logger.error(`Error fetching updated works: ${err.message}`);
      return { works: [], nextCursor: null };
    }

    const works: SourceWorkSummary[] = [];
    const seen = new Set<string>();

    // 1. Primary parser: inkra-catalog-card (custom WordPress theme)
    if (html.includes('inkra-catalog-card')) {
      const cardSplits = html.split(/<article[^>]*class="[^"]*inkra-catalog-card[^"]*"[^>]*>/i);
      for (let i = 1; i < cardSplits.length; i++) {
        const chunk = cardSplits[i].split('</article>')[0];
        const linkMatch = chunk.match(/<a[^>]*class="[^"]*inkra-catalog-card__media[^"]*"[^>]*href="([^"]+)"[^>]*>/i) ||
                          chunk.match(/<a[^>]*href="([^"]*\/manga\/[^"]+)"[^>]*>/i);
        const titleMatch = chunk.match(/<h3[^>]*>([\s\S]*?)<\/h3>/i) ||
                           chunk.match(/title="([^"]+)"/i);
        const imgMatch = chunk.match(/<img[^>]+(?:data-src|data-lazy-src|src)="([^"]+)"/i);

        if (!linkMatch || !titleMatch) continue;

        const workUrl = linkMatch[1].trim();
        const rawTitle = titleMatch[1].replace(/<[^>]+>/g, '').trim();
        let coverUrl = imgMatch ? imgMatch[1].trim() : null;

        if (coverUrl?.startsWith('data:')) {
          const ds = chunk.match(/data-src=["']([^"']+)["']/i) || chunk.match(/data-lazy-src=["']([^"']+)["']/i);
          if (ds && !ds[1].startsWith('data:')) coverUrl = ds[1].trim();
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
          coverUrl: coverUrl ? (coverUrl.startsWith('http') ? coverUrl : `${this.baseUrl}/${coverUrl.replace(/^\//, '')}`) : null,
        });

        if (isMaintenance && works.length >= 24) break;
      }
    }

    // 2. Fallback parser: standard MangaThemesia (.bsx)
    if (works.length === 0) {
      const fallbackResult = await super.fetchUpdatedWorks(cursor, options);
      return fallbackResult;
    }

    const hasNext = html.includes('page-numbers next') ||
                    html.includes('next page-numbers') ||
                    html.includes(`/page/${page + 1}/`);

    return {
      works,
      nextCursor: hasNext && !isMaintenance ? (page + 1).toString() : null,
    };
  }

  override async fetchWorkDetails(sourceWorkId: string): Promise<SourceWorkDetails> {
    const workUrl = sourceWorkId.startsWith('http')
      ? sourceWorkId
      : `${this.baseUrl}/manga/${sourceWorkId}/`;

    let html: string;
    try {
      html = await this.fetchHtml(workUrl);
    } catch (err: any) {
      this.logger.error(`Error fetching work details for ${sourceWorkId}: ${err.message}`);
      return super.fetchWorkDetails(sourceWorkId);
    }

    // Check if inkra theme details are present
    const titleMatch = html.match(/<h1[^>]*class="[^"]*inkra-page-title[^"]*"[^>]*>([\s\S]*?)<\/h1>/i) ||
                       html.match(/<h1[^>]*class="[^"]*entry-title[^"]*"[^>]*>([\s\S]*?)<\/h1>/i) ||
                       html.match(/<h1[^>]*>([\s\S]*?)<\/h1>/i);
    const title = titleMatch ? stripHtml(decodeHtmlEntities(titleMatch[1])).trim() : sourceWorkId;

    // Cover
    const coverMatch = html.match(/<div[^>]*class="[^"]*inkra-series-cover[^"]*"[^>]*>[\s\S]*?<img[^>]+(?:data-src|src)="([^"]+)"/i) ||
                       html.match(/<div class="thumb"[^>]*>[\s\S]*?<img[^>]+(?:data-src|src)="([^"]+)"/i);
    let coverUrl = coverMatch ? coverMatch[1].trim() : null;
    if (coverUrl?.startsWith('data:')) {
      const ds = html.match(/class="inkra-series-cover"[\s\S]*?data-src="([^"]+)"/i) ||
                 html.match(/class="thumb"[\s\S]*?data-src="([^"]+)"/i);
      coverUrl = ds ? ds[1].trim() : null;
    }

    // Synopsis
    const synMatch = html.match(/<div[^>]*class="[^"]*inkra-series-synopsis[^"]*"[^>]*>([\s\S]*?)<\/div>/i) ||
                     html.match(/<div class="entry-content entry-content-single"[^>]*>([\s\S]*?)<\/div>/i) ||
                     html.match(/id="synopsis"[^>]*>([\s\S]*?)<\/div>/i);
    const synopsis = synMatch ? stripHtml(decodeHtmlEntities(synMatch[1])).trim() : undefined;

    // Author
    const authorMatch = html.match(/class="inkra-series-tech__card"[^>]*>[\s\S]*?<strong>([\s\S]*?)<\/strong>/i);
    const author = authorMatch ? stripHtml(decodeHtmlEntities(authorMatch[1])).trim() : undefined;

    // Genres
    const genreMatches = Array.from(html.matchAll(/rel="tag">([^<]+)<\/a>/gi));
    const genres = Array.from(new Set(genreMatches.map(m => decodeHtmlEntities(m[1].trim()))));

    // Kind detection
    let kind: SourceWorkDetails['kind'] = 'UNKNOWN';
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
      author,
      genres,
      kind,
      status: 'ONGOING',
    };
  }

  override async fetchChapters(sourceWorkId: string): Promise<SourceChapterSummary[]> {
    const workUrl = sourceWorkId.startsWith('http')
      ? sourceWorkId
      : `${this.baseUrl}/manga/${sourceWorkId}/`;

    const html = await this.fetchHtml(workUrl);

    // 1. Primary parser: inkra-chapter-item
    if (html.includes('inkra-chapter-item')) {
      const chapRegex = /<article[^>]*class="[^"]*inkra-chapter-item[^"]*"[^>]*>[\s\S]*?<a[^>]*class="[^"]*inkra-chapter-item__link[^"]*"[^>]*href="([^"]+)"[^>]*>[\s\S]*?<span[^>]*class="[^"]*inkra-chapter-item__label[^"]*"[^>]*>([\s\S]*?)<\/span>/gi;
      const seenUrls = new Set<string>();
      const chapters: SourceChapterSummary[] = [];

      for (const m of html.matchAll(chapRegex)) {
        const chapUrl = m[1].trim();
        const rawLabel = m[2].replace(/<[^>]+>/g, '').trim();
        if (!chapUrl || seenUrls.has(chapUrl)) continue;
        seenUrls.add(chapUrl);

        const num = extractChapterNumber(rawLabel) || extractChapterNumber(chapUrl);
        chapters.push({
          sourceChapterId: chapUrl,
          number: num,
          title: decodeHtmlEntities(rawLabel) || `Capítulo ${num}`,
        });
      }

      if (chapters.length > 0) {
        chapters.sort((a, b) => a.number - b.number);
        return chapters;
      }
    }

    // 2. Fallback to base MangaThemesia parser
    return super.fetchChapters(sourceWorkId);
  }

  override async fetchChapterPages(sourceChapterId: string, _chapterNumber?: number): Promise<string[]> {
    const chapterUrl = sourceChapterId.startsWith('http')
      ? sourceChapterId
      : `${this.baseUrl}${sourceChapterId.startsWith('/') ? '' : '/'}${sourceChapterId}`;

    const html = await this.fetchHtml(chapterUrl);

    const pages: string[] = [];
    const seen = new Set<string>();

    // 1. Primary parser: inkra-reader-page figure
    if (html.includes('inkra-reader-page')) {
      const figureRegex = /<figure[^>]*class="[^"]*inkra-reader-page[^"]*"[^>]*>[\s\S]*?<img[^>]+(?:data-src|src)="([^"]+)"/gi;
      for (const m of html.matchAll(figureRegex)) {
        let u = m[1].trim();
        if (u.startsWith('data:')) {
          const ds = m[0].match(/data-src=["']([^"']+)["']/i);
          if (ds && !ds[1].startsWith('data:')) u = ds[1].trim();
          else continue;
        }
        if (/logo|banner|advert|discord/i.test(u)) continue;
        if (!seen.has(u)) {
          seen.add(u);
          pages.push(u);
        }
      }

      if (pages.length > 0) {
        return pages;
      }
    }

    // 2. Secondary parser: container inkra-reader-pages
    const containerMatch = html.match(/class="[^"]*inkra-reader-pages[^"]*"[\s\S]*?<\/main>/i) ||
                           html.match(/class="[^"]*inkra-reader-pages[^"]*"[\s\S]*?<\/div>\s*<\/div>/i);
    if (containerMatch) {
      const imgs = Array.from(containerMatch[0].matchAll(/<img[^>]+(?:data-src|src)="([^"]+)"/gi));
      for (const m of imgs) {
        let u = m[1].trim();
        if (u.startsWith('data:')) {
          const ds = m[0].match(/data-src=["']([^"']+)["']/i);
          if (ds && !ds[1].startsWith('data:')) u = ds[1].trim();
          else continue;
        }
        if (/logo|banner|advert|discord/i.test(u)) continue;
        if (!seen.has(u)) {
          seen.add(u);
          pages.push(u);
        }
      }
      if (pages.length > 0) return pages;
    }

    // 3. Fallback to base MangaThemesia readerarea / ts_reader.run
    return super.fetchChapterPages(sourceChapterId, _chapterNumber);
  }

  override getImageHeaders(_url: string): Record<string, string> {
    return {
      Referer: `${this.baseUrl}/`,
      'User-Agent':
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
    };
  }
}
