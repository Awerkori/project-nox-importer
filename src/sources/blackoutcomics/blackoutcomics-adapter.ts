import { SourceAdapter, SourceWorkSummary, SourceWorkDetails, SourceChapterSummary } from '../types.js';
import { HostRateLimiter } from '../../core/rate-limiter.js';
import { Logger } from '../../core/logger.js';
import { slugify, decodeHtmlEntities, stripHtml, extractChapterNumber } from '../common/html-utils.js';

export class BlackoutComicsAdapter implements SourceAdapter {
  readonly id = 'blackoutcomics';
  readonly name = 'Blackout Comics';
  readonly baseUrl = 'https://blackoutcomics.com';

  private logger = new Logger('BlackoutComicsAdapter');

  // Single-flight authentication mutex to prevent login storms
  private loginPromise: Promise<boolean> | null = null;
  private sessionCookies: Map<string, string> = new Map();
  private lastLoginAttempt = 0;

  constructor(
    private rateLimiter: HostRateLimiter = new HostRateLimiter(2.0),
    private transport: typeof fetch = fetch
  ) {
    this.rateLimiter.setHostRate('blackoutcomics.com', 2.0, 4, 4.0);
  }

  private get baseHeaders(): Record<string, string> {
    return {
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
  }

  private getCookieHeader(): string {
    // Always include age_gate_consent
    if (!this.sessionCookies.has('age_gate_consent')) {
      const now = Date.now();
      const expires = now + 6 * 24 * 60 * 60 * 1000;
      this.sessionCookies.set('age_gate_consent', `%7B%22consentAt%22%3A${now}%2C%22expiresAt%22%3A${expires}%7D`);
    }

    return Array.from(this.sessionCookies.entries())
      .map(([k, v]) => `${k}=${v}`)
      .join('; ');
  }

  private storeCookiesFromResponse(res: Response): void {
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
        if (k && v) {
          this.sessionCookies.set(k, v);
        }
      }
    }
  }

  /**
   * Single-flight synchronized authentication mutex
   */
  async ensureAuthenticated(forceReauth = false): Promise<boolean> {
    if (!forceReauth && this.sessionCookies.has('blackout-comics-session')) {
      return true;
    }

    if (this.loginPromise) {
      return this.loginPromise;
    }

    const email = process.env.BLACKOUT_EMAIL?.trim();
    const password = process.env.BLACKOUT_PASSWORD?.trim();

    if (!email || !password) {
      this.logger.warn('Blackout Comics credentials not configured (BLACKOUT_EMAIL / BLACKOUT_PASSWORD)');
      return false;
    }

    const now = Date.now();
    if (now - this.lastLoginAttempt < 5000) {
      await new Promise((r) => setTimeout(r, 5000 - (now - this.lastLoginAttempt)));
    }
    this.lastLoginAttempt = Date.now();

    this.loginPromise = (async () => {
      try {
        await this.rateLimiter.acquire('blackoutcomics.com');
        // 1. Fetch homepage to get fresh CSRF token and initial cookies
        const homeRes = await this.transport(this.baseUrl, {
          headers: {
            ...this.baseHeaders,
            Cookie: this.getCookieHeader(),
          },
          signal: AbortSignal.timeout(20_000),
        });

        this.storeCookiesFromResponse(homeRes);
        const homeHtml = await homeRes.text();

        let tokenMatch = homeHtml.match(/name="csrf-token"\s+content="([^"]+)"/i) ||
                         homeHtml.match(/content="([^"]+)"\s+name="csrf-token"/i) ||
                         homeHtml.match(/<input[^>]*name="_token"[^>]*value="([^"]+)"/i);
        let csrfToken = tokenMatch ? tokenMatch[1] : '';

        if (!csrfToken && this.sessionCookies.has('XSRF-TOKEN')) {
          csrfToken = decodeURIComponent(this.sessionCookies.get('XSRF-TOKEN')!);
        }

        // Fallback: try /entrar if not found on home
        if (!csrfToken) {
          const entrarRes = await this.transport(`${this.baseUrl}/entrar`, {
            headers: {
              ...this.baseHeaders,
              Cookie: this.getCookieHeader(),
            },
            signal: AbortSignal.timeout(20_000),
          });
          this.storeCookiesFromResponse(entrarRes);
          const entrarHtml = await entrarRes.text();
          tokenMatch = entrarHtml.match(/name="csrf-token"\s+content="([^"]+)"/i) ||
                       entrarHtml.match(/content="([^"]+)"\s+name="csrf-token"/i) ||
                       entrarHtml.match(/<input[^>]*name="_token"[^>]*value="([^"]+)"/i);
          if (tokenMatch) csrfToken = tokenMatch[1];
          else if (this.sessionCookies.has('XSRF-TOKEN')) {
            csrfToken = decodeURIComponent(this.sessionCookies.get('XSRF-TOKEN')!);
          }
        }

        if (!csrfToken) {
          this.logger.warn(`Blackout Comics: CSRF token not found (home HTTP ${homeRes.status})`, {
            homeSnippet: homeHtml.slice(0, 200).replace(/\s+/g, ' '),
          });
          return false;
        }

        const form = new URLSearchParams();
        form.append('_token', csrfToken);
        form.append('USE_EMAIL', email);
        form.append('password', password);

        await this.rateLimiter.acquire('blackoutcomics.com');
        const loginRes = await this.transport(`${this.baseUrl}/entrar`, {
          method: 'POST',
          headers: {
            ...this.baseHeaders,
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'X-CSRF-TOKEN': csrfToken,
            'X-Requested-With': 'XMLHttpRequest',
            Origin: this.baseUrl,
            Referer: `${this.baseUrl}/`,
            Cookie: this.getCookieHeader(),
          },
          body: form.toString(),
          signal: AbortSignal.timeout(20_000),
        });

        this.storeCookiesFromResponse(loginRes);

        if (loginRes.ok && this.sessionCookies.has('blackout-comics-session')) {
          this.logger.info('Blackout Comics authentication successful (session cached in memory)');
          return true;
        }

        this.logger.warn(`Blackout Comics login failed: HTTP ${loginRes.status}`);
        return false;
      } catch (err: any) {
        this.logger.warn('Blackout Comics authentication error', { error: err?.message });
        return false;
      } finally {
        this.loginPromise = null;
      }
    })();

    return this.loginPromise;
  }

  private async fetchHtml(url: string, options: RequestInit = {}): Promise<string> {
    await this.ensureAuthenticated();
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
            ...this.baseHeaders,
            Cookie: this.getCookieHeader(),
            ...(options.headers || {}),
          },
          signal: AbortSignal.timeout(30_000),
        });

        this.storeCookiesFromResponse(res);

        if (res.status === 401 || res.status === 403) {
          // Re-authenticate and retry
          this.logger.warn(`Blackout Comics HTTP ${res.status}, re-authenticating...`);
          await this.ensureAuthenticated(true);
          continue;
        }

        if (res.status === 429) {
          const retryAfter = res.headers.get('Retry-After');
          this.rateLimiter.handle429(parsed.host, retryAfter, attempts);
          if (attempts >= maxAttempts) throw new Error(`HTTP 429 rate limit on ${parsed.host}`);
          continue;
        }

        if (!res.ok) {
          throw new Error(`Blackout Comics request failed: HTTP ${res.status}`);
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

  getImageHeaders(_url: string): Record<string, string> {
    return {
      Accept: 'image/avif,image/webp,image/apng,image/*,*/*;q=0.8',
      Referer: `${this.baseUrl}/`,
      Cookie: this.getCookieHeader(),
      'User-Agent':
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
    };
  }

  async fetchUpdatedWorks(
    _cursor?: string | null,
    _options?: { mode?: 'bootstrap' | 'maintenance' }
  ): Promise<{ works: SourceWorkSummary[]; nextCursor: string | null }> {
    const url = `${this.baseUrl}/atualizados-recente`;
    const html = await this.fetchHtml(url);

    const works: SourceWorkSummary[] = [];
    const seenIds = new Set<string>();

    const linkMatches = [...html.matchAll(/href="([^"]*\/comics\/(\d+)[^"]*)"/g)];
    for (const m of linkMatches) {
      const id = m[2];
      if (seenIds.has(id)) continue;
      seenIds.add(id);

      // Search nearby title or alt
      const idx = m.index || 0;
      const snippet = html.slice(idx, idx + 400);
      const titleMatch = snippet.match(/class="card-title"[^>]*>\s*<span[^>]*>([\s\S]*?)<\/span>/i) ||
                         snippet.match(/alt="([^"]+)"/i);
      const title = titleMatch ? decodeHtmlEntities(stripHtml(titleMatch[1])) : `Obra ${id}`;

      const imgMatch = snippet.match(/<img[^>]+(?:src|data-src)="([^"]+)"/i);
      const coverUrl = imgMatch ? imgMatch[1].trim() : null;

      works.push({
        sourceWorkId: id,
        title,
        slug: slugify(title) || `blackout-${id}`,
        coverUrl,
      });
    }

    return { works, nextCursor: null };
  }

  async fetchWorkDetails(sourceWorkId: string): Promise<SourceWorkDetails> {
    const id = sourceWorkId.replace(/[^0-9]/g, '');
    const url = `${this.baseUrl}/comics/${id}`;
    const html = await this.fetchHtml(url);

    const titleMatch = html.match(/<h1 class="[^"]*project-title[^"]*"[^>]*>([\s\S]*?)<\/h1>/i) ||
                       html.match(/property="og:title"\s+content="([^"]+)"/i);
    let title = titleMatch ? decodeHtmlEntities(stripHtml(titleMatch[1])).replace(/\s*\|\s*Blackout.*$/i, '').trim() : `Comic ${id}`;

    const coverMatch = html.match(/<img class="[^"]*project-cover[^"]*"[^>]+(?:src|data-src)="([^"]+)"/i) ||
                       html.match(/property="og:image"\s+content="([^"]+)"/i);
    const coverUrl = coverMatch ? coverMatch[1].trim() : null;

    const descMatch = html.match(/<div class="[^"]*project-description[^"]*"[\s\S]*?<p>([\s\S]*?)<\/p>/i);
    const synopsis = descMatch ? decodeHtmlEntities(stripHtml(descMatch[1])) : '';

    const authorMatch = html.match(/class="[^"]*quick-info-item[^"]*"[\s\S]*?fa-pen-nib[\s\S]*?<strong>([\s\S]*?)<\/strong>/i);
    const author = authorMatch ? decodeHtmlEntities(stripHtml(authorMatch[1])) : undefined;

    const artistMatch = html.match(/class="[^"]*quick-info-item[^"]*"[\s\S]*?fa-palette[\s\S]*?<strong>([\s\S]*?)<\/strong>/i);
    const artist = artistMatch ? decodeHtmlEntities(stripHtml(artistMatch[1])) : undefined;

    const genres: string[] = [];
    const genreMatches = [...html.matchAll(/class="[^"]*genre-tag[^"]*"[^>]*>([\s\S]*?)<\/span>/gi)];
    for (const gm of genreMatches) {
      const g = decodeHtmlEntities(stripHtml(gm[1]));
      if (g && !genres.includes(g)) genres.push(g);
    }

    let status: SourceWorkDetails['status'] = 'ONGOING';
    if (/status-pill[\s\S]*?complet/i.test(html) || /conclu[ií]d/i.test(html)) {
      status = 'COMPLETED';
    } else if (/status-pill[\s\S]*?hiat/i.test(html)) {
      status = 'HIATUS';
    } else if (/status-pill[\s\S]*?cancel/i.test(html)) {
      status = 'CANCELLED';
    }

    return {
      sourceWorkId: id,
      title,
      slug: slugify(title) || `blackout-${id}`,
      coverUrl,
      synopsis,
      author,
      artist,
      kind: 'MANHWA',
      status,
      ageRating: 18,
      genres,
      raw: { sourceWorkId: id },
    };
  }

  async fetchChapters(sourceWorkId: string): Promise<SourceChapterSummary[]> {
    const id = sourceWorkId.replace(/[^0-9]/g, '');
    const url = `${this.baseUrl}/comics/${id}`;
    const html = await this.fetchHtml(url);

    const chapters: SourceChapterSummary[] = [];
    const seenNumbers = new Set<number>();

    // Extract chapter items
    const epMatches = [...html.matchAll(/<li class="[^"]*normal_ep[^"]*"[\s\S]*?<\/li>/gi)].map((m) => m[0]);

    for (const block of epMatches) {
      // Find chapter URL from onclick or window.location.href
      const urlMatch = block.match(/\/comics\/\d+\/ler\/([a-zA-Z0-9_-]+)/i);
      const chSlug = urlMatch ? urlMatch[1] : null;

      const numMatch = block.match(/class="[^"]*num text-white[^"]*"[^>]*>([\s\S]*?)<\/span>/i);
      const capNum = numMatch ? extractChapterNumber(numMatch[1]) : (chSlug ? extractChapterNumber(chSlug) : 0);

      if (seenNumbers.has(capNum)) continue;
      seenNumbers.add(capNum);

      const resolvedSlug = chSlug || `capitulo-${String(Math.floor(capNum)).padStart(2, '0')}`;

      chapters.push({
        sourceChapterId: `${id}/ler/${resolvedSlug}`,
        number: capNum,
        title: `Capítulo ${capNum}`,
      });
    }

    chapters.sort((a, b) => a.number - b.number);
    return chapters;
  }

  async fetchChapterPages(sourceChapterId: string, _chapterNumber?: number): Promise<string[]> {
    const cleanPath = sourceChapterId.replace(/^\//, '').replace(/\/$/, '');
    const url = `${this.baseUrl}/comics/${cleanPath}`;

    let html = await this.fetchHtml(url);
    let payloadMatch = html.match(/id="reader-payload"[^>]*data-payload="([^"]+)"/i) ||
                       html.match(/data-payload="([^"]+)"[^>]*id="reader-payload"/i);

    if (!payloadMatch) {
      // Session might have expired or need fresh login
      await this.ensureAuthenticated(true);
      html = await this.fetchHtml(url);
      payloadMatch = html.match(/id="reader-payload"[^>]*data-payload="([^"]+)"/i) ||
                     html.match(/data-payload="([^"]+)"[^>]*id="reader-payload"/i);
    }

    if (!payloadMatch) {
      throw new Error(`Reader payload not found for Blackout chapter ${sourceChapterId}`);
    }

    try {
      const decoded = Buffer.from(payloadMatch[1], 'base64').toString('utf-8');
      const pages: string[] = JSON.parse(decoded);
      return pages.filter((u) => typeof u === 'string' && u.startsWith('http'));
    } catch (err: any) {
      throw new Error(`Failed to decode Blackout reader payload: ${err?.message}`);
    }
  }

  async searchWorks(query: string): Promise<SourceWorkSummary[]> {
    const url = `${this.baseUrl}/comics?src=${encodeURIComponent(query)}`;
    const html = await this.fetchHtml(url);

    const works: SourceWorkSummary[] = [];
    const seenIds = new Set<string>();

    const linkMatches = [...html.matchAll(/href="([^"]*\/comics\/(\d+)[^"]*)"/g)];
    for (const m of linkMatches) {
      const id = m[2];
      if (seenIds.has(id)) continue;
      seenIds.add(id);

      const idx = m.index || 0;
      const snippet = html.slice(idx, idx + 400);
      const titleMatch = snippet.match(/class="card-title"[^>]*>\s*<span[^>]*>([\s\S]*?)<\/span>/i) ||
                         snippet.match(/alt="([^"]+)"/i);
      const title = titleMatch ? decodeHtmlEntities(stripHtml(titleMatch[1])) : `Comic ${id}`;

      const imgMatch = snippet.match(/<img[^>]+(?:src|data-src)="([^"]+)"/i);
      const coverUrl = imgMatch ? imgMatch[1].trim() : null;

      works.push({
        sourceWorkId: id,
        title,
        slug: slugify(title) || `blackout-${id}`,
        coverUrl,
      });
    }

    return works;
  }
}
