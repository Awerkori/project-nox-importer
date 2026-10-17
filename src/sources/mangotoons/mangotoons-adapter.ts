import { SourceAdapter, SourceWorkSummary, SourceWorkDetails, SourceChapterSummary } from '../types.js';
import { HostRateLimiter } from '../../core/rate-limiter.js';
import { Logger } from '../../core/logger.js';
import { decryptMangoPayload, DEFAULT_MANGOTOONS_ENC_KEY, DEFAULT_MANGOTOONS_SALT } from './mangotoons-decryptor.js';

function slugify(text: string): string {
  return text
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '');
}

export class MangoToonsAdapter implements SourceAdapter {
  readonly id = 'mangotoons';
  readonly name = 'Mango Toons';
  readonly baseUrl = 'https://mangotoons.com';

  // The public reader moved from /api to the versioned API.  Keep this
  // separate from the browser base URL: pages are served by the CDN while
  // catalogue, metadata and manifests come from this stable API surface.
  private apiUrl = 'https://api.mangotoons.com/api/v1';
  private cdnUrl = 'https://cdn.mangotoons.com';
  private logger = new Logger('MangoToonsAdapter');
  private encKey = DEFAULT_MANGOTOONS_ENC_KEY;
  private salt = DEFAULT_MANGOTOONS_SALT;
  private token: string | null = null;

  constructor(
    private rateLimiter: HostRateLimiter = new HostRateLimiter(2.0),
    private transport: typeof fetch = fetch
  ) {
    this.rateLimiter.setHostRate('api.mangotoons.com', 2.0, 4, 4.0);
    this.rateLimiter.setHostRate('cdn.mangotoons.com', 8.0, 16, 16.0);

    if (process.env.MANGOTOONS_TOKEN) {
      this.token = process.env.MANGOTOONS_TOKEN;
    }
  }

  async login(): Promise<boolean> {
    const email = process.env.MANGOTOONS_EMAIL;
    const password = process.env.MANGOTOONS_PASSWORD;

    if (!email || !password) {
      return false;
    }

    try {
      await this.rateLimiter.acquire('api.mangotoons.com');
      const res = await this.transport(`${this.apiUrl}/auth/login`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          Accept: 'application/json',
          'User-Agent': '-',
          'sec-fetch-mode': 'none',
          Referer: `${this.baseUrl}/`,
        },
        body: JSON.stringify({ email, senha: password }),
      });

      if (res.ok) {
        const data = await res.json();
        if (data.token || data.access_token) {
          this.token = data.token || data.access_token;
          this.logger.info('Successfully authenticated with MangoToons');
          return true;
        }
      }
      this.logger.warn('Failed to authenticate with MangoToons', { status: res.status });
      return false;
    } catch (err: any) {
      this.logger.warn('Error during MangoToons login', { error: err?.message });
      return false;
    }
  }

  private async requestApi(path: string, options: RequestInit = {}): Promise<any> {
    const url = path.startsWith('http') ? path : `${this.apiUrl}${path}`;
    const parsed = new URL(url);
    await this.rateLimiter.acquire(parsed.host);

    const headers: Record<string, string> = {
      'User-Agent': '-',
      'sec-fetch-mode': 'none',
      Referer: `${this.baseUrl}/`,
      Accept: 'application/json',
      ...((options.headers as Record<string, string>) || {}),
    };

    if (this.token) {
      headers['Authorization'] = `Bearer ${this.token}`;
    }

    let res = await this.transport(url, { ...options, headers });

    // Auto-retry once on 401 if credentials are configured
    if (res.status === 401 && (process.env.MANGOTOONS_EMAIL && process.env.MANGOTOONS_PASSWORD)) {
      this.logger.info('Received 401 from MangoToons, re-authenticating...');
      this.token = null;
      const loggedIn = await this.login();
      if (loggedIn && this.token) {
        headers['Authorization'] = `Bearer ${this.token}`;
        res = await this.transport(url, { ...options, headers });
      }
    }

    if (!res.ok) {
      const err: any = new Error(`MangoToons HTTP ${res.status} for ${url}`);
      err.status = res.status;
      err.headers = res.headers;
      throw err;
    }

    this.rateLimiter.recordSuccess(parsed.host);

    const isEncrypted = res.headers.get('x-encrypted') === 'true';
    const text = await res.text();

    if (isEncrypted || (!text.trim().startsWith('{') && !text.trim().startsWith('['))) {
      return decryptMangoPayload(text, this.encKey, this.salt);
    }

    return JSON.parse(text);
  }

  async fetchUpdatedWorks(
    cursor?: string | null,
    options?: { mode?: 'bootstrap' | 'maintenance' }
  ): Promise<{ works: SourceWorkSummary[]; nextCursor: string | null }> {
    // The releases endpoint is the provider's current public update feed.
    // It gives us real published chapters and, unlike walking the full work
    // catalogue, keeps routine maintenance bounded. Cursor pairs are issued
    // by the provider and preserve a durable, deterministic traversal.
    const params = new URLSearchParams({ limite: options?.mode === 'maintenance' ? '24' : '48' });
    if (cursor) {
      const separator = cursor.lastIndexOf('|');
      if (separator > 0) {
        params.set('cursor_data', cursor.slice(0, separator));
        params.set('cursor_id', cursor.slice(separator + 1));
      }
    }
    const rawReleases = await this.requestApi(`/obras/lancamentos?${params.toString()}`);
    const releases = Array.isArray(rawReleases) ? rawReleases : rawReleases.items || rawReleases.dados || [];
    const seen = new Set<string>();
    const works: SourceWorkSummary[] = [];
    for (const release of releases) {
      const sourceWorkId = String(release.obra_id || release.work_id || release.id_obra || '');
      if (!sourceWorkId || seen.has(sourceWorkId)) continue;
      seen.add(sourceWorkId);
      const title = release.obra_nome || release.work_title || release.nome || release.title;
      works.push({
        sourceWorkId,
        title: title || sourceWorkId,
        slug: release.obra_slug || release.slug || slugify(title || sourceWorkId),
        coverUrl: this.resolveCoverUrl(release.imagem || release.coverImage || release.banner_imagem),
        updatedAt: release.published_at || release.updated_at || undefined,
      });
    }
    const last = releases[releases.length - 1];
    const nextCursor = last?.published_at && last?.id ? `${last.published_at}|${last.id}` : null;
    return { works, nextCursor };
  }

  async fetchWorkDetails(sourceWorkId: string): Promise<SourceWorkDetails> {
    const data = await this.requestApi(`/obras/${sourceWorkId}`);
    const w = data.obra || data.dados || data;

    const title = w.nome || w.title || `Work ${sourceWorkId}`;
    const slug = w.slug || w.nome_url || slugify(title);

    let kind: SourceWorkDetails['kind'] = 'UNKNOWN';
    const fmt = (w.formato_nome || w.formato || '').toLowerCase();
    if (fmt.includes('manga')) kind = 'UNKNOWN';
    else if (fmt.includes('manhua')) kind = 'MANHUA';
    else if (fmt.includes('webtoon') || fmt.includes('comic')) kind = 'WEBTOON';

    let status: SourceWorkDetails['status'] = 'UNKNOWN';
    const st = (w.status_nome || w.status || '').toLowerCase();
    if (st.includes('conclu')) status = 'COMPLETED';
    else if (st.includes('hiat') || st.includes('paus')) status = 'HIATUS';
    else if (st.includes('cancel')) status = 'CANCELLED';
    else if (st.includes('ongo') || st.includes('andamento') || st.includes('releasing') || st.includes('ativo') || st.includes('lanç') || st.includes('lanc')) status = 'ONGOING';

    const genres: string[] = (w.tags || []).map((t: any) => typeof t === 'string' ? t : t.nome || t.name).filter(Boolean);

    return {
      sourceWorkId: String(w.id || sourceWorkId),
      title,
      slug,
      coverUrl: this.resolveCoverUrl(w.imagem || w.coverImage || w.banner_imagem),
      synopsis: w.descricao || undefined,
      kind,
      status,
      genres,
      raw: w,
    };
  }

  async fetchChapters(sourceWorkId: string): Promise<SourceChapterSummary[]> {
    const data = await this.requestApi(`/obras/${sourceWorkId}/capitulos?pagina=1&limite=500`);
    const chaptersList = Array.isArray(data) ? data : data.capitulos || data.items || data.dados || [];

    const summaries: SourceChapterSummary[] = chaptersList.map((ch: any) => {
      const num = typeof ch.numero === 'number' ? ch.numero : parseFloat(ch.numero);
      return {
        // The public manifest is keyed by its displayed chapter value, not
        // by the internal database ID. Preserve its exact source spelling so
        // fractional/special chapters are not collapsed.
        sourceChapterId: `${sourceWorkId}:${encodeURIComponent(String(ch.numero ?? ch.id))}`,
        number: isNaN(num) ? 0 : num,
        title: ch.nome || ch.title || undefined,
        createdAt: ch.published_at || ch.criado_em || ch.atualizado_em || undefined,
        pageCount: ch.total_paginas || undefined,
      };
    });

    // Return chapters sorted by chapter number ascending for clarity
    return summaries.sort((a, b) => a.number - b.number);
  }

  async fetchChapterPages(sourceChapterId: string, chapterNumber?: number): Promise<string[]> {
    // sourceChapterId is formatted as "${sourceWorkId}:${chapterIdOrNumber}"
    let workId = '';
    let chapterValue = '';

    if (sourceChapterId.includes(':')) {
      const [wId, cId] = sourceChapterId.split(':', 2);
      workId = wId;
      chapterValue = decodeURIComponent(cId || (chapterNumber !== undefined ? String(chapterNumber) : '1'));
    } else {
      workId = sourceChapterId;
      chapterValue = chapterNumber !== undefined ? String(chapterNumber) : '1';
    }

    const data = await this.requestApi(
      `/leitor/obras/${encodeURIComponent(workId)}/capitulos/${encodeURIComponent(chapterValue)}/manifest`,
    );
    const pages = data.pages || [];
    const sortedPages = [...pages].sort((a: any, b: any) => (a.index ?? a.numero ?? 0) - (b.index ?? b.numero ?? 0));

    return sortedPages
      .map((p: any) => {
        const url = p.url || p.cdn_id || p.imagem || p.image || p.src || p.link || p.path;
        if (!url) return null;
        if (url.startsWith('http')) return url;
        return `${this.cdnUrl}/${url.replace(/^\//, '')}`;
      })
      .filter((url): url is string => Boolean(url));
  }

  private resolveCoverUrl(url?: string | null): string | null {
    if (!url) return null;
    if (url.startsWith('http')) return url;
    return `${this.cdnUrl}/${url.replace(/^\//, '')}`;
  }

  async searchWorks(query: string): Promise<SourceWorkSummary[]> {
    const clean = query.trim();
    if (!clean) return [];

    try {
      const data = await this.requestApi(`/obras?pagina=1&limite=20&busca=${encodeURIComponent(clean)}`);
      const rawWorks = data.obras || data.dados || data.items || [];

      return rawWorks.map((w: any) => ({
        sourceWorkId: String(w.id),
        title: w.nome || w.title,
        slug: w.slug || w.nome_url || slugify(w.nome || w.title || String(w.id)),
        coverUrl: this.resolveCoverUrl(w.imagem || w.coverImage || w.banner_imagem),
        updatedAt: w.atualizada_em || w.criada_em || undefined,
      }));
    } catch (err: any) {
      this.logger.warn(`Search failed on MangoToons for query "${query}"`, { error: err?.message });
      return [];
    }
  }
}
