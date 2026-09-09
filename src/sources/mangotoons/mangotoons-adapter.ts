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
  readonly baseUrl = 'https://api.mangotoons.com';

  private apiUrl = 'https://api.mangotoons.com/api';
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
    const mode = options?.mode || 'bootstrap';

    if (mode === 'bootstrap') {
      const page = parseInt(cursor || '1', 10);
      const limit = 24;
      const data = await this.requestApi(`/obras?pagina=${page}&limite=${limit}`);
      const rawWorks = data.obras || data.dados || data.items || [];

      const works: SourceWorkSummary[] = rawWorks.map((w: any) => ({
        sourceWorkId: String(w.id),
        title: w.nome || w.title,
        slug: w.slug || w.nome_url || slugify(w.nome || w.title || String(w.id)),
        coverUrl: this.resolveCoverUrl(w.imagem || w.coverImage || w.banner_imagem),
        updatedAt: w.atualizada_em || w.criada_em || undefined,
      }));

      const hasNext = data.pagination?.hasNextPage ?? (rawWorks.length === limit);
      const nextCursor = hasNext ? String(page + 1) : null;

      return { works, nextCursor };
    } else {
      // Maintenance mode: check recent chapters/updates
      const data = await this.requestApi('/capitulos/recentes?pagina=1&limite=24');
      const rawWorks = data.obras || data.dados || data.items || [];

      const works: SourceWorkSummary[] = rawWorks.map((w: any) => ({
        sourceWorkId: String(w.id),
        title: w.nome || w.title,
        slug: w.slug || w.nome_url || slugify(w.nome || w.title || String(w.id)),
        coverUrl: this.resolveCoverUrl(w.imagem || w.coverImage || w.banner_imagem),
        updatedAt: w.atualizada_em || w.criada_em || undefined,
      }));

      return { works, nextCursor: null };
    }
  }

  async fetchWorkDetails(sourceWorkId: string): Promise<SourceWorkDetails> {
    const data = await this.requestApi(`/obras/${sourceWorkId}`);
    const w = data.obra || data.dados || data;

    const title = w.nome || w.title || `Work ${sourceWorkId}`;
    const slug = w.slug || w.nome_url || slugify(title);

    let kind: SourceWorkDetails['kind'] = 'MANHWA';
    const fmt = (w.formato_nome || '').toLowerCase();
    if (fmt.includes('manga')) kind = 'MANGA';
    else if (fmt.includes('manhua')) kind = 'MANHUA';
    else if (fmt.includes('webtoon') || fmt.includes('comic')) kind = 'WEBTOON';

    let status: SourceWorkDetails['status'] = 'ONGOING';
    const st = (w.status_nome || '').toLowerCase();
    if (st.includes('conclu')) status = 'COMPLETED';
    else if (st.includes('hiat') || st.includes('paus')) status = 'HIATUS';
    else if (st.includes('cancel')) status = 'CANCELLED';

    const genres: string[] = (w.tags || []).map((t: any) => t.nome || t.name).filter(Boolean);

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
    const data = await this.requestApi(`/obras/${sourceWorkId}`);
    const w = data.obra || data.dados || data;
    const chaptersList = w.capitulos || [];

    const summaries: SourceChapterSummary[] = chaptersList.map((ch: any) => {
      const num = typeof ch.numero === 'number' ? ch.numero : parseFloat(ch.numero);
      return {
        // Encode both workId and chapterId/number in sourceChapterId so fetchChapterPages has all context
        sourceChapterId: `${sourceWorkId}:${ch.id || ch.numero}`,
        number: isNaN(num) ? 0 : num,
        title: ch.nome || ch.title || undefined,
        createdAt: ch.criado_em || ch.atualizado_em || undefined,
        pageCount: ch.total_paginas || undefined,
      };
    });

    // Return chapters sorted by chapter number ascending for clarity
    return summaries.sort((a, b) => a.number - b.number);
  }

  async fetchChapterPages(sourceChapterId: string, chapterNumber?: number): Promise<string[]> {
    // sourceChapterId is formatted as "${sourceWorkId}:${chapterIdOrNumber}"
    let workId = '';
    let chIdentifier = '';

    if (sourceChapterId.includes(':')) {
      const [wId, cId] = sourceChapterId.split(':');
      workId = wId;
      chIdentifier = chapterNumber !== undefined ? String(chapterNumber) : cId;
    } else {
      workId = sourceChapterId;
      chIdentifier = chapterNumber !== undefined ? String(chapterNumber) : '1';
    }

    const data = await this.requestApi(`/obras/${workId}/capitulos/${chIdentifier}`);
    const cap = data.capitulo || data.dados || data;
    const pages = cap.paginas || [];

    const sortedPages = [...pages].sort((a: any, b: any) => (a.numero || 0) - (b.numero || 0));

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
      const data = await this.requestApi(`/obras?pesquisa=${encodeURIComponent(clean)}&limite=20`);
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
