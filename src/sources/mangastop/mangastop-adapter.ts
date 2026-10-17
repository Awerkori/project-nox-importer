import { HostRateLimiter } from '../../core/rate-limiter.js';
import { extractChapterNumber, slugify, stripHtml } from '../common/html-utils.js';
import { SourceAdapter, SourceChapterSummary, SourceWorkDetails, SourceWorkSnapshot, SourceWorkSummary } from '../types.js';

type StopWork = Record<string, any>;

/** Public first-party WordPress API; no browser token, cookie or DOM selector. */
export class MangaStopAdapter implements SourceAdapter {
  readonly id = 'mangastop';
  readonly name = 'Manga Stop';
  readonly baseUrl = 'https://mangastop.net';
  private readonly apiUrl = `${this.baseUrl}/wp-json/mangastop/v1`;
  // The marketing page is behind a browser challenge, while the documented
  // first-party API used by this adapter remains publicly available. Probe
  // the actual execution surface rather than a page the importer never uses.
  readonly probeUrl = `${this.baseUrl}/wp-json/mangastop/v1/recentes?pagina=1&por_pagina=1`;

  constructor(
    private readonly rateLimiter: HostRateLimiter = new HostRateLimiter(2),
    private readonly transport: typeof fetch = fetch,
  ) {
    this.rateLimiter.setHostRate(new URL(this.baseUrl).host, 2, 4, 4);
    this.rateLimiter.setHostRate('images.mangastop.wtf', 8, 16, 16);
  }

  private async request<T>(path: string): Promise<T> {
    const url = `${this.apiUrl}${path}`;
    const host = new URL(url).host;
    await this.rateLimiter.acquire(host);
    const response = await this.transport(url, {
      headers: { Accept: 'application/json', Origin: this.baseUrl, Referer: `${this.baseUrl}/` },
      signal: AbortSignal.timeout(15_000),
    });
    if (!response.ok) throw new Error(`MangaStop HTTP ${response.status} for ${path}`);
    this.rateLimiter.recordSuccess(host);
    return response.json() as Promise<T>;
  }

  private summary(work: StopWork): SourceWorkSummary {
    const title = String(work.titulo || work.title || `Obra ${work.id}`);
    return {
      sourceWorkId: String(work.id), title, slug: work.slug || slugify(title),
      coverUrl: work.thumbnail || work.capa_url || null,
      updatedAt: work.capitulos?.[0]?.data || work.updated_at || undefined,
    };
  }

  private details(work: StopWork): SourceWorkDetails {
    const title = String(work.titulo || work.title || `Obra ${work.id}`);
    const type = String(work.tipo || '').toLocaleLowerCase();
    const kind: SourceWorkDetails['kind'] = type.includes('manhua') ? 'MANHUA'
      : type.includes('webtoon') ? 'WEBTOON'
        : type.includes('pornhwa') ? 'PORNHWA'
          : type.includes('manhwa') ? 'MANHWA'
            : type.includes('manga') ? 'MANGA' : 'UNKNOWN';
    const statusText = String(work.status || '').toLocaleLowerCase();
    const status: SourceWorkDetails['status'] = statusText.includes('complete') || statusText.includes('conclu') ? 'COMPLETED'
      : statusText.includes('hiat') ? 'HIATUS'
        : statusText.includes('ongoing') || statusText.includes('andamento') ? 'ONGOING' : 'UNKNOWN';
    const alternate = typeof work.alternativo === 'string' ? work.alternativo.trim() : '';
    const genres: string[] = (Array.isArray(work.generos) ? work.generos : [])
      .map((genre: any): string | undefined => typeof genre === 'string' ? genre : genre?.nome || genre?.name)
      .filter((genre: string | undefined): genre is string => Boolean(genre)) as string[];
    return {
      sourceWorkId: String(work.id), title, slug: work.slug || slugify(title),
      coverUrl: work.capa_url || work.thumbnail || null,
      synopsis: typeof work.sinopse === 'string' ? stripHtml(work.sinopse).trim() || undefined : undefined,
      author: work.autor || work.manga_autor?.map((author: any) => author.nome).filter(Boolean).join(', ') || undefined,
      artist: work.artista || work.manga_artista?.map((artist: any) => artist.nome).filter(Boolean).join(', ') || undefined,
      kind, status, genres: genres.length ? [...new Set(genres)] : undefined,
      alternativeTitles: alternate && alternate.localeCompare(title, undefined, { sensitivity: 'accent' }) !== 0 ? [alternate] : undefined,
      raw: work,
    };
  }

  private chapters(work: StopWork): SourceChapterSummary[] {
    const sourceChapters: any[] = Array.isArray(work.capitulos) ? work.capitulos : [];
    return sourceChapters.map((chapter: any): SourceChapterSummary => {
      const displayNumber = String(chapter.numero || chapter.id);
      const parsed = Number.parseFloat(displayNumber.replace(',', '.'));
      return {
        sourceChapterId: String(chapter.id), displayNumber,
        number: Number.isFinite(parsed) ? parsed : extractChapterNumber(String(chapter.titulo_post || chapter.titulo || displayNumber)),
        title: chapter.titulo_post || chapter.titulo || `Capítulo ${displayNumber}`,
        createdAt: chapter.data_publicacao || chapter.data || undefined,
      };
    }).sort((a, b) => a.number - b.number);
  }

  async fetchUpdatedWorks(cursor?: string | null): Promise<{ works: SourceWorkSummary[]; nextCursor: string | null }> {
    const page = Math.max(1, Number.parseInt(cursor || '1', 10) || 1);
    const data = await this.request<{ mangas?: StopWork[]; paginacao?: { tem_proxima?: boolean } }>(`/recentes?pagina=${page}&por_pagina=24`);
    return { works: (data.mangas || []).map((work) => this.summary(work)), nextCursor: data.paginacao?.tem_proxima ? String(page + 1) : null };
  }

  async fetchWorkDetails(sourceWorkId: string): Promise<SourceWorkDetails> {
    return this.details(await this.request<StopWork>(`/obra/${encodeURIComponent(sourceWorkId)}`));
  }

  async fetchWorkSnapshot(sourceWorkId: string): Promise<SourceWorkSnapshot> {
    const work = await this.request<StopWork>(`/obra/${encodeURIComponent(sourceWorkId)}`);
    return { details: this.details(work), chapters: this.chapters(work) };
  }

  async fetchChapters(sourceWorkId: string): Promise<SourceChapterSummary[]> {
    return this.chapters(await this.request<StopWork>(`/obra/${encodeURIComponent(sourceWorkId)}`));
  }

  async fetchChapterPages(sourceChapterId: string): Promise<string[]> {
    const data = await this.request<{ imagens?: Array<{ url?: string }> }>(`/leitor/${encodeURIComponent(sourceChapterId)}`);
    return (data.imagens || []).map((image) => image.url).filter((url): url is string => Boolean(url));
  }

  async searchWorks(query: string): Promise<SourceWorkSummary[]> {
    const data = await this.request<{ obras?: { lista?: StopWork[] } }>(`/busca?pagina=1&q=${encodeURIComponent(query)}`);
    return (data.obras?.lista || []).map((work) => this.summary(work));
  }

  getImageHeaders(): Record<string, string> {
    return { Referer: `${this.baseUrl}/`, Accept: 'image/avif,image/webp,image/*,*/*;q=0.8' };
  }
}
