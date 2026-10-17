import { describe, expect, it, vi } from 'vitest';
import { HostRateLimiter } from '../../src/core/rate-limiter.js';
import { MangaOnlineAdapter } from '../../src/sources/mangaonline/mangaonline-adapter.js';

const catalogHtml = [
  '<article class="home-manga-card">',
  '<a class="home-manga-cover" href="https://mangaonline.love/manga/eleceed/"><img src="/cover.webp"></a>',
  '<h3><a href="https://mangaonline.love/manga/eleceed/">Eleceed</a></h3>',
  '</article>',
].join('');

const workHtml = [
  '<h1 class="manga-title">Eleceed</h1>',
  '<div class="manga-cover"><img src="/cover.webp"></div>',
  '<div class="synopsis-content">Uma sinopse.</div>',
  '<div class="manga-tags"><a>Ação</a><a>Fantasia</a></div>',
  '<div class="manga-meta-item"><span class="meta-label">Status:</span><span class="meta-value">Em Andamento</span></div>',
  '<ul class="chapters-list">',
  '<li class="chapter-item"><a href="https://mangaonline.love/capitulo/eleceed-capitulo-2/" class="chapter-link"><span class="chapter-number">Capítulo 2</span></a></li>',
  '<li class="chapter-item"><a href="https://mangaonline.love/capitulo/outro-capitulo-1/" class="chapter-link"><span class="chapter-number">Capítulo 1</span></a></li>',
  '</ul>',
].join('');

const readerHtml = [
  '<div class="chapter-images">',
  '<div class="chapter-image-container"><img class="chapter-image" src="/pages/001.webp"></div>',
  '<div class="chapter-image-container"><img class="chapter-image" data-src="/pages/002.webp"></div>',
  '</div>',
].join('');

describe('MangaOnlineAdapter', () => {
  it('uses the current custom-theme catalog, manifest and reader without accepting another work chapter', async () => {
    const transport = vi.fn(async (url: string | URL) => {
      const value = String(url);
      if (value.includes('atualizacoes=1')) return new Response(catalogHtml);
      if (value.includes('/manga/eleceed/')) return new Response(workHtml);
      if (value.includes('/capitulo/eleceed-capitulo-2/')) return new Response(readerHtml);
      return new Response('not found', { status: 404 });
    });
    const adapter = new MangaOnlineAdapter(new HostRateLimiter(100), transport as typeof fetch);

    const { works } = await adapter.fetchUpdatedWorks(null, { mode: 'maintenance' });
    expect(works).toHaveLength(1);
    expect(works[0].sourceWorkId).toBe('eleceed');

    const details = await adapter.fetchWorkDetails('eleceed');
    expect(details.title).toBe('Eleceed');
    expect(details.genres).toEqual(['Ação', 'Fantasia']);
    expect(details.status).toBe('ONGOING');

    const chapters = await adapter.fetchChapters('eleceed');
    expect(chapters).toHaveLength(1);
    expect(chapters[0]).toMatchObject({ displayNumber: 'Capítulo 2', number: 2 });
    expect(adapter.isChapterOwnedByWork('eleceed', 'https://mangaonline.love/capitulo/eleceed-capitulo-2/')).toBe(true);
    expect(adapter.isChapterOwnedByWork('eleceed', 'https://mangaonline.love/capitulo/outro-capitulo-1/')).toBe(false);

    await expect(adapter.fetchChapterPages(chapters[0].sourceChapterId)).resolves.toEqual([
      'https://mangaonline.love/pages/001.webp',
      'https://mangaonline.love/pages/002.webp',
    ]);
  });
});
