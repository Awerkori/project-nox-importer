import { describe, expect, it } from 'vitest';
import { MangaOnlineTvAdapter } from '../src/sources/mangaonlinetv/mangaonlinetv-adapter';
import { MangaLivreToAdapter } from '../src/sources/mangalivreto/mangalivreto-adapter';

describe('Madara reader manifests', () => {
  it('reads every page of a paged chapter instead of sidebar recommendations', async () => {
    const urls = Array.from({ length: 21 }, (_, i) => `https://cdn.example/chapter/${i + 1}.webp`);
    const html = `<img class="wp-manga-chapter-img" src="${urls[0]}"><script>var chapter_preloaded_images = ${JSON.stringify(urls)}, chapter_images_per_page = 1;</script><img src="https://mangaonline.tv/wp-content/uploads/2025/07/cover-75x106.webp">`;
    const adapter = new MangaOnlineTvAdapter(undefined, async () => new Response(html));
    expect(await adapter.fetchChapterPages('/manga/story/capitulo-51/', 51)).toEqual(urls);
  });
  it('excludes sidebar covers when the chapter uses ordinary img tags', async () => {
    const html = '<img src="https://mangaonline.tv/wp-content/uploads/2025/07/cover-75x106.webp"><img class="wp-manga-chapter-img" data-src="https://cdn.example/1.jpg">';
    const adapter = new MangaOnlineTvAdapter(undefined, async () => new Response(html));
    expect(await adapter.fetchChapterPages('/chapter/', 1)).toEqual(['https://cdn.example/1.jpg']);
  });

  it('never treats a sidebar chapter from another work as a chapter of the requested work', async () => {
    const html = `
      <a href="https://mangalivre.to/manga/tokyo-fears-rhapsody/capitulo-1/">Capítulo 1</a>
      <aside><a href="https://mangalivre.to/manga/madrasta-vem-primeiro-sem-censura/capitulo-10/">Capítulo 10</a></aside>
    `;
    const adapter = new MangaLivreToAdapter(undefined, async () => new Response(html));

    await expect(adapter.fetchChapters('tokyo-fears-rhapsody')).resolves.toEqual([
      expect.objectContaining({
        sourceChapterId: 'https://mangalivre.to/manga/tokyo-fears-rhapsody/capitulo-1/',
        number: 1,
      }),
    ]);
    expect(adapter.isChapterOwnedByWork(
      'tokyo-fears-rhapsody',
      'https://mangalivre.to/manga/madrasta-vem-primeiro-sem-censura/capitulo-10/',
    )).toBe(false);
  });

  it('detects prose web novel chapters and returns an empty manifest instead of scraping theme mascots', async () => {
    const novelHtml = `
      <div class="reading-content">
        <div class="chapter-warning alert alert-warning">Sigam a gente</div>
        <img class="img-responsive" src="https://nocfsb.com/wp-content/uploads/2023/06/escolhida-1.webp" alt="Nocturne Fansub"/>
        <div class="text-left">
          <div>Aproveitando a oportunidade enquanto a Serpente Alada estava ocupada com o Pássaro de Chama macho...</div>
          <div>O sangue da serpente chovia do céu enquanto os dois guerreiros avançavam em direção ao centro do vale sagrado.</div>
        </div>
      </div>
    `;
    const adapter = new MangaOnlineTvAdapter(undefined, async () => new Response(novelHtml));
    const pages = await adapter.fetchChapterPages('https://nocfsb.com/manga/aventura/capitulo-40/', 40);
    expect(pages).toEqual([]);
  });

  it('filters out known mascot/banner filenames when real comic images are present', async () => {
    const html = `
      <div class="reading-content">
        <img class="wp-manga-chapter-img" src="https://nocfsb.com/wp-content/uploads/2023/06/escolhida-1.webp"/>
        <img class="wp-manga-chapter-img" src="https://nocfsb.com/wp-content/uploads/WP-manga/data/manga_123/page-1.webp"/>
        <img class="wp-manga-chapter-img" src="https://nocfsb.com/wp-content/uploads/WP-manga/data/manga_123/page-2.webp"/>
      </div>
    `;
    const adapter = new MangaOnlineTvAdapter(undefined, async () => new Response(html));
    const pages = await adapter.fetchChapterPages('https://nocfsb.com/manga/test/capitulo-1/', 1);
    expect(pages).toEqual([
      'https://nocfsb.com/wp-content/uploads/WP-manga/data/manga_123/page-1.webp',
      'https://nocfsb.com/wp-content/uploads/WP-manga/data/manga_123/page-2.webp',
    ]);
  });
});
