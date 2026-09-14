import { describe, expect, it } from 'vitest';
import { MangaOnlineTvAdapter } from '../src/sources/mangaonlinetv/mangaonlinetv-adapter';

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
});
