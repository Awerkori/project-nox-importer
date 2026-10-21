import { describe, expect, it } from 'vitest';
import { MadaraAdapter } from '../madara-adapter.js';

describe('MadaraAdapter chapter ownership', () => {
  it('accepts a chapter beneath a subpath base URL without accepting another work', () => {
    const adapter = new MadaraAdapter({
      id: 'subpath', name: 'Subpath', baseUrl: 'https://source.example/bruxonas', mangaSubString: 'manga',
    });

    expect(adapter.isChapterOwnedByWork('mary-jane',
      'https://source.example/bruxonas/manga/mary-jane/capitulo-74/')).toBe(true);
    expect(adapter.isChapterOwnedByWork('mary-jane',
      'https://source.example/bruxonas/manga/another-work/capitulo-74/')).toBe(false);
    expect(adapter.isChapterOwnedByWork('mary-jane',
      'https://source.example/manga/mary-jane/capitulo-74/')).toBe(false);
  });

  it('keeps the ordinary root-hosted Madara route scoped by work', () => {
    const adapter = new MadaraAdapter({
      id: 'root', name: 'Root', baseUrl: 'https://source.example', mangaSubString: 'manga',
    });
    expect(adapter.isChapterOwnedByWork('solo-leveling',
      'https://source.example/manga/solo-leveling/capitulo-1/')).toBe(true);
    expect(adapter.isChapterOwnedByWork('solo-leveling',
      'https://source.example/manga/one-piece/capitulo-1/')).toBe(false);
  });

  it('extracts alternative titles from standard PT-BR Madara Alternativo / Associados labels and preserves short/CJK aliases', async () => {
    const html = `
      <div class="post-title"><h1>Bleach</h1></div>
      <div class="post-content_item">
        <div class="summary-heading"><h5>Alternativo</h5></div>
        <div class="summary-content">ブリーチ; 死神; X; 86</div>
      </div>
    `;
    const adapter = new MadaraAdapter(
      { id: 'madara-test', name: 'Madara Test', baseUrl: 'https://source.example' },
      undefined,
      (async () => new Response(html, { status: 200, headers: { 'content-type': 'text/html' } })) as any,
    );
    const details = await adapter.fetchWorkDetails('bleach');
    expect(details.alternativeTitles).toEqual(['ブリーチ', '死神', 'X', '86']);
  });
});
