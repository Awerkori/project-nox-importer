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
});
