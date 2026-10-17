import { describe, expect, it } from 'vitest';
import { ZeistMangaAdapter } from '../zeistmanga-adapter.js';

describe('ZeistMangaAdapter work details', () => {
  it('prefers source metadata over a theme header and accepts either meta attribute order', async () => {
    const adapter = new ZeistMangaAdapter(
      { id: 'test', name: 'Test', baseUrl: 'https://source.test' },
      undefined,
      async () => new Response(`
        <meta content='Real Series' property='og:title'>
        <meta property='og:image' content='https://images.test/cover.jpg'>
        <h1>Theme Site Name</h1>
        <main data-title='Real Series'><h1 itemprop='name'>Real Series</h1></main>
      `),
    );

    const details = await adapter.fetchWorkDetails('https://source.test/series.html|Series');

    expect(details.title).toBe('Real Series');
    expect(details.coverUrl).toBe('https://images.test/cover.jpg');
  });
});
