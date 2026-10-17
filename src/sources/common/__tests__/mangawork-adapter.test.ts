import { describe, expect, it } from 'vitest';
import { MangaWorkAdapter } from '../mangawork-adapter.js';

describe('MangaWorkAdapter work details', () => {
  it('uses a configured document-title fallback only when the template h1 is a login form', async () => {
    const adapter = new MangaWorkAdapter(
      { id: 'test', name: 'Test', baseUrl: 'https://source.test', titleSuffixes: ['Source'] },
      undefined,
      async () => new Response('<title>Real Work – Source</title><h1>Login</h1>'),
    );

    await expect(adapter.fetchWorkDetails('real-work')).resolves.toMatchObject({ title: 'Real Work' });
  });

  it('does not alter an editorial h1 that happens to include a dash', async () => {
    const adapter = new MangaWorkAdapter(
      { id: 'test', name: 'Test', baseUrl: 'https://source.test', titleSuffixes: ['Source'] },
      undefined,
      async () => new Response('<title>Other – Source</title><h1>Hero - Reborn</h1>'),
    );

    await expect(adapter.fetchWorkDetails('hero')).resolves.toMatchObject({ title: 'Hero - Reborn' });
  });
});
