import { describe, it, expect, vi } from 'vitest';
import { MangaFlixAdapter } from '../../src/sources/mangaflix/mangaflix-adapter.js';
import { HostRateLimiter } from '../../src/core/rate-limiter.js';

describe('MangaFlixAdapter', () => {
  it('correctly fetches updated works in maintenance mode', async () => {
    const mockTransport = vi.fn().mockResolvedValue({
      ok: true,
      status: 200,
      headers: new Headers(),
      json: async () => ({
        data: [
          {
            _id: 'mf-work-1',
            name: 'Solo Necromancer',
            poster: { default_url: 'https://static.mangaflix.net/cover.jpg' },
            chapters: [{ created_at: '2026-09-08T10:00:00Z' }],
          },
        ],
      }),
    });

    const rateLimiter = new HostRateLimiter(10.0);
    const adapter = new MangaFlixAdapter(rateLimiter, mockTransport as any);

    const { works, nextCursor } = await adapter.fetchUpdatedWorks(null, { mode: 'maintenance' });
    expect(works.length).toBe(1);
    expect(works[0].sourceWorkId).toBe('mf-work-1');
    expect(works[0].title).toBe('Solo Necromancer');
    expect(works[0].slug).toBe('solo-necromancer');
    expect(works[0].coverUrl).toBe('https://static.mangaflix.net/cover.jpg');
    expect(nextCursor).toBe('2026-09-08T10:00:00Z');
  });

  it('correctly fetches work details and chapters', async () => {
    const mockTransport = vi.fn().mockResolvedValue({
      ok: true,
      status: 200,
      headers: new Headers(),
      json: async () => ({
        data: {
          _id: 'mf-work-1',
          name: 'Solo Necromancer',
          description: 'A dark fantasy necromancy story.',
          poster: { default_url: 'https://static.mangaflix.net/cover.jpg' },
          genres: [{ name: 'Action' }, { name: 'Fantasy' }],
          content_type: 'manhwa',
          chapters: [
            {
              _id: 'mf-ch-1',
              number: '1',
              name: 'Capítulo 1',
              created_at: '2026-09-01T00:00:00Z',
              number_of_complete_pages: 15,
            },
            {
              _id: 'mf-ch-2',
              number: '2',
              name: 'Capítulo 2',
              created_at: '2026-09-02T00:00:00Z',
              number_of_complete_pages: 18,
            },
          ],
        },
      }),
    });

    const rateLimiter = new HostRateLimiter(10.0);
    const adapter = new MangaFlixAdapter(rateLimiter, mockTransport as any);

    const details = await adapter.fetchWorkDetails('mf-work-1');
    expect(details.sourceWorkId).toBe('mf-work-1');
    expect(details.kind).toBe('MANHWA');
    expect(details.genres).toEqual(['Action', 'Fantasy']);

    const chapters = await adapter.fetchChapters('mf-work-1');
    expect(chapters.length).toBe(2);
    expect(chapters[0].number).toBe(1);
    expect(chapters[0].pageCount).toBe(15);
    expect(chapters[1].number).toBe(2);
    expect(chapters[1].pageCount).toBe(18);
  });

  it('correctly fetches chapter page URLs sorted by order', async () => {
    const mockTransport = vi.fn().mockResolvedValue({
      ok: true,
      status: 200,
      headers: new Headers(),
      json: async () => ({
        data: {
          images: [
            { default_url: 'https://static.mangaflix.net/p2.webp', order: 2 },
            { default_url: 'https://static.mangaflix.net/p1.webp', order: 1 },
          ],
        },
      }),
    });

    const rateLimiter = new HostRateLimiter(10.0);
    const adapter = new MangaFlixAdapter(rateLimiter, mockTransport as any);

    const pages = await adapter.fetchChapterPages('mf-ch-1');
    expect(pages).toEqual([
      'https://static.mangaflix.net/p1.webp',
      'https://static.mangaflix.net/p2.webp',
    ]);
  });
});
