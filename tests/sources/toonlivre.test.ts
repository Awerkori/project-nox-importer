import { describe, it, expect, vi } from 'vitest';
import { ToonLivreAdapter } from '../../src/sources/toonlivre/toonlivre-adapter.js';
import { HostRateLimiter } from '../../src/core/rate-limiter.js';

describe('ToonLivreAdapter', () => {
  it('correctly discovers works via mangas search endpoint', async () => {
    const mockTransport = vi.fn().mockResolvedValue({
      ok: true,
      status: 200,
      headers: new Headers(),
      json: async () => ({
        mangas: [
          {
            id: 'obra-123',
            title: 'Tower of God',
            uploadSlug: 'tower-of-god',
            coverUrl: 'https://cdn.toonlivre.net/covers/tog.webp',
            recentChapters: [{ timestamp: 1788700000000 }],
          },
        ],
        pagination: { hasNextPage: true },
      }),
    });

    const rateLimiter = new HostRateLimiter(10.0);
    const adapter = new ToonLivreAdapter(rateLimiter, mockTransport as any);

    const { works, nextCursor } = await adapter.fetchUpdatedWorks(null, { mode: 'maintenance' });
    expect(works.length).toBe(1);
    expect(works[0].sourceWorkId).toBe('obra-123');
    expect(works[0].title).toBe('Tower of God');
    expect(works[0].slug).toBe('tower-of-god');
    expect(nextCursor).toBe('2');
  });

  it('correctly fetches work details and chapters', async () => {
    const mockTransport = vi.fn().mockResolvedValue({
      ok: true,
      status: 200,
      headers: new Headers(),
      json: async () => ({
        id: 'obra-123',
        title: 'Tower of God',
        coverUrl: 'https://cdn.toonlivre.net/covers/tog.webp',
        type: 'Manhwa',
        description: 'What do you desire? Fortune? Glory? Power? Revenge?',
        authors: ['SIU'],
        genres: ['Action', 'Fantasy', 'Supernatural'],
        status: 'Ongoing',
        chapters: [
          { id: 'cap-1', number: '1', timestamp: 1600000000000, pageCount: 20 },
          { id: 'cap-2', number: '2', timestamp: 1600100000000, pageCount: 22 },
        ],
      }),
    });

    const rateLimiter = new HostRateLimiter(10.0);
    const adapter = new ToonLivreAdapter(rateLimiter, mockTransport as any);

    const details = await adapter.fetchWorkDetails('obra-123');
    expect(details.sourceWorkId).toBe('obra-123');
    expect(details.kind).toBe('MANHWA');
    expect(details.status).toBe('ONGOING');
    expect(details.author).toBe('SIU');

    const chapters = await adapter.fetchChapters('obra-123');
    expect(chapters.length).toBe(2);
    expect(chapters[0].number).toBe(1);
    expect(chapters[0].pageCount).toBe(20);
    expect(chapters[1].number).toBe(2);
    expect(chapters[1].pageCount).toBe(22);
  });

  it('fails safely with descriptive VERIFICATION_FAILED error for reader pages protected by Turnstile', async () => {
    const rateLimiter = new HostRateLimiter(10.0);
    const adapter = new ToonLivreAdapter(rateLimiter);

    await expect(adapter.fetchChapterPages('cap-1', 1)).rejects.toThrow(
      /Turnstile verification challenge required/
    );
  });
});
