import { describe, it, expect } from 'vitest';
import { NexusAdapter } from '../src/sources/nexus/nexus-adapter.js';
import { HostRateLimiter } from '../src/core/rate-limiter.js';

describe('NexusAdapter', () => {
  it('parses work details and maps types and statuses', async () => {
    const mockTransport: typeof fetch = async (url, init) => {
      const urlStr = url.toString();
      if (urlStr.includes('/works?id=eq.nx-1')) {
        return {
          ok: true,
          status: 200,
          json: async () => [
            {
              id: 'nx-1',
              title: 'Super Returner',
              slug: 'super-returner',
              cover_url: 'https://cdn.nexusmangas.com/works/nx-1/cover.webp',
              description: 'Awesome fantasy series',
              alternative_title: 'Great Return, Returner Hero',
              status: 'RELEASING',
              type: 'MANHWA',
              content_rating: 'SAFE',
              release_year: 2024,
              author: 'Author Nim',
              artist: 'Artist Nim',
              work_genres: [{ genre: { name: 'Ação' } }, { genre: { name: 'Fantasia' } }],
            },
          ],
        } as any;
      }
      return { ok: false, status: 404 } as any;
    };

    const adapter = new NexusAdapter(new HostRateLimiter(100), mockTransport);
    const details = await adapter.fetchWorkDetails('nx-1');

    expect(details.title).toBe('Super Returner');
    expect(details.kind).toBe('MANHWA');
    expect(details.status).toBe('ONGOING');
    expect(details.genres).toEqual(['Ação', 'Fantasia']);
    expect(details.alternativeTitles).toEqual(['Great Return', 'Returner Hero']);
    expect(details.year).toBe(2024);
  });

  it('fetches chapter list and parses chapter numbers', async () => {
    const mockTransport: typeof fetch = async (url) => {
      if (url.toString().includes('/chapters?work_id=eq.nx-1')) {
        return {
          ok: true,
          status: 200,
          json: async () => [
            { id: 'ch-1', number: 1, title: 'Prologue', created_at: '2026-01-01T00:00:00Z', page_count: 30 },
            { id: 'ch-2', number: 2.5, title: 'Side Story', created_at: '2026-01-02T00:00:00Z', page_count: 15 },
          ],
        } as any;
      }
      return { ok: false, status: 404 } as any;
    };

    const adapter = new NexusAdapter(new HostRateLimiter(100), mockTransport);
    const chapters = await adapter.fetchChapters('nx-1');

    expect(chapters.length).toBe(2);
    expect(chapters[0].number).toBe(1);
    expect(chapters[1].number).toBe(2.5);
    expect(chapters[0].pageCount).toBe(30);
  });

  it('fetches chapter pages by invoking the reader-v3 function', async () => {
    let capturedHeaders: any;
    let capturedBody: any;

    const mockTransport: typeof fetch = async (url, init) => {
      if (url.toString().includes('/functions/v1/read-chapter')) {
        capturedHeaders = init?.headers;
        capturedBody = JSON.parse(init?.body as string);
        return {
          ok: true,
          status: 200,
          json: async () => ({
            success: true,
            chapter: {
              id: 'ch-1',
              pages: [
                'https://cdn.nexusmangas.com/page1.webp',
                'https://cdn.nexusmangas.com/page2.webp',
              ],
            },
          }),
        } as any;
      }
      return { ok: false, status: 404 } as any;
    };

    const adapter = new NexusAdapter(new HostRateLimiter(100), mockTransport);
    const pages = await adapter.fetchChapterPages('ch-1');

    expect(pages).toEqual([
      'https://cdn.nexusmangas.com/page1.webp',
      'https://cdn.nexusmangas.com/page2.webp',
    ]);
    expect(capturedBody.chapterId).toBe('ch-1');
    expect(capturedHeaders['x-nexus-client']).toBe('reader-v3');
  });
});
