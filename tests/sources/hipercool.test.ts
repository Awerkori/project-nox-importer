import { describe, it, expect, vi } from 'vitest';
import { HipercoolAdapter } from '../../src/sources/hipercool/hipercool-adapter.js';

describe('HipercoolAdapter alternative titles parsing', () => {
  it('parses JSON string encoded alternativeTitles array', async () => {
    const mockFetch = vi.fn().mockImplementation(async (url: string) => {
      if (url.includes('bySlugWithGenres')) {
        return {
          ok: true,
          status: 200,
          headers: new Headers(),
          json: async () => [
            { result: { data: { json: null } } },
            {
              result: {
                data: {
                  json: {
                    id: 1234,
                    slug: 'ritounohohoemi',
                    title: 'Ritounohohoemi',
                    alternativeTitles: '["The Island Smile", "リトウノホホエミ"]',
                    genres: [{ name: 'Manga' }],
                  },
                },
              },
            },
          ],
        };
      }
      return {
        ok: true,
        status: 200,
        headers: new Headers({ 'set-cookie': '__st=session; Path=/' }),
        text: async () => '<html></html>',
      };
    });

    const adapter = new HipercoolAdapter(undefined as any, mockFetch as any);
    const details = await adapter.fetchWorkDetails('ritounohohoemi');

    expect(details.title).toBe('Ritounohohoemi');
    expect(details.alternativeTitles).toEqual(['The Island Smile', 'リトウノホホエミ']);
  });

  it('parses array of strings alternativeTitles', async () => {
    const mockFetch = vi.fn().mockImplementation(async (url: string) => {
      if (url.includes('bySlugWithGenres')) {
        return {
          ok: true,
          status: 200,
          headers: new Headers(),
          json: async () => [
            { result: { data: { json: null } } },
            {
              result: {
                data: {
                  json: {
                    id: 1234,
                    slug: 'another-work',
                    title: 'Another Work',
                    alternativeTitles: ['Alias 1', 'Alias 2'],
                    genres: [],
                  },
                },
              },
            },
          ],
        };
      }
      return {
        ok: true,
        status: 200,
        headers: new Headers({ 'set-cookie': '__st=session; Path=/' }),
        text: async () => '<html></html>',
      };
    });

    const adapter = new HipercoolAdapter(undefined as any, mockFetch as any);
    const details = await adapter.fetchWorkDetails('another-work');

    expect(details.alternativeTitles).toEqual(['Alias 1', 'Alias 2']);
  });
});
