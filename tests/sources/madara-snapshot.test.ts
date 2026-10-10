import { describe, expect, it, vi } from 'vitest';
import { CafeComYaoiAdapter } from '../../src/sources/cafecomyaoi/cafecomyaoi-adapter.js';
import { HostRateLimiter } from '../../src/core/rate-limiter.js';

describe('MadaraAdapter work snapshots', () => {
  it('parses metadata and owned chapters from one work-page request', async () => {
    const mockTransport = vi.fn().mockResolvedValue({
      ok: true,
      status: 200,
      headers: new Headers(),
      text: async () => `
        <h1>Example Work</h1>
        <li class="wp-manga-chapter"><a href="/manga/example-work/capitulo-1/">Capítulo 1</a></li>
      `,
    });
    const adapter = new CafeComYaoiAdapter(new HostRateLimiter(10), mockTransport as any);

    const snapshot = await adapter.fetchWorkSnapshot('example-work');

    expect(snapshot.details.title).toBe('Example Work');
    expect(snapshot.chapters).toEqual([
      expect.objectContaining({
        sourceChapterId: 'https://cafecomyaoi.com.br/manga/example-work/capitulo-1/',
        number: 1,
      }),
    ]);
    expect(mockTransport).toHaveBeenCalledTimes(1);
  });
});
