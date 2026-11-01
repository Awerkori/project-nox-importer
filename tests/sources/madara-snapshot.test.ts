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
  it('aborts a source request when its monitor deadline is cancelled', async () => {
    const transport=vi.fn(async (_url:string, options:RequestInit) => new Promise((_resolve,reject)=>{
      options.signal!.addEventListener('abort',()=>reject(options.signal!.reason),{once:true});
    }));
    const adapter=new CafeComYaoiAdapter(new HostRateLimiter(10),transport as any);
    const controller=new AbortController();
    const promise=adapter.fetchWorkSnapshot('example-work',controller.signal);
    await vi.waitFor(()=>expect(transport).toHaveBeenCalledTimes(1));
    controller.abort(new Error('monitor timeout'));
    await expect(promise).rejects.toThrow('monitor timeout');
    expect(transport).toHaveBeenCalledTimes(1);
  });

  it('does not report an empty manifest when chapter endpoints fail', async () => {
    const adapter=new CafeComYaoiAdapter(new HostRateLimiter(10));
    (adapter as any).fetchHtml=vi.fn().mockResolvedValueOnce('<h1>Example Work</h1>')
      .mockRejectedValue(new Error('upstream unavailable'));
    await expect(adapter.fetchWorkSnapshot('example-work')).rejects.toThrow('upstream unavailable');
  });

});
