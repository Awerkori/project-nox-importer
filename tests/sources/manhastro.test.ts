import { describe, it, expect, vi } from 'vitest';
import { ManhastroAdapter } from '../../src/sources/manhastro/manhastro-adapter.js';
import { HostRateLimiter } from '../../src/core/rate-limiter.js';

describe('ManhastroAdapter', () => {
  it('correctly handles prefix-corrupted json and parses works', async () => {
    const corruptedJson = `)]}'\n{"success":true,"data":[{"manga_id":9901,"titulo":"Martial Peak","titulo_brasil":"Ápice Marcial","imagem":"temp.manhastro.com/cover.jpg","ultimo_capitulo":"2026-09-08 05:00:00"}]}`;

    const mockTransport = vi.fn().mockResolvedValue({
      ok: true,
      status: 200,
      headers: new Headers(),
      text: async () => corruptedJson,
    });

    const rateLimiter = new HostRateLimiter(10.0);
    const adapter = new ManhastroAdapter(rateLimiter, mockTransport as any);

    const { works } = await adapter.fetchUpdatedWorks(null, { mode: 'maintenance' });
    expect(works.length).toBe(1);
    expect(works[0].sourceWorkId).toBe('9901');
    expect(works[0].title).toBe('Ápice Marcial');
    expect(works[0].slug).toBe('apice-marcial');
    expect(works[0].coverUrl).toBe('https://temp.manhastro.com/cover.jpg');
  });

  it('correctly fetches work details and chapters with regex number extraction', async () => {
    const detailsJson = `{"success":true,"data":[{"manga_id":9901,"titulo":"Martial Peak","titulo_brasil":"Ápice Marcial","descricao_brasil":"Epic cultivation story","imagem":"https://cdn.manhastro.com/c.jpg","generos":["Cultivation","Action"],"categoria":"manhua","status":"on-going"}]}`;
    const chaptersJson = `{"success":true,"data":[{"capitulo_id":1234,"capitulo_nome":"Capitulo 105.5","capitulo_data":"2026-09-07 10:00:00"},{"capitulo_id":1235,"capitulo_nome":"Capitulo 106","capitulo_data":"2026-09-08 10:00:00"}]}`;

    const mockTransport = vi.fn().mockImplementation(async (url: string) => {
      if (url.includes('manga_id=9901')) {
        return { ok: true, status: 200, headers: new Headers(), text: async () => detailsJson };
      }
      return { ok: true, status: 200, headers: new Headers(), text: async () => chaptersJson };
    });

    const rateLimiter = new HostRateLimiter(10.0);
    const adapter = new ManhastroAdapter(rateLimiter, mockTransport as any);

    const details = await adapter.fetchWorkDetails('9901');
    expect(details.sourceWorkId).toBe('9901');
    expect(details.title).toBe('Ápice Marcial');
    expect(details.alternativeTitles).toEqual(['Martial Peak']);
    expect(details.kind).toBe('MANHUA');
    expect(details.status).toBe('ONGOING');

    const chapters = await adapter.fetchChapters('9901');
    expect(chapters.length).toBe(2);
    expect(chapters[0].number).toBe(105.5);
    expect(chapters[1].number).toBe(106);
  });

  it('does not count upstream errors or malformed chapter manifests as successful checks', async () => {
    const adapter = new ManhastroAdapter(new HostRateLimiter(10));
    for (const response of [{success:false,data:[]}, {success:true}, {success:true,data:[{capitulo_nome:'Chapter 1'}]}]) {
      (adapter as any).request = vi.fn().mockResolvedValue(response);
      await expect(adapter.fetchChapters('123')).rejects.toThrow('MANHASTRO_INVALID_CHAPTER_RESPONSE');
    }
    (adapter as any).request = vi.fn().mockResolvedValue({success:true,data:[]});
    await expect(adapter.fetchChapters('123')).resolves.toEqual([]);
  });

  it('correctly constructs page URLs from baseUrl, hash and filename array', async () => {
    const pagesJson = `{"success":true,"data":{"chapter":{"baseUrl":"https://albums.manhastro.net/t/123/temp","hash":"manga_abc/cap-1","data":["1.webp","2.webp","3.webp"]}}}`;

    const mockTransport = vi.fn().mockResolvedValue({
      ok: true,
      status: 200,
      headers: new Headers(),
      text: async () => pagesJson,
    });

    const rateLimiter = new HostRateLimiter(10.0);
    const adapter = new ManhastroAdapter(rateLimiter, mockTransport as any);

    const pages = await adapter.fetchChapterPages('1234');
    expect(pages).toEqual([
      'https://albums.manhastro.net/t/123/temp/manga_abc/cap-1/1.webp',
      'https://albums.manhastro.net/t/123/temp/manga_abc/cap-1/2.webp',
      'https://albums.manhastro.net/t/123/temp/manga_abc/cap-1/3.webp',
    ]);
  });

  it('propagates the first 429 to Lite instead of immediately retrying the same provider request', async () => {
    const limiter = {
      setHostRate: vi.fn(),
      acquire: vi.fn(async () => undefined),
      handle429: vi.fn(() => 17),
      recordSuccess: vi.fn(),
    } as unknown as HostRateLimiter;
    const mockTransport = vi.fn().mockResolvedValue({
      ok: false, status: 429, headers: new Headers({ 'Retry-After': '15' }), text: async () => 'slow down',
    });
    const adapter = new ManhastroAdapter(limiter, mockTransport as any);

    await expect(adapter.fetchChapterPages('429')).rejects.toMatchObject({ status: 429, retryAfter: 17 });

    expect(mockTransport).toHaveBeenCalledOnce();
    expect((limiter as any).acquire).toHaveBeenCalledOnce();
    expect((limiter as any).handle429).toHaveBeenCalledOnce();
  });
});
