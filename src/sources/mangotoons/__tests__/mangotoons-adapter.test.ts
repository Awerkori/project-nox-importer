import { describe, expect, it } from 'vitest';
import { MangoToonsAdapter } from '../mangotoons-adapter.js';

describe('MangoToonsAdapter v1 API', () => {
  it('uses the public releases feed and reader manifest without a session token', async () => {
    const requested: string[] = [];
    const adapter = new MangoToonsAdapter(undefined, async (url) => {
      requested.push(String(url));
      if (String(url).includes('/obras/lancamentos')) {
        return Response.json([{ id: 9, obra_id: 42, obra_nome: 'Obra', obra_slug: 'obra', imagem: 'https://cover.test/a.webp', published_at: '2026-01-01T00:00:00Z' }]);
      }
      if (String(url).includes('/leitor/obras/42/capitulos/7.5/manifest')) {
        return Response.json({ pages: [{ index: 1, url: '/cdn/p/1' }, { index: 0, url: '/cdn/p/0' }] });
      }
      return Response.json({});
    });

    const feed = await adapter.fetchUpdatedWorks();
    const pages = await adapter.fetchChapterPages('42:7.5');

    expect(feed.works).toMatchObject([{ sourceWorkId: '42', title: 'Obra' }]);
    expect(feed.nextCursor).toBe('2026-01-01T00:00:00Z|9');
    expect(pages).toEqual(['https://cdn.mangotoons.com/cdn/p/0', 'https://cdn.mangotoons.com/cdn/p/1']);
    expect(requested.some((url) => url.includes('/api/v1/obras/lancamentos'))).toBe(true);
  });
});
