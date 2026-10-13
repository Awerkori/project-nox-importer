import { describe, expect, it } from 'vitest';
import { WpGalleryAdapter } from '../../src/sources/common/wpgallery-adapter.js';

describe('WordPress gallery metadata', () => {
  it('preserves explicit adult taxonomy and aliases without deriving either from prose', async () => {
    const adapter = new WpGalleryAdapter(
      { id: 'mundohentai-fixture', name: 'Fixture', baseUrl: 'https://fixture.invalid' },
      undefined,
      async () => new Response(JSON.stringify({
        id: 42,
        slug: 'obra-fixture',
        title: { rendered: 'Obra principal' },
        excerpt: { rendered: '<p>Uma descrição sem tags inventadas.</p>' },
        alternative_titles: ['Titulo Original', '  Título Alternativo  '],
        meta: { native_title: '原題' },
        yoast_head_json: { articleSection: ['Hentai', 'MILF', 'Hentai'] },
        _embedded: {
          'wp:term': [[
            { taxonomy: 'category', name: 'Adulto (+18)' },
            { taxonomy: 'post_tag', name: 'Vanilla' },
            { taxonomy: 'category', name: 'Uncategorized' },
          ]],
        },
      }), { status: 200, headers: { 'content-type': 'application/json' } }) as Response,
    );

    const details = await adapter.fetchWorkDetails('42');

    expect(details.genres).toEqual(['Adulto (+18)', 'Vanilla', 'Hentai', 'MILF']);
    expect(details.alternativeTitles).toEqual(['Titulo Original', 'Título Alternativo', '原題']);
  });

  it('does not treat normal prose as an alias or taxonomy', async () => {
    const adapter = new WpGalleryAdapter(
      { id: 'wp-fixture', name: 'Fixture', baseUrl: 'https://fixture.invalid' },
      undefined,
      async () => new Response(JSON.stringify({
        id: 7, slug: 'sem-taxonomia', title: { rendered: 'Sem taxonomia' },
        excerpt: { rendered: '<p>Romance, aventura e uma descrição longa.</p>' },
      }), { status: 200, headers: { 'content-type': 'application/json' } }) as Response,
    );

    const details = await adapter.fetchWorkDetails('7');
    expect(details.genres).toBeUndefined();
    expect(details.alternativeTitles).toBeUndefined();
  });
});
