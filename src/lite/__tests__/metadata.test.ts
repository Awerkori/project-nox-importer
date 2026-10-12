import { describe, expect, it } from 'vitest';
import { mergeMetadata, normalizeSourceTag, sanitizeSynopsis } from '../metadata.js';

describe('Lite metadata policy', () => {
  it('retains specific adult taxonomy instead of replacing it with a generic rating', () => {
    const metadata = mergeMetadata({}, { tags: ['MILF', 'Yaoi', 'Yuri', 'Hentai', 'Adulto (+18)'] });
    expect(metadata.tags?.value).toEqual(['MILF', 'Yaoi', 'Yuri', 'Hentai', 'Adulto (+18)']);
    expect(normalizeSourceTag('Hentai')).toBe('Hentai');
  });

  it('does not overwrite protected editorial metadata or accept Cloudflare copy as a synopsis', () => {
    const merged = mergeMetadata({ title: { value: 'Título editorial', protected: true, validated: true } }, { title: 'Título fraco', synopsis: 'Just a moment... Cloudflare' });
    expect(merged.title?.value).toBe('Título editorial');
    expect(merged.synopsis).toBeUndefined();
    expect(sanitizeSynopsis('<p>Uma sinopse &amp; válida.</p><p>Segundo parágrafo.</p>')).toBe('Uma sinopse & válida.\n\nSegundo parágrafo.');
  });

  it('merges aliases from confirmed metadata sources without losing native scripts', () => {
    const first = mergeMetadata({}, {
      title: 'Solo Leveling',
      aliases: ['나 혼자만 레벨업', 'Only I Level Up']
    });
    const merged = mergeMetadata(first, {
      aliases: [' only-i-level-up ', '俺だけレベルアップな件']
    });

    expect(merged.aliases?.value).toEqual([
      '나 혼자만 레벨업',
      'Only I Level Up',
      '俺だけレベルアップな件'
    ]);
  });
});
