import { describe, expect, it } from 'vitest';
import { extractStoredMappingMetadata } from '../mapping-metadata.js';

describe('stored mapping metadata enrichment', () => {
  it('uses only explicit aliases and preserves original Unicode display text', () => {
    const metadata = extractStoredMappingMetadata({
      alternativeTitles: [' 나 혼자만 레벨업 ', 'Solo Leveling'],
      description: 'I Alone Level Up must not be guessed from prose',
      nativeTitle: '俺だけレベルアップな件',
    }, 'Solo Leveling');

    expect(metadata.aliases).toEqual(['나 혼자만 레벨업', '俺だけレベルアップな件']);
  });

  it('extracts explicit tags/categories without treating status or prose as a tag', () => {
    const metadata = extractStoredMappingMetadata({
      genres: [{ name: 'MILF' }, { name: ' Romance ' }],
      tags: ['Hentai', { label: 'Vanilla' }],
      categories: [{ title: 'Adulto' }],
      synopsis: 'Tags such as Fake must never be parsed from this text.',
    }, 'Example');

    expect(metadata.genres).toEqual(['MILF', 'Romance', 'Hentai', 'Vanilla', 'Adulto']);
  });
});
