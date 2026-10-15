import { describe, expect, it } from 'vitest';
import { classifyExplicitSourceMetadata, extractStoredMappingMetadata } from '../mapping-metadata.js';

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

  it('decodes a legacy JSON-array string only from an explicit metadata field', () => {
    const metadata = extractStoredMappingMetadata({
      alternativeTitles: '["Nome em português", "한국어 제목", "Primary"]',
      genres: '["MILF", {"name":"Vanilla"}]',
      synopsis: '["This looks like JSON but is prose and must not be used"]',
    }, 'Primary');

    expect(metadata.aliases).toEqual(['Nome em português', '한국어 제목']);
    expect(metadata.genres).toEqual(['MILF', 'Vanilla']);
  });

  it('preserves Portuguese tag names in legacy source mappings', () => {
    const metadata = extractStoredMappingMetadata({
      tags: [{ id: 1, nome: 'Ação' }, { id: 2, nome: 'Fantasia' }],
    }, 'Example');

    expect(metadata.genres).toEqual(['Ação', 'Fantasia']);
  });

  it('recognizes singular upstream title fields and nested localized titles', () => {
    const metadata = extractStoredMappingMetadata({
      alternative_title: [{ language: 'ko', title: '한국 제목' }],
      native_name: '日本語タイトル',
      genre: [{ nome: 'Yuri' }],
    }, 'Example');

    expect(metadata.aliases).toEqual(['한국 제목','日本語タイトル']);
    expect(metadata.genres).toEqual(['Yuri']);
  });

  it('classifies explicit metadata without mistaking a primary-title echo for a parser error', () => {
    expect(classifyExplicitSourceMetadata({ aliases: ['Primary'], genres: ['Vanilla'] }, 'Primary')).toMatchObject({
      aliasesState: 'NO_ALIASES', tagsState: 'ENRICHED', parserError: false,
    });
    expect(classifyExplicitSourceMetadata({ aliases: { unsupported: 'Native title' } }, 'Primary')).toMatchObject({
      aliasesState: 'PARSER_ERROR', tagsState: 'NO_TAGS', parserError: true,
    });
  });
});
