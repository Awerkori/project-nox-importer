import { describe, expect, it } from 'vitest';
import { buildDiscoveryQueryCandidates, classifyExplicitSourceMetadata, extractStoredMappingMetadata, isProviderDisplayTitleAlias } from '../mapping-metadata.js';

describe('stored mapping metadata enrichment', () => {
  it('accepts a display title stored as a slug but rejects machine fallbacks', () => {
    expect(isProviderDisplayTitleAlias('Distant Sky', 'Distant Sky', 'Céu Distante')).toBe(true);
    expect(isProviderDisplayTitleAlias('distant-sky', 'distant-sky', 'Céu Distante')).toBe(false);
    expect(isProviderDisplayTitleAlias('10823', '10823', 'Imperador Mágico')).toBe(false);
    expect(isProviderDisplayTitleAlias('unknown', 'unknown', 'Imperador Mágico')).toBe(false);
  });

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

  it('recovers explicit Portuguese provider fields without mining prose', () => {
    const metadata = extractStoredMappingMetadata({
      titulo_alternative: ['Nome original', 'English Publisher Title', 'Título BR'],
      titulo_original: '원작 제목',
      generos: [{ nome: 'Ação' }, { name: 'Fantasia' }],
      categoria: 'Manhua',
      descricao: 'An English title mentioned in prose is not an alias.',
    }, 'Título BR');

    expect(metadata.aliases).toEqual(['Nome original', 'English Publisher Title', '원작 제목']);
    expect(metadata.genres).toEqual(['Ação', 'Fantasia', 'Manhua']);
  });

  it('classifies explicit metadata without mistaking a primary-title echo for a parser error', () => {
    expect(classifyExplicitSourceMetadata({ aliases: ['Primary'], genres: ['Vanilla'] }, 'Primary')).toMatchObject({
      aliasesState: 'NO_ALIASES', tagsState: 'ENRICHED', parserError: false,
    });
    expect(classifyExplicitSourceMetadata({ aliases: { unsupported: 'Native title' } }, 'Primary')).toMatchObject({
      aliasesState: 'PARSER_ERROR', tagsState: 'NO_TAGS', parserError: true,
    });
  });

  it('builds bounded query candidates with primary phrase extraction and aliases', () => {
    // Punctuated title with 0 aliases
    const candidatesA = buildDiscoveryQueryCandidates('Summon The Demon, For I Am The Abyss', []);
    expect(candidatesA).toEqual([
      'Summon The Demon, For I Am The Abyss',
      'Summon The Demon',
    ]);

    // Title with multiple aliases, capped at 5 and deduplicated
    const candidatesB = buildDiscoveryQueryCandidates('One Piece', [
      'Um Pedaço', 'One Piece', 'OP', 'Wan Pīsu', 'Un Trésor', 'Extra Ignored'
    ]);
    expect(candidatesB).toEqual([
      'One Piece',
      'Um Pedaço',
      'OP',
      'Wan Pīsu',
      'Un Trésor',
    ]);
  });

  it('splits delimited alternative titles from raw strings and extracts new Portuguese and Asian title fields', () => {
    const raw = {
      nome_alternativo: 'God of Martial Arts, Jueshi Wushen, Peerless Martial God',
      sinonimos: 'Infinite Leveling: Murim; Unlimited Level-ups in Murim',
      japanese_title: 'ワンピース',
      titulo_coreano: '무한 레벨업 in 무림',
      alt_names: ['The Eunuch Regains His Manhood in His Second Life', '2회차 환관이 남성을 되찾음'],
    };

    const extracted = extractStoredMappingMetadata(raw, 'Deus das Artes Marciais');
    expect(extracted.aliases).toContain('God of Martial Arts');
    expect(extracted.aliases).toContain('Jueshi Wushen');
    expect(extracted.aliases).toContain('Peerless Martial God');
    expect(extracted.aliases).toContain('Infinite Leveling: Murim');
    expect(extracted.aliases).toContain('Unlimited Level-ups in Murim');
    expect(extracted.aliases).toContain('ワンピース');
    expect(extracted.aliases).toContain('무한 레벨업 in 무림');
    expect(extracted.aliases).toContain('The Eunuch Regains His Manhood in His Second Life');
    expect(extracted.aliases).toContain('2회차 환관이 남성을 되찾음');

    const classified = classifyExplicitSourceMetadata(raw, 'Deus das Artes Marciais');
    expect(classified.aliasesState).toBe('ENRICHED');
    expect(classified.parserError).toBe(false);
  });

  it('protects compound titles from comma-shredding and rejects genre/numeric placeholders', () => {
    const raw = {
      alternative_title: 'Summon The Demon, For I Am The Abyss',
      sinonimos: 'Black, Yet Pure White',
      alt_names: ['Hello, Fluffy Griffin!', '+18', 'Adulto', '10803', 'Drama'],
    };

    const extracted = extractStoredMappingMetadata(raw, 'Example Work');
    expect(extracted.aliases).toContain('Summon The Demon, For I Am The Abyss');
    expect(extracted.aliases).not.toContain('Summon The Demon');
    expect(extracted.aliases).not.toContain('For I Am The Abyss');

    expect(extracted.aliases).toContain('Black, Yet Pure White');
    expect(extracted.aliases).not.toContain('Black');
    expect(extracted.aliases).not.toContain('Yet Pure White');

    expect(extracted.aliases).toContain('Hello, Fluffy Griffin!');
    expect(extracted.aliases).not.toContain('Hello');
    expect(extracted.aliases).not.toContain('Fluffy Griffin!');

    // Numeric and genre placeholders must be completely rejected
    expect(extracted.aliases).not.toContain('+18');
    expect(extracted.aliases).not.toContain('Adulto');
    expect(extracted.aliases).not.toContain('10803');
    expect(extracted.aliases).not.toContain('Drama');
  });
});
