import { describe, expect, it } from 'vitest';
import {
  evaluateMetadataMatchEvidence,
  matchWorkCandidate,
  normalizeTitle,
} from '../matching.js';

describe('work matching safety', () => {
  it('normalizes accents and compact titles deterministically', () => {
    expect(normalizeTitle('  Ação: O Herói! ')).toBe('acao o heroi');
    expect(matchWorkCandidate({ title: 'OnePiece' }, { title: 'One Piece' }).matched).toBe(true);
  });

  it('accepts curated cross-language aliases without weakening guards', () => {
    const result = matchWorkCandidate(
      { title: 'Magic Emperor' },
      { title: 'Imperador Demoníaco' },
    );
    expect(result.matched).toBe(true);
  });

  it('matches an explicit native-script alias without transliterating it', () => {
    const result = matchWorkCandidate(
      { title: 'Solo Leveling', aliases: ['나 혼자만 레벨업'] },
      { title: '나 혼자만 레벨업' },
    );
    expect(result.matched).toBe(true);
    expect(result.matchMethod).toBe('ALIAS_EXACT');
  });

  it('only upgrades creator metadata when title, kind and synopsis corroborate it', () => {
    const evidence = evaluateMetadataMatchEvidence(
      {
        title: 'Tower Magic Warrior', kind: 'MANHWA', author: 'Han Seo',
        synopsis: 'A warrior climbs the ancient tower to rescue the kingdom from a ruthless immortal king.',
      },
      {
        title: 'Magic Tower Warrior', kind: 'MANHWA', author: 'Han Seo',
        synopsis: 'A warrior climbs the ancient tower to rescue the kingdom from a ruthless immortal king.',
      },
    );
    expect(evidence.autoMatch).toBe(true);
    expect(evidence.ambiguous).toBe(false);
  });

  it('sends weak creator evidence to review instead of auto-merging', () => {
    const evidence = evaluateMetadataMatchEvidence(
      {
        title: 'Tower Magic Warrior', kind: 'MANHWA', author: 'Han Seo',
        synopsis: 'A warrior climbs the ancient tower to rescue the kingdom from a ruthless immortal king.',
      },
      {
        title: 'Magic Tower Warrior', kind: 'MANHWA', author: 'Han Seo',
        synopsis: 'A chef opens a quiet restaurant and learns recipes from travelling merchants every day.',
      },
    );
    expect(evidence.autoMatch).toBe(false);
    expect(evidence.ambiguous).toBe(true);
  });

  it('never lets metadata bypass novel, season, or spin-off identity guards', () => {
    const base = {
      kind: 'MANHWA', author: 'Han Seo',
      synopsis: 'A warrior climbs the ancient tower to rescue the kingdom from a ruthless immortal king.',
    };
    expect(evaluateMetadataMatchEvidence(
      { ...base, title: 'Tower Hero Season 1' },
      { ...base, title: 'Tower Hero Season 2' },
    ).autoMatch).toBe(false);
    expect(evaluateMetadataMatchEvidence(
      { ...base, title: 'Tower Hero Novel', kind: 'NOVEL' },
      { ...base, title: 'Tower Hero', kind: 'MANHWA' },
    ).autoMatch).toBe(false);
    expect(evaluateMetadataMatchEvidence(
      { ...base, title: 'Tower Hero Side Story' },
      { ...base, title: 'Tower Hero' },
    ).autoMatch).toBe(false);
  });

  it('preserves Japanese Kana dakuten, Kanji, Hangul, and Hanzi in normalizeTitle and matches across CJK aliases', () => {
    expect(normalizeTitle('ブリーチ')).toBe('ブリーチ');
    expect(normalizeTitle('나 혼자만 레벨업')).toBe('나 혼자만 레벨업');
    expect(normalizeTitle('百炼成神')).toBe('百炼成神');

    expect(matchWorkCandidate(
      { title: 'Bleach', aliases: ['ブリーチ'] },
      { title: 'BLEACH', aliases: ['ブリーチ', '死神'] },
    ).matched).toBe(true);

    expect(matchWorkCandidate(
      { title: 'X' },
      { title: 'X' },
    ).matched).toBe(true);

    expect(matchWorkCandidate(
      { title: '86', aliases: ['86: Eighty-Six'] },
      { title: '86: Eighty-Six' },
    ).matched).toBe(true);
  });

  it('rejects false positive merges between parent works and distinct sequels, spin-offs, or editions', () => {
    expect(matchWorkCandidate(
      { title: 'Dragon Ball' },
      { title: 'Dragon Ball Super' },
    ).matched).toBe(false);

    expect(matchWorkCandidate(
      { title: 'Dragon Ball' },
      { title: 'Dragon Ball Z' },
    ).matched).toBe(false);

    expect(matchWorkCandidate(
      { title: 'Naruto' },
      { title: 'Naruto Shippuden' },
    ).matched).toBe(false);

    expect(matchWorkCandidate(
      { title: 'Naruto' },
      { title: 'Boruto: Naruto Next Generations' },
    ).matched).toBe(false);

    expect(matchWorkCandidate(
      { title: 'Boku no Hero Academia' },
      { title: 'Boku no Hero Academia: Vigilantes' },
    ).matched).toBe(false);

    expect(matchWorkCandidate(
      { title: 'Tokyo Ghoul' },
      { title: 'Tokyo Ghoul:re' },
    ).matched).toBe(false);

    expect(matchWorkCandidate(
      { title: 'The Breaker' },
      { title: 'The Breaker: New Waves' },
    ).matched).toBe(false);
  });
});
