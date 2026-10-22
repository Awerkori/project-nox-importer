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

  it('rejects cross-matching between unrelated works sharing only ultra-short or generic conversational aliases', () => {
    // Two works sharing the generic single word alias "Sorry"
    expect(matchWorkCandidate(
      { title: 'Desculpa, Eu Gosto de Você', aliases: ['Sorry', 'I like you'] },
      { title: 'Sorry, Did You Wait for Me?', aliases: ['Sorry', 'Perdão'] },
    ).matched).toBe(false);

    // Candidate title matching a generic single word alias when target title is completely unrelated
    expect(matchWorkCandidate(
      { title: 'Desculpa, Eu Gosto de Você', aliases: ['Sorry'] },
      { title: 'Sorry' },
    ).matched).toBe(false);

    // Two unrelated works sharing the generic conversational phrase "Por Favor"
    expect(matchWorkCandidate(
      { title: 'Por Favor, Me Leia', aliases: ['Por Favor', 'Please Read Me'] },
      { title: 'Por Favor, Olhe para a Jasmine', aliases: ['Por Favor', 'Please Look at Me'] },
    ).matched).toBe(false);

    // But substantial, specific shared aliases must still match
    expect(matchWorkCandidate(
      { title: 'Ataque dos Titãs', aliases: ['Shingeki no Kyojin'] },
      { title: 'Attack on Titan', aliases: ['Shingeki no Kyojin'] },
    ).matched).toBe(true);
  });

  it('matches composite candidate titles containing sub-clause variants', () => {
    expect(matchWorkCandidate(
      { title: 'Summer Break' },
      { title: 'Natsuyasumi! | Summer Break!' },
    ).matched).toBe(true);
  });

  it('matches candidate title that concatenates target title with known alias phrase', () => {
    expect(matchWorkCandidate(
      {
        title: 'A Dona de Casa Shizuka Tachibana',
        aliases: ['Que Pediu A Doação da Minha Porra', 'Não Entende De Nada!'],
      },
      {
        title: 'A Dona de Casa Shizuka Tachibana Que Pediu A Doação da Minha Porra Não Entende De Nada!',
      },
    ).matched).toBe(true);
  });

  it('rejects matching candidate with different base title against target when candidate merely contains target as a comma-separated subclause without declared aliases', () => {
    expect(matchWorkCandidate(
      { title: 'No Way Out!' },
      { title: "Pixiu's Eatery, No Way Out" },
    ).matched).toBe(false);

    expect(matchWorkCandidate(
      { title: 'No Way Out' },
      { title: "Pixiu's Eatery, No Way Out", aliases: ["Pixiu's Eatery, No Way Out"] },
    ).matched).toBe(false);
  });
});
