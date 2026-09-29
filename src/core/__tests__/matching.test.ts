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
});
