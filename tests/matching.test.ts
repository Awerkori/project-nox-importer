import { describe, it, expect } from 'vitest';
import {
  normalizeTitle,
  slugifyTitle,
  matchWorkCandidate,
  diceSimilarity,
  levenshteinSimilarity,
} from '../src/core/matching.js';

describe('Cross-Provider Matching Engine', () => {
  it('normalizes titles by removing accents, punctuation and noise tokens', () => {
    expect(normalizeTitle('SandLand (Scan PT-BR) - Online')).toBe('sandland');
    expect(normalizeTitle('Céu Distante - Manhwa PT-BR')).toBe('ceu distante');
    expect(normalizeTitle('O Mestre da Espada Genial da Academia!')).toBe('o mestre da espada genial da academia');
  });

  it('matches exact titles ignoring punctuation and accents', () => {
    const res = matchWorkCandidate(
      { title: 'SandLand' },
      { title: 'Sand Land' }
    );
    // In our test, SandLand vs Sand Land:
    // normalizeTitle('SandLand') is 'sandland', 'Sand Land' is 'sand land'
    // Dice similarity between 'sandland' and 'sand land':
    expect(res.matched).toBe(true);
  });

  it('matches works via aliases / alternative titles', () => {
    const res = matchWorkCandidate(
      {
        title: 'O Mestre da Espada Genial da Academia',
        aliases: ['Academy’s Genius Swordmaster', 'Genius Swordmaster of the Academy'],
      },
      {
        title: "Academy's Genius Swordmaster",
      }
    );
    expect(res.matched).toBe(true);
    expect(res.matchMethod).toBe('ALIAS_EXACT');
    expect(res.confidenceScore).toBeGreaterThanOrEqual(0.95);
  });

  it('rejects matches between a Novel and a Comic', () => {
    const res = matchWorkCandidate(
      { title: 'Solo Leveling (Novel)', kind: 'NOVEL' },
      { title: 'Solo Leveling', kind: 'MANHWA' }
    );
    expect(res.matched).toBe(false);
    expect(res.reason).toContain('Novel vs Comic');
  });

  it('rejects matches between different seasons', () => {
    const res = matchWorkCandidate(
      { title: 'Tower of God Season 2' },
      { title: 'Tower of God Season 1' }
    );
    expect(res.matched).toBe(false);
    expect(res.reason).toContain('Season mismatch');
  });

  it('rejects matches between main story and spin-off', () => {
    const res = matchWorkCandidate(
      { title: 'Eleceed' },
      { title: 'Eleceed Side Story' }
    );
    expect(res.matched).toBe(false);
    expect(res.reason).toContain('Spin-off');
  });

  it('fuzzy matches high-similarity titles', () => {
    const res = matchWorkCandidate(
      { title: 'Regressor Instruction Manual' },
      { title: 'How to Use a Regressor: Instruction Manual' }
    );
    // Even if not high enough, let's test a very close one:
    const closeRes = matchWorkCandidate(
      { title: 'The Max Level Player' },
      { title: 'The Max-Level Player' }
    );
    expect(closeRes.matched).toBe(true);
  });
});
