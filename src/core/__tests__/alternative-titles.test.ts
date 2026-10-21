import { describe, expect, it } from 'vitest';
import { alternativeTitleKey, cleanAlternativeTitle, mergeAlternativeTitles } from '../alternative-titles.js';

describe('alternative title metadata', () => {
  it('keeps native scripts and romanizations as distinct display values', () => {
    expect(mergeAlternativeTitles([], [
      '나 혼자만 레벨업',
      '俺だけレベルアップな件',
      'Ore dake Level Up na Ken',
    ], 'Solo Leveling')).toEqual([
      '나 혼자만 레벨업',
      '俺だけレベルアップな件',
      'Ore dake Level Up na Ken',
    ]);
  });

  it('deduplicates cosmetic Unicode, spacing, case and punctuation variants without changing the first display value', () => {
    expect(mergeAlternativeTitles([], [
      'Only I Level Up',
      ' only-i-level-up ',
      'ONLY I LEVEL UP',
      'C++',
      'C Sharp',
    ])).toEqual(['Only I Level Up', 'C++', 'C Sharp']);
    expect(alternativeTitleKey('  俺だけ・レベルアップな件  ')).toBe('俺だけ レベルアップな件');
  });

  it('does not store a cosmetic duplicate of the selected primary title', () => {
    expect(mergeAlternativeTitles([' solo-leveling ', 'I Alone Level Up'], [], 'Solo Leveling'))
      .toEqual(['I Alone Level Up']);
  });

  it('preserves short, single-letter, numeric, CJK, accented, colon, dash, and parenthetical titles', () => {
    expect(cleanAlternativeTitle('X')).toBe('X');
    expect(cleanAlternativeTitle('86')).toBe('86');
    expect(cleanAlternativeTitle('7 Seeds')).toBe('7 Seeds');
    expect(cleanAlternativeTitle('Bleach')).toBe('Bleach');
    expect(cleanAlternativeTitle('Akira')).toBe('Akira');
    expect(cleanAlternativeTitle('ブリーチ')).toBe('ブリーチ');
    expect(cleanAlternativeTitle('死神')).toBe('死神');
    expect(cleanAlternativeTitle('나 혼자만 레벨업')).toBe('나 혼자만 레벨업');
    expect(cleanAlternativeTitle('百炼成神')).toBe('百炼成神');
    expect(cleanAlternativeTitle('+18')).toBeNull();
    expect(cleanAlternativeTitle('18+')).toBeNull();
    expect(cleanAlternativeTitle('10803')).toBeNull();

    // Subtitle / compound primary title must NOT block standalone parts as aliases
    expect(mergeAlternativeTitles([], [
      'Demon Slayer',
      'Kimetsu no Yaiba',
      '鬼滅の刃',
    ], 'Demon Slayer: Kimetsu no Yaiba')).toEqual([
      'Demon Slayer',
      'Kimetsu no Yaiba',
      '鬼滅の刃',
    ]);

    expect(mergeAlternativeTitles([], [
      'Re:Zero',
      'Starting Life in Another World',
      'Re:Zero - Starting Life in Another World',
    ], 'Re:Zero kara Hajimeru Isekai Seikatsu')).toEqual([
      'Re:Zero',
      'Starting Life in Another World',
      'Re:Zero - Starting Life in Another World',
    ]);

    expect(mergeAlternativeTitles([], [
      'Kakushite! Makina-san!!',
      'かくして！ マキナさん!!',
    ], 'Hide Gently, Makina-san!')).toEqual([
      'Kakushite! Makina-san!!',
      'かくして! マキナさん!!',
    ]);
  });
});
