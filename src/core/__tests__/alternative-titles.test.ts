import { describe, expect, it } from 'vitest';
import { alternativeTitleKey, mergeAlternativeTitles } from '../alternative-titles.js';

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
});
