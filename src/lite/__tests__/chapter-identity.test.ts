import { describe, expect, it } from 'vitest';
import { canonicalChapterKey, chapterKind, chapterOrderKey } from '../chapter-identity.js';

describe('Lite chapter identity', () => {
  it('never collapses decimal text, special chapters, or editions into a float identity', () => {
    const decimalOne = canonicalChapterKey({ displayNumber: '12.1', language: 'pt-BR' });
    const decimalTen = canonicalChapterKey({ displayNumber: '12.10', language: 'pt-BR' });
    const special = canonicalChapterKey({ displayNumber: '12', title: 'Extra', language: 'pt-BR' });
    const translated = canonicalChapterKey({ displayNumber: '12', language: 'en', edition: 'official' });
    expect(new Set([decimalOne, decimalTen, special, translated]).size).toBe(4);
    expect(chapterOrderKey({ displayNumber: '12.1' })).not.toBe(chapterOrderKey({ displayNumber: '12.10' }));
    expect(canonicalChapterKey({ displayNumber: '12', title: 'Chapter Twelve' }))
      .toBe(canonicalChapterKey({ displayNumber: '12', title: 'Capítulo Doze' }));
    expect(chapterKind({ displayNumber: 'Prólogo', title: 'Prólogo' })).toBe('special');
    expect(canonicalChapterKey({ displayNumber: 'Prólogo', title: 'Prólogo', language: 'pt-BR' }))
      .toBe('pt-br|default|special|pr%C3%B3logo|pr%C3%B3logo');
  });
});
