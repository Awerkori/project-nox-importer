import { describe, expect, it } from 'vitest';
import { extractChapterNumber } from '../../src/sources/common/html-utils.js';

describe('extractChapterNumber', () => {
  it('decodes URL escapes before parsing an episodio number', () => {
    expect(
      extractChapterNumber(
        'https://nocfsb.com/manga/title/%e2%86%ab%e2%94%80%e2%98%ab-episodio-89/'
      )
    ).toBe(89);
  });

  it('keeps common chapter and decimal forms', () => {
    expect(extractChapterNumber('Capítulo 10.5')).toBe(10.5);
    expect(extractChapterNumber('https://example.test/manga/title/chapter-12/')).toBe(12);
  });
});
