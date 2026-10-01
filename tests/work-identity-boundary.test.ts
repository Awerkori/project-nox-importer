import { describe, expect, it } from 'vitest';
import { MangaLivreToAdapter } from '../src/sources/mangalivreto/mangalivreto-adapter';

describe('chapter work-identity boundary', () => {
  const adapter = new MangaLivreToAdapter();

  it('accepts only a chapter URL scoped to its mapped source work', () => {
    expect(adapter.isChapterOwnedByWork(
      'kimetsu-no-yaiba',
      'https://mangalivre.to/manga/kimetsu-no-yaiba/capitulo-205/',
    )).toBe(true);
  });

  it('fails closed for a chapter URL from another source work', () => {
    expect(adapter.isChapterOwnedByWork(
      'kimetsu-no-yaiba',
      'https://mangalivre.to/manga/the-beginning-after-the-end-ptbr/capitulo-245/',
    )).toBe(false);
  });

  it('fails closed for another host and malformed source-work scope', () => {
    expect(adapter.isChapterOwnedByWork(
      'kimetsu-no-yaiba',
      'https://example.invalid/manga/kimetsu-no-yaiba/capitulo-205/',
    )).toBe(false);
    expect(adapter.isChapterOwnedByWork(
      'kimetsu/no-yaiba',
      'https://mangalivre.to/manga/kimetsu-no-yaiba/capitulo-205/',
    )).toBe(false);
  });
});
