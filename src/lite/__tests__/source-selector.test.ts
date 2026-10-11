import { describe, expect, it } from 'vitest';
import { selectChapterSource } from '../source-selector.js';
import type { ChapterSourceRoute } from '../types.js';

const route = (patch: Partial<ChapterSourceRoute>): ChapterSourceRoute => ({
  source: 'mangaflix', sourceChapterId: 'a', canonicalChapterKey: 'chapter', workId: 'work', language: 'pt-BR',
  edition: 'default', sourceRank: 10, health: 'HEALTHY', confirmed: true, hasChapter: true, ...patch,
});

describe('Lite source fallback', () => {
  it('uses a healthy confirmed alternative when a preferred source is cooling down', () => {
    const selected = selectChapterSource([
      route({ source: 'mangaflix', cooldownUntil: new Date(Date.now() + 60_000), sourceRank: 10 }),
      route({ source: 'manhastro', sourceChapterId: 'b', sourceRank: 20 }),
    ], { language: 'pt-BR', edition: 'default' });
    expect(selected.route?.source).toBe('manhastro');
  });

  it('refuses an alternative for a different edition or language', () => {
    const selected = selectChapterSource([route({ language: 'en', edition: 'official' })], { language: 'pt-BR', edition: 'default' });
    expect(selected).toEqual({ route: null, reason: 'NO_CONFIRMED_ROUTE' });
  });

  it('does not let a large unhealthy source hide another executable route', () => {
    const routes = Array.from({ length: 1300 }, (_, i) => route({ source: `slow-${i}`, sourceChapterId: String(i), health: 'BROKEN', sourceRank: i }));
    routes.push(route({ source: 'healthy', sourceChapterId: 'good', sourceRank: 9999 }));
    expect(selectChapterSource(routes, {}).route?.source).toBe('healthy');
  });
});
