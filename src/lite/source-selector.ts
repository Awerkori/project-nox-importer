import type { ChapterSourceRoute, SourceSelection } from './types.js';

/**
 * Source preference is deliberately below identity, health and cooldown.
 * A rank is a tie breaker only; it can never turn a bad preferred source into
 * an exclusive source.
 */
export function selectChapterSource(
  routes: ChapterSourceRoute[],
  expected: { language?: string | null; edition?: string | null },
  now = new Date(),
): SourceSelection {
  const matching = routes.filter((route) =>
    route.confirmed && route.hasChapter &&
    (!expected.language || !route.language || route.language === expected.language) &&
    (!expected.edition || !route.edition || route.edition === expected.edition),
  );
  if (matching.length === 0) return { route: null, reason: 'NO_CONFIRMED_ROUTE' };
  const eligible = matching.filter((route) =>
    route.health !== 'BROKEN' && route.health !== 'DISABLED' &&
    (!route.cooldownUntil || route.cooldownUntil <= now),
  );
  if (eligible.length === 0) {
    const anyCooldown = matching.some((route) => route.cooldownUntil && route.cooldownUntil > now);
    return { route: null, reason: anyCooldown ? 'ALL_ROUTES_COOLDOWN' : 'ALL_ROUTES_UNHEALTHY' };
  }
  eligible.sort((a, b) => a.sourceRank - b.sourceRank || a.source.localeCompare(b.source));
  return { route: eligible[0] };
}

export function nextSourceCooldown(routes: ChapterSourceRoute[], now = new Date()): Date | null {
  const dates = routes
    .map((route) => route.cooldownUntil)
    .filter((value): value is Date => Boolean(value && value > now))
    .sort((a, b) => a.getTime() - b.getTime());
  return dates[0] || null;
}
