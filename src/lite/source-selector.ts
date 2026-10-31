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
  sourceSpeedRank?: (source: string) => number,
): SourceSelection {
  const candidates = selectableChapterSources(routes, expected, now, sourceSpeedRank);
  if (candidates.routes.length) return { route: candidates.routes[0] };
  return { route: null, reason: candidates.reason };
}

/**
 * Ordered alternatives for one already-canonical chapter.  The executor uses
 * this list to fail over in the same claim after a source-only failure; task
 * priority is intentionally absent from this decision.
 */
export function selectableChapterSources(
  routes: ChapterSourceRoute[],
  expected: { language?: string | null; edition?: string | null },
  now = new Date(),
  sourceSpeedRank?: (source: string) => number,
): { routes: ChapterSourceRoute[]; reason?: SourceSelection['reason'] } {
  const matching = routes.filter((route) =>
    route.confirmed && route.hasChapter &&
    (!expected.language || !route.language || route.language === expected.language) &&
    (!expected.edition || !route.edition || route.edition === expected.edition),
  );
  if (matching.length === 0) return { routes: [], reason: 'NO_CONFIRMED_ROUTE' };
  const eligible = matching.filter((route) =>
    route.health !== 'BROKEN' && route.health !== 'DISABLED' &&
    (!route.cooldownUntil || route.cooldownUntil <= now),
  );
  if (eligible.length === 0) {
    const anyCooldown = matching.some((route) => route.cooldownUntil && route.cooldownUntil > now);
    return { routes: [], reason: anyCooldown ? 'ALL_ROUTES_COOLDOWN' : 'ALL_ROUTES_UNHEALTHY' };
  }
  eligible.sort((a, b) =>
    (sourceSpeedRank ? sourceSpeedRank(a.source) - sourceSpeedRank(b.source) : 0) ||
    a.sourceRank - b.sourceRank ||
    a.source.localeCompare(b.source),
  );
  return { routes: eligible };
}

export function nextSourceCooldown(routes: ChapterSourceRoute[], now = new Date()): Date | null {
  const dates = routes
    .map((route) => route.cooldownUntil)
    .filter((value): value is Date => Boolean(value && value > now))
    .sort((a, b) => a.getTime() - b.getTime());
  return dates[0] || null;
}
