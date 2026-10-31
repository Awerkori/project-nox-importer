import type { ChapterSourceRoute, SourceSelection } from './types.js';
/**
 * Source preference is deliberately below identity, health and cooldown.
 * A rank is a tie breaker only; it can never turn a bad preferred source into
 * an exclusive source.
 */
export declare function selectChapterSource(routes: ChapterSourceRoute[], expected: {
    language?: string | null;
    edition?: string | null;
}, now?: Date, sourceSpeedRank?: (source: string) => number): SourceSelection;
/**
 * Ordered alternatives for one already-canonical chapter.  The executor uses
 * this list to fail over in the same claim after a source-only failure; task
 * priority is intentionally absent from this decision.
 */
export declare function selectableChapterSources(routes: ChapterSourceRoute[], expected: {
    language?: string | null;
    edition?: string | null;
}, now?: Date, sourceSpeedRank?: (source: string) => number): {
    routes: ChapterSourceRoute[];
    reason?: SourceSelection['reason'];
};
export declare function nextSourceCooldown(routes: ChapterSourceRoute[], now?: Date): Date | null;
