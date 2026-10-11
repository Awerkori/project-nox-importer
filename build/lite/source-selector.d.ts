import type { ChapterSourceRoute, SourceSelection } from './types.js';
/**
 * Source preference is deliberately below identity, health and cooldown.
 * A rank is a tie breaker only; it can never turn a bad preferred source into
 * an exclusive source.
 */
export declare function selectChapterSource(routes: ChapterSourceRoute[], expected: {
    language?: string | null;
    edition?: string | null;
}, now?: Date): SourceSelection;
export declare function nextSourceCooldown(routes: ChapterSourceRoute[], now?: Date): Date | null;
