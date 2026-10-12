/**
 * Display values are retained exactly enough for readers; comparison keys are
 * deliberately separate.  This avoids losing native scripts while still
 * coalescing cosmetic source variants such as whitespace and curly quotes.
 */
export declare function cleanAlternativeTitle(value: unknown): string | null;
/**
 * Comparison-only key. It intentionally does not transliterate or translate
 * titles: Japanese, Korean, Chinese and romanized variants remain distinct
 * aliases unless a source supplies the same value again.
 */
export declare function alternativeTitleKey(value: unknown): string;
/** Merge aliases without changing the first valid display spelling. */
export declare function mergeAlternativeTitles(existing: readonly unknown[] | null | undefined, incoming: readonly unknown[] | null | undefined, primaryTitle?: unknown, max?: number): string[];
