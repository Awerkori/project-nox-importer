/**
 * Chapter numbers are display data, not primary keys.  In particular, 12.1,
 * 12.10, an extra named 12 and a different language edition must survive as
 * distinct identities.  The source id remains the provider-level identity;
 * this key is the conservative canonical comparison key.
 */
export interface ChapterIdentityInput {
    displayNumber: string | number | null | undefined;
    title?: string | null;
    kind?: string | null;
    language?: string | null;
    edition?: string | null;
}
/**
 * A lossless, text-only view of the opaque canonical key.  This exists for
 * durable-manifest recovery only: callers must not turn `displayNumber` into
 * a number (in particular, `12.10` is not `12.1`).
 */
export interface ParsedCanonicalChapterKey {
    language: string;
    edition: string;
    kind: string;
    displayNumber: string;
    title: string;
}
export declare function chapterKind(input: ChapterIdentityInput): string;
/** A stable opaque key; callers must never parse it back into a float. */
export declare function canonicalChapterKey(input: ChapterIdentityInput): string;
export declare function parseCanonicalChapterKey(value: string): ParsedCanonicalChapterKey | null;
/**
 * A display-only sortable tuple. Decimal text is intentionally retained as a
 * tie breaker: 12.1 and 12.10 may sort together but can never become equal.
 */
export declare function chapterOrderKey(input: ChapterIdentityInput): string;
