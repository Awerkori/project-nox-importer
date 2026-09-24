/**
 * Gap Safety & Permanent Absence Validation Module
 *
 * Enforces strict architectural invariants:
 * - 403, Cloudflare, Turnstile, ASN block, timeout, lease expiry, CDN error, and 5xx transient errors
 *   MUST NEVER create permanent gaps (is_gap = true). They are classified as TEMPORARY_UNAVAILABLE.
 * - Permanent gap requires positive structural evidence:
 *   1. Chapter explicitly confirmed absent in source catalog (e.g. 404 on canonical endpoint, or omitted from source list)
 *   2. No active alternative work mappings or alternative sources possessing the chapter
 *   3. Non-transient confirmation across repeated probes
 */
export interface GapValidationResult {
    isPermanentGap: boolean;
    classification: 'PERMANENT_GAP' | 'TEMPORARY_UNAVAILABLE' | 'AVAILABLE_ELSEWHERE' | 'UNVERIFIED';
    reason: string;
    httpStatus?: number | string;
    alternativeSourcesAvailable: string[];
    safeToMarkGap: boolean;
}
/**
 * Checks if a given error, status code, or message indicates a transient issue
 * that should NEVER be treated as a permanent gap.
 */
export declare function isTransientError(errorOrStatus: string | number | undefined | null): boolean;
/**
 * Validates whether a missing chapter can safely be considered a permanent gap.
 */
export declare function validatePermanentGapCandidate(params: {
    workId: string;
    chapterNumber: number | string;
    chapterSortKey: number;
    source: string;
    httpStatus?: number | string;
    errorMessage?: string;
    alternativeSources?: Array<{
        source: string;
        hasChapter: boolean;
    }>;
}): Promise<GapValidationResult>;
