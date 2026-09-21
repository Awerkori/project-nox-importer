/**
 * Cross-Provider Matching and Normalization Engine
 * Provides deterministic and robust title matching across different manga sources.
 */
export interface WorkMatchResult {
    matched: boolean;
    confidenceScore: number;
    matchMethod: 'EXACT_TITLE' | 'EXACT_SLUG' | 'ALIAS_EXACT' | 'FUZZY_HIGH' | 'NO_MATCH';
    reason?: string;
}
export interface MatchCandidate {
    title: string;
    slug?: string;
    aliases?: string[];
    kind?: string;
}
/**
 * Normalizes title string by removing accents, lowercasing, stripping punctuation
 * and filtering common scan/format noise words.
 */
export declare function normalizeTitle(text: string): string;
/**
 * Converts a title to a standardized URL-friendly slug.
 */
export declare function slugifyTitle(text: string): string;
/**
 * Extracts season identifier if present (e.g., 's2', 'season 2', '2a temporada', 'season ii', 'parte 2')
 */
export declare function extractSeason(text: string): string | null;
/**
 * Checks whether text indicates a light novel / webnovel
 */
export declare function isNovel(text: string, kind?: string): boolean;
/**
 * Checks whether text indicates a spin-off, side story, or gaiden
 */
export declare function extractSpinOff(text: string): boolean;
/**
 * Computes Sørensen-Dice coefficient between two strings based on bigrams.
 */
export declare function diceSimilarity(str1: string, str2: string): number;
/**
 * Computes Levenshtein edit-distance similarity ratio [0, 1].
 */
export declare function levenshteinSimilarity(s1: string, s2: string): number;
/**
 * Token Jaccard overlap ratio.
 */
export declare function tokenOverlapRatio(s1: string, s2: string): number;
/**
 * Evaluates whether candidate work matches target work with safety constraints.
 */
export declare function matchWorkCandidate(target: MatchCandidate, candidate: MatchCandidate): WorkMatchResult;
