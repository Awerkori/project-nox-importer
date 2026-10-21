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
    author?: string;
    artist?: string;
    synopsis?: string;
    year?: number | null;
}
export interface MetadataMatchEvidence {
    score: number;
    authorExact: boolean;
    artistExact: boolean;
    kindCompatible: boolean;
    yearCompatible: boolean;
    titleSimilarity: number;
    synopsisSimilarity: number;
    autoMatch: boolean;
    ambiguous: boolean;
}
/**
 * Curated Canonical Synonym Groups.
 * Bridges disparate translation titles (Portuguese, English, Romanized, Pinyin)
 * for popular works to prevent multi-source catalog fragmentation.
 */
export declare const KNOWN_CANONICAL_SYNONYM_GROUPS: string[][];
/**
 * Returns alternative titles for a known canonical synonym group if any match.
 */
export declare function getCanonicalSynonyms(text: string): string[];
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
 * Detects whether two titles share a base work name but one carries a distinct
 * sequel, prequel, spin-off or edition qualifier absent from the other
 * (e.g. "Dragon Ball" vs "Dragon Ball Super", "Naruto" vs "Naruto Shippuden",
 * "Tokyo Ghoul" vs "Tokyo Ghoul:re", "Boku no Hero Academia" vs "Boku no Hero Academia: Vigilantes").
 */
export declare function hasSequelOrEditionConflict(titleA: string, titleB: string): string | null;
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
 * Metadata is deliberately corroborative. It may upgrade a candidate only
 * when creator, kind, synopsis and title evidence agree; weaker evidence is
 * retained for an AMBIGUOUS review rather than creating a risky auto-merge.
 */
export declare function evaluateMetadataMatchEvidence(target: MatchCandidate, candidate: MatchCandidate): MetadataMatchEvidence;
/**
 * Evaluates whether candidate work matches target work with safety constraints.
 */
export declare function matchWorkCandidate(target: MatchCandidate, candidate: MatchCandidate): WorkMatchResult;
