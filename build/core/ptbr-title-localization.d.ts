export declare function isPortugueseWord(w: string): boolean;
export declare const CONSECRATED_PROPER_TITLES: Set<string>;
/**
 * Owner-specified editorial title preferences with confirmed work identity.
 * Keyed by canonical work UUID or canonical slug.
 */
export declare const OWNER_EDITORIAL_TITLE_PREFERENCES: Map<string, string>;
/**
 * Titles specifically rejected from promotion because they belong to European Portuguese (PT-PT)
 * or non-Brazilian translations.
 */
export declare const PT_PT_REJECTED_ALIASES: Set<string>;
export declare function cleanPtBrCandidateTitle(raw: string): string | null;
export declare function isPtBrTitle(rawTitle: string): boolean;
export declare function isConsecratedProperTitle(rawTitle: string): boolean;
export interface AutomaticPtBrTitlePromotionInput {
    workId?: string;
    slug?: string;
    title: string;
    aliases: readonly string[];
    provenance?: Record<string, any> | null;
    coverTitle?: string | null;
}
export interface AutomaticPtBrTitlePromotionResult {
    promoted: boolean;
    newTitle: string;
    newAliases: string[];
    method?: 'OWNER_EDITORIAL_PREFERENCE' | 'PTBR_PROMOTED_EXISTING_ALIAS' | 'PTBR_FOUND_COVER';
    ownerEditorialPreference?: boolean;
}
/**
 * 4-TIER LOCALIZATION POLICY:
 * 1. Explicit Owner Editorial Preference (e.g. One Piece -> Um Pedaço).
 * 2. Protected PT-BR Title (manual, ptbr_primary_titles_v1, or validated PT-BR) -> immutable anti-reversion.
 * 3. Verified PT-BR Alias Promotion (promote clean PT-BR alias; franchise fame does not block promotion).
 * 4. Preserved Consecrated / Proper Name / International Title when no PT-BR alias exists.
 */
export declare function resolveAutomaticPtBrTitlePromotion(input: AutomaticPtBrTitlePromotionInput): AutomaticPtBrTitlePromotionResult;
