/**
 * Adult-only taxonomy has one stable identity. This is intentionally an exact
 * equivalence list, never a fuzzy classifier for arbitrary upstream prose.
 */
export declare const CANONICAL_ADULT_TAG_NAME = "Adulto (+18)";
export declare const CANONICAL_ADULT_TAG_SLUG = "adulto-18";
/** Returns the canonical adult term for an exact known alias, otherwise null. */
export declare function canonicalAdultTagName(raw: string): string | null;
