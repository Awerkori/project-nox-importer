/**
 * Adult-only taxonomy has one stable identity. This is intentionally an exact
 * equivalence list, never a fuzzy classifier for arbitrary upstream prose.
 */
export const CANONICAL_ADULT_TAG_NAME = 'Adulto (+18)';
export const CANONICAL_ADULT_TAG_SLUG = 'adulto-18';
const ADULT_ALIASES = new Set([
    'adulto',
    '+18',
    '18+',
    'adult',
    'adulto +18',
    'adulto (+18)',
    'adults only',
    'mature',
]);
/** Returns the canonical adult term for an exact known alias, otherwise null. */
export function canonicalAdultTagName(raw) {
    const normalized = raw
        .replace(/\s+/g, ' ')
        .trim()
        .toLowerCase()
        .replace(/[’']/g, '')
        .replace(/[_-]+/g, ' ')
        .replace(/\s+/g, ' ');
    return ADULT_ALIASES.has(normalized) ? CANONICAL_ADULT_TAG_NAME : null;
}
