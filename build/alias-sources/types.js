/**
 * Active ALIAS_ONLY sources used in permanent runtime enrichment and new retroactive runs.
 * Comix ('comix') is excluded from active polling due to external API token/WAF restrictions,
 * while remaining in `AliasSourceId` to preserve historical audit and rollback records.
 */
export const ALIAS_ONLY_SOURCE_IDS = [
    'mangaball',
    'mangadex',
    'mangafire',
];
