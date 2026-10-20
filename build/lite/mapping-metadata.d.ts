/**
 * A mapping title is first-party evidence only after its mapping is attached
 * to the canonical work. Older adapters sometimes copied a machine slug (or
 * API id) into this field; reject just those fallbacks while accepting a
 * reader-facing title even when the provider uses it as the slug as well.
 */
export declare function isProviderDisplayTitleAlias(sourceTitle: unknown, sourceSlug: unknown, primaryTitle: unknown): boolean;
export declare const STORED_MAPPING_ALIAS_FIELDS: string[];
export declare const STORED_MAPPING_TAG_FIELDS: string[];
/**
 * Only explicitly named upstream metadata fields are eligible for the
 * no-network historical enrichment pass. It must never mine prose, URLs, or
 * a page title into a fabricated alias or tag.
 */
export declare function extractStoredMappingMetadata(raw: unknown, primaryTitle: string): {
    aliases: string[];
    genres: string[];
};
/**
 * Classify metadata that was explicitly returned by a provider.  This is
 * shared by a normal catalog pass and a retroactive source refetch so a new
 * mapping cannot be silently left PENDING after its first successful fetch.
 */
export declare function classifyExplicitSourceMetadata(raw: unknown, primaryTitle: string): {
    aliases: string[];
    genres: string[];
    aliasesState: 'ENRICHED' | 'NO_ALIASES' | 'PARSER_ERROR';
    tagsState: 'ENRICHED' | 'NO_TAGS' | 'PARSER_ERROR';
    parserError: boolean;
};
/**
 * Generate a bounded list of search query candidates for cross-source discovery.
 * Includes the primary title, the main title clause if punctuated (e.g. "Title, Subtitle" -> "Title"),
 * and up to 4 distinct aliases, deduplicated and capped at 5 total queries.
 */
export declare function buildDiscoveryQueryCandidates(title: string, aliases?: string[]): string[];
