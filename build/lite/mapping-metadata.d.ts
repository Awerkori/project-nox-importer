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
