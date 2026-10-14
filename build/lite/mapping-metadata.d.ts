/**
 * Only explicitly named upstream metadata fields are eligible for the
 * no-network historical enrichment pass. It must never mine prose, URLs, or
 * a page title into a fabricated alias or tag.
 */
export declare function extractStoredMappingMetadata(raw: unknown, primaryTitle: string): {
    aliases: string[];
    genres: string[];
};
