export interface LiteMetadata {
    title?: string | null;
    aliases?: string[] | null;
    synopsis?: string | null;
    coverUrl?: string | null;
    author?: string | null;
    artist?: string | null;
    year?: number | null;
    status?: string | null;
    kind?: string | null;
    ageRating?: number | null;
    tags?: string[] | null;
}
export interface MetadataField<T> {
    value: T;
    protected?: boolean;
    validated?: boolean;
    sourceRank?: number;
}
export type MetadataRecord = {
    [K in keyof LiteMetadata]?: MetadataField<NonNullable<LiteMetadata[K]>>;
};
export declare function sanitizeSynopsis(value: string | null | undefined): string | null;
export declare function normalizeSourceTag(raw: string): string | null;
export declare function mergeMetadata(existing: MetadataRecord, incoming: LiteMetadata, sourceRank?: number): MetadataRecord;
