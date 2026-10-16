import type { SupabaseClient } from '@supabase/supabase-js';
export interface DeduplicationResult {
    workId: string | null;
    mappingId: string;
    status: 'EXISTING_MAPPING' | 'NEW_WORK' | 'AMBIGUOUS' | 'FAILED';
    slug: string;
    reason?: string;
    metadataDelta?: {
        aliasesAdded: number;
        tagsAdded: number;
    };
}
interface MetadataMergeDelta {
    aliasesAdded: number;
    tagsAdded: number;
}
export declare const ADULT_SOURCES: Set<string>;
export interface CandidateWork {
    source: string;
    sourceWorkId: string;
    title: string;
    slug: string;
    synopsis?: string;
    author?: string;
    artist?: string;
    kind?: 'MANGA' | 'MANHWA' | 'MANHUA' | 'WEBTOON' | 'PORNHWA' | 'UNKNOWN';
    status?: 'ONGOING' | 'COMPLETED' | 'HIATUS' | 'CANCELLED' | 'UNKNOWN';
    year?: number;
    ageRating?: number;
    contentRating?: 'GENERAL' | 'ADULT_18';
    coverId?: string | null;
    /**
     * Cover registration can cost a source request and a storage upload. It is
     * intentionally lazy so an ambiguous work or healthy canonical cover does
     * not pay for media that will be discarded.
     */
    loadCoverId?: () => Promise<string | null>;
    aliases?: string[];
    genres?: string[];
    rawMetadata?: Record<string, any>;
}
export declare function computeCanonicalChapterKey(chapterNumber: number | string, chapterTitle?: string): {
    normalizedNumber: number;
    sortKey: number;
    isSpecial: boolean;
    specialCategory?: 'prologue' | 'extra' | 'special' | 'side';
};
export interface EditorialValidationResult {
    valid: boolean;
    reason?: string;
}
export declare function validateEditorialTitle(rawTitle: string): EditorialValidationResult;
export declare class DeduplicationEngine {
    private supabase;
    private logger;
    private tagCatalog;
    private tagCatalogExpiresAt;
    private tagCatalogFlight;
    private readonly tagCatalogTtlMs;
    constructor(supabase: SupabaseClient);
    /** Merge a work whose exact source mapping and canonical work ID are already loaded. */
    mergeKnownMappedWork(candidate: CandidateWork, workId: string, source: string): Promise<DeduplicationResult>;
    /**
     * Persist a mapping only after its caller has independently established a
     * high-confidence cross-provider identity match.  This is deliberately
     * narrower than resolveWork(): a complementary-source probe must never
     * create a second canonical work merely because a remote search result
     * changed between its summary and detail responses.
     */
    attachVerifiedMapping(candidate: CandidateWork, workId: string, source: string): Promise<DeduplicationResult>;
    private resolveCandidateCover;
    /**
     * Resolve or register a work conservatively.
     * Never blindly overwrite or perform destructive merges on fuzzy matches.
     */
    resolveWork(candidate: CandidateWork): Promise<DeduplicationResult>;
    /**
     * Applies field-level metadata precedence:
     * Priority: MANUAL (Admin/Editor) > KURO > OTHER SOURCES
     * Rules:
     * 1. Manual edit provenance is strictly immutable.
     * 2. Kuro upgrades non-manual fields if candidate has valid data.
     * 3. Other sources only fill empty/null fields.
     * 4. Never overwrite valid data with empty/null.
     */
    applyMetadataPrecedence(workId: string, candidate: CandidateWork, source: string): Promise<MetadataMergeDelta>;
    /**
     * Synchronize canonical adult tags and upstream genres to public.work_tags
     */
    /**
     * Normalize and resolve a tag name to its canonical form
     */
    private normalizeTagName;
    private isGarbageTag;
    private tagLookupKey;
    private getTagCatalog;
    /**
     * Legacy importer versions wrote type/status vocabulary as automatic tags.
     * This removes only those machine-generated associations; staff-curated
     * associations are deliberately untouched.
     */
    removeLegacySystemGeneratedNonGenreTags(workId: string): Promise<void>;
    syncWorkTags(workId: string, candidate: CandidateWork, _isAdult: boolean, _kind?: string, source?: string): Promise<number>;
    private sanitizeSlug;
}
export {};
