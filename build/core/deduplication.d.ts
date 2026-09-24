import type { SupabaseClient } from '@supabase/supabase-js';
export interface DeduplicationResult {
    workId: string | null;
    mappingId: string;
    status: 'EXISTING_MAPPING' | 'NEW_WORK' | 'AMBIGUOUS' | 'FAILED';
    slug: string;
    reason?: string;
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
    constructor(supabase: SupabaseClient);
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
    applyMetadataPrecedence(workId: string, candidate: CandidateWork, source: string): Promise<void>;
    /**
     * Synchronize canonical adult tags and upstream genres to public.work_tags
     */
    /**
     * Normalize and resolve a tag name to its canonical form
     */
    private normalizeTagName;
    private isGarbageTag;
    private getProviderDefaultTags;
    syncWorkTags(workId: string, candidate: CandidateWork, isAdult: boolean, kind?: string, source?: string): Promise<void>;
    private sanitizeSlug;
}
