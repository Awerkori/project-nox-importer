import type { SupabaseClient } from '@supabase/supabase-js';
export interface DeduplicationResult {
    workId: string | null;
    mappingId: string;
    status: 'EXISTING_MAPPING' | 'NEW_WORK' | 'AMBIGUOUS' | 'FAILED';
    slug: string;
    reason?: string;
}
export interface CandidateWork {
    source: string;
    sourceWorkId: string;
    title: string;
    slug: string;
    synopsis?: string;
    author?: string;
    artist?: string;
    kind?: 'MANGA' | 'MANHWA' | 'MANHUA' | 'WEBTOON';
    status?: 'ONGOING' | 'COMPLETED' | 'HIATUS' | 'CANCELLED';
    year?: number;
    ageRating?: number;
    coverId?: string | null;
    aliases?: string[];
    rawMetadata?: Record<string, any>;
}
export declare function computeCanonicalChapterKey(chapterNumber: number | string, chapterTitle?: string): {
    normalizedNumber: number;
    sortKey: number;
    isSpecial: boolean;
    specialCategory?: 'prologue' | 'extra' | 'special' | 'side';
};
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
    private sanitizeSlug;
}
