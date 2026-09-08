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
export declare class DeduplicationEngine {
    private supabase;
    private logger;
    constructor(supabase: SupabaseClient);
    /**
     * Resolve or register a work conservatively.
     * Never blindly overwrite or perform destructive merges on fuzzy matches.
     */
    resolveWork(candidate: CandidateWork): Promise<DeduplicationResult>;
    private sanitizeSlug;
}
