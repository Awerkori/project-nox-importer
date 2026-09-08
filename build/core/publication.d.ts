import type { SupabaseClient } from '@supabase/supabase-js';
export interface StageChapterParams {
    workId: string;
    chapterId: string;
    chapterNumber: number;
    sortKey: number;
    source: string;
    sourceChapterId: string;
    workMappingId: string;
    pageCount: number;
    isPageProvider?: boolean;
}
export interface BarrierCheckResult {
    canPublish: boolean;
    reason: string;
    blockingCount: number;
    blockingSortKeys: number[];
}
export declare class PublicationBarrier {
    private supabase;
    private logger;
    private workLocks;
    constructor(supabase: SupabaseClient);
    private getWorkLock;
    /**
     * Check if a chapter can be published according to the canonical sort key order,
     * verifying that discovery is complete and all preceding chapters are published or gaps.
     */
    checkBarrier(workId: string, targetSortKey: number): Promise<BarrierCheckResult>;
    /**
     * Stage chapter after successful page downloads & storage.
     * Chapter remains with published_at = NULL in public.chapters.
     */
    stageChapter(params: StageChapterParams): Promise<void>;
    /**
     * Try to publish a chapter if the barrier is cleared.
     * If publication succeeds, immediately triggers cascade to publish any consecutive STAGED chapters.
     */
    tryPublish(workId: string, sortKey: number, chapterId: string): Promise<{
        published: boolean;
        reason?: string;
    }>;
    /**
     * Executes atomic DB publication for a single chapter.
     */
    private executePublish;
    /**
     * Cascading publication of all consecutive STAGED chapters for a work.
     */
    private runCascadeUnderLock;
    /**
     * Handle definitive failure of a chapter after max attempts exhausted.
     * Checks for fallback in other sources; if none, registers a gap and releases subsequent chapters.
     */
    handleDefiniteFailure(workId: string, chapterNumber: number, sortKey: number, failedSource: string): Promise<void>;
    /**
     * Periodic or startup sweep: checks all works that have STAGED chapters
     * and attempts to publish them.
     */
    sweepStagedPublications(): Promise<number>;
}
