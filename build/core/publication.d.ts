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
export interface PublishResult {
    published: boolean;
    reason?: string;
    timings?: {
        barrierCheckMs: number;
        publishUpdateMs: number;
        cascadeMs: number;
        lockWaitMs: number;
    };
}
export declare class PublicationBarrier {
    private supabase;
    private logger;
    private workLocks;
    onPublished?: (isFreshRelease: boolean) => void;
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
    /**
     * Try to publish a chapter if the barrier is cleared.
     * If publication succeeds, immediately triggers cascade to publish any consecutive STAGED chapters.
     */
    tryPublish(workId: string, sortKey: number, chapterId: string, isFreshRelease?: boolean): Promise<PublishResult>;
    /**
     * Executes atomic DB publication for a single chapter.
     */
    private executePublish;
    /**
     * Cascading publication of all consecutive STAGED chapters for a work.
     * Bounded by maxBatch to guarantee multi-work fairness.
     */
    private runCascadeUnderLock;
    /**
     * Handle definitive failure of a chapter after max attempts exhausted.
     * Checks for fallback in other sources; if none, registers a gap and releases subsequent chapters.
     */
    handleDefiniteFailure(workId: string, chapterNumber: number, sortKey: number, failedSource: string): Promise<void>;
    /**
     * Periodic or startup sweep: checks all works that have STAGED chapters
     * and publishes them in round-robin batches across distinct works to ensure fairness.
     */
    sweepStagedPublications(maxTotalPublications?: number, perWorkBurst?: number): Promise<number>;
}
