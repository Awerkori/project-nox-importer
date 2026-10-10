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
/**
 * Finds a bounded batch of staged frontiers that are already eligible for
 * publication. The frontier aggregation must cover all works before LIMIT is
 * applied; otherwise an old blocked prefix can starve later publishable work.
 */
export declare const PUBLISHABLE_STAGED_WORKS_QUERY = "\n  WITH staged_frontiers AS MATERIALIZED (\n    -- Select one durable staged frontier per work before applying the global\n    -- bound.  Selecting an arbitrary prefix of STAGED rows first lets old\n    -- blocked works consume every sweep turn and hides later work that the\n    -- same publication barrier could safely release now.\n    SELECT m.work_id, MIN(m.chapter_sort_key) AS frontier_sort_key\n    FROM importer_chapter_mappings m\n    WHERE m.status IN ('STAGED', 'WAITING_FOR_GAP')\n      AND m.work_id IS NOT NULL\n    GROUP BY m.work_id\n  ), frontier_state AS MATERIALIZED (\n    SELECT f.work_id,\n           f.frontier_sort_key,\n           MAX(c.number) AS max_published\n    FROM staged_frontiers f\n    LEFT JOIN chapters c\n      ON c.work_id = f.work_id\n     AND c.published_at IS NOT NULL\n    GROUP BY f.work_id, f.frontier_sort_key\n  )\n  SELECT f.work_id\n  FROM frontier_state f\n  WHERE\n    -- Normal monotonic advance: publication barrier permits the immediate\n    -- next canonical chapter (including decimal chapter keys).\n    (f.max_published IS NOT NULL AND f.frontier_sort_key <= f.max_published + 1.05)\n    -- A confirmed canonical gap permits the next staged frontier, exactly as\n    -- the barrier RPC does.  Local absence is never treated as a gap.\n    OR EXISTS (\n      SELECT 1\n      FROM importer_confirmed_gaps g\n      WHERE g.work_id = f.work_id\n        AND g.start_sort_key <= COALESCE(f.max_published + 1, 1)\n        AND g.end_sort_key >= f.frontier_sort_key - 1\n    )\n    -- Initial chapter safety: do not jump over a known non-gap predecessor.\n    OR (\n      f.max_published IS NULL\n      AND f.frontier_sort_key <= 1.5\n      AND NOT EXISTS (\n        SELECT 1\n        FROM importer_chapter_mappings predecessor\n        WHERE predecessor.work_id = f.work_id\n          AND predecessor.chapter_sort_key < f.frontier_sort_key\n          AND predecessor.is_gap IS NOT TRUE\n          AND predecessor.status NOT IN ('STAGED', 'WAITING_FOR_GAP')\n      )\n    )\n  ORDER BY f.frontier_sort_key ASC, f.work_id ASC\n  LIMIT 40;\n";
export declare class PublicationBarrier {
    private supabase;
    private logger;
    private workLocks;
    private coverWarmQueue;
    private warmedCoverIds;
    private readonly maxRememberedWarmCovers;
    private pendingCoverWarmRetries;
    private readonly maxPendingCoverWarmRetries;
    private lastCoverWarmFailureLogAt;
    onPublished?: (isFreshRelease: boolean, durableRateEvent?: boolean) => void;
    constructor(supabase: SupabaseClient);
    private warmPublishedCover;
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
     * Public cascade runner for a work (thread-safe under workLock).
     */
    runCascade(workId: string, maxBatch?: number): Promise<number>;
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
