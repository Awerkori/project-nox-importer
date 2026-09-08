import { Logger } from './logger.js';
import { AsyncSemaphore } from './concurrency.js';
export class PublicationBarrier {
    supabase;
    logger = new Logger('PublicationBarrier');
    workLocks = new Map();
    constructor(supabase) {
        this.supabase = supabase;
    }
    getWorkLock(workId) {
        let sem = this.workLocks.get(workId);
        if (!sem) {
            sem = new AsyncSemaphore(1);
            this.workLocks.set(workId, sem);
        }
        return sem;
    }
    /**
     * Check if a chapter can be published according to the canonical sort key order,
     * verifying that discovery is complete and all preceding chapters are published or gaps.
     */
    async checkBarrier(workId, targetSortKey) {
        const { data, error } = await this.supabase.rpc('importer_check_publication_barrier', {
            p_work_id: workId,
            p_target_sort_key: targetSortKey,
        });
        if (error) {
            this.logger.error('Error querying publication barrier RPC', { workId, targetSortKey, error: error.message });
            return {
                canPublish: false,
                reason: `RPC_ERROR: ${error.message}`,
                blockingCount: 1,
                blockingSortKeys: [],
            };
        }
        if (!data || data.length === 0) {
            return { canPublish: true, reason: 'OK', blockingCount: 0, blockingSortKeys: [] };
        }
        const res = data[0];
        return {
            canPublish: Boolean(res.can_publish),
            reason: res.reason || 'UNKNOWN',
            blockingCount: res.blocking_count || 0,
            blockingSortKeys: res.blocking_sort_keys || [],
        };
    }
    /**
     * Stage chapter after successful page downloads & storage.
     * Chapter remains with published_at = NULL in public.chapters.
     */
    async stageChapter(params) {
        const lock = this.getWorkLock(params.workId);
        await lock.runExclusive(async () => {
            const { error } = await this.supabase.from('importer_chapter_mappings').upsert({
                source: params.source,
                source_chapter_id: params.sourceChapterId,
                chapter_id: params.chapterId,
                work_id: params.workId,
                work_mapping_id: params.workMappingId,
                chapter_number: params.chapterNumber,
                chapter_sort_key: params.sortKey,
                page_count: params.pageCount,
                is_page_provider: params.isPageProvider ?? true,
                status: 'STAGED',
                is_gap: false,
                last_error: null,
                updated_at: new Date().toISOString(),
            }, { onConflict: 'source,source_chapter_id' });
            if (error) {
                this.logger.error('Failed to stage chapter mapping', {
                    workId: params.workId,
                    chapterNumber: params.chapterNumber,
                    error: error.message,
                });
                throw error;
            }
            this.logger.info('Chapter staged with pages stored (awaiting publication barrier)', {
                workId: params.workId,
                chapterNumber: params.chapterNumber,
                sortKey: params.sortKey,
                source: params.source,
            });
        });
    }
    /**
     * Try to publish a chapter if the barrier is cleared.
     * If publication succeeds, immediately triggers cascade to publish any consecutive STAGED chapters.
     */
    async tryPublish(workId, sortKey, chapterId) {
        const lock = this.getWorkLock(workId);
        return lock.runExclusive(async () => {
            const check = await this.checkBarrier(workId, sortKey);
            if (!check.canPublish) {
                this.logger.info('Chapter publication blocked by barrier', {
                    workId,
                    sortKey,
                    reason: check.reason,
                    blockingCount: check.blockingCount,
                    blockingSortKeys: check.blockingSortKeys,
                });
                return { published: false, reason: check.reason };
            }
            // Barrier cleared! Publish this chapter
            await this.executePublish(workId, chapterId, new Date().toISOString());
            this.logger.info('Chapter published via barrier', { workId, sortKey, chapterId });
            // Run immediate cascade for subsequent STAGED chapters of this work
            await this.runCascadeUnderLock(workId);
            return { published: true };
        });
    }
    /**
     * Executes atomic DB publication for a single chapter.
     */
    async executePublish(workId, chapterId, publishedAtIso) {
        // 1. Mark public.chapters.published_at
        const { error: chErr } = await this.supabase
            .from('chapters')
            .update({ published_at: publishedAtIso })
            .eq('id', chapterId);
        if (chErr)
            throw chErr;
        // 2. Mark public.importer_chapter_mappings.status = 'COMPLETED'
        const { error: mapErr } = await this.supabase
            .from('importer_chapter_mappings')
            .update({ status: 'COMPLETED', updated_at: new Date().toISOString() })
            .eq('chapter_id', chapterId);
        if (mapErr)
            throw mapErr;
        // 3. Mark public.works.published = true if draft
        await this.supabase
            .from('works')
            .update({ published: true, updated_at: new Date().toISOString() })
            .eq('id', workId)
            .eq('published', false);
    }
    /**
     * Cascading publication of all consecutive STAGED chapters for a work.
     */
    async runCascadeUnderLock(workId) {
        let cascadeCount = 0;
        while (true) {
            // Find the next STAGED chapter with lowest sort key
            const { data: stagedList, error } = await this.supabase
                .from('importer_chapter_mappings')
                .select('chapter_id, chapter_sort_key, chapter_number')
                .eq('work_id', workId)
                .eq('status', 'STAGED')
                .order('chapter_sort_key', { ascending: true })
                .limit(1);
            if (error || !stagedList || stagedList.length === 0) {
                break;
            }
            const candidate = stagedList[0];
            const check = await this.checkBarrier(workId, candidate.chapter_sort_key);
            if (!check.canPublish) {
                // Next chapter still has unfulfilled dependencies
                break;
            }
            cascadeCount++;
            const monotonicTimestamp = new Date(Date.now() + cascadeCount * 50).toISOString();
            await this.executePublish(workId, candidate.chapter_id, monotonicTimestamp);
            this.logger.info(`Cascade published chapter ${candidate.chapter_number} (sort: ${candidate.chapter_sort_key})`, {
                workId,
                chapterNumber: candidate.chapter_number,
                sortKey: candidate.chapter_sort_key,
                cascadeStep: cascadeCount,
            });
        }
        return cascadeCount;
    }
    /**
     * Handle definitive failure of a chapter after max attempts exhausted.
     * Checks for fallback in other sources; if none, registers a gap and releases subsequent chapters.
     */
    async handleDefiniteFailure(workId, chapterNumber, sortKey, failedSource) {
        const lock = this.getWorkLock(workId);
        await lock.runExclusive(async () => {
            this.logger.warn(`Definite failure for chapter ${chapterNumber} on source ${failedSource}`, {
                workId,
                chapterNumber,
                sortKey,
            });
            // Check if another mapped source already has a mapping or can provide this chapter
            const { data: otherMappings } = await this.supabase
                .from('importer_chapter_mappings')
                .select('id, source, status')
                .eq('work_id', workId)
                .eq('chapter_sort_key', sortKey)
                .neq('source', failedSource);
            const hasViableAlternative = (otherMappings || []).some((m) => m.status === 'QUEUED' || m.status === 'IMPORTING' || m.status === 'STAGED' || m.status === 'COMPLETED');
            if (hasViableAlternative) {
                this.logger.info(`Alternative source already in progress for chapter ${chapterNumber}`, { workId, sortKey });
                return;
            }
            // No alternative source available: register GAP so the work does not stall
            this.logger.warn(`Registering publication GAP for chapter ${chapterNumber} to unblock sequence`, {
                workId,
                sortKey,
                chapterNumber,
            });
            await this.supabase
                .from('importer_chapter_mappings')
                .update({
                status: 'FAILED',
                is_gap: true,
                last_error: `Definite failure on ${failedSource}. Gap registered to unblock work sequence.`,
                updated_at: new Date().toISOString(),
            })
                .eq('work_id', workId)
                .eq('chapter_sort_key', sortKey);
            // Trigger cascade to unblock waiting STAGED chapters
            await this.runCascadeUnderLock(workId);
        });
    }
    /**
     * Periodic or startup sweep: checks all works that have STAGED chapters
     * and attempts to publish them.
     */
    async sweepStagedPublications() {
        try {
            const { data: stagedWorks, error } = await this.supabase
                .from('importer_chapter_mappings')
                .select('work_id')
                .eq('status', 'STAGED')
                .not('work_id', 'is', null);
            if (error || !stagedWorks || stagedWorks.length === 0) {
                return 0;
            }
            const distinctWorkIds = Array.from(new Set(stagedWorks.map((r) => r.work_id)));
            let publishedTotal = 0;
            for (const workId of distinctWorkIds) {
                const lock = this.getWorkLock(workId);
                const count = await lock.runExclusive(async () => {
                    return this.runCascadeUnderLock(workId);
                });
                publishedTotal += count;
            }
            if (publishedTotal > 0) {
                this.logger.info(`Sweep published ${publishedTotal} staged chapter(s) across ${distinctWorkIds.length} work(s)`);
            }
            return publishedTotal;
        }
        catch (err) {
            this.logger.error('Error during sweepStagedPublications', { error: err?.message });
            return 0;
        }
    }
}
