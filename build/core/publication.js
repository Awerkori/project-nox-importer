import { getYugabytePool } from '../db/yugabyte-direct.js';
import { Logger } from './logger.js';
import { AsyncSemaphore } from './concurrency.js';
import { computeCanonicalChapterKey } from './deduplication.js';
export class PublicationBarrier {
    supabase;
    logger = new Logger('PublicationBarrier');
    workLocks = new Map();
    // Cover warming is presentation work, never publication work.  Keep it
    // process-bounded and serial so a burst of newly visible chapters cannot
    // create a parallel Telegram/edge stampede.
    coverWarmQueue = new AsyncSemaphore(1, 'cover_warm_queue');
    warmedCoverIds = new Set();
    maxRememberedWarmCovers = 256;
    lastCoverWarmFailureLogAt = 0;
    onPublished;
    constructor(supabase) {
        this.supabase = supabase;
    }
    warmPublishedCover(coverId) {
        if (!coverId || !/^[0-9a-f-]{36}$/i.test(coverId) || this.warmedCoverIds.has(coverId))
            return;
        this.warmedCoverIds.add(coverId);
        if (this.warmedCoverIds.size > this.maxRememberedWarmCovers) {
            const oldest = this.warmedCoverIds.values().next().value;
            if (oldest)
                this.warmedCoverIds.delete(oldest);
        }
        void this.coverWarmQueue.runExclusive(async () => {
            let warmed = false;
            try {
                // NOX_MANGA_URL is the validated runtime endpoint used by the storage
                // bridge and sentinel. Do not rely on an untracked legacy alias here.
                const siteUrl = process.env.NOX_MANGA_URL || 'https://manga.project-nox-awerkori.workers.dev';
                // Sequential variants avoid duplicate origin reads. This happens after
                // commit, outside the YSQL client and behind a single low-priority lane.
                for (const variant of ['thumb', 'hero']) {
                    const warmUrl = `${siteUrl.replace(/\/$/, '')}/media/${coverId}?size=${variant}&v=3`;
                    const response = await fetch(warmUrl, { signal: AbortSignal.timeout(10_000) }).catch(() => null);
                    if (response?.ok) {
                        warmed = true;
                        await response.arrayBuffer().catch(() => { });
                    }
                }
            }
            finally {
                // A transient network failure must not suppress the next legitimate
                // publication attempt for this cover. Normal media delivery remains
                // authoritative regardless of warming outcome.
                if (!warmed) {
                    this.warmedCoverIds.delete(coverId);
                    // A failed warmer is operationally meaningful, but one warning per
                    // minute keeps a bad edge route from becoming a hot-path log storm.
                    if (Date.now() - this.lastCoverWarmFailureLogAt >= 60_000) {
                        this.lastCoverWarmFailureLogAt = Date.now();
                        this.logger.warn('Cover warming failed; normal media delivery remains authoritative', { coverId });
                    }
                }
            }
        }).catch(() => this.warmedCoverIds.delete(coverId));
    }
    getWorkLock(workId) {
        let sem = this.workLocks.get(workId);
        if (!sem) {
            sem = new AsyncSemaphore(1);
            this.workLocks.set(workId, sem);
        }
        const lock = sem;
        return {
            runExclusive: async (fn) => {
                try {
                    return await lock.runExclusive(fn);
                }
                finally {
                    if (lock.active === 0 && lock.queued === 0 && this.workLocks.get(workId) === lock)
                        this.workLocks.delete(workId);
                }
            },
        };
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
    /**
     * Try to publish a chapter if the barrier is cleared.
     * If publication succeeds, immediately triggers cascade to publish any consecutive STAGED chapters.
     */
    async tryPublish(workId, sortKey, chapterId, isFreshRelease = false) {
        const lock = this.getWorkLock(workId);
        const lockWaitStart = performance.now();
        return lock.runExclusive(async () => {
            const lockWaitMs = Math.round(performance.now() - lockWaitStart);
            const t0 = performance.now();
            const check = await this.checkBarrier(workId, sortKey);
            const barrierCheckMs = Math.round(performance.now() - t0);
            if (!check.canPublish) {
                this.logger.info('Chapter publication blocked by barrier', {
                    workId,
                    sortKey,
                    reason: check.reason,
                    blockingCount: check.blockingCount,
                    blockingSortKeys: check.blockingSortKeys,
                });
                if (check.reason.includes('GAP') || check.reason.includes('WAITING')) {
                    await this.supabase
                        .from('importer_chapter_mappings')
                        .update({ status: 'WAITING_FOR_GAP', updated_at: new Date().toISOString() })
                        .eq('work_id', workId)
                        .eq('chapter_id', chapterId)
                        .eq('status', 'STAGED');
                }
                return {
                    published: false,
                    reason: check.reason,
                    timings: { barrierCheckMs, publishUpdateMs: 0, cascadeMs: 0, lockWaitMs },
                };
            }
            // Barrier cleared! Publish this chapter
            const t1 = performance.now();
            await this.executePublish(workId, chapterId, new Date().toISOString(), sortKey, isFreshRelease);
            const publishUpdateMs = Math.round(performance.now() - t1);
            this.logger.info('Chapter published via barrier', { workId, sortKey, chapterId, isFreshRelease });
            // Run immediate cascade for subsequent STAGED chapters of this work
            const t2 = performance.now();
            await this.runCascadeUnderLock(workId);
            const cascadeMs = Math.round(performance.now() - t2);
            return {
                published: true,
                timings: { barrierCheckMs, publishUpdateMs, cascadeMs, lockWaitMs },
            };
        });
    }
    /**
     * Executes atomic DB publication for a single chapter.
     */
    async executePublish(workId, chapterId, publishedAtIso, sortKey, isFreshRelease = false) {
        const pool = typeof this.supabase?.getPool === 'function' ? this.supabase.getPool() : null;
        if (pool) {
            // Direct YSQL atomic fast-path (production)
            const client = await pool.connect();
            let isNewlyVisible = false;
            let coverIdToWarm;
            try {
                await client.query('BEGIN');
                // 1. Fetch current work details for cover/publication verification
                const workRes = await client.query('SELECT id, title, slug, cover_id, published, latest_chapter_published_at FROM works WHERE id = $1::uuid', [workId]);
                const currentWork = workRes.rows[0];
                // 2. Determine shouldPublishWork
                let shouldPublishWork = false;
                if (currentWork?.published === true) {
                    shouldPublishWork = true;
                }
                else if (currentWork?.cover_id && currentWork.title && currentWork.slug) {
                    const coverRes = await client.query('SELECT id, storage_ready, bytes FROM media WHERE id = $1::uuid', [currentWork.cover_id]);
                    const coverMedia = coverRes.rows[0];
                    if (coverMedia && coverMedia.storage_ready && (coverMedia.bytes || 0) >= 1500) {
                        shouldPublishWork = true;
                    }
                    else {
                        this.logger.warn('Work cover is not storage_ready or too small, publication barrier withheld published=true', {
                            workId,
                            coverId: currentWork.cover_id,
                            bytes: coverMedia?.bytes,
                        });
                    }
                }
                else {
                    this.logger.warn('Work missing cover_id or canonical metadata, publication barrier withheld published=true', {
                        workId,
                        hasCover: Boolean(currentWork?.cover_id),
                        hasTitle: Boolean(currentWork?.title),
                        hasSlug: Boolean(currentWork?.slug),
                    });
                }
                // 3. Mark public.chapters.published_at and set is_fresh_release
                const chUpdateRes = await client.query(`WITH visible AS (
             UPDATE chapters SET published_at = clock_timestamp(), is_fresh_release = $2
             WHERE id = $1::uuid AND published_at IS NULL
             RETURNING id, published_at, is_fresh_release
           ), event AS (
             INSERT INTO importer_publication_events (chapter_id, transition_at, recorded_at, bucket_minute, is_fresh_release)
             SELECT id, published_at, clock_timestamp(), date_trunc('minute', published_at AT TIME ZONE 'UTC') AT TIME ZONE 'UTC', is_fresh_release FROM visible
             ON CONFLICT (chapter_id) DO NOTHING
             RETURNING bucket_minute, is_fresh_release
           ), bucket AS (
             INSERT INTO importer_rate_buckets (bucket_minute, completed_jobs, fresh_visible, visible_published, updated_at)
             SELECT bucket_minute, 0, CASE WHEN is_fresh_release THEN 1 ELSE 0 END, 1, clock_timestamp() FROM event
             ON CONFLICT (bucket_minute) DO UPDATE
             SET fresh_visible = importer_rate_buckets.fresh_visible + EXCLUDED.fresh_visible,
                 visible_published = importer_rate_buckets.visible_published + EXCLUDED.visible_published,
                 updated_at = EXCLUDED.updated_at
           ) SELECT true AS newly_visible, published_at FROM visible
           UNION ALL SELECT false AS newly_visible, published_at FROM chapters
           WHERE id = $1::uuid AND NOT EXISTS (SELECT 1 FROM visible)`, [chapterId, Boolean(isFreshRelease)]);
                isNewlyVisible = chUpdateRes.rows[0]?.newly_visible === true;
                if (!chUpdateRes.rows[0]?.published_at)
                    throw new Error('Publication target chapter missing');
                publishedAtIso = new Date(chUpdateRes.rows[0].published_at).toISOString();
                // 4. Mark importer_chapter_mappings status = 'COMPLETED'
                await client.query(`UPDATE importer_chapter_mappings
           SET status = 'COMPLETED', updated_at = NOW()
           WHERE work_id = $1::uuid AND chapter_id = $2::uuid`, [workId, chapterId]);
                // 5. Cancel / auto-complete redundant QUEUED/RETRY jobs in importer_queue
                if (sortKey !== undefined && sortKey !== null) {
                    await client.query(`UPDATE importer_queue
             SET status = 'COMPLETED',
                 updated_at = NOW(),
                 last_error = 'CANONICAL_ALREADY_SATISFIED'
             WHERE task_type = 'IMPORT_CHAPTER'
               AND status IN ('QUEUED', 'RETRY')
               AND chapter_sort_key = $1
               AND (payload->>'workId') = $2`, [sortKey, workId]);
                    await client.query(`UPDATE importer_chapter_mappings
             SET status = 'COMPLETED',
                 is_page_provider = false,
                 chapter_id = $1::uuid,
                 updated_at = NOW()
             WHERE work_id = $2::uuid
               AND chapter_sort_key = $3
               AND status IN ('PENDING', 'QUEUED')`, [chapterId, workId, sortKey]);
                }
                // 6. Update public.works: published and latest_chapter_published_at
                await client.query(`UPDATE works
           SET published = (CASE WHEN $2::boolean THEN true ELSE published END),
               latest_chapter_published_at = GREATEST(COALESCE(latest_chapter_published_at, $3::timestamptz), $3::timestamptz),
               updated_at = NOW()
           WHERE id = $1::uuid
           RETURNING slug`, [workId, shouldPublishWork, publishedAtIso]);
                // Any work promoted in a release feed can be rendered immediately on
                // Home/Lançamentos. Warm its immutable derivatives after commit; never
                // make the publication transaction or an existing work wait on edge I/O.
                coverIdToWarm = currentWork?.cover_id;
                await client.query('COMMIT');
            }
            catch (txErr) {
                await client.query('ROLLBACK').catch(() => { });
                throw txErr;
            }
            finally {
                client.release();
            }
            if (isNewlyVisible) {
                try {
                    this.onPublished?.(Boolean(isFreshRelease), true);
                }
                catch { }
            }
            // A cold cover otherwise makes the first visitor wait on Telegram and
            // progressively stream the image. This is detached, bounded and does not
            // hold a database client.
            if (isNewlyVisible)
                this.warmPublishedCover(coverIdToWarm);
            // Update importer_chapter_manifest status to PUBLISHED if available (asynchronously)
            void (async () => {
                try {
                    const manQuery = this.supabase.from('importer_chapter_manifest');
                    if (manQuery && typeof manQuery.update === 'function') {
                        let sKey = sortKey;
                        if (sKey === undefined) {
                            const { data: chInfo } = await this.supabase
                                .from('chapters')
                                .select('number')
                                .eq('id', chapterId)
                                .maybeSingle();
                            if (chInfo?.number !== undefined) {
                                sKey = computeCanonicalChapterKey(chInfo.number).sortKey;
                            }
                        }
                        if (sKey !== undefined) {
                            await manQuery
                                .update({
                                status: 'PUBLISHED',
                                last_checked_at: new Date().toISOString(),
                            })
                                .eq('work_id', workId)
                                .eq('chapter_sort_key', sKey);
                        }
                    }
                }
                catch { }
            })();
            return;
        }
        // Fallback path for unit tests / mock clients without direct pool
        const [workRes, chRes] = await Promise.all([
            this.supabase
                .from('works')
                .select('id, title, slug, cover_id, published, latest_chapter_published_at')
                .eq('id', workId)
                .maybeSingle(),
            this.supabase
                .from('chapters')
                .select('published_at')
                .eq('id', chapterId)
                .maybeSingle(),
        ]);
        const currentWork = workRes?.data;
        const existingLatest = currentWork?.latest_chapter_published_at;
        const existingCh = chRes?.data;
        const isNewlyVisible = !existingCh?.published_at;
        const finalPublishedAt = existingCh?.published_at || publishedAtIso;
        const [chUpdateRes, mapUpdateRes] = await Promise.all([
            this.supabase
                .from('chapters')
                .update({
                published_at: finalPublishedAt,
                is_fresh_release: isFreshRelease,
            })
                .eq('id', chapterId),
            this.supabase
                .from('importer_chapter_mappings')
                .update({ status: 'COMPLETED', updated_at: new Date().toISOString() })
                .eq('work_id', workId)
                .eq('chapter_id', chapterId),
        ]);
        if (chUpdateRes.error)
            throw chUpdateRes.error;
        if (mapUpdateRes.error)
            throw mapUpdateRes.error;
        let shouldPublishWork = false;
        if (currentWork?.published === true) {
            shouldPublishWork = true;
        }
        else if (currentWork?.cover_id && currentWork.title && currentWork.slug) {
            try {
                const { data: coverMedia } = await this.supabase
                    .from('media')
                    .select('id, storage_ready, bytes')
                    .eq('id', currentWork.cover_id)
                    .maybeSingle();
                if (coverMedia && coverMedia.storage_ready && (coverMedia.bytes || 0) >= 1500) {
                    shouldPublishWork = true;
                }
            }
            catch { }
        }
        const workUpdate = {
            updated_at: new Date().toISOString(),
        };
        if (shouldPublishWork) {
            workUpdate.published = true;
        }
        workUpdate.latest_chapter_published_at =
            !existingLatest || new Date(publishedAtIso) > new Date(existingLatest)
                ? publishedAtIso
                : existingLatest;
        await this.supabase
            .from('works')
            .update(workUpdate)
            .eq('id', workId);
        if (isNewlyVisible) {
            try {
                this.onPublished?.(Boolean(isFreshRelease));
            }
            catch { }
        }
    }
    /**
     * Public cascade runner for a work (thread-safe under workLock).
     */
    async runCascade(workId, maxBatch = 8) {
        const lock = this.getWorkLock(workId);
        return lock.runExclusive(async () => {
            return this.runCascadeUnderLock(workId, maxBatch);
        });
    }
    /**
     * Cascading publication of all consecutive STAGED chapters for a work.
     * Bounded by maxBatch to guarantee multi-work fairness.
     */
    async runCascadeUnderLock(workId, maxBatch = 8) {
        let cascadeCount = 0;
        while (cascadeCount < maxBatch) {
            // Find the next STAGED or WAITING_FOR_GAP chapter with lowest sort key
            const { data: stagedList, error } = await this.supabase
                .from('importer_chapter_mappings')
                .select('chapter_id, chapter_sort_key, chapter_number')
                .eq('work_id', workId)
                .in('status', ['STAGED', 'WAITING_FOR_GAP'])
                .order('chapter_sort_key', { ascending: true })
                .limit(1);
            if (error || !stagedList || stagedList.length === 0) {
                break;
            }
            const candidate = stagedList[0];
            const check = await this.checkBarrier(workId, candidate.chapter_sort_key);
            if (!check.canPublish) {
                // Next chapter still has unfulfilled dependencies
                if (check.reason.includes('GAP') || check.reason.includes('WAITING') || check.blockingCount > 0) {
                    await this.supabase
                        .from('importer_chapter_mappings')
                        .update({ status: 'WAITING_FOR_GAP', updated_at: new Date().toISOString() })
                        .eq('work_id', workId)
                        .eq('chapter_id', candidate.chapter_id)
                        .eq('status', 'STAGED');
                }
                break;
            }
            cascadeCount++;
            const monotonicTimestamp = new Date(Date.now() + cascadeCount * 50).toISOString();
            await this.executePublish(workId, candidate.chapter_id, monotonicTimestamp, candidate.chapter_sort_key);
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
            const hasViableAlternative = (otherMappings || []).some((m) => m.status === 'PENDING' || m.status === 'QUEUED' || m.status === 'IMPORTING' || m.status === 'STAGED' || m.status === 'COMPLETED');
            if (hasViableAlternative) {
                this.logger.info(`Alternative source available/in progress for chapter ${chapterNumber}`, { workId, sortKey });
                // Mark only the failed source as failed, without declaring a gap for the whole chapter
                await this.supabase
                    .from('importer_chapter_mappings')
                    .update({
                    status: 'FAILED',
                    is_page_provider: false,
                    last_error: `Definite failure on ${failedSource}. Alternative source available.`,
                    updated_at: new Date().toISOString(),
                })
                    .eq('work_id', workId)
                    .eq('chapter_sort_key', sortKey)
                    .eq('source', failedSource);
                return;
            }
            // No alternative source available: mark as FAILED but DO NOT register as intentional gap.
            // The sequence must remain blocked until the gap is genuinely resolved or explicitly marked.
            this.logger.warn(`Definite failure on all sources for chapter ${chapterNumber}. Registering UNRESOLVED GAP (sequence remains blocked).`, {
                workId,
                sortKey,
                chapterNumber,
            });
            await this.supabase
                .from('importer_chapter_mappings')
                .update({
                status: 'FAILED',
                is_gap: false,
                last_error: `Definite failure on ${failedSource}. Unresolved gap, sequence blocked.`,
                updated_at: new Date().toISOString(),
            })
                .eq('work_id', workId)
                .eq('chapter_sort_key', sortKey)
                .eq('source', failedSource);
            // Do NOT trigger cascade because the work sequence is correctly blocked until explicit gap resolution.
        });
    }
    /**
     * Periodic or startup sweep: checks all works that have STAGED chapters
     * and publishes them in round-robin batches across distinct works to ensure fairness.
     */
    async sweepStagedPublications(maxTotalPublications = 40, perWorkBurst = 6) {
        try {
            let distinctWorkIds = [];
            try {
                const pool = getYugabytePool();
                // Priority 1: Works with STAGED/WAITING_FOR_GAP chapters that are immediately publishable (100% index-driven)
                const res = await pool.query(`
          WITH staged_works AS (
            SELECT 
              m.work_id,
              MIN(m.chapter_sort_key) as frontier_sort_key
            FROM importer_chapter_mappings m
            WHERE m.status IN ('STAGED', 'WAITING_FOR_GAP') AND m.work_id IS NOT NULL
            GROUP BY m.work_id
            ORDER BY MIN(m.chapter_sort_key) ASC, m.work_id ASC
            LIMIT 40
          ),
          works_with_published AS (
            SELECT 
              sw.work_id,
              sw.frontier_sort_key,
              (
                SELECT MAX(c.number) 
                FROM chapters c 
                WHERE c.work_id = sw.work_id 
                  AND c.published_at IS NOT NULL
              ) as max_published,
              EXISTS (
                SELECT 1 
                FROM importer_chapter_mappings pm
                WHERE pm.work_id = sw.work_id
                  AND pm.chapter_sort_key < sw.frontier_sort_key
                  AND pm.is_gap = false
                  AND pm.status NOT IN ('STAGED', 'WAITING_FOR_GAP')
              ) as has_predecessor_in_mapping,
              EXISTS (
                SELECT 1 
                FROM importer_queue pq
                WHERE (pq.payload->>'workId') = sw.work_id::text
                  AND pq.task_type = 'IMPORT_CHAPTER'
                  AND pq.status IN ('QUEUED', 'RETRY', 'IMPORTING')
                  AND pq.chapter_sort_key < sw.frontier_sort_key
              ) as has_predecessor_in_queue
            FROM staged_works sw
          )
          SELECT work_id
          FROM works_with_published
          WHERE (max_published IS NOT NULL AND frontier_sort_key <= max_published + 1.05 AND NOT has_predecessor_in_queue)
             OR (max_published IS NULL AND NOT has_predecessor_in_mapping AND NOT has_predecessor_in_queue)
          LIMIT 20;
        `);
                distinctWorkIds = res.rows.map((r) => r.work_id);
            }
            catch (poolErr) {
                const { data: stagedWorks, error } = await this.supabase
                    .from('importer_chapter_mappings')
                    .select('work_id')
                    .eq('status', 'STAGED')
                    .not('work_id', 'is', null)
                    .limit(200);
                if (error || !stagedWorks || stagedWorks.length === 0) {
                    return 0;
                }
                distinctWorkIds = Array.from(new Set(stagedWorks.map((r) => r.work_id)));
            }
            if (distinctWorkIds.length === 0) {
                return 0;
            }
            let publishedTotal = 0;
            let activeWorkIds = [...distinctWorkIds];
            // Round-robin iteration across distinct works (one round per sweep)
            for (const workId of activeWorkIds) {
                if (publishedTotal >= maxTotalPublications)
                    break;
                try {
                    const lock = this.getWorkLock(workId);
                    const count = await lock.runExclusive(async () => {
                        return this.runCascadeUnderLock(workId, perWorkBurst);
                    });
                    publishedTotal += count;
                }
                catch (workErr) {
                    this.logger.error('Error cascading work in sweep', { workId, error: workErr?.message });
                }
            }
            if (publishedTotal > 0) {
                this.logger.info(`Sweep published ${publishedTotal} staged chapter(s) fairly across ${distinctWorkIds.length} work(s)`);
            }
            return publishedTotal;
        }
        catch (err) {
            this.logger.error('Error during sweepStagedPublications', { error: err?.message });
            return 0;
        }
    }
}
