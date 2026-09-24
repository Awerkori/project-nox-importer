import type { SupabaseClient } from '@supabase/supabase-js';
import { getYugabytePool } from '../db/yugabyte-direct.js';
import { Logger } from './logger.js';
import { AsyncSemaphore } from './concurrency.js';
import { computeCanonicalChapterKey } from './deduplication.js';

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

export class PublicationBarrier {
  private logger = new Logger('PublicationBarrier');
  private workLocks = new Map<string, AsyncSemaphore>();
  public onPublished?: (isFreshRelease: boolean) => void;

  constructor(private supabase: SupabaseClient) {}

  private getWorkLock(workId: string): AsyncSemaphore {
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
  async checkBarrier(workId: string, targetSortKey: number): Promise<BarrierCheckResult> {
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
  async stageChapter(params: StageChapterParams): Promise<void> {
    const lock = this.getWorkLock(params.workId);
    await lock.runExclusive(async () => {
      const { error } = await this.supabase.from('importer_chapter_mappings').upsert(
        {
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
        },
        { onConflict: 'source,source_chapter_id' }
      );

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
  async tryPublish(
    workId: string,
    sortKey: number,
    chapterId: string,
    isFreshRelease: boolean = false
  ): Promise<{ published: boolean; reason?: string }> {
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

        if (check.reason.includes('GAP') || check.reason.includes('WAITING')) {
          await this.supabase
            .from('importer_chapter_mappings')
            .update({ status: 'WAITING_FOR_GAP', updated_at: new Date().toISOString() })
            .eq('chapter_id', chapterId)
            .eq('status', 'STAGED');
        }

        return { published: false, reason: check.reason };
      }

      // Barrier cleared! Publish this chapter
      await this.executePublish(workId, chapterId, new Date().toISOString(), sortKey, isFreshRelease);
      this.logger.info('Chapter published via barrier', { workId, sortKey, chapterId, isFreshRelease });

      // Run immediate cascade for subsequent STAGED chapters of this work
      await this.runCascadeUnderLock(workId);

      return { published: true };
    });
  }

  /**
   * Executes atomic DB publication for a single chapter.
   */
  private async executePublish(
    workId: string,
    chapterId: string,
    publishedAtIso: string,
    sortKey?: number | string,
    isFreshRelease: boolean = false
  ): Promise<void> {
    // 0. Query current work status and latest_chapter_published_at
    const { data: workInfo } = await this.supabase
      .from('works')
      .select('latest_chapter_published_at, slug')
      .eq('id', workId)
      .maybeSingle();

    const existingLatest = workInfo?.latest_chapter_published_at;

    // 1. Mark public.chapters.published_at (preserving existing published_at if already published)
    const { data: existingCh } = await this.supabase
      .from('chapters')
      .select('published_at')
      .eq('id', chapterId)
      .maybeSingle();

    const finalPublishedAt = existingCh?.published_at || publishedAtIso;

    const { error: chErr } = await this.supabase
      .from('chapters')
      .update({
        published_at: finalPublishedAt,
        is_fresh_release: isFreshRelease,
      })
      .eq('id', chapterId);

    if (chErr) throw chErr;

    // 2. Mark public.importer_chapter_mappings.status = 'COMPLETED'
    const { error: mapErr } = await this.supabase
      .from('importer_chapter_mappings')
      .update({ status: 'COMPLETED', updated_at: new Date().toISOString() })
      .eq('chapter_id', chapterId);

    if (mapErr) throw mapErr;

    // 2b. Cancel / auto-complete any remaining QUEUED/RETRY jobs in importer_queue for this chapter
    try {
      if (sortKey !== undefined && sortKey !== null) {
        const pool = getYugabytePool();
        await pool.query(`
          UPDATE importer_queue
          SET status = 'COMPLETED',
              updated_at = NOW(),
              last_error = 'CANONICAL_ALREADY_SATISFIED'
          WHERE task_type = 'IMPORT_CHAPTER'
            AND status IN ('QUEUED', 'RETRY')
            AND chapter_sort_key = $1
            AND (payload->>'workId') = $2;
        `, [sortKey, workId]);

        await pool.query(`
          UPDATE importer_chapter_mappings
          SET status = 'COMPLETED',
              is_page_provider = false,
              chapter_id = $1,
              updated_at = NOW()
          WHERE work_id = $2::uuid
            AND chapter_sort_key = $3
            AND status IN ('PENDING', 'QUEUED');
        `, [chapterId, workId, sortKey]);
      }
    } catch (cancelErr: any) {
      this.logger.warn('Failed to auto-cancel redundant queue jobs on publish', { error: cancelErr?.message });
    }

    // 3. Update public.works: enforce publication barrier (valid metadata + valid cover)
    let shouldPublishWork = false;
    try {
      const { data: currentWork } = await this.supabase
        .from('works')
        .select('id, title, slug, cover_id, published')
        .eq('id', workId)
        .maybeSingle();

      if (currentWork?.cover_id && currentWork.title && currentWork.slug) {
        const { data: coverMedia } = await this.supabase
          .from('media')
          .select('id, storage_ready, bytes')
          .eq('id', currentWork.cover_id)
          .maybeSingle();

        if (coverMedia && coverMedia.storage_ready && (coverMedia.bytes || 0) >= 1500) {
          shouldPublishWork = true;
        } else {
          this.logger.warn('Work cover is not storage_ready or too small, publication barrier withheld published=true', {
            workId,
            coverId: currentWork.cover_id,
            bytes: coverMedia?.bytes,
          });
        }
      } else {
        this.logger.warn('Work missing cover_id or canonical metadata, publication barrier withheld published=true', {
          workId,
          hasCover: Boolean(currentWork?.cover_id),
          hasTitle: Boolean(currentWork?.title),
          hasSlug: Boolean(currentWork?.slug),
        });
      }
    } catch (barrierErr: any) {
      this.logger.warn('Error checking publication barrier for work', { workId, error: barrierErr?.message });
    }

    const workUpdate: Record<string, any> = {
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

    try {
      this.onPublished?.(isFreshRelease);
    } catch {}

    // 4. Invalidate edge cache (ALWAYS invalidate Home & Lançamentos whenever ANY chapter is published)
    try {
      const siteUrl = process.env.MANGA_SITE_URL || 'https://manga.project-nox-awerkori.workers.dev';
      const token = process.env.NOX_STORAGE_BRIDGE_TOKEN;
      fetch(`${siteUrl}/api/internal/cache/invalidate`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          ...(token ? { Authorization: `Bearer ${token}` } : {}),
        },
        body: JSON.stringify({
          type: 'CHAPTER_PUBLISHED',
          workId,
          workSlug: workInfo?.slug,
          chapterId,
        }),
        signal: AbortSignal.timeout(2000),
      }).catch(() => {});
    } catch {}

    // 4. Update importer_chapter_manifest status to PUBLISHED if available
    try {
      const manQuery = this.supabase.from('importer_chapter_manifest');
      if (manQuery && typeof manQuery.update === 'function') {
        let sKey: number | string | undefined = sortKey;
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
    } catch {
      // Non-blocking telemetry
    }
  }

  /**
   * Cascading publication of all consecutive STAGED chapters for a work.
   * Bounded by maxBatch to guarantee multi-work fairness.
   */
  private async runCascadeUnderLock(workId: string, maxBatch: number = 8): Promise<number> {
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
  async handleDefiniteFailure(
    workId: string,
    chapterNumber: number,
    sortKey: number,
    failedSource: string
  ): Promise<void> {
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

      const hasViableAlternative = (otherMappings || []).some(
        (m) => m.status === 'PENDING' || m.status === 'QUEUED' || m.status === 'IMPORTING' || m.status === 'STAGED' || m.status === 'COMPLETED'
      );

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
  async sweepStagedPublications(maxTotalPublications: number = 40, perWorkBurst: number = 6): Promise<number> {
    try {
      let distinctWorkIds: string[] = [];
      try {
        const pool = getYugabytePool();
        const res = await pool.query<{ work_id: string }>(`
          SELECT DISTINCT work_id
          FROM importer_chapter_mappings
          WHERE status = 'STAGED' AND work_id IS NOT NULL
          LIMIT 40;
        `);
        distinctWorkIds = res.rows.map((r) => r.work_id);
      } catch (poolErr) {
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
        if (publishedTotal >= maxTotalPublications) break;

        try {
          const lock = this.getWorkLock(workId);
          const count = await lock.runExclusive(async () => {
            return this.runCascadeUnderLock(workId, perWorkBurst);
          });
          publishedTotal += count;
        } catch (workErr: any) {
          this.logger.error('Error cascading work in sweep', { workId, error: workErr?.message });
        }
      }

      if (publishedTotal > 0) {
        this.logger.info(`Sweep published ${publishedTotal} staged chapter(s) fairly across ${distinctWorkIds.length} work(s)`);
      }

      return publishedTotal;
    } catch (err: any) {
      this.logger.error('Error during sweepStagedPublications', { error: err?.message });
      return 0;
    }
  }
}
