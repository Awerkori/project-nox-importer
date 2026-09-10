import type { SupabaseClient } from '@supabase/supabase-js';
import { Logger } from './logger.js';
import { ImporterQueue } from './queue.js';
import { SourceRegistry } from '../sources/registry.js';
import { computeCanonicalChapterKey } from './deduplication.js';
import { matchWorkCandidate, MatchCandidate } from './matching.js';
import { SourceChapterSummary } from '../sources/types.js';

export interface ReconciliationStats {
  worksScanned: number;
  worksWithConfirmedGaps: number;
  confirmedGapsDiscovered: number;
  newChaptersDiscovered: number;
  jobsEnqueued: number;
  stagedSkipped: number;
  duplicatesAvoided: number;
}

export interface WorkReconciliationResult {
  workId: string;
  title: string;
  totalKnownChapters: number;
  totalImportedChapters: number;
  missingStart: boolean;
  firstChapterNumber: number | null;
  latestChapterNumber: number | null;
  gaps: Array<{ from: number; to: number; type: 'MISSING_START' | 'INTERNAL_GAP' }>;
  unresolvedGaps: number[];
  providersSummary: Array<{
    provider: string;
    chaptersAvailable: number;
    active: boolean;
    confidenceScore?: number;
    matchMethod?: string;
  }>;
  enqueuedCount: number;
  healthStatus: 'HEALTHY' | 'INCOMPLETE' | 'RECONCILING' | 'UNVERIFIED' | 'BLOCKED';
}

const SOURCE_PRIORITY_ORDER: Record<string, number> = {
  kuro: 100,
  nexus: 80,
  manhastro: 60,
  mangaflix: 40,
  mangotoons: 20,
};

export class ExistingWorksReconciler {
  private logger = new Logger('ExistingWorksReconciler');
  private lastReconciliationAt = new Map<string, number>();

  constructor(
    private supabase: SupabaseClient,
    private queue: ImporterQueue,
    private registry: SourceRegistry
  ) {}

  private isSourceOperationallyAvailable(
    sourceId: string,
    sourcesState: Map<string, { status: string; enabled: boolean; cooldownUntil: number | null }>
  ): boolean {
    const state = sourcesState.get(sourceId);
    if (!state) {
      return sourcesState.size === 0;
    }
    if (!state.enabled) return false;
    if (state.status !== 'ACTIVE') return false;
    if (state.cooldownUntil && state.cooldownUntil > Date.now()) return false;
    return true;
  }

  /**
   * Discovers alternative provider mappings for a given work by searching across
   * all registered source adapters and matching candidate titles.
   */
  async discoverCrossProviderMappings(
    workOrId:
      | {
          id: string;
          title: string;
          slug: string;
          aliases?: string[];
          kind?: string;
        }
      | string,
    sourcesState?: Map<string, { status: string; enabled: boolean; cooldownUntil: number | null }>
  ): Promise<number> {
    let work: { id: string; title: string; slug: string; aliases?: string[]; kind?: string };
    if (typeof workOrId === 'string') {
      const { data } = await this.supabase
        .from('works')
        .select('id, title, slug, aliases, kind')
        .eq('id', workOrId)
        .maybeSingle();
      if (!data) return 0;
      work = data;
    } else {
      work = workOrId;
    }

    let newMappingsCount = 0;

    const mapQuery = this.supabase.from('importer_work_mappings');
    // 0. Skip discovery if work is frozen by staff
    const { data: frozenCheck } = await mapQuery
      .select('id, sync_status')
      .eq('work_id', work.id)
      .eq('sync_status', 'FROZEN_BY_STAFF');

    if (frozenCheck?.some((m: any) => m.sync_status === 'FROZEN_BY_STAFF')) {
      return 0;
    }

    // 1. Get already mapped sources for this work
    const { data: existingMappings } = await mapQuery
      .select('source, source_work_id')
      .eq('work_id', work.id);

    const mappedSources = new Set((existingMappings || []).map((m: any) => m.source));
    const slugWords = work.slug ? work.slug.replace(/[-_]+/g, ' ').trim() : '';
    const candidateAliases = [...(work.aliases || [])];
    if (slugWords && !candidateAliases.some((a) => a.toLowerCase() === slugWords.toLowerCase())) {
      candidateAliases.push(slugWords);
    }

    const targetCandidate: MatchCandidate = {
      title: work.title,
      slug: work.slug,
      aliases: candidateAliases,
      kind: work.kind,
    };

    // 2. Query unmapped registered adapters
    for (const adapter of this.registry.getAll()) {
      if (mappedSources.has(adapter.id)) {
        continue;
      }
      if (sourcesState && !this.isSourceOperationallyAvailable(adapter.id, sourcesState)) {
        continue;
      }

      try {
        if (typeof adapter.searchWorks !== 'function') continue;

        // Search by primary title
        let candidates = await adapter.searchWorks(work.title);

        // If no results and work has aliases, search by top aliases
        if (candidates.length === 0 && candidateAliases.length > 0) {
          for (const alias of candidateAliases.slice(0, 3)) {
            const aliasCandidates = await adapter.searchWorks(alias);
            if (aliasCandidates.length > 0) {
              candidates = aliasCandidates;
              break;
            }
          }
        }

        // If still no results and work has a slug, search by slug words
        if (candidates.length === 0 && slugWords && slugWords.length >= 3) {
          const slugCandidates = await adapter.searchWorks(slugWords);
          if (slugCandidates.length > 0) {
            candidates = slugCandidates;
          }
        }

        // Evaluate candidate matches
        for (const cand of candidates) {
          const candMatchInput: MatchCandidate = {
            title: cand.title,
            slug: cand.slug,
          };

          const matchResult = matchWorkCandidate(targetCandidate, candMatchInput);

          if (matchResult.matched && matchResult.confidenceScore >= 0.85) {
            this.logger.info(`Discovered cross-provider mapping for "${work.title}" on ${adapter.id}`, {
              workId: work.id,
              source: adapter.id,
              sourceWorkId: cand.sourceWorkId,
              matchedTitle: cand.title,
              confidence: matchResult.confidenceScore,
              method: matchResult.matchMethod,
            });

            const upsertQuery = this.supabase.from('importer_work_mappings');
            if (upsertQuery && typeof upsertQuery.upsert === 'function') {
              const { error: upsertErr } = await upsertQuery.upsert(
                {
                  source: adapter.id,
                  source_work_id: cand.sourceWorkId,
                  work_id: work.id,
                  source_title: cand.title,
                  source_slug: cand.slug,
                  confidence_score: matchResult.confidenceScore,
                  match_method: matchResult.matchMethod,
                  is_primary: false,
                  sync_status: 'SYNCED',
                  last_synced_at: new Date().toISOString(),
                },
                { onConflict: 'source,source_work_id' }
              );

              if (upsertErr) {
                this.logger.warn(`Failed to upsert mapping for ${adapter.id}: ${upsertErr.message}`, {
                  workId: work.id,
                });
              } else {
                newMappingsCount++;
                mappedSources.add(adapter.id);
              }
            }
            break; // Stop after highest confidence match for this adapter
          }
        }
      } catch (err: any) {
        this.logger.warn(`Cross-provider search failed on ${adapter.id} for "${work.title}"`, {
          error: err?.message,
        });
      }
    }

    return newMappingsCount;
  }

  /**
   * Reconciles a single work end-to-end:
   * 1. Discovers cross-provider mappings across all sources.
   * 2. Fetches chapters from all mapped sources concurrently.
   * 3. Merges into a canonical chapter manifest.
   * 4. Detects missing beginning and internal gaps.
   * 5. Enqueues missing chapters using the best available provider with fallback sources.
   * 6. Marks true missing chapters without any provider as UNRESOLVED_GAP.
   * 7. Updates `importer_work_health` and `importer_chapter_manifest`.
   */
  async reconcileWorkManifest(
    workId: string,
    options?: { forceEnqueue?: boolean; priority?: number }
  ): Promise<WorkReconciliationResult> {
    // 0. Pre-flight check: If work is FROZEN_BY_STAFF, skip reconciliation completely
    const { data: frozenMaps } = await this.supabase
      .from('importer_work_mappings')
      .select('id, sync_status, freeze_reason')
      .eq('work_id', workId)
      .eq('sync_status', 'FROZEN_BY_STAFF');

    if (frozenMaps?.some((m: any) => m.sync_status === 'FROZEN_BY_STAFF')) {
      this.logger.info(`Work ${workId} is FROZEN_BY_STAFF. Skipping reconciliation.`);
      return {
        workId,
        title: '',
        totalKnownChapters: 0,
        totalImportedChapters: 0,
        missingStart: false,
        firstChapterNumber: null,
        latestChapterNumber: null,
        gaps: [],
        unresolvedGaps: [],
        providersSummary: [],
        enqueuedCount: 0,
        healthStatus: 'BLOCKED',
      };
    }

    // 1. Fetch work details (with fallback if works table query is simple or mapped)
    let workTitle = workId;
    let workSlug = '';
    let workKind = undefined;
    let workAliases: string[] = [];

    const worksQuery = this.supabase.from('works');
    if (worksQuery && typeof worksQuery.select === 'function') {
      const { data: w } = await worksQuery
        .select('id, title, slug, kind, aliases')
        .eq('id', workId)
        .maybeSingle();

      if (w) {
        workTitle = w.title || workId;
        workSlug = w.slug || '';
        workKind = w.kind;
        workAliases = w.aliases || [];
      }
    }

    // 2. Fetch sources health state
    const sourcesQuery = this.supabase.from('importer_sources');
    const { data: dbSources } = sourcesQuery && typeof sourcesQuery.select === 'function'
      ? await sourcesQuery.select('id, status, enabled, cooldown_until')
      : { data: [] };

    const sourcesState = new Map<string, { status: string; enabled: boolean; cooldownUntil: number | null }>();
    for (const s of dbSources || []) {
      sourcesState.set(s.id, {
        status: s.status,
        enabled: s.enabled !== false,
        cooldownUntil: s.cooldown_until ? new Date(s.cooldown_until).getTime() : null,
      });
    }

    // 3. Discover new cross-provider mappings dynamically (skipping unoperational providers)
    await this.discoverCrossProviderMappings(
      {
        id: workId,
        title: workTitle,
        slug: workSlug,
        aliases: workAliases,
        kind: workKind,
      },
      sourcesState
    );

    // 4. Fetch all active work mappings
    const mapQuery = this.supabase.from('importer_work_mappings');
    const { data: rawMappings } = mapQuery && typeof mapQuery.select === 'function'
      ? await mapQuery
          .select('id, source, source_work_id, confidence_score, match_method, is_primary')
          .eq('work_id', workId)
          .eq('sync_status', 'SYNCED')
      : { data: [] };

    const mappings = rawMappings || [];

    // 5. Concurrently fetch chapter lists from operational mapped sources
    const sourceChaptersMap = new Map<string, SourceChapterSummary[]>();
    await Promise.all(
      mappings.map(async (m: any) => {
        if (!this.isSourceOperationallyAvailable(m.source, sourcesState)) {
          this.logger.debug(`Skipping chapter fetch from ${m.source}: source is not operationally available`);
          return;
        }
        const adapter = this.registry.get(m.source);
        if (!adapter) return;
        try {
          const chapters = await adapter.fetchChapters(m.source_work_id);
          sourceChaptersMap.set(m.source, chapters);
        } catch (err: any) {
          this.logger.warn(`Failed to fetch chapters for ${workTitle} from ${m.source}`, {
            error: err?.message,
          });
        }
      })
    );

    // 6. Fetch existing DB chapters
    const chQuery = this.supabase.from('chapters');
    let publishedChapters: any[] = [];
    if (chQuery && typeof chQuery.select === 'function') {
      let q = chQuery.select('id, number, published_at').eq('work_id', workId);
      if (typeof q.not === 'function') {
        const { data } = await q.not('published_at', 'is', null);
        publishedChapters = data || [];
      } else {
        const { data } = await q;
        publishedChapters = data || [];
      }
    }

    const publishedNumbers = new Set(
      publishedChapters.map((c) => Number(Number(c.number).toFixed(4)))
    );
    const publishedSortKeys = new Set(
      publishedChapters.map((c) => computeCanonicalChapterKey(c.number).sortKey)
    );
    const maxPublishedSort = publishedChapters.reduce(
      (max, c) => Math.max(max, Number(c.number)),
      0
    );

    // 7. Fetch existing chapter mappings (STAGED & COMPLETED)
    const cmQuery = this.supabase.from('importer_chapter_mappings');
    const { data: chapterMappings } = cmQuery && typeof cmQuery.select === 'function'
      ? await cmQuery
          .select('chapter_sort_key, chapter_number, status, source')
          .eq('work_id', workId)
      : { data: [] };

    const stagedSortKeys = new Set<number>();
    const completedSortKeys = new Set<number>();

    for (const cm of chapterMappings || []) {
      const sKey = Number(cm.chapter_sort_key);
      if (cm.status === 'STAGED') stagedSortKeys.add(sKey);
      else if (cm.status === 'COMPLETED') completedSortKeys.add(sKey);
    }

    // 8. Fetch active queue items
    const qQuery = this.supabase.from('importer_queue');
    let activeQueue: any[] = [];
    if (qQuery && typeof qQuery.select === 'function') {
      let q = qQuery
        .select('chapter_sort_key, status, payload')
        .eq('task_type', 'IMPORT_CHAPTER')
        .eq('payload->>workId', workId);
      if (typeof q.in === 'function') {
        const { data } = await q.in('status', ['QUEUED', 'IMPORTING', 'RETRY']);
        activeQueue = data || [];
      }
    }

    const queuedSortKeys = new Set<number>(
      activeQueue.map((q) => Number(q.chapter_sort_key))
    );

    // 9. Build Canonical Chapter Manifest
    interface ManifestCandidate {
      sortKey: number;
      chapterNumber: number;
      chapterTitle?: string;
      pageCount: number;
      sources: Array<{
        source: string;
        sourceWorkId: string;
        sourceChapterId: string;
        chapterNumber: number;
        pageCount: number;
        priorityScore: number;
        mappingId: string;
      }>;
    }

    const manifestMap = new Map<number, ManifestCandidate>();

    for (const m of mappings) {
      const chapters = sourceChaptersMap.get(m.source) || [];
      const priorityScore = SOURCE_PRIORITY_ORDER[m.source] || 10;

      for (const ch of chapters) {
        const canonical = computeCanonicalChapterKey(ch.number, ch.title);
        const sortKey = canonical.sortKey;

        let entry = manifestMap.get(sortKey);
        if (!entry) {
          entry = {
            sortKey,
            chapterNumber: canonical.normalizedNumber,
            chapterTitle: ch.title,
            pageCount: ch.pageCount || 0,
            sources: [],
          };
          manifestMap.set(sortKey, entry);
        }

        entry.sources.push({
          source: m.source,
          sourceWorkId: m.source_work_id,
          sourceChapterId: ch.sourceChapterId,
          chapterNumber: ch.number,
          pageCount: ch.pageCount || 0,
          priorityScore,
          mappingId: m.id,
        });

        if (ch.pageCount && ch.pageCount > entry.pageCount) {
          entry.pageCount = ch.pageCount;
        }
      }
    }

    // Include published chapters in manifest
    for (const pub of publishedChapters) {
      const canonical = computeCanonicalChapterKey(pub.number);
      const sortKey = canonical.sortKey;
      if (!manifestMap.has(sortKey)) {
        manifestMap.set(sortKey, {
          sortKey,
          chapterNumber: canonical.normalizedNumber,
          chapterTitle: `Capítulo ${pub.number}`,
          pageCount: 0,
          sources: [],
        });
      }
    }

    // 10. Check Staff Priority
    let isStaffPriority = options?.priority === 100;
    if (!isStaffPriority) {
      try {
        const staffQuery = this.supabase.from('importer_staff_requests');
        if (staffQuery && typeof staffQuery.select === 'function') {
          const { data: staffReq } = await staffQuery
            .select('id')
            .eq('work_id', workId)
            .in('status', ['QUEUED', 'IMPORTING', 'RETRYING'])
            .maybeSingle();

          if (staffReq) isStaffPriority = true;
        }
      } catch {
        // Safe fallback
      }
    }

    // 11. Highest milestone for separating gaps vs new releases
    const highestMilestone = Math.max(
      maxPublishedSort,
      ...Array.from(stagedSortKeys.values()),
      ...Array.from(completedSortKeys.values())
    );

    const sortedSortKeys = Array.from(manifestMap.keys()).sort((a, b) => a - b);
    const firstChapterNumber = sortedSortKeys.length > 0 ? sortedSortKeys[0] : null;
    const latestChapterNumber = sortedSortKeys.length > 0 ? sortedSortKeys[sortedSortKeys.length - 1] : null;

    const missingStart = firstChapterNumber !== null && firstChapterNumber > 1;

    const gaps: Array<{ from: number; to: number; type: 'MISSING_START' | 'INTERNAL_GAP' }> = [];
    const unresolvedGaps: number[] = [];

    if (missingStart && firstChapterNumber !== null) {
      gaps.push({ from: 1, to: firstChapterNumber - 1, type: 'MISSING_START' });
    }

    for (let i = 0; i < sortedSortKeys.length - 1; i++) {
      const current = sortedSortKeys[i];
      const next = sortedSortKeys[i + 1];
      if (Math.floor(next) - Math.floor(current) > 1) {
        gaps.push({
          from: Math.floor(current) + 1,
          to: Math.floor(next) - 1,
          type: 'INTERNAL_GAP',
        });
      }
    }

    // Load existing chapter mappings to detect known permanent gaps
    const { data: existingChapterMappings } = await this.supabase
      .from('importer_chapter_mappings')
      .select('chapter_sort_key, status, is_gap, last_error')
      .eq('work_id', workId);

    const permanentGapSortKeys = new Set<number>();
    if (existingChapterMappings) {
      for (const ecm of existingChapterMappings) {
        if (
          ecm.status === 'FAILED' &&
          ecm.is_gap &&
          (ecm.last_error?.includes('404') || ecm.last_error?.includes('sem fallback'))
        ) {
          permanentGapSortKeys.add(Number(ecm.chapter_sort_key));
        }
      }
    }

    // 12. Enqueue missing chapters
    let enqueuedCount = 0;
    const manifestUpserts: any[] = [];

    for (const sortKey of sortedSortKeys) {
      const candidate = manifestMap.get(sortKey)!;
      const isPublished = publishedNumbers.has(candidate.chapterNumber) || publishedSortKeys.has(sortKey);
      const isStaged = stagedSortKeys.has(sortKey);
      const isQueued = queuedSortKeys.has(sortKey);

      const operationalSources = candidate.sources.filter((s) =>
        this.isSourceOperationallyAvailable(s.source, sourcesState)
      );

      operationalSources.sort((a, b) => b.priorityScore - a.priorityScore);
      const primary = operationalSources[0] || null;

      let status: 'PUBLISHED' | 'STAGED' | 'QUEUED' | 'UNRESOLVED_GAP' | 'SKIPPED' = 'QUEUED';

      const isKnownPermanentGap = permanentGapSortKeys.has(sortKey);

      if (isPublished) {
        status = 'PUBLISHED';
      } else if (isStaged) {
        status = 'STAGED';
      } else if (isQueued) {
        status = 'QUEUED';
      } else if (!primary) {
        status = 'UNRESOLVED_GAP';
        unresolvedGaps.push(candidate.chapterNumber);
      } else if (isKnownPermanentGap && operationalSources.length <= 1) {
        // Known permanent 404 gap and no alternative fallback source appeared
        status = 'UNRESOLVED_GAP';
        unresolvedGaps.push(candidate.chapterNumber);
      } else {
        status = 'QUEUED';

        const fallbacks = operationalSources
          .filter((s) => s.source !== primary.source)
          .map((s) => ({
            source: s.source,
            sourceChapterId: s.sourceChapterId,
            sourceWorkId: s.sourceWorkId,
            mappingId: s.mappingId,
          }));

        const isGap = sortKey < highestMilestone;
        const assignedPriority = options?.priority ?? (isStaffPriority ? 100 : isGap ? 70 : 80);
        const canonicalDedupeKey = `work:${workId}:chapter:${sortKey}`;

        // Upsert mappings for chapter sources
        for (const s of candidate.sources) {
          const chmUpsert = this.supabase.from('importer_chapter_mappings');
          if (chmUpsert && typeof chmUpsert.upsert === 'function') {
            await chmUpsert.upsert(
              {
                source: s.source,
                source_chapter_id: s.sourceChapterId,
                work_id: workId,
                work_mapping_id: s.mappingId,
                chapter_number: candidate.chapterNumber,
                chapter_sort_key: candidate.sortKey,
                page_count: candidate.pageCount,
                is_page_provider: s.source === primary.source,
                status: 'PENDING',
                is_gap: isGap,
                last_error: null,
              },
              { onConflict: 'source,source_chapter_id' }
            );
          }
        }

        const enqueued = await this.queue.enqueue(
          'IMPORT_CHAPTER',
          primary.source,
          canonicalDedupeKey,
          {
            sourceWorkId: primary.sourceWorkId,
            sourceChapterId: primary.sourceChapterId,
            workId,
            workMappingId: primary.mappingId,
            chapterNumber: candidate.chapterNumber,
            chapterTitle: candidate.chapterTitle,
            expectedPageCount: candidate.pageCount || null,
            isGapBackfill: isGap,
            fallbackSources: fallbacks,
          },
          assignedPriority,
          candidate.sortKey
        );

        if (enqueued) {
          enqueuedCount++;
          queuedSortKeys.add(sortKey);
        }
      }

      manifestUpserts.push({
        work_id: workId,
        chapter_number: candidate.chapterNumber,
        chapter_sort_key: candidate.sortKey,
        status,
        selected_source: primary?.source || null,
        available_sources: candidate.sources.map((s) => ({
          source: s.source,
          source_chapter_id: s.sourceChapterId,
          chapter_number: s.chapterNumber,
          page_count: s.pageCount,
        })),
        page_count: candidate.pageCount,
        is_gap: status === 'UNRESOLVED_GAP',
        ...(isKnownPermanentGap ? { gap_reason: 'PERMANENT_404_UNRESOLVED' } : {}),
        last_checked_at: new Date().toISOString(),
      });
    }

    // 13. Persist manifest if table available
    const manQuery = this.supabase.from('importer_chapter_manifest');
    if (manQuery && typeof manQuery.upsert === 'function') {
      for (let i = 0; i < manifestUpserts.length; i += 50) {
        const chunk = manifestUpserts.slice(i, i + 50);
        await manQuery.upsert(chunk, { onConflict: 'work_id,chapter_sort_key' });
      }
    }

    // 14. Compute Health Status
    const totalKnown = sortedSortKeys.length;
    const totalImported = publishedSortKeys.size + stagedSortKeys.size;

    let healthStatus: 'HEALTHY' | 'INCOMPLETE' | 'RECONCILING' | 'UNVERIFIED' | 'BLOCKED' = 'INCOMPLETE';

    if (queuedSortKeys.size > 0 || enqueuedCount > 0) {
      healthStatus = 'RECONCILING';
    } else if (totalImported >= totalKnown && unresolvedGaps.length === 0 && !missingStart) {
      healthStatus = 'HEALTHY';
    } else if (unresolvedGaps.length > 0 && totalImported + unresolvedGaps.length >= totalKnown) {
      healthStatus = 'INCOMPLETE';
    } else {
      healthStatus = 'INCOMPLETE';
    }

    // 15. Summary of providers
    const providersSummary = mappings.map((m: any) => {
      const chs = sourceChaptersMap.get(m.source) || [];
      const isAvailable = this.isSourceOperationallyAvailable(m.source, sourcesState);
      return {
        provider: m.source,
        chaptersAvailable: chs.length,
        active: isAvailable,
        confidenceScore: m.confidence_score ?? 1.0,
        matchMethod: m.match_method ?? 'MANUAL',
      };
    });

    // 16. Upsert health if table available
    const healthQuery = this.supabase.from('importer_work_health');
    if (healthQuery && typeof healthQuery.upsert === 'function') {
      await healthQuery.upsert(
        {
          work_id: workId,
          health_status: healthStatus,
          total_known_chapters: totalKnown,
          total_imported_chapters: totalImported,
          missing_start: missingStart,
          first_chapter_number: firstChapterNumber,
          latest_chapter_number: latestChapterNumber,
          gaps: gaps,
          unresolved_gaps: unresolvedGaps,
          providers_summary: providersSummary,
          last_reconciled_at: new Date().toISOString(),
        },
        { onConflict: 'work_id' }
      );
    }

    this.logger.info(`Reconciled work manifest for "${workTitle}"`, {
      workId,
      healthStatus,
      totalKnown,
      totalImported,
      missingStart,
      enqueuedCount,
      unresolvedGapsCount: unresolvedGaps.length,
    });

    return {
      workId,
      title: workTitle,
      totalKnownChapters: totalKnown,
      totalImportedChapters: totalImported,
      missingStart,
      firstChapterNumber,
      latestChapterNumber,
      gaps,
      unresolvedGaps,
      providersSummary,
      enqueuedCount,
      healthStatus,
    };
  }

  /**
   * Reconciles existing works in batches, respecting rate limits and avoiding redundant work.
   */
  async reconcileExistingWorks(batchSize: number = 20): Promise<ReconciliationStats> {
    const stats: ReconciliationStats = {
      worksScanned: 0,
      worksWithConfirmedGaps: 0,
      confirmedGapsDiscovered: 0,
      newChaptersDiscovered: 0,
      jobsEnqueued: 0,
      stagedSkipped: 0,
      duplicatesAvoided: 0,
    };

    // 0. Query sources status
    const sourcesQuery = this.supabase.from('importer_sources');
    const { data: dbSources } = sourcesQuery && typeof sourcesQuery.select === 'function'
      ? await sourcesQuery.select('id, status, enabled, cooldown_until')
      : { data: [] };

    const sourcesState = new Map<string, { status: string; enabled: boolean; cooldownUntil: number | null }>();
    for (const s of dbSources || []) {
      sourcesState.set(s.id, {
        status: s.status,
        enabled: s.enabled !== false,
        cooldownUntil: s.cooldown_until ? new Date(s.cooldown_until).getTime() : null,
      });
    }

    // 1. Fetch work mappings
    const mapQuery = this.supabase.from('importer_work_mappings');
    if (!mapQuery || typeof mapQuery.select !== 'function') return stats;

    let query: any = mapQuery
      .select('id, work_id, source, source_work_id, updated_at, works!inner(id, title, slug, published, updated_at)')
      .eq('sync_status', 'SYNCED');

    if (typeof query.not === 'function') {
      query = query.not('work_id', 'is', null);
    }

    const { data: rawMappings, error: mapErr } = await query
      .order('updated_at', { ascending: false })
      .limit(batchSize * 3);

    if (mapErr) {
      this.logger.error('Failed to query work mappings for reconciliation', { error: mapErr.message });
      return stats;
    }

    const mappings = (rawMappings || []).filter((m: any) => m.work_id != null);
    if (!mappings || mappings.length === 0) return stats;

    // 2. Group by work_id
    const worksMap = new Map<string, Array<typeof mappings[0]>>();
    for (const m of mappings) {
      const list = worksMap.get(m.work_id) || [];
      list.push(m);
      worksMap.set(m.work_id, list);
      if (worksMap.size >= batchSize) break;
    }

    const now = Date.now();

    for (const [workId, sourceMappings] of worksMap.entries()) {
      const lastCheck = this.lastReconciliationAt.get(workId) || 0;
      if (now - lastCheck < 10 * 60 * 1000) continue;
      this.lastReconciliationAt.set(workId, now);
      stats.worksScanned++;

      const workTitle = (sourceMappings[0] as any).works?.title || workId;

      // 3. Published chapters
      const chQuery = this.supabase.from('chapters');
      let publishedChapters: any[] = [];
      if (chQuery && typeof chQuery.select === 'function') {
        let q = chQuery.select('id, number, published_at').eq('work_id', workId);
        if (typeof q.not === 'function') {
          const { data } = await q.not('published_at', 'is', null);
          publishedChapters = data || [];
        } else {
          const { data } = await q;
          publishedChapters = data || [];
        }
      }

      const publishedNumbers = new Set(
        publishedChapters.map((c) => Number(Number(c.number).toFixed(4)))
      );
      const maxPublishedSort = publishedChapters.reduce(
        (max, c) => Math.max(max, Number(c.number)),
        0
      );

      // 4. Staged & Completed
      const cmQuery = this.supabase.from('importer_chapter_mappings');
      const { data: chapterMappings } = cmQuery && typeof cmQuery.select === 'function'
        ? await cmQuery.select('chapter_sort_key, chapter_number, status').eq('work_id', workId)
        : { data: [] };

      const stagedSortKeys = new Set<number>();
      const completedSortKeys = new Set<number>();

      for (const cm of chapterMappings || []) {
        const sKey = Number(cm.chapter_sort_key);
        if (cm.status === 'STAGED') stagedSortKeys.add(sKey);
        else if (cm.status === 'COMPLETED') completedSortKeys.add(sKey);
      }

      // 5. Active queue
      const qQuery = this.supabase.from('importer_queue');
      let activeQueue: any[] = [];
      if (qQuery && typeof qQuery.select === 'function') {
        let q = qQuery
          .select('chapter_sort_key, status, payload')
          .eq('task_type', 'IMPORT_CHAPTER')
          .eq('payload->>workId', workId);
        if (typeof q.in === 'function') {
          const { data } = await q.in('status', ['QUEUED', 'IMPORTING', 'RETRY']);
          activeQueue = data || [];
        }
      }

      const queuedSortKeys = new Set<number>(
        activeQueue.map((q) => Number(q.chapter_sort_key))
      );

      // 6. Candidates by sort key
      interface CandidateInfo {
        sortKey: number;
        chapterNumber: number;
        chapterTitle: string;
        expectedPages: number;
        sources: Array<{
          source: string;
          sourceWorkId: string;
          sourceChapterId: string;
          mappingId: string;
          priorityScore: number;
        }>;
      }
      const candidatesBySortKey = new Map<number, CandidateInfo>();

      for (const sm of sourceMappings) {
        const adapter = this.registry.get(sm.source);
        if (!adapter) continue;

        try {
          const sourceChapters = await adapter.fetchChapters(sm.source_work_id);

          for (const ch of sourceChapters) {
            const canonicalKey = computeCanonicalChapterKey(ch.number, ch.title);
            const sortKey = canonicalKey.sortKey;
            const normNum = canonicalKey.normalizedNumber;

            if (publishedNumbers.has(normNum) || completedSortKeys.has(sortKey)) {
              continue;
            }

            if (stagedSortKeys.has(sortKey)) {
              stats.stagedSkipped++;
              continue;
            }

            if (queuedSortKeys.has(sortKey)) {
              continue;
            }

            const priorityScore = SOURCE_PRIORITY_ORDER[sm.source] || 10;
            const sourceEntry = {
              source: sm.source,
              sourceWorkId: sm.source_work_id,
              sourceChapterId: ch.sourceChapterId,
              mappingId: sm.id,
              priorityScore,
            };

            let existingCandidate = candidatesBySortKey.get(sortKey);
            if (!existingCandidate) {
              existingCandidate = {
                sortKey,
                chapterNumber: ch.number,
                chapterTitle: ch.title || '',
                expectedPages: ch.pageCount || 0,
                sources: [],
              };
              candidatesBySortKey.set(sortKey, existingCandidate);
            } else {
              stats.duplicatesAvoided++;
            }
            existingCandidate.sources.push(sourceEntry);
          }
        } catch (err: any) {
          this.logger.warn(`Failed to fetch chapters metadata for work ${workTitle} from ${sm.source}`, {
            error: err?.message,
          });
        }
      }

      if (candidatesBySortKey.size === 0) continue;

      const highestMilestone = Math.max(
        maxPublishedSort,
        ...Array.from(stagedSortKeys.values()),
        ...Array.from(completedSortKeys.values())
      );

      const confirmedGaps: CandidateInfo[] = [];
      const newChapters: CandidateInfo[] = [];

      for (const cand of candidatesBySortKey.values()) {
        if (cand.sortKey < highestMilestone) {
          confirmedGaps.push(cand);
        } else {
          newChapters.push(cand);
        }
      }

      if (confirmedGaps.length > 0) stats.worksWithConfirmedGaps++;

      confirmedGaps.sort((a, b) => a.sortKey - b.sortKey);
      newChapters.sort((a, b) => a.sortKey - b.sortKey);

      // Enqueue gaps (Priority 70)
      for (const cand of confirmedGaps) {
        const operationalSources = cand.sources.filter((s) =>
          this.isSourceOperationallyAvailable(s.source, sourcesState)
        );

        if (operationalSources.length === 0) {
          for (const s of cand.sources) {
            const chmUpsert = this.supabase.from('importer_chapter_mappings');
            if (chmUpsert && typeof chmUpsert.upsert === 'function') {
              await chmUpsert.upsert(
                {
                  source: s.source,
                  source_chapter_id: s.sourceChapterId,
                  work_id: workId,
                  work_mapping_id: s.mappingId,
                  chapter_number: cand.chapterNumber,
                  chapter_sort_key: cand.sortKey,
                  page_count: cand.expectedPages,
                  is_page_provider: false,
                  status: 'PENDING',
                  is_gap: true,
                  last_error: null,
                },
                { onConflict: 'source,source_chapter_id' }
              );
            }
          }
          continue;
        }

        operationalSources.sort((a, b) => b.priorityScore - a.priorityScore);
        const primary = operationalSources[0];

        const operationalFallbackSources = operationalSources
          .filter((s) => s.source !== primary.source)
          .map((s) => ({
            source: s.source,
            sourceChapterId: s.sourceChapterId,
            sourceWorkId: s.sourceWorkId,
            mappingId: s.mappingId,
          }));

        const canonicalDedupeKey = `work:${workId}:chapter:${cand.sortKey}`;

        for (const s of cand.sources) {
          const chmUpsert = this.supabase.from('importer_chapter_mappings');
          if (chmUpsert && typeof chmUpsert.upsert === 'function') {
            await chmUpsert.upsert(
              {
                source: s.source,
                source_chapter_id: s.sourceChapterId,
                work_id: workId,
                work_mapping_id: s.mappingId,
                chapter_number: cand.chapterNumber,
                chapter_sort_key: cand.sortKey,
                page_count: cand.expectedPages,
                is_page_provider: s.source === primary.source,
                status: 'PENDING',
                is_gap: true,
                last_error: null,
              },
              { onConflict: 'source,source_chapter_id' }
            );
          }
        }

        const enqueued = await this.queue.enqueue(
          'IMPORT_CHAPTER',
          primary.source,
          canonicalDedupeKey,
          {
            sourceWorkId: primary.sourceWorkId,
            sourceChapterId: primary.sourceChapterId,
            workId,
            workMappingId: primary.mappingId,
            chapterNumber: cand.chapterNumber,
            chapterTitle: cand.chapterTitle,
            expectedPageCount: cand.expectedPages || null,
            isGapBackfill: true,
            fallbackSources: operationalFallbackSources,
          },
          70,
          cand.sortKey
        );

        if (enqueued) {
          stats.confirmedGapsDiscovered++;
          stats.jobsEnqueued++;
        }
      }

      // Enqueue new chapters (Priority 80)
      for (const cand of newChapters) {
        const operationalSources = cand.sources.filter((s) =>
          this.isSourceOperationallyAvailable(s.source, sourcesState)
        );

        if (operationalSources.length === 0) {
          for (const s of cand.sources) {
            const chmUpsert = this.supabase.from('importer_chapter_mappings');
            if (chmUpsert && typeof chmUpsert.upsert === 'function') {
              await chmUpsert.upsert(
                {
                  source: s.source,
                  source_chapter_id: s.sourceChapterId,
                  work_id: workId,
                  work_mapping_id: s.mappingId,
                  chapter_number: cand.chapterNumber,
                  chapter_sort_key: cand.sortKey,
                  page_count: cand.expectedPages,
                  is_page_provider: false,
                  status: 'PENDING',
                  is_gap: false,
                  last_error: null,
                },
                { onConflict: 'source,source_chapter_id' }
              );
            }
          }
          continue;
        }

        operationalSources.sort((a, b) => b.priorityScore - a.priorityScore);
        const primary = operationalSources[0];

        const operationalFallbackSources = operationalSources
          .filter((s) => s.source !== primary.source)
          .map((s) => ({
            source: s.source,
            sourceChapterId: s.sourceChapterId,
            sourceWorkId: s.sourceWorkId,
            mappingId: s.mappingId,
          }));

        const canonicalDedupeKey = `work:${workId}:chapter:${cand.sortKey}`;

        for (const s of cand.sources) {
          const chmUpsert = this.supabase.from('importer_chapter_mappings');
          if (chmUpsert && typeof chmUpsert.upsert === 'function') {
            await chmUpsert.upsert(
              {
                source: s.source,
                source_chapter_id: s.sourceChapterId,
                work_id: workId,
                work_mapping_id: s.mappingId,
                chapter_number: cand.chapterNumber,
                chapter_sort_key: cand.sortKey,
                page_count: cand.expectedPages,
                is_page_provider: s.source === primary.source,
                status: 'PENDING',
                is_gap: false,
                last_error: null,
              },
              { onConflict: 'source,source_chapter_id' }
            );
          }
        }

        const enqueued = await this.queue.enqueue(
          'IMPORT_CHAPTER',
          primary.source,
          canonicalDedupeKey,
          {
            sourceWorkId: primary.sourceWorkId,
            sourceChapterId: primary.sourceChapterId,
            workId,
            workMappingId: primary.mappingId,
            chapterNumber: cand.chapterNumber,
            chapterTitle: cand.chapterTitle,
            expectedPageCount: cand.expectedPages || null,
            isNewRelease: true,
            fallbackSources: operationalFallbackSources,
          },
          80,
          cand.sortKey
        );

        if (enqueued) {
          stats.newChaptersDiscovered++;
          stats.jobsEnqueued++;
        }
      }
    }

    this.logger.info('Reconciliation cycle completed', stats);
    return stats;
  }
}
