import type { SupabaseClient } from '@supabase/supabase-js';
import { Logger } from './logger.js';
import { ImporterQueue } from './queue.js';
import { SourceRegistry } from '../sources/registry.js';
import { computeCanonicalChapterKey } from './deduplication.js';

export interface ReconciliationStats {
  worksScanned: number;
  worksWithConfirmedGaps: number;
  confirmedGapsDiscovered: number;
  newChaptersDiscovered: number;
  jobsEnqueued: number;
  stagedSkipped: number;
  duplicatesAvoided: number;
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

  /**
   * Reconcilia obras existentes de forma paginada e com baixo custo de rede.
   * Utiliza deduplicação estritamente canônica (workId:sortKey) e protege capítulos STAGED.
   *
   * @param batchSize Quantidade de obras a reconciliar por rodada (default: 20)
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

    // 1. Busca mapeamentos de obras ativas em lotes ordenados por atualização
    const { data: mappings, error: mapErr } = await this.supabase
      .from('importer_work_mappings')
      .select('id, work_id, source, source_work_id, updated_at, works!inner(id, title, slug, published, updated_at)')
      .eq('status', 'ACTIVE')
      .order('updated_at', { ascending: false })
      .limit(batchSize * 3);

    if (mapErr) {
      this.logger.error('Failed to query work mappings for reconciliation', { error: mapErr.message });
      return stats;
    }

    if (!mappings || mappings.length === 0) {
      return stats;
    }

    // 2. Agrupa por obra (work_id) para análise multi-source unificada
    const worksMap = new Map<string, Array<typeof mappings[0]>>();
    for (const m of mappings) {
      const list = worksMap.get(m.work_id) || [];
      list.push(m);
      worksMap.set(m.work_id, list);
      if (worksMap.size >= batchSize) break;
    }

    const now = Date.now();

    for (const [workId, sourceMappings] of worksMap.entries()) {
      // Controle de frequência adaptativa: não reverificar a mesma obra antes de 10 minutos
      const lastCheck = this.lastReconciliationAt.get(workId) || 0;
      if (now - lastCheck < 10 * 60 * 1000) {
        continue;
      }
      this.lastReconciliationAt.set(workId, now);
      stats.worksScanned++;

      const workTitle = (sourceMappings[0] as any).works?.title || workId;

      // 3. Busca capítulos publicados no Nox
      const { data: publishedChapters } = await this.supabase
        .from('chapters')
        .select('id, number, published_at')
        .eq('work_id', workId)
        .not('published_at', 'is', null);

      const publishedNumbers = new Set(
        (publishedChapters || []).map((c) => Number(Number(c.number).toFixed(4)))
      );
      const maxPublishedSort = (publishedChapters || []).reduce(
        (max, c) => Math.max(max, Number(c.number)),
        0
      );

      // 4. Busca capítulos já em STAGED ou COMPLETED nos mappings (Proteção Rigorosa de STAGED)
      const { data: chapterMappings } = await this.supabase
        .from('importer_chapter_mappings')
        .select('chapter_sort_key, chapter_number, status')
        .eq('work_id', workId);

      const stagedSortKeys = new Set<number>();
      const completedSortKeys = new Set<number>();

      for (const cm of chapterMappings || []) {
        const sKey = Number(cm.chapter_sort_key);
        if (cm.status === 'STAGED') {
          stagedSortKeys.add(sKey);
        } else if (cm.status === 'COMPLETED') {
          completedSortKeys.add(sKey);
        }
      }

      // 5. Busca capítulos ativos na fila (QUEUED, IMPORTING, RETRY)
      const { data: activeQueue } = await this.supabase
        .from('importer_queue')
        .select('chapter_sort_key, status, payload')
        .eq('task_type', 'IMPORT_CHAPTER')
        .eq('payload->>workId', workId)
        .in('status', ['QUEUED', 'IMPORTING', 'RETRY']);

      const queuedSortKeys = new Set<number>(
        (activeQueue || []).map((q) => Number(q.chapter_sort_key))
      );

      // 6. Consulta de metadados em todas as fontes mapeadas (Multi-Source Metadata Discovery)
      // Agrupa candidatos por canonical sortKey
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
          // Busca barata de lista de capítulos (sem download de imagens)
          const sourceChapters = await adapter.fetchChapters(sm.source_work_id);

          for (const ch of sourceChapters) {
            const canonicalKey = computeCanonicalChapterKey(ch.number, ch.title);
            const sortKey = canonicalKey.sortKey;
            const normNum = canonicalKey.normalizedNumber;

            // REGRA 1: Se já publicado, ignora
            if (publishedNumbers.has(normNum) || completedSortKeys.has(sortKey)) {
              continue;
            }

            // REGRA 2: Se já estiver em STAGED, JAMAIS reenfileira!
            if (stagedSortKeys.has(sortKey)) {
              stats.stagedSkipped++;
              continue;
            }

            // REGRA 3: Se já na fila ativa, ignora
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

      // 7. Separa em GAPS CONFIRMADOS (< highestMilestone) e CAPÍTULOS NOVOS (>= highestMilestone)
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

      // Ordenação determinística estritamente crescente para ambos
      confirmedGaps.sort((a, b) => a.sortKey - b.sortKey);
      newChapters.sort((a, b) => a.sortKey - b.sortKey);

      // 8. Enfileira GAPS CONFIRMADOS com PRIORIDADE 70 (Backfill ordenado ASC)
      for (const cand of confirmedGaps) {
        cand.sources.sort((a, b) => b.priorityScore - a.priorityScore);
        const primary = cand.sources[0];

        // Deduplicação estritamente canônica
        const canonicalDedupeKey = `work:${workId}:chapter:${cand.sortKey}`;

        for (const s of cand.sources) {
          await this.supabase.from('importer_chapter_mappings').upsert(
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
            expectedPageCount: cand.expectedPages,
            isGapBackfill: true,
            fallbackSources: cand.sources.slice(1).map((s) => ({
              source: s.source,
              sourceChapterId: s.sourceChapterId,
              sourceWorkId: s.sourceWorkId,
              mappingId: s.mappingId,
            })),
          },
          70, // PRIORIDADE 70: Gap confirmado de obra existente!
          cand.sortKey
        );

        if (enqueued) {
          stats.confirmedGapsDiscovered++;
          stats.jobsEnqueued++;
        }
      }

      // 9. Enfileira CAPÍTULOS NOVOS com PRIORIDADE 80 (Fresh releases rápidas!)
      for (const cand of newChapters) {
        // Escolhe o provedor de maior prioridade (Kuro > Nexus > Manhastro...)
        cand.sources.sort((a, b) => b.priorityScore - a.priorityScore);
        const primary = cand.sources[0];

        // Deduplicação estritamente canônica: chave baseada em workId + sortKey
        const canonicalDedupeKey = `work:${workId}:chapter:${cand.sortKey}`;

        // Registra mappings de todas as fontes disponíveis
        for (const s of cand.sources) {
          await this.supabase.from('importer_chapter_mappings').upsert(
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
            expectedPageCount: cand.expectedPages,
            isNewRelease: true,
            fallbackSources: cand.sources.slice(1).map((s) => ({
              source: s.source,
              sourceChapterId: s.sourceChapterId,
              sourceWorkId: s.sourceWorkId,
              mappingId: s.mappingId,
            })),
          },
          80, // PRIORIDADE 80: Atualização recente de obra existente!
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
