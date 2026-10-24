import { performance } from 'node:perf_hooks';
import { cleanAlternativeTitle } from '../core/alternative-titles.js';
import { normalizeTitle } from '../core/matching.js';
import { ComixAliasSource } from './comix-alias-source.js';
import {
  computeMergedWorkAliases,
  evaluateSourceCandidates,
  isPortugueseAlias,
  sanitizeCandidateAliases,
} from './identity-verifier.js';
import { MangaBallAliasSource } from './mangaball-alias-source.js';
import { MangaDexAliasSource } from './mangadex-alias-source.js';
import { MangaFireAliasSource } from './mangafire-alias-source.js';
import {
  ALIAS_ONLY_SOURCE_IDS,
  type AliasOnlySourceAdapter,
  type AliasSourceCandidate,
  type AliasSourceId,
  type ExternalCrossLinks,
  type NoxWorkAliasTarget,
  type SourceVerificationRecord,
  type WorkAliasEnrichmentResult,
} from './types.js';

const CJK_REGEX = /[\u3040-\u30ff\u3400-\u9fff\uf900-\ufaff\uac00-\ud7af]/u;
const SEARCH_NOISE_SUFFIX_REGEX = /\s*[\[(]\s*(?:one[\s-]*shot|oneshot|completo|pt[\s-]*br|mang[aá]|manhwa|manhua)\s*[\])]\s*$/gi;

/**
 * Builds up to `maxQueries` non-redundant search queries for a work, prioritizing
 * international/English/Romaji strings first (highest cross-catalog hit rate),
 * followed by the primary title and distinct CJK/PT-BR variants.
 */
export function buildEfficientSearchQueries(
  primaryTitle: string,
  aliases: readonly string[],
  extraDiscoveredTitles: readonly string[] = [],
  maxQueries = 3,
): string[] {
  const rawPool = [primaryTitle, ...extraDiscoveredTitles, ...(aliases || [])];
  const candidates: Array<{ query: string; norm: string; score: number }> = [];
  const seenNorm = new Set<string>();

  for (let idx = 0; idx < rawPool.length; idx++) {
    const cleaned = cleanAlternativeTitle(rawPool[idx]);
    if (!cleaned) continue;
    const query = cleaned.replace(SEARCH_NOISE_SUFFIX_REGEX, '').trim();
    if (query.length < 2 || query.length > 140) continue;
    const norm = normalizeTitle(query);
    if (!norm || seenNorm.has(norm)) continue;
    seenNorm.add(norm);

    const hasCjk = CJK_REGEX.test(query);
    const isPt = !hasCjk && isPortugueseAlias(query);
    // International Latin/English/Romaji titles match across ALL 4 catalogs simultaneously
    let score = 50;
    if (!hasCjk && !isPt) score = 100;
    else if (idx === 0) score = 85; // Primary title
    else if (hasCjk) score = 70;
    else if (isPt) score = 60;

    // Slight bonus for 2..8 word titles (cleanest search query specificity)
    const wordCount = norm.split(/\s+/).length;
    if (wordCount >= 2 && wordCount <= 8) score += 5;

    candidates.push({ query, norm, score });
  }

  candidates.sort((a, b) => b.score - a.score);
  return candidates.slice(0, maxQueries).map((c) => c.query);
}

function mergeExternalLinks(target: ExternalCrossLinks, incoming?: ExternalCrossLinks): void {
  if (!incoming) return;
  if (!target.mangadexId && incoming.mangadexId) target.mangadexId = incoming.mangadexId;
  if (!target.anilistId && incoming.anilistId) target.anilistId = incoming.anilistId;
  if (!target.malId && incoming.malId) target.malId = incoming.malId;
  if (!target.mangaUpdatesId && incoming.mangaUpdatesId) target.mangaUpdatesId = incoming.mangaUpdatesId;
  if (!target.mangaBakaId && incoming.mangaBakaId) target.mangaBakaId = incoming.mangaBakaId;
}

export interface AliasEnrichmentServiceOptions {
  adapters?: Partial<Record<AliasSourceId, AliasOnlySourceAdapter>>;
  maxQueriesPerSource?: number;
  mangaFireMaxDetailProbes?: number;
}

export class AliasOnlyEnrichmentService {
  readonly adapters: Record<AliasSourceId, AliasOnlySourceAdapter>;
  private readonly maxQueriesPerSource: number;
  private readonly mangaFireMaxDetailProbes: number;

  constructor(options: AliasEnrichmentServiceOptions = {}) {
    this.adapters = {
      mangaball: options.adapters?.mangaball ?? new MangaBallAliasSource(),
      mangadex: options.adapters?.mangadex ?? new MangaDexAliasSource(),
      mangafire: options.adapters?.mangafire ?? new MangaFireAliasSource(),
      comix: options.adapters?.comix ?? new ComixAliasSource(),
    };
    this.maxQueriesPerSource = options.maxQueriesPerSource ?? 2;
    this.mangaFireMaxDetailProbes = options.mangaFireMaxDetailProbes ?? 2;
  }

  getHealthSnapshots() {
    return {
      mangadex: this.adapters.mangadex.getHealthSnapshot(),
      mangafire: this.adapters.mangafire.getHealthSnapshot(),
      mangaball: this.adapters.mangaball.getHealthSnapshot(),
      comix: this.adapters.comix.getHealthSnapshot(),
    };
  }

  /**
   * Probes a single ALIAS_ONLY source for `nox` using bounded queries + optional persistent ID shortcut.
   */
  private async probeSingleSource(
    sourceId: AliasSourceId,
    nox: NoxWorkAliasTarget,
    queries: string[],
    corroborated: { confirmedTitles: string[]; confirmedLinks: ExternalCrossLinks },
  ): Promise<{
    record: SourceVerificationRecord;
    confirmedCandidate: AliasSourceCandidate | null;
    persistableAliases: Array<{ title: string; language?: string }>;
  }> {
    const adapter = this.adapters[sourceId];
    const started = performance.now();
    let totalLatencyMs = 0;

    // Shortcut 1: Direct persistentMangaDex UUID lookup when discovered by Wave-1 source!
    if (sourceId === 'mangadex' && corroborated.confirmedLinks.mangadexId) {
      const directRes = await adapter.fetchWorkAliases(corroborated.confirmedLinks.mangadexId);
      totalLatencyMs += directRes.latencyMs;
      if (directRes.ok && directRes.candidate) {
        const evalRes = evaluateSourceCandidates(nox, [directRes.candidate], corroborated);
        if (evalRes.status === 'CONFIRMED_MATCH' && evalRes.candidate) {
          const persistable = sanitizeCandidateAliases(nox, evalRes.candidate);
          return {
            confirmedCandidate: evalRes.candidate,
            persistableAliases: persistable,
            record: {
              source: sourceId,
              status: 'CONFIRMED_MATCH',
              responded: true,
              sourceWorkId: evalRes.candidate.sourceWorkId,
              sourceUrl: evalRes.candidate.url,
              sourcePrimaryTitle: evalRes.candidate.primaryTitle,
              discoveredAliases: persistable.map((p) => p.title),
              addedAliases: [],
              ptBrAliasesAdded: [],
              confidence: evalRes.confidence,
              matchMethod: evalRes.matchMethod,
              matchReason: evalRes.matchReason,
              evidence: evalRes.evidence,
              latencyMs: Math.round(performance.now() - started),
            },
          };
        }
      }
    }

    let anySearchSucceeded = false;
    let lastError: string | undefined;
    let bestAmbiguous: ReturnType<typeof evaluateSourceCandidates> | null = null;
    const detailFetchedIds = new Set<string>();

    for (const query of queries) {
      const searchRes = await adapter.searchWorks(query, 12);
      totalLatencyMs += searchRes.latencyMs;

      if (!searchRes.ok) {
        lastError = searchRes.errorCode || searchRes.errorMessage || 'SEARCH_FAILED';
        // If circuit is open or access restricted, do not repeat further queries on this source
        if (
          searchRes.errorCode?.includes('CIRCUIT') ||
          searchRes.errorCode?.includes('RESTRICTED') ||
          searchRes.errorCode?.includes('CHALLENGE')
        ) {
          break;
        }
        continue;
      }

      anySearchSucceeded = true;
      if (!searchRes.candidates.length) continue;

      let candidatesToEvaluate = searchRes.candidates;

      // For sources whose search returns summaries only (`detailsLoaded === false`, e.g. MangaFire / Comix API),
      // load full detail (`altTitles`, `authors`, `contentRating`) for the best-matching summary first,
      // plus at most `mangaFireMaxDetailProbes` ranked summaries when searching by a translated alias!
      if (candidatesToEvaluate.some((c) => !c.detailsLoaded)) {
        const prelimEval = evaluateSourceCandidates(nox, candidatesToEvaluate, corroborated);
        const toHydrate: AliasSourceCandidate[] = [];
        if (prelimEval.candidate && !prelimEval.candidate.detailsLoaded) {
          toHydrate.push(prelimEval.candidate);
        }
        for (const c of candidatesToEvaluate) {
          if (toHydrate.length >= this.mangaFireMaxDetailProbes) break;
          if (!c.detailsLoaded && !toHydrate.includes(c)) {
            toHydrate.push(c);
          }
        }

        const hydratedMap = new Map<string, AliasSourceCandidate>();
        await Promise.all(
          toHydrate.map(async (item) => {
            if (detailFetchedIds.has(item.sourceWorkId)) return;
            detailFetchedIds.add(item.sourceWorkId);
            const det = await adapter.fetchWorkAliases(item.sourceWorkId);
            totalLatencyMs += det.latencyMs;
            if (det.ok && det.candidate) {
              hydratedMap.set(item.sourceWorkId, det.candidate);
            }
          }),
        );

        candidatesToEvaluate = candidatesToEvaluate.map(
          (c) => hydratedMap.get(c.sourceWorkId) ?? c,
        );
      }

      const evalRes = evaluateSourceCandidates(nox, candidatesToEvaluate, corroborated);

      if (evalRes.status === 'CONFIRMED_MATCH' && evalRes.candidate) {
        // Ensure full details are loaded if somehow not yet hydrated
        let finalCandidate = evalRes.candidate;
        if (!finalCandidate.detailsLoaded) {
          const det = await adapter.fetchWorkAliases(finalCandidate.sourceWorkId);
          totalLatencyMs += det.latencyMs;
          if (det.ok && det.candidate) finalCandidate = det.candidate;
        }
        const persistable = sanitizeCandidateAliases(nox, finalCandidate);
        return {
          confirmedCandidate: finalCandidate,
          persistableAliases: persistable,
          record: {
            source: sourceId,
            status: 'CONFIRMED_MATCH',
            responded: true,
            sourceWorkId: finalCandidate.sourceWorkId,
            sourceUrl: finalCandidate.url,
            sourcePrimaryTitle: finalCandidate.primaryTitle,
            discoveredAliases: persistable.map((p) => p.title),
            addedAliases: [],
            ptBrAliasesAdded: [],
            confidence: evalRes.confidence,
            matchMethod: evalRes.matchMethod,
            matchReason: evalRes.matchReason,
            evidence: { ...evalRes.evidence, matchedViaQuery: query },
            latencyMs: Math.round(performance.now() - started),
          },
        };
      }

      if (evalRes.status === 'AMBIGUOUS_MATCH') {
        if (!bestAmbiguous || evalRes.confidence > bestAmbiguous.confidence) {
          bestAmbiguous = evalRes;
        }
      }
    }

    if (!anySearchSucceeded && lastError) {
      return {
        confirmedCandidate: null,
        persistableAliases: [],
        record: {
          source: sourceId,
          status: 'SOURCE_ERROR',
          responded: false,
          discoveredAliases: [],
          addedAliases: [],
          ptBrAliasesAdded: [],
          confidence: 0,
          matchMethod: null,
          matchReason: lastError,
          evidence: {},
          errorMessage: lastError,
          latencyMs: Math.round(performance.now() - started) || totalLatencyMs,
        },
      };
    }

    if (bestAmbiguous && bestAmbiguous.candidate) {
      return {
        confirmedCandidate: null,
        persistableAliases: [],
        record: {
          source: sourceId,
          status: 'AMBIGUOUS_MATCH',
          responded: true,
          sourceWorkId: bestAmbiguous.candidate.sourceWorkId,
          sourceUrl: bestAmbiguous.candidate.url,
          sourcePrimaryTitle: bestAmbiguous.candidate.primaryTitle,
          discoveredAliases: bestAmbiguous.candidate.aliases.map((a) => a.title).slice(0, 15),
          addedAliases: [],
          ptBrAliasesAdded: [],
          confidence: bestAmbiguous.confidence,
          matchMethod: bestAmbiguous.matchMethod,
          matchReason: bestAmbiguous.matchReason,
          evidence: bestAmbiguous.evidence,
          latencyMs: Math.round(performance.now() - started),
        },
      };
    }

    return {
      confirmedCandidate: null,
      persistableAliases: [],
      record: {
        source: sourceId,
        status: 'NOT_FOUND',
        responded: true,
        discoveredAliases: [],
        addedAliases: [],
        ptBrAliasesAdded: [],
        confidence: 0,
        matchMethod: null,
        matchReason: 'NOT_FOUND',
        evidence: { queriesTried: queries },
        latencyMs: Math.round(performance.now() - started),
      },
    };
  }

  /**
   * Enriches a single Nox work across all 4 ALIAS_ONLY sources:
   * - Wave 1 queries MangaBall + MangaDex in parallel (highest multi-language & PT-BR recall, 1-call detail).
   * - Propagates any confirmed persistent IDs (`mangadexId`, `malId`, `anilistId`) and newly discovered
   *   English/Romaji/CJK titles into Wave 2 (`MangaFire` + `Comix`, plus direct UUID follow-up on `MangaDex`
   *   if Wave 1 MangaBall revealed `mangadexId` while MangaDex text search missed a PT-BR-only title!).
   */
  async enrichWork(
    nox: NoxWorkAliasTarget,
    onlySources?: readonly AliasSourceId[],
  ): Promise<WorkAliasEnrichmentResult> {
    const started = performance.now();
    const activeSources = new Set<AliasSourceId>(
      onlySources && onlySources.length > 0
        ? onlySources
        : ALIAS_ONLY_SOURCE_IDS,
    );

    const corroborated: { confirmedTitles: string[]; confirmedLinks: ExternalCrossLinks } = {
      confirmedTitles: [],
      confirmedLinks: {},
    };

    const wave1Queries = buildEfficientSearchQueries(nox.title, nox.aliases, [], this.maxQueriesPerSource);

    const perSourcePartial: Partial<Record<AliasSourceId, SourceVerificationRecord>> = {};
    const perSourcePersistable: Partial<Record<AliasSourceId, Array<{ title: string; language?: string }>>> = {};

    // Wave 1: MangaBall + MangaDex in parallel
    const wave1Ids = (['mangaball', 'mangadex'] as const).filter((s) => activeSources.has(s));
    const wave1Outcomes = await Promise.all(
      wave1Ids.map(async (sourceId) => {
        const res = await this.probeSingleSource(sourceId, nox, wave1Queries, {
          confirmedTitles: [],
          confirmedLinks: {},
        });
        return { sourceId, res };
      }),
    );

    for (const { sourceId, res } of wave1Outcomes) {
      perSourcePartial[sourceId] = res.record;
      perSourcePersistable[sourceId] = res.persistableAliases;
      if (res.confirmedCandidate && res.record.confidence >= 0.94) {
        mergeExternalLinks(corroborated.confirmedLinks, res.confirmedCandidate.links);
        for (const p of res.persistableAliases) {
          if (!corroborated.confirmedTitles.includes(p.title)) {
            corroborated.confirmedTitles.push(p.title);
          }
        }
      }
    }

    // If MangaBall confirmed a match and discovered a MangaDex UUID or English title while MangaDex returned NOT_FOUND,
    // do a single fast follow-up on MangaDex using that UUID / newly discovered English title!
    if (
      activeSources.has('mangadex') &&
      perSourcePartial.mangadex?.status === 'NOT_FOUND' &&
      (corroborated.confirmedLinks.mangadexId || corroborated.confirmedTitles.length > 0)
    ) {
      const followUpQueries = corroborated.confirmedLinks.mangadexId
        ? []
        : buildEfficientSearchQueries(nox.title, nox.aliases, corroborated.confirmedTitles, 2).filter(
            (q) => !wave1Queries.includes(q),
          );
      if (corroborated.confirmedLinks.mangadexId || followUpQueries.length > 0) {
        const mdFollowUp = await this.probeSingleSource('mangadex', nox, followUpQueries, corroborated);
        if (mdFollowUp.record.status === 'CONFIRMED_MATCH' || mdFollowUp.record.status === 'AMBIGUOUS_MATCH') {
          perSourcePartial.mangadex = mdFollowUp.record;
          perSourcePersistable.mangadex = mdFollowUp.persistableAliases;
          if (mdFollowUp.confirmedCandidate && mdFollowUp.record.confidence >= 0.94) {
            mergeExternalLinks(corroborated.confirmedLinks, mdFollowUp.confirmedCandidate.links);
            for (const p of mdFollowUp.persistableAliases) {
              if (!corroborated.confirmedTitles.includes(p.title)) {
                corroborated.confirmedTitles.push(p.title);
              }
            }
          }
        }
      }
    }

    // Wave 2: MangaFire + Comix in parallel, enriched with Wave-1 confirmed titles & external IDs!
    const wave2Queries = buildEfficientSearchQueries(
      nox.title,
      nox.aliases,
      corroborated.confirmedTitles,
      this.maxQueriesPerSource,
    );
    const wave2Ids = (['mangafire', 'comix'] as const).filter((s) => activeSources.has(s));
    const wave2Outcomes = await Promise.all(
      wave2Ids.map(async (sourceId) => {
        const res = await this.probeSingleSource(sourceId, nox, wave2Queries, corroborated);
        return { sourceId, res };
      }),
    );

    for (const { sourceId, res } of wave2Outcomes) {
      perSourcePartial[sourceId] = res.record;
      perSourcePersistable[sourceId] = res.persistableAliases;
    }

    // Fill defaults for any non-requested source
    const allOrder: AliasSourceId[] = ['mangadex', 'mangafire', 'mangaball', 'comix'];
    const perSource = {} as Record<AliasSourceId, SourceVerificationRecord>;

    // Compute per-source attribution AND combined merge in deterministic priority order:
    // MangaDex (explicit language tags) -> MangaBall -> MangaFire -> Comix
    const attributionOrder: AliasSourceId[] = ['mangadex', 'mangaball', 'mangafire', 'comix'];
    let runningAliases = Array.isArray(nox.aliases) ? [...nox.aliases] : [];
    const allIncoming: Array<{ title: string; language?: string }> = [];

    for (const src of attributionOrder) {
      const rec =
        perSourcePartial[src] ??
        ({
          source: src,
          status: 'NOT_FOUND',
          responded: false,
          discoveredAliases: [],
          addedAliases: [],
          ptBrAliasesAdded: [],
          confidence: 0,
          evidence: { skipped: true },
          latencyMs: 0,
        } satisfies SourceVerificationRecord);

      if (rec.status === 'CONFIRMED_MATCH') {
        const persistable = perSourcePersistable[src] || [];
        allIncoming.push(...persistable);
        const stepMerge = computeMergedWorkAliases(
          { ...nox, aliases: runningAliases },
          persistable,
        );
        rec.addedAliases = stepMerge.addedAliases;
        rec.ptBrAliasesAdded = stepMerge.ptBrAliasesAdded;
        runningAliases = stepMerge.aliasesAfter;
      }
      perSource[src] = rec;
    }

    const finalMerge = computeMergedWorkAliases(nox, allIncoming);

    const respondedSources = allOrder.filter((s) => perSource[s].responded);
    const confirmedSources = allOrder.filter((s) => perSource[s].status === 'CONFIRMED_MATCH');
    const ambiguousSources = allOrder.filter((s) => perSource[s].status === 'AMBIGUOUS_MATCH');
    const errorSources = allOrder.filter((s) => perSource[s].status === 'SOURCE_ERROR');

    return {
      workId: nox.id,
      aliasesBefore: Array.isArray(nox.aliases) ? [...nox.aliases] : [],
      aliasesAfter: finalMerge.aliasesAfter,
      addedAliases: finalMerge.addedAliases,
      ptBrAliasesAdded: finalMerge.ptBrAliasesAdded,
      aliasesChanged: finalMerge.aliasesChanged,
      respondedSources,
      confirmedSources,
      ambiguousSources,
      errorSources,
      perSource,
      durationMs: Math.round(performance.now() - started),
    };
  }
}
