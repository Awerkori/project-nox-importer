import { type AliasOnlySourceAdapter, type AliasSourceId, type NoxWorkAliasTarget, type WorkAliasEnrichmentResult } from './types.js';
/**
 * Builds up to `maxQueries` non-redundant search queries for a work, prioritizing
 * international/English/Romaji strings first (highest cross-catalog hit rate),
 * followed by the primary title and distinct CJK/PT-BR variants.
 */
export declare function buildEfficientSearchQueries(primaryTitle: string, aliases: readonly string[], extraDiscoveredTitles?: readonly string[], maxQueries?: number): string[];
export interface AliasEnrichmentServiceOptions {
    adapters?: Partial<Record<AliasSourceId, AliasOnlySourceAdapter>>;
    maxQueriesPerSource?: number;
    mangaFireMaxDetailProbes?: number;
}
export declare class AliasOnlyEnrichmentService {
    readonly adapters: Record<AliasSourceId, AliasOnlySourceAdapter>;
    private readonly maxQueriesPerSource;
    private readonly mangaFireMaxDetailProbes;
    constructor(options?: AliasEnrichmentServiceOptions);
    getHealthSnapshots(): {
        mangadex: import("./types.js").AliasSourceHealthSnapshot;
        mangafire: import("./types.js").AliasSourceHealthSnapshot;
        mangaball: import("./types.js").AliasSourceHealthSnapshot;
    };
    /**
     * Probes a single ALIAS_ONLY source for `nox` using bounded queries + optional persistent ID shortcut.
     */
    private probeSingleSource;
    /**
     * Enriches a single Nox work across all 3 ALIAS_ONLY sources:
     * - Wave 1 queries MangaBall + MangaDex in parallel (highest multi-language & PT-BR recall, 1-call detail).
     * - Propagates any confirmed persistent IDs (`mangadexId`, `malId`, `anilistId`) and newly discovered
     *   English/Romaji/CJK titles into Wave 2 (`MangaFire`, plus direct UUID follow-up on `MangaDex`
     *   if Wave 1 MangaBall revealed `mangadexId` while MangaDex text search missed a PT-BR-only title!).
     */
    enrichWork(nox: NoxWorkAliasTarget, onlySources?: readonly AliasSourceId[]): Promise<WorkAliasEnrichmentResult>;
}
