export type AliasSourceId = 'mangadex' | 'mangafire' | 'mangaball';

/**
 * Active ALIAS_ONLY sources used in permanent runtime enrichment and retroactive runs.
 */
export const ALIAS_ONLY_SOURCE_IDS: readonly AliasSourceId[] = [
  'mangaball',
  'mangadex',
  'mangafire',
] as const;

export type SourceCategory = 'ALIAS_ONLY';

export type AliasMatchStatus =
  | 'CONFIRMED_MATCH'
  | 'AMBIGUOUS_MATCH'
  | 'NOT_FOUND'
  | 'SOURCE_ERROR';

export interface LocalizedAlias {
  title: string;
  language?: string;
  isPrimary?: boolean;
}

export interface ExternalCrossLinks {
  mangadexId?: string;
  anilistId?: string;
  malId?: string;
  mangaUpdatesId?: string;
  mangaBakaId?: string;
}

export interface AliasSourceCandidate {
  source: AliasSourceId;
  sourceWorkId: string;
  url: string;
  slug?: string;
  primaryTitle: string;
  aliases: LocalizedAlias[];
  /** Raw unfiltered alias strings for cross-matching (even in non-target scripts) */
  rawMatchAliases?: string[];
  authors?: string[];
  artists?: string[];
  year?: number | null;
  kind?: string;
  originalLanguage?: string;
  contentRating?: 'safe' | 'suggestive' | 'erotica' | 'pornographic' | string;
  isAdult?: boolean;
  tags?: string[];
  links?: ExternalCrossLinks;
  /** Whether full detail aliases have already been loaded (false if search only returned summary) */
  detailsLoaded: boolean;
}

export interface AliasSourceSearchOutcome {
  source: AliasSourceId;
  ok: boolean;
  candidates: AliasSourceCandidate[];
  errorCode?: string;
  errorMessage?: string;
  statusCode?: number;
  latencyMs: number;
  fromCache?: boolean;
}

export interface AliasSourceDetailOutcome {
  source: AliasSourceId;
  ok: boolean;
  candidate: AliasSourceCandidate | null;
  errorCode?: string;
  errorMessage?: string;
  statusCode?: number;
  latencyMs: number;
  fromCache?: boolean;
}

export interface AliasOnlySourceAdapter {
  readonly id: AliasSourceId;
  readonly name: string;
  readonly baseUrl: string;
  readonly category: SourceCategory;
  searchWorks(query: string, limit?: number): Promise<AliasSourceSearchOutcome>;
  fetchWorkAliases(sourceWorkId: string): Promise<AliasSourceDetailOutcome>;
  getHealthSnapshot(): AliasSourceHealthSnapshot;
}

export interface AliasSourceHealthSnapshot {
  source: AliasSourceId;
  requestsTotal: number;
  cacheHits: number;
  successTotal: number;
  errorTotal: number;
  rateLimitedTotal: number;
  circuitOpen: boolean;
  circuitReason?: string;
  circuitOpenUntil?: number;
  avgLatencyMs: number;
}

export interface NoxWorkAliasTarget {
  id: string;
  title: string;
  slug: string;
  aliases: string[];
  author?: string | null;
  artist?: string | null;
  kind?: string | null;
  year?: number | null;
  ageRating?: number | null;
  contentRating?: string | null;
}

export interface SourceVerificationRecord {
  source: AliasSourceId;
  status: AliasMatchStatus;
  responded: boolean;
  sourceWorkId?: string | null;
  sourceUrl?: string | null;
  sourcePrimaryTitle?: string | null;
  discoveredAliases: string[];
  addedAliases: string[];
  ptBrAliasesAdded: string[];
  confidence: number;
  matchMethod?: string | null;
  matchReason?: string | null;
  evidence: Record<string, unknown>;
  errorMessage?: string | null;
  latencyMs: number;
}

export interface WorkAliasEnrichmentResult {
  workId: string;
  aliasesBefore: string[];
  aliasesAfter: string[];
  addedAliases: string[];
  ptBrAliasesAdded: string[];
  aliasesChanged: boolean;
  respondedSources: AliasSourceId[];
  confirmedSources: AliasSourceId[];
  ambiguousSources: AliasSourceId[];
  errorSources: AliasSourceId[];
  perSource: Record<AliasSourceId, SourceVerificationRecord>;
  durationMs: number;
}
