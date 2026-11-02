/** P0's source catalog is allowed to discover mappings, never canonical works. */
export declare const P0_ELIGIBLE_WORK_COUNT = 10749;
export declare const P2_FROZEN_CANONICAL_WORK_COUNT = 24945;
export type P0CycleStatus = 'IDLE' | 'RUNNING' | 'COMPLETED';
export type CatalogMatchMethod = 'title' | 'alias' | 'slug' | 'trigram';
export type P0CoverageState = 'MAPPED' | 'NOT_FOUND' | 'PENDING' | 'FAILED' | 'DEFERRED' | 'STALE';
export interface CatalogItem {
    sourceWorkId: string;
    title: string;
    slug?: string | null;
    aliases?: string[] | null;
    url?: string | null;
    source?: string;
    metadata?: Record<string, unknown>;
}
export interface CanonicalWork {
    id: string;
    title: string;
    aliases?: string[] | null;
    slug?: string | null;
}
export interface SourceCatalogMatch {
    item: CatalogItem;
    work: CanonicalWork;
    method: CatalogMatchMethod;
    normalizedCandidate: string;
    score: number;
}
export interface P0CycleMetrics {
    cycleId: string | null;
    status: P0CycleStatus;
    startedAt: string | null;
    completedAt: string | null;
    durationMs: number | null;
    eligibleWorks: number;
    canonicalWorks: number;
    sourcesProcessed: number;
    catalogItemsSeen: number;
    worksVerified: number;
    candidatesMatched: number;
    ambiguousCandidates: number;
    unmatchedCandidates: number;
    newMappingsDiscovered: number;
}
export interface P0AccumulatedMetrics {
    cyclesCompleted: number;
    sourcesProcessed: number;
    catalogItemsSeen: number;
    worksVerified: number;
    candidatesMatched: number;
    ambiguousCandidates: number;
    unmatchedCandidates: number;
    newMappingsDiscovered: number;
}
export interface P0CoverageEntry {
    source: string;
    workId: string;
    state: P0CoverageState;
    firstSeenAt: string;
    updatedAt: string;
    lastCheckedAt: string | null;
    nextRevalidationAt: string | null;
    attempts: number;
    error: string | null;
}
export interface PersistedP0Cycle {
    id: string;
    status: P0CycleStatus;
    startedAt: string;
    completedAt: string | null;
    metrics: P0CycleMetrics;
    sourceProgress: Record<string, number>;
    /** Optional fields keep persisted cycles written by older deployments readable. */
    sourceCatalogs?: Record<string, CatalogItem[]>;
    coverage?: Record<string, P0CoverageEntry>;
    verifiedWorkIds?: string[];
    history?: P0CycleMetrics[];
    accumulatedMetrics?: P0AccumulatedMetrics;
}
export interface P0MappingInput {
    source: string;
    item: CatalogItem;
    work: CanonicalWork;
    method: CatalogMatchMethod;
    score: number;
}
export interface P0CyclePersistence {
    loadLatestCycle(): Promise<PersistedP0Cycle | null>;
    saveCycle(cycle: PersistedP0Cycle): Promise<void>;
    /** Returns true only when a previously absent/unassigned mapping was created. */
    persistMapping(input: P0MappingInput): Promise<boolean>;
}
export interface P2AdmissionPolicy {
    isSuspended: () => boolean;
}
export interface P0DatabaseResult<Row extends Record<string, unknown> = Record<string, unknown>> {
    rows: Row[];
    rowCount?: number | null;
}
export interface P0Database {
    query<Row extends Record<string, unknown> = Record<string, unknown>>(text: string, values?: readonly unknown[]): Promise<P0DatabaseResult<Row>>;
}
export interface P0CycleCoordinatorOptions {
    db?: P0Database;
    persistence?: P0CyclePersistence;
    matcher: SourceCatalogMatcher;
    eligibleWorkCount?: number;
    canonicalWorkCount?: number;
    now?: () => Date;
    p2AdmissionPolicy?: P2AdmissionPolicy;
    onNewMappingDiscovered?: (mapping: P0MappingInput) => Promise<void>;
    negativeCacheTtlMs?: number;
}
/**
 * Unicode-safe title key used by both catalog and canonical indexes.
 * Punctuation, accents and slug separators intentionally do not affect a key.
 */
export declare function normalizeCatalogTitle(value: string | null | undefined): string;
/**
 * In-memory O(M) catalog matcher. Canonical titles/aliases are indexed once;
 * each incoming catalog item then performs O(1) exact lookups and only a
 * bounded trigram comparison when no exact key exists.
 */
export declare class SourceCatalogMatcher {
    readonly canonicalWorks: readonly CanonicalWork[];
    private readonly exactIndex;
    private readonly trigramIndex;
    constructor(canonicalWorks: readonly CanonicalWork[]);
    private addExact;
    match(item: CatalogItem): SourceCatalogMatch | null;
    matchMany(items: readonly CatalogItem[]): SourceCatalogMatch[];
}
/** A deterministic store useful for unit tests and local dry-runs. */
export declare class InMemoryP0CyclePersistence implements P0CyclePersistence {
    private cycle;
    readonly mappings: Map<string, P0MappingInput>;
    readonly existingWorkIds: ReadonlySet<string>;
    constructor(existingWorks?: readonly CanonicalWork[]);
    loadLatestCycle(): Promise<PersistedP0Cycle | null>;
    saveCycle(cycle: PersistedP0Cycle): Promise<void>;
    persistMapping(input: P0MappingInput): Promise<boolean>;
}
/** SQL persistence for Yugabyte/PostgreSQL, with no write path to `works`. */
export declare class SqlP0CyclePersistence implements P0CyclePersistence {
    private readonly db;
    private schemaReady;
    constructor(db: P0Database);
    private pkColumn;
    ensureSchema(): Promise<void>;
    loadLatestCycle(): Promise<PersistedP0Cycle | null>;
    saveCycle(cycle: PersistedP0Cycle): Promise<void>;
    persistMapping(input: P0MappingInput): Promise<boolean>;
}
/**
 * Deployment-safe fallback for installations where a migration cannot create
 * a table yet. `settings` is already a durable, service-role-owned table in
 * the importer schema; mapping writes continue to use the same works fence.
 */
export declare class SettingsP0CyclePersistence implements P0CyclePersistence {
    private readonly db;
    private readonly mappingPersistence;
    static readonly KEY = "importer_p0_cycle_state";
    constructor(db: P0Database, mappingPersistence: P0CyclePersistence);
    loadLatestCycle(): Promise<PersistedP0Cycle | null>;
    saveCycle(cycle: PersistedP0Cycle): Promise<void>;
    persistMapping(input: P0MappingInput): Promise<boolean>;
}
/** Tries the cycle table and falls back to the durable settings row on DDL errors. */
export declare class ResilientP0CyclePersistence implements P0CyclePersistence {
    private readonly primary;
    private readonly fallback;
    private usingFallback;
    constructor(primary: SqlP0CyclePersistence, fallback: SettingsP0CyclePersistence);
    loadLatestCycle(): Promise<PersistedP0Cycle | null>;
    saveCycle(cycle: PersistedP0Cycle): Promise<void>;
    persistMapping(input: P0MappingInput): Promise<boolean>;
}
/** Durable lifecycle coordinator for one bounded P0 catalog cycle. */
export declare class P0CycleCoordinator {
    private matcher;
    private readonly persistence;
    private eligibleWorkCount;
    private canonicalWorkCount;
    private readonly now;
    private readonly p2AdmissionPolicy?;
    private readonly onNewMappingDiscovered?;
    private readonly negativeCacheTtlMs;
    private state;
    private readonly sourceItems;
    private readonly verifiedWorkIds;
    private readonly coverage;
    private loaded;
    constructor(options: P0CycleCoordinatorOptions);
    /**
     * Rehydrates the last cycle, including its catalogs and audit matrix. A
     * RUNNING cycle is intentionally resumed with the same id after a reboot.
     */
    ensureLoaded(): Promise<PersistedP0Cycle | null>;
    private requireRunning;
    private persist;
    private createCycle;
    /** Starts the first cycle or advances a completed cycle to a new cycle id. */
    resumeOrCreateCycle(): Promise<P0CycleMetrics>;
    startCycle(): Promise<P0CycleMetrics>;
    /** Explicit alias for callers that want to make the continuous transition visible. */
    advanceToNextCycle(): Promise<P0CycleMetrics>;
    /** P2 suspension only blocks creating canonical works; mappings never do that. */
    isP2Suspended(): boolean;
    canCreateCanonicalWork(): boolean;
    assertCanonicalWorkCreationAllowed(): void;
    private coverageEntry;
    private setCoverageEntry;
    private prepareSourceCoverage;
    recordSourceCatalog(source: string, items: readonly CatalogItem[], options?: {
        isFullCatalog?: boolean;
    }): Promise<P0CycleMetrics>;
    recordSourceCoverage(source: string, coverageState: P0CoverageState, error?: string | null): Promise<void>;
    matchCandidateWorks(items?: readonly CatalogItem[], options?: {
        isFullCatalog?: boolean;
    }): Promise<SourceCatalogMatch[]>;
    completeCycle(): Promise<P0CycleMetrics>;
    setCoverageState(source: string, workId: string, state: P0CoverageState, error?: string | null): Promise<P0CoverageEntry>;
    getCoverageMatrix(source?: string): P0CoverageEntry[];
    getCoverageSnapshot(source?: string): P0CoverageEntry[];
    getCycleHistory(): P0CycleMetrics[];
    getAccumulatedMetrics(): P0AccumulatedMetrics;
    getSourceProgress(): Record<string, number>;
    getCycleMetrics(): P0CycleMetrics;
    get canonicalWorks(): readonly CanonicalWork[];
    getMatcher(): SourceCatalogMatcher;
    setMatcher(matcher: SourceCatalogMatcher, eligibleCount?: number): void;
}
