import { randomUUID } from 'node:crypto';

/** P0's source catalog is allowed to discover mappings, never canonical works. */
export const P0_ELIGIBLE_WORK_COUNT = 10_749;
export const P2_FROZEN_CANONICAL_WORK_COUNT = 24_945;

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
  query<Row extends Record<string, unknown> = Record<string, unknown>>(
    text: string,
    values?: readonly unknown[],
  ): Promise<P0DatabaseResult<Row>>;
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
export function normalizeCatalogTitle(value: string | null | undefined): string {
  return String(value ?? '')
    .normalize('NFKD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLocaleLowerCase('en-US')
    .replace(/[\p{P}\p{S}]+/gu, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function uniqueNonEmpty(values: Array<string | null | undefined>): string[] {
  return [...new Set(values.map(normalizeCatalogTitle).filter(Boolean))];
}

function trigrams(value: string): Set<string> {
  const padded = `  ${value}  `;
  const result = new Set<string>();
  for (let index = 0; index <= padded.length - 3; index += 1) {
    result.add(padded.slice(index, index + 3));
  }
  return result;
}

function trigramSimilarity(left: string, right: string): number {
  if (left === right) return 1;
  if (!left || !right) return 0;
  const a = trigrams(left);
  const b = trigrams(right);
  let intersection = 0;
  for (const value of a) if (b.has(value)) intersection += 1;
  return (2 * intersection) / (a.size + b.size);
}

interface IndexedTitle {
  work: CanonicalWork;
  method: CatalogMatchMethod;
}

/**
 * In-memory O(M) catalog matcher. Canonical titles/aliases are indexed once;
 * each incoming catalog item then performs O(1) exact lookups and only a
 * bounded trigram comparison when no exact key exists.
 */
export class SourceCatalogMatcher {
  private readonly exactIndex = new Map<string, IndexedTitle[]>();
  private readonly trigramIndex = new Map<string, Array<{ key: string; work: CanonicalWork }>>();

  constructor(readonly canonicalWorks: readonly CanonicalWork[]) {
    for (const work of canonicalWorks) {
      const title = normalizeCatalogTitle(work.title);
      if (title) {
        this.addExact(title, { work, method: 'title' });
        const ref = { key: title, work };
        const padded = `  ${title}  `;
        const seen = new Set<string>();
        for (let i = 0; i <= padded.length - 3; i++) {
          const tri = padded.slice(i, i + 3);
          if (!seen.has(tri)) {
            seen.add(tri);
            let bucket = this.trigramIndex.get(tri);
            if (!bucket) {
              bucket = [];
              this.trigramIndex.set(tri, bucket);
            }
            bucket.push(ref);
          }
        }
      }
      for (const alias of uniqueNonEmpty(work.aliases ?? [])) {
        this.addExact(alias, { work, method: 'alias' });
      }
      const slug = normalizeCatalogTitle(work.slug);
      if (slug) this.addExact(slug, { work, method: 'slug' });
    }
  }

  private addExact(key: string, value: IndexedTitle): void {
    let bucket = this.exactIndex.get(key);
    if (!bucket) {
      bucket = [];
      this.exactIndex.set(key, bucket);
    }
    bucket.push(value);
  }

  match(item: CatalogItem): SourceCatalogMatch | null {
    const candidateKeys = [
      ...uniqueNonEmpty([item.title]),
      ...uniqueNonEmpty(item.aliases ?? []),
      ...uniqueNonEmpty([item.slug]),
    ];
    const exactCandidates = new Map<string, SourceCatalogMatch>();
    for (const key of candidateKeys) {
      const bucket = this.exactIndex.get(key) ?? [];
      const distinctWorks = [...new Map(bucket.map((entry) => [entry.work.id, entry])).values()];
      if (distinctWorks.length !== 1) continue;
      const entry = distinctWorks[0];
      exactCandidates.set(entry.work.id, {
        item,
        work: entry.work,
        method: entry.method,
        normalizedCandidate: key,
        score: 1,
      });
    }
    if (exactCandidates.size === 1) return exactCandidates.values().next().value as SourceCatalogMatch;
    // Conflicting exact aliases are intentionally fail-closed.
    if (exactCandidates.size > 1) return null;

    const titleKey = normalizeCatalogTitle(item.title);
    if (!titleKey || titleKey.length < 4 || titleKey.split(' ').length < 2) return null;
    const indexedCandidates = new Map<string, { key: string; work: CanonicalWork }>();
    for (const trigram of trigrams(titleKey)) {
      for (const entry of this.trigramIndex.get(trigram) ?? []) {
        indexedCandidates.set(`${entry.work.id}:${entry.key}`, entry);
      }
    }
    const ranked = [...indexedCandidates.values()]
      .map((entry) => ({ entry, score: trigramSimilarity(titleKey, entry.key) }))
      .filter((entry) => entry.score >= 0.86)
      .sort((left, right) => right.score - left.score);
    const best = ranked[0];
    const second = ranked[1];
    if (!best || (second && best.score - second.score < 0.04)) return null;
    return {
      item,
      work: best.entry.work,
      method: 'trigram',
      normalizedCandidate: titleKey,
      score: best.score,
    };
  }

  matchMany(items: readonly CatalogItem[]): SourceCatalogMatch[] {
    const result: SourceCatalogMatch[] = [];
    for (const item of items) {
      const match = this.match(item);
      if (match) result.push(match);
    }
    return result;
  }
}

/** A deterministic store useful for unit tests and local dry-runs. */
export class InMemoryP0CyclePersistence implements P0CyclePersistence {
  private cycle: PersistedP0Cycle | null = null;
  readonly mappings = new Map<string, P0MappingInput>();
  readonly existingWorkIds: ReadonlySet<string>;

  constructor(existingWorks: readonly CanonicalWork[] = []) {
    this.existingWorkIds = new Set(existingWorks.map((work) => work.id));
  }

  async loadLatestCycle(): Promise<PersistedP0Cycle | null> {
    return this.cycle ? structuredClone(this.cycle) : null;
  }

  async saveCycle(cycle: PersistedP0Cycle): Promise<void> {
    this.cycle = structuredClone(cycle);
  }

  async persistMapping(input: P0MappingInput): Promise<boolean> {
    // This store models the same safety boundary as SQL: no canonical work,
    // no mapping. It never has an operation that creates a work.
    if (!this.existingWorkIds.has(input.work.id)) return false;
    const key = `${input.source}:${input.item.sourceWorkId}`;
    if (this.mappings.has(key)) return false;
    this.mappings.set(key, input);
    return true;
  }
}

/** SQL persistence for Yugabyte/PostgreSQL, with no write path to `works`. */
export class SqlP0CyclePersistence implements P0CyclePersistence {
  private schemaReady = false;

  constructor(private readonly db: P0Database) {}

  private pkColumn: 'id' | 'cycle_id' = 'cycle_id';

  async ensureSchema(): Promise<void> {
    if (this.schemaReady) return;
    try {
      const res = await this.db.query<any>(`
        SELECT column_name FROM information_schema.columns 
        WHERE table_name = 'importer_p0_cycles' AND column_name IN ('id', 'cycle_id')`);
      if (res.rows.some((r: any) => r.column_name === 'id')) {
        this.pkColumn = 'id';
      } else {
        this.pkColumn = 'cycle_id';
      }
    } catch {
      this.pkColumn = 'cycle_id';
    }
    this.schemaReady = true;
  }

  async loadLatestCycle(): Promise<PersistedP0Cycle | null> {
    await this.ensureSchema();
    const result = await this.db.query<any>(`
      SELECT * FROM public.importer_p0_cycles ORDER BY started_at DESC LIMIT 1`);
    const row = result.rows[0];
    if (!row) return null;
    const cycleId = String(row.id || row.cycle_id || '');
    const completedAt = row.completed_at || row.finished_at;
    const rawMetrics = typeof row.metrics === 'string' ? JSON.parse(row.metrics) : (row.metrics || {});
    const rawMeta = typeof row.metadata === 'string' ? JSON.parse(row.metadata) : (row.metadata || {});
    const meta = { ...rawMeta, ...rawMetrics };
    const rawProgress = typeof row.source_progress === 'string' ? JSON.parse(row.source_progress) : (row.source_progress || {});
    const progress = { ...rawProgress, ...(meta.sourceProgress || {}) };
    return {
      id: cycleId,
      status: row.status,
      startedAt: new Date(row.started_at).toISOString(),
      completedAt: completedAt ? new Date(completedAt).toISOString() : null,
      metrics: meta.metrics || {
        ...defaultMetrics(P2_FROZEN_CANONICAL_WORK_COUNT, Number(row.total_eligible_works || P0_ELIGIBLE_WORK_COUNT)),
        cycleId,
        status: row.status,
        startedAt: new Date(row.started_at).toISOString(),
        completedAt: completedAt ? new Date(completedAt).toISOString() : null,
        durationMs: row.duration_seconds ? Math.round(Number(row.duration_seconds) * 1000) : 0,
        worksVerified: Number(row.unique_works_verified || 0),
        sourcesProcessed: Number(row.sources_inspected || 0),
        newMappingsDiscovered: Number(row.new_mappings_discovered || 0),
      },
      sourceProgress: progress,
      sourceCatalogs: meta.sourceCatalogs || {},
      coverage: meta.coverage || {},
      verifiedWorkIds: meta.verifiedWorkIds || [],
      history: meta.history || [],
      accumulatedMetrics: meta.accumulatedMetrics,
    };
  }

  async saveCycle(cycle: PersistedP0Cycle): Promise<void> {
    await this.ensureSchema();
    const metaPayload = {
      metrics: cycle.metrics,
      sourceProgress: cycle.sourceProgress,
      sourceCatalogs: cycle.sourceCatalogs || {},
      coverage: cycle.coverage || {},
      verifiedWorkIds: cycle.verifiedWorkIds || [],
      history: cycle.history || [],
      accumulatedMetrics: cycle.accumulatedMetrics,
    };
    if (this.pkColumn === 'id') {
      await this.db.query(`
        INSERT INTO public.importer_p0_cycles
          (id, status, started_at, completed_at, metrics, source_progress, updated_at)
        VALUES ($1::uuid, $2, $3::timestamptz, $4::timestamptz, $5::jsonb, $6::jsonb, now())
        ON CONFLICT (id) DO UPDATE SET
          status = EXCLUDED.status,
          completed_at = EXCLUDED.completed_at,
          metrics = EXCLUDED.metrics,
          source_progress = EXCLUDED.source_progress,
          updated_at = now()`,
      [cycle.id, cycle.status, cycle.startedAt, cycle.completedAt, JSON.stringify(metaPayload), JSON.stringify(cycle.sourceProgress)]);
    } else {
      const durationSeconds = cycle.metrics.durationMs ? (cycle.metrics.durationMs / 1000).toFixed(2) : null;
      await this.db.query(`
        INSERT INTO public.importer_p0_cycles
          (cycle_id, status, started_at, finished_at, duration_seconds, total_eligible_works, unique_works_verified, sources_inspected, new_mappings_discovered, metadata)
        VALUES ($1::uuid, $2, $3::timestamptz, $4::timestamptz, $5::numeric, $6::int, $7::int, $8::int, $9::int, $10::jsonb)
        ON CONFLICT (cycle_id) DO UPDATE SET
          status = EXCLUDED.status,
          finished_at = EXCLUDED.finished_at,
          duration_seconds = EXCLUDED.duration_seconds,
          unique_works_verified = EXCLUDED.unique_works_verified,
          sources_inspected = EXCLUDED.sources_inspected,
          new_mappings_discovered = EXCLUDED.new_mappings_discovered,
          metadata = EXCLUDED.metadata`,
      [cycle.id, cycle.status, cycle.startedAt, cycle.completedAt, durationSeconds, cycle.metrics.eligibleWorks,
       cycle.metrics.worksVerified, cycle.metrics.sourcesProcessed, cycle.metrics.newMappingsDiscovered, JSON.stringify(metaPayload)]);
    }
  }

  async persistMapping(input: P0MappingInput): Promise<boolean> {
    // The SELECT from works is the admission fence. There is deliberately no
    // INSERT/UPSERT against works anywhere in this coordinator.
    const inserted = await this.db.query<{ id: string }>(`
      INSERT INTO public.importer_work_mappings
        (source,source_work_id,work_id,source_slug,source_title,sync_status,metadata,last_synced_at,updated_at)
      SELECT $1,$2,w.id,$3,$4,'SYNCED',jsonb_build_object(
        'discovery', 'p0-catalog', 'matchMethod', $5, 'matchScore', $6::numeric,
        'cycleVersion', 'p0-om-v1'),now(),now()
      FROM public.works w
      WHERE w.id=$7::uuid
        AND NOT EXISTS (
          SELECT 1 FROM public.importer_work_mappings m
          WHERE m.source=$1 AND m.source_work_id=$2)
      RETURNING id`, [
      input.source,
      input.item.sourceWorkId,
      input.item.slug || normalizeCatalogTitle(input.item.title).replace(/\s+/g, '-'),
      input.item.title,
      input.method,
      input.score,
      input.work.id,
    ]);
    if ((inserted.rowCount ?? inserted.rows.length) > 0) return true;

    // Repair an unassigned pre-existing source row, still fenced by works.
    const assigned = await this.db.query<{ id: string }>(`
      UPDATE public.importer_work_mappings m
      SET work_id=$3::uuid, source_slug=$4, source_title=$5,
          sync_status='SYNCED', updated_at=now()
      FROM public.works w
      WHERE w.id=$3::uuid AND m.source=$1 AND m.source_work_id=$2 AND m.work_id IS NULL
      RETURNING m.id`, [
      input.source,
      input.item.sourceWorkId,
      input.work.id,
      input.item.slug || normalizeCatalogTitle(input.item.title).replace(/\s+/g, '-'),
      input.item.title,
    ]);
    return (assigned.rowCount ?? assigned.rows.length) > 0;
  }
}

/**
 * Deployment-safe fallback for installations where a migration cannot create
 * a table yet. `settings` is already a durable, service-role-owned table in
 * the importer schema; mapping writes continue to use the same works fence.
 */
export class SettingsP0CyclePersistence implements P0CyclePersistence {
  static readonly KEY = 'importer_p0_cycle_state';

  constructor(private readonly db: P0Database, private readonly mappingPersistence: P0CyclePersistence) {}

  async loadLatestCycle(): Promise<PersistedP0Cycle | null> {
    const result = await this.db.query<{ value: string }>(
      `SELECT value FROM public.settings WHERE key=$1`, [SettingsP0CyclePersistence.KEY]);
    const value = result.rows[0]?.value;
    return value ? JSON.parse(value) as PersistedP0Cycle : null;
  }

  async saveCycle(cycle: PersistedP0Cycle): Promise<void> {
    await this.db.query(`
      INSERT INTO public.settings(key,value) VALUES($1,$2)
      ON CONFLICT(key) DO UPDATE SET value=EXCLUDED.value`,
    [SettingsP0CyclePersistence.KEY, JSON.stringify(cycle)]);
  }

  async persistMapping(input: P0MappingInput): Promise<boolean> {
    return this.mappingPersistence.persistMapping(input);
  }
}

/** Tries the cycle table and falls back to the durable settings row on DDL errors. */
export class ResilientP0CyclePersistence implements P0CyclePersistence {
  private usingFallback = false;

  constructor(private readonly primary: SqlP0CyclePersistence, private readonly fallback: SettingsP0CyclePersistence) {}

  async loadLatestCycle(): Promise<PersistedP0Cycle | null> {
    if (this.usingFallback) return this.fallback.loadLatestCycle();
    try {
      return await this.primary.loadLatestCycle();
    } catch {
      this.usingFallback = true;
      return this.fallback.loadLatestCycle();
    }
  }

  async saveCycle(cycle: PersistedP0Cycle): Promise<void> {
    if (this.usingFallback) return this.fallback.saveCycle(cycle);
    try {
      await this.primary.saveCycle(cycle);
    } catch {
      this.usingFallback = true;
      await this.fallback.saveCycle(cycle);
    }
  }

  async persistMapping(input: P0MappingInput): Promise<boolean> {
    return this.primary.persistMapping(input);
  }
}

function defaultMetrics(canonicalWorks: number, eligibleWorks: number): P0CycleMetrics {
  return {
    cycleId: null,
    status: 'IDLE',
    startedAt: null,
    completedAt: null,
    durationMs: null,
    eligibleWorks,
    canonicalWorks,
    sourcesProcessed: 0,
    catalogItemsSeen: 0,
    worksVerified: 0,
    candidatesMatched: 0,
    ambiguousCandidates: 0,
    unmatchedCandidates: 0,
    newMappingsDiscovered: 0,
  };
}

function defaultAccumulatedMetrics(): P0AccumulatedMetrics {
  return {
    cyclesCompleted: 0,
    sourcesProcessed: 0,
    catalogItemsSeen: 0,
    worksVerified: 0,
    candidatesMatched: 0,
    ambiguousCandidates: 0,
    unmatchedCandidates: 0,
    newMappingsDiscovered: 0,
  };
}

function coverageKey(source: string, workId: string): string {
  return JSON.stringify([source, workId]);
}

function cloneAccumulatedMetrics(value: P0AccumulatedMetrics | undefined): P0AccumulatedMetrics {
  return structuredClone(value || defaultAccumulatedMetrics());
}

function addCycleToAccumulated(
  accumulated: P0AccumulatedMetrics,
  metrics: P0CycleMetrics,
): P0AccumulatedMetrics {
  return {
    cyclesCompleted: accumulated.cyclesCompleted + 1,
    sourcesProcessed: accumulated.sourcesProcessed + metrics.sourcesProcessed,
    catalogItemsSeen: accumulated.catalogItemsSeen + metrics.catalogItemsSeen,
    worksVerified: accumulated.worksVerified + metrics.worksVerified,
    candidatesMatched: accumulated.candidatesMatched + metrics.candidatesMatched,
    ambiguousCandidates: accumulated.ambiguousCandidates + metrics.ambiguousCandidates,
    unmatchedCandidates: accumulated.unmatchedCandidates + metrics.unmatchedCandidates,
    newMappingsDiscovered: accumulated.newMappingsDiscovered + metrics.newMappingsDiscovered,
  };
}

/** Durable lifecycle coordinator for one bounded P0 catalog cycle. */
export class P0CycleCoordinator {
  private readonly matcher: SourceCatalogMatcher;
  private readonly persistence: P0CyclePersistence;
  private readonly eligibleWorkCount: number;
  private readonly canonicalWorkCount: number;
  private readonly now: () => Date;
  private readonly p2AdmissionPolicy?: P2AdmissionPolicy;
  private readonly onNewMappingDiscovered?: (mapping: P0MappingInput) => Promise<void>;
  private readonly negativeCacheTtlMs: number;
  private state: PersistedP0Cycle | null = null;
  private readonly sourceItems = new Map<string, Map<string, CatalogItem>>();
  private readonly verifiedWorkIds = new Set<string>();
  private readonly coverage = new Map<string, P0CoverageEntry>();
  private loaded = false;

  constructor(options: P0CycleCoordinatorOptions) {
    this.matcher = options.matcher;
    if (options.persistence) {
      this.persistence = options.persistence;
    } else if (options.db) {
      const primary = new SqlP0CyclePersistence(options.db);
      this.persistence = new ResilientP0CyclePersistence(
        primary,
        new SettingsP0CyclePersistence(options.db, primary),
      );
    } else {
      this.persistence = new InMemoryP0CyclePersistence(this.matcher.canonicalWorks);
    }
    this.eligibleWorkCount = options.eligibleWorkCount ?? P0_ELIGIBLE_WORK_COUNT;
    this.canonicalWorkCount = options.canonicalWorkCount ?? P2_FROZEN_CANONICAL_WORK_COUNT;
    this.now = options.now ?? (() => new Date());
    this.p2AdmissionPolicy = options.p2AdmissionPolicy;
    this.onNewMappingDiscovered = options.onNewMappingDiscovered;
    this.negativeCacheTtlMs = Math.max(0, options.negativeCacheTtlMs ?? 24 * 60 * 60 * 1000);
  }

  /**
   * Rehydrates the last cycle, including its catalogs and audit matrix. A
   * RUNNING cycle is intentionally resumed with the same id after a reboot.
   */
  async ensureLoaded(): Promise<PersistedP0Cycle | null> {
    if (this.loaded) return this.state ? structuredClone(this.state) : null;
    this.state = await this.persistence.loadLatestCycle();
    this.sourceItems.clear();
    this.coverage.clear();
    this.verifiedWorkIds.clear();
    for (const [source, items] of Object.entries(this.state?.sourceCatalogs || {})) {
      this.sourceItems.set(source, new Map(items.map((item) => [item.sourceWorkId, item])));
    }
    for (const [key, entry] of Object.entries(this.state?.coverage || {})) this.coverage.set(key, entry);
    for (const workId of this.state?.verifiedWorkIds || []) this.verifiedWorkIds.add(workId);
    this.loaded = true;
    return this.state ? structuredClone(this.state) : null;
  }

  private requireRunning(): PersistedP0Cycle {
    if (!this.state || this.state.status !== 'RUNNING') throw new Error('P0 cycle must be RUNNING for this operation');
    return this.state;
  }

  private async persist(): Promise<void> {
    if (!this.state) return;
    this.state.sourceCatalogs = Object.fromEntries(
      [...this.sourceItems.entries()].map(([source, items]) => [source, [...items.values()]]),
    );
    this.state.coverage = Object.fromEntries(this.coverage.entries());
    this.state.verifiedWorkIds = [...this.verifiedWorkIds];
    this.state.history = this.state.history || [];
    this.state.accumulatedMetrics = cloneAccumulatedMetrics(this.state.accumulatedMetrics);
    await this.persistence.saveCycle(this.state);
  }

  private createCycle(): P0CycleMetrics {
    const startedAt = this.now().toISOString();
    const cycleId = randomUUID();
    const metrics = defaultMetrics(this.canonicalWorkCount, this.eligibleWorkCount);
    metrics.cycleId = cycleId;
    metrics.status = 'RUNNING';
    metrics.startedAt = startedAt;
    const priorHistory = this.state?.history || [];
    const priorAccumulated = cloneAccumulatedMetrics(this.state?.accumulatedMetrics);
    this.state = {
      id: cycleId,
      status: 'RUNNING',
      startedAt,
      completedAt: null,
      metrics,
      sourceProgress: {},
      history: priorHistory,
      accumulatedMetrics: priorAccumulated,
    };
    this.sourceItems.clear();
    this.coverage.clear();
    this.verifiedWorkIds.clear();
    return this.getCycleMetrics();
  }

  /** Starts the first cycle or advances a completed cycle to a new cycle id. */
  async resumeOrCreateCycle(): Promise<P0CycleMetrics> {
    await this.ensureLoaded();
    if (this.state?.status === 'RUNNING') return this.getCycleMetrics();
    const metrics = this.createCycle();
    await this.persist();
    return metrics;
  }

  async startCycle(): Promise<P0CycleMetrics> {
    return this.resumeOrCreateCycle();
  }

  /** Explicit alias for callers that want to make the continuous transition visible. */
  async advanceToNextCycle(): Promise<P0CycleMetrics> {
    return this.resumeOrCreateCycle();
  }

  /** P2 suspension only blocks creating canonical works; mappings never do that. */
  isP2Suspended(): boolean {
    return this.p2AdmissionPolicy?.isSuspended() === true;
  }

  canCreateCanonicalWork(): boolean {
    return !this.isP2Suspended();
  }

  assertCanonicalWorkCreationAllowed(): void {
    if (!this.canCreateCanonicalWork()) {
      throw new Error('P2 admission is suspended: canonical work creation is not allowed');
    }
  }

  private coverageEntry(source: string, workId: string): P0CoverageEntry | undefined {
    return this.coverage.get(coverageKey(source, workId));
  }

  private setCoverageEntry(
    source: string,
    workId: string,
    state: P0CoverageState,
    error: string | null = null,
  ): P0CoverageEntry {
    const now = this.now().toISOString();
    const key = coverageKey(source, workId);
    const previous = this.coverage.get(key);
    const entry: P0CoverageEntry = {
      source,
      workId,
      state,
      firstSeenAt: previous?.firstSeenAt || now,
      updatedAt: now,
      lastCheckedAt: state === 'PENDING' ? previous?.lastCheckedAt || null : now,
      nextRevalidationAt: state === 'NOT_FOUND'
        ? new Date(this.now().getTime() + this.negativeCacheTtlMs).toISOString()
        : null,
      attempts: state === 'PENDING' && previous ? previous.attempts : (previous?.attempts || 0) + 1,
      error,
    };
    this.coverage.set(key, entry);
    return entry;
  }

  private prepareSourceCoverage(source: string): void {
    const now = this.now().getTime();
    for (const work of this.matcher.canonicalWorks) {
      const entry = this.coverageEntry(source, work.id);
      if (!entry) {
        this.setCoverageEntry(source, work.id, 'PENDING');
      } else if (entry.state === 'NOT_FOUND' && entry.nextRevalidationAt && now >= Date.parse(entry.nextRevalidationAt)) {
        this.setCoverageEntry(source, work.id, 'STALE');
      }
    }
  }

  async recordSourceCatalog(
    source: string,
    items: readonly CatalogItem[],
    options?: { isFullCatalog?: boolean },
  ): Promise<P0CycleMetrics> {
    await this.ensureLoaded();
    const state = this.requireRunning();
    const cleanSource = source.trim();
    if (!cleanSource) throw new Error('source is required');
    const bucket = this.sourceItems.get(cleanSource) ?? new Map<string, CatalogItem>();
    for (const item of items) {
      if (!item.sourceWorkId || !item.title) continue;
      bucket.set(item.sourceWorkId, { ...item, source: cleanSource });
    }
    this.sourceItems.set(cleanSource, bucket);
    if (options?.isFullCatalog !== false) {
      this.prepareSourceCoverage(cleanSource);
    }
    state.sourceProgress[cleanSource] = bucket.size;
    state.metrics.sourcesProcessed = Object.keys(state.sourceProgress).length;
    state.metrics.catalogItemsSeen = Object.values(state.sourceProgress).reduce((sum, count) => sum + count, 0);
    await this.persist();
    return this.getCycleMetrics();
  }

  async recordSourceCoverage(
    source: string,
    coverageState: P0CoverageState,
    error: string | null = null,
  ): Promise<void> {
    await this.ensureLoaded();
    this.requireRunning();
    const cleanSource = source.trim();
    if (!cleanSource) throw new Error('source is required');
    for (const work of this.matcher.canonicalWorks) {
      const entry = this.coverageEntry(cleanSource, work.id);
      if (!entry || entry.state === 'PENDING' || entry.state === 'STALE') {
        this.setCoverageEntry(cleanSource, work.id, coverageState, error);
      }
    }
    await this.persist();
  }

  async matchCandidateWorks(
    items?: readonly CatalogItem[],
    options?: { isFullCatalog?: boolean },
  ): Promise<SourceCatalogMatch[]> {
    await this.ensureLoaded();
    const state = this.requireRunning();
    const isFullCatalog = options?.isFullCatalog ?? true;
    const candidates = items
      ? [...items]
      : [...this.sourceItems.values()].flatMap((bucket) => [...bucket.values()]);
    const matches: SourceCatalogMatch[] = [];
    const verifiedWorks = new Set<string>();
    let ambiguous = 0;
    for (const item of candidates) {
      const match = this.matcher.match(item);
      if (match) {
        matches.push(match);
        verifiedWorks.add(match.work.id);
        if (item.source && !this.sourceItems.has(item.source)) {
          await this.recordSourceCatalog(item.source, [item], { isFullCatalog: false });
        }
        if (item.source) {
          const mapping: P0MappingInput = {
            source: item.source,
            item,
            work: match.work,
            method: match.method,
            score: match.score,
          };
          const created = await this.persistence.persistMapping(mapping);
          if (created) {
            state.metrics.newMappingsDiscovered += 1;
            if (this.onNewMappingDiscovered) await this.onNewMappingDiscovered(mapping);
          }
          this.setCoverageEntry(item.source, match.work.id, 'MAPPED');
        }
      } else if (normalizeCatalogTitle(item.title)) {
        // The matcher is fail-closed for both ambiguous and unknown titles.
        const possible = this.matcher.canonicalWorks.filter((work) =>
          normalizeCatalogTitle(work.title) === normalizeCatalogTitle(item.title));
        if (possible.length > 1) ambiguous += 1;
      }
    }
    if (isFullCatalog) {
      const processedSources = items
        ? new Set(candidates.map((item) => item.source).filter((source): source is string => Boolean(source)))
        : new Set(this.sourceItems.keys());
      for (const source of processedSources) {
        for (const work of this.matcher.canonicalWorks) {
          const entry = this.coverageEntry(source, work.id);
          if (entry?.state === 'MAPPED') continue;
          if (entry?.state === 'NOT_FOUND' && entry.nextRevalidationAt &&
              this.now().getTime() < Date.parse(entry.nextRevalidationAt)) continue;
          if (!entry || entry.state === 'PENDING' || entry.state === 'STALE') {
            this.setCoverageEntry(source, work.id, 'NOT_FOUND');
          }
        }
      }
    }
    state.metrics.candidatesMatched += matches.length;
    for (const workId of verifiedWorks) {
      if (!this.verifiedWorkIds.has(workId)) {
        this.verifiedWorkIds.add(workId);
        state.metrics.worksVerified += 1;
      }
    }
    state.metrics.ambiguousCandidates += ambiguous;
    state.metrics.unmatchedCandidates += Math.max(0, candidates.length - matches.length - ambiguous);
    await this.persist();
    return matches;
  }

  async completeCycle(): Promise<P0CycleMetrics> {
    await this.ensureLoaded();
    const state = this.requireRunning();
    const completedAt = this.now();
    state.status = 'COMPLETED';
    state.completedAt = completedAt.toISOString();
    state.metrics.status = 'COMPLETED';
    state.metrics.completedAt = state.completedAt;
    state.metrics.durationMs = completedAt.getTime() - new Date(state.startedAt).getTime();
    state.history = [...(state.history || []), structuredClone(state.metrics)];
    state.accumulatedMetrics = addCycleToAccumulated(
      cloneAccumulatedMetrics(state.accumulatedMetrics),
      state.metrics,
    );
    await this.persist();
    return this.getCycleMetrics();
  }

  async setCoverageState(
    source: string,
    workId: string,
    state: P0CoverageState,
    error: string | null = null,
  ): Promise<P0CoverageEntry> {
    await this.ensureLoaded();
    this.requireRunning();
    if (!this.matcher.canonicalWorks.some((work) => work.id === workId)) {
      throw new Error(`Cannot update coverage for unknown canonical work: ${workId}`);
    }
    const entry = this.setCoverageEntry(source.trim(), workId, state, error);
    await this.persist();
    return structuredClone(entry);
  }

  getCoverageMatrix(source?: string): P0CoverageEntry[] {
    const entries = [...this.coverage.values()]
      .filter((entry) => !source || entry.source === source)
      .sort((left, right) => left.source.localeCompare(right.source) || left.workId.localeCompare(right.workId));
    return structuredClone(entries);
  }

  getCoverageSnapshot(source?: string): P0CoverageEntry[] {
    return this.getCoverageMatrix(source);
  }

  getCycleHistory(): P0CycleMetrics[] {
    return structuredClone(this.state?.history || []);
  }

  getAccumulatedMetrics(): P0AccumulatedMetrics {
    return cloneAccumulatedMetrics(this.state?.accumulatedMetrics);
  }

  getCycleMetrics(): P0CycleMetrics {
    if (!this.state) return defaultMetrics(this.canonicalWorkCount, this.eligibleWorkCount);
    return structuredClone(this.state.metrics);
  }

  get canonicalWorks(): readonly CanonicalWork[] {
    return this.matcher.canonicalWorks;
  }

  getMatcher(): SourceCatalogMatcher {
    return this.matcher;
  }
}
