import { randomUUID } from 'node:crypto';

/** P0's source catalog is allowed to discover mappings, never canonical works. */
export const P0_ELIGIBLE_WORK_COUNT = 10_749;
export const P2_FROZEN_CANONICAL_WORK_COUNT = 24_945;

export type P0CycleStatus = 'IDLE' | 'RUNNING' | 'COMPLETED';
export type CatalogMatchMethod = 'title' | 'alias' | 'slug' | 'trigram';

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

export interface PersistedP0Cycle {
  id: string;
  status: P0CycleStatus;
  startedAt: string;
  completedAt: string | null;
  metrics: P0CycleMetrics;
  sourceProgress: Record<string, number>;
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
        for (const trigram of trigrams(title)) {
          const bucket = this.trigramIndex.get(trigram) ?? [];
          if (!bucket.some((entry) => entry.work.id === work.id && entry.key === title)) bucket.push({ key: title, work });
          this.trigramIndex.set(trigram, bucket);
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
    const bucket = this.exactIndex.get(key) ?? [];
    if (!bucket.some((entry) => entry.work.id === value.work.id && entry.method === value.method)) {
      bucket.push(value);
    }
    this.exactIndex.set(key, bucket);
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
    if (this.existingWorkIds.size > 0 && !this.existingWorkIds.has(input.work.id)) return false;
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

  async ensureSchema(): Promise<void> {
    if (this.schemaReady) return;
    await this.db.query(`
      CREATE TABLE IF NOT EXISTS public.importer_p0_cycles (
        cycle_id uuid PRIMARY KEY,
        status text NOT NULL CHECK (status IN ('IDLE','RUNNING','COMPLETED')),
        started_at timestamptz NOT NULL,
        finished_at timestamptz,
        duration_seconds numeric,
        total_eligible_works integer NOT NULL DEFAULT 10749,
        unique_works_verified integer NOT NULL DEFAULT 0,
        sources_inspected integer NOT NULL DEFAULT 0,
        new_mappings_discovered integer NOT NULL DEFAULT 0,
        metadata jsonb NOT NULL DEFAULT '{}'::jsonb
      )`);
    this.schemaReady = true;
  }

  async loadLatestCycle(): Promise<PersistedP0Cycle | null> {
    await this.ensureSchema();
    const result = await this.db.query<{
      cycle_id: string;
      status: P0CycleStatus;
      started_at: string | Date;
      finished_at: string | Date | null;
      duration_seconds: number | string | null;
      total_eligible_works: number;
      unique_works_verified: number;
      sources_inspected: number;
      new_mappings_discovered: number;
      metadata: any;
    }>(`SELECT cycle_id, status, started_at, finished_at, duration_seconds, total_eligible_works, unique_works_verified, sources_inspected, new_mappings_discovered, metadata
        FROM public.importer_p0_cycles ORDER BY started_at DESC LIMIT 1`);
    const row = result.rows[0];
    if (!row) return null;
    const meta = typeof row.metadata === 'string' ? JSON.parse(row.metadata) : (row.metadata || {});
    return {
      id: row.cycle_id,
      status: row.status,
      startedAt: new Date(row.started_at).toISOString(),
      completedAt: row.finished_at ? new Date(row.finished_at).toISOString() : null,
      metrics: meta.metrics || {
        ...defaultMetrics(P2_FROZEN_CANONICAL_WORK_COUNT, Number(row.total_eligible_works || P0_ELIGIBLE_WORK_COUNT)),
        status: row.status,
        startedAt: new Date(row.started_at).toISOString(),
        completedAt: row.finished_at ? new Date(row.finished_at).toISOString() : null,
        durationMs: row.duration_seconds ? Math.round(Number(row.duration_seconds) * 1000) : 0,
        worksVerified: Number(row.unique_works_verified || 0),
        sourcesProcessed: Number(row.sources_inspected || 0),
        newMappingsDiscovered: Number(row.new_mappings_discovered || 0),
      },
      sourceProgress: meta.sourceProgress || {},
    };
  }

  async saveCycle(cycle: PersistedP0Cycle): Promise<void> {
    await this.ensureSchema();
    const durationSeconds = cycle.metrics.durationMs ? (cycle.metrics.durationMs / 1000).toFixed(2) : null;
    const meta = JSON.stringify({ metrics: cycle.metrics, sourceProgress: cycle.sourceProgress });
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
    [
      cycle.id,
      cycle.status,
      cycle.startedAt,
      cycle.completedAt,
      durationSeconds,
      cycle.metrics.eligibleWorks,
      cycle.metrics.worksVerified,
      cycle.metrics.sourcesProcessed,
      cycle.metrics.newMappingsDiscovered,
      meta
    ]);
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

/** Durable lifecycle coordinator for one bounded P0 catalog cycle. */
export class P0CycleCoordinator {
  private readonly matcher: SourceCatalogMatcher;
  private readonly persistence: P0CyclePersistence;
  private readonly eligibleWorkCount: number;
  private readonly canonicalWorkCount: number;
  private readonly now: () => Date;
  private state: PersistedP0Cycle | null = null;
  private readonly sourceItems = new Map<string, Map<string, CatalogItem>>();
  private readonly verifiedWorkIds = new Set<string>();
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
    if (this.canonicalWorkCount !== P2_FROZEN_CANONICAL_WORK_COUNT) {
      throw new Error(`P2 canonical work count is frozen at ${P2_FROZEN_CANONICAL_WORK_COUNT}`);
    }
  }

  private async ensureLoaded(): Promise<void> {
    if (this.loaded) return;
    this.state = await this.persistence.loadLatestCycle();
    this.loaded = true;
  }

  private requireRunning(): PersistedP0Cycle {
    if (!this.state || this.state.status !== 'RUNNING') throw new Error('P0 cycle must be RUNNING for this operation');
    return this.state;
  }

  private async persist(): Promise<void> {
    if (this.state) await this.persistence.saveCycle(this.state);
  }

  async startCycle(): Promise<P0CycleMetrics> {
    await this.ensureLoaded();
    if (this.state?.status === 'RUNNING') return this.getCycleMetrics();
    const startedAt = this.now().toISOString();
    const cycleId = randomUUID();
    const metrics = defaultMetrics(this.canonicalWorkCount, this.eligibleWorkCount);
    metrics.cycleId = cycleId;
    metrics.status = 'RUNNING';
    metrics.startedAt = startedAt;
    this.state = { id: cycleId, status: 'RUNNING', startedAt, completedAt: null, metrics, sourceProgress: {} };
    this.sourceItems.clear();
    this.verifiedWorkIds.clear();
    await this.persist();
    return this.getCycleMetrics();
  }

  async recordSourceCatalog(source: string, items: readonly CatalogItem[]): Promise<P0CycleMetrics> {
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
    state.sourceProgress[cleanSource] = bucket.size;
    state.metrics.sourcesProcessed = Object.keys(state.sourceProgress).length;
    state.metrics.catalogItemsSeen = Object.values(state.sourceProgress).reduce((sum, count) => sum + count, 0);
    await this.persist();
    return this.getCycleMetrics();
  }

  async matchCandidateWorks(items?: readonly CatalogItem[]): Promise<SourceCatalogMatch[]> {
    await this.ensureLoaded();
    const state = this.requireRunning();
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
          await this.recordSourceCatalog(item.source, [item]);
        }
        if (item.source) {
          const created = await this.persistence.persistMapping({
            source: item.source,
            item,
            work: match.work,
            method: match.method,
            score: match.score,
          });
          if (created) state.metrics.newMappingsDiscovered += 1;
        }
      } else if (normalizeCatalogTitle(item.title)) {
        // The matcher is fail-closed for both ambiguous and unknown titles.
        const possible = this.matcher.canonicalWorks.filter((work) =>
          normalizeCatalogTitle(work.title) === normalizeCatalogTitle(item.title));
        if (possible.length > 1) ambiguous += 1;
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
    await this.persist();
    return this.getCycleMetrics();
  }

  getCycleMetrics(): P0CycleMetrics {
    if (!this.state) return defaultMetrics(this.canonicalWorkCount, this.eligibleWorkCount);
    return structuredClone(this.state.metrics);
  }
}
