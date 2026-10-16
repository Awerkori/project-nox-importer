import type { Pool } from 'pg';
import { performance } from 'node:perf_hooks';
import { MAX_IMAGE_BODY_BYTES, readImageBody } from '../core/bounded-body.js';
import { calculateSha256, inspectImage } from '../storage/media.js';
import type { StorageProvider } from '../storage/provider.js';
import type { SourceRegistry } from '../sources/registry.js';
import { LiteRepository, type UploadReceiptInput, type UploadReservationInput } from './repository.js';
import { selectableChapterSources } from './source-selector.js';
import type { ChapterSourceRoute, LiteTask } from './types.js';
import { AsyncSemaphore, SOURCE_CONCURRENCY_LIMITS } from '../core/concurrency.js';
import { HostRateLimiter } from '../core/rate-limiter.js';
import { telemetryCollector } from '../core/telemetry-collector.js';
import { BoundedSamples } from '../core/bounded-samples.js';
import { chapterKind, chapterOrderKey } from './chapter-identity.js';
import { PageByteBudget, type PageByteReservation, unknownPageInitialReservationBytes } from './page-byte-budget.js';

// A pre-upload intent gates Telegram, so keep its join window short. A
// confirmed receipt is deferred and can safely coalesce a little longer while
// the next bounded page wave proceeds. This separates pool amortisation from
// pre-upload latency without changing a full-batch flush or any concurrency.
// Yugabyte's write latency is material relative to an individual page.  At
// the approved six chapter workers the former 120 ms/360 ms joins produced
// mostly 3--4 page writes, which serialized the page pipeline on the small
// shared YSQL pool.  These remain bounded, sub-second joins; the pre-upload
// intent is still committed before Telegram is called and receipts remain
// scalar-only after Telegram has confirmed the page.
export const DURABLE_RESERVATION_BATCH_JOIN_MS = 300;
export const DURABLE_RECEIPT_BATCH_JOIN_MS = 600;

async function mapBounded<T, R>(items: T[], limit: number, fn: (item: T, index: number) => Promise<R>): Promise<R[]> {
  const result = new Array<R>(items.length);
  let cursor = 0;
  let failed = false;
  let firstError: unknown;
  const worker = async () => {
    while (true) {
      // Do not start more network/media work after a sibling failed, but do
      // drain the operations already in flight. Returning immediately from
      // Promise.all would race a late successful receipt against source
      // fallback and could mix two manifests in one durable chapter.
      if (failed) return;
      const index = cursor++;
      if (index >= items.length) return;
      try {
        result[index] = await fn(items[index], index);
      } catch (error) {
        if (!failed) {
          failed = true;
          firstError = error;
        }
        return;
      }
    }
  };
  await Promise.all(Array.from({ length: Math.max(1, Math.min(limit, items.length)) }, worker));
  if (failed) throw firstError;
  return result;
}

type SourceFailure = Error & { sourceFailure?: boolean; retryAfter?: number };
type LegacyMappedChapter = { chapterId: string; publishedAt: Date | null };
type ChapterRouteSnapshot = { routes: ChapterSourceRoute[]; legacy: LegacyMappedChapter | null };

function sourceFailure(error: unknown): SourceFailure {
  const normalized: SourceFailure = error instanceof Error ? error : new Error(String(error || 'SOURCE_FAILURE'));
  normalized.sourceFailure = true;
  return normalized;
}

function isSourceFailure(error: unknown): error is SourceFailure {
  return Boolean((error as SourceFailure | undefined)?.sourceFailure);
}

function hostFor(url: string): string | null {
  try { return new URL(url).host; } catch { return null; }
}

function batchPercentile(values: number[], percentile: number): number {
  if (!values.length) return 0;
  const sorted = [...values].sort((left, right) => left - right);
  return Math.round(sorted[Math.min(sorted.length - 1, Math.floor(sorted.length * percentile))] * 10) / 10;
}

function batchAverage(values: number[]): number {
  if (!values.length) return 0;
  return Math.round((values.reduce((sum, value) => sum + value, 0) / values.length) * 10) / 10;
}

export type UploadBatchSnapshot = {
  sampledFlushes: number;
  totalFlushes: number;
  totalItems: number;
  totalWrites: number;
  failedFlushes: number;
  batchSize: { avg: number; p50: number; p95: number; max: number };
  flushDurationMs: { avg: number; p50: number; p95: number; max: number };
};

/**
 * Small, in-process evidence for the existing durable batchers.  It retains
 * only numeric samples and is emitted with the heartbeat that already exists;
 * it never records a page, task, source, or new database row.
 */
class UploadBatchStats {
  private readonly sizes = new BoundedSamples<number>(256);
  private readonly durations = new BoundedSamples<number>(256);
  private totalFlushes = 0;
  private totalItems = 0;
  private totalWrites = 0;
  private failedFlushes = 0;

  record(size: number, durationMs: number, writes: number, succeeded: boolean): void {
    this.sizes.push(size);
    this.durations.push(Math.max(0, durationMs));
    this.totalFlushes++;
    this.totalItems += size;
    this.totalWrites += writes;
    if (!succeeded) this.failedFlushes++;
  }

  snapshot(): UploadBatchSnapshot {
    const max = (values: number[]) => values.length ? Math.max(...values) : 0;
    return {
      sampledFlushes: this.sizes.length,
      totalFlushes: this.totalFlushes,
      totalItems: this.totalItems,
      totalWrites: this.totalWrites,
      failedFlushes: this.failedFlushes,
      batchSize: { avg: batchAverage(this.sizes), p50: batchPercentile(this.sizes, 0.5), p95: batchPercentile(this.sizes, 0.95), max: max(this.sizes) },
      flushDurationMs: { avg: batchAverage(this.durations), p50: batchPercentile(this.durations, 0.5), p95: batchPercentile(this.durations, 0.95), max: max(this.durations) },
    };
  }
}

/** Persist a safe operational class, never a raw provider response or secret. */
function uploadFailureClass(error: unknown): string {
  const detail = error as {
    code?: unknown;
    statusCode?: unknown;
    message?: unknown;
    cause?: { code?: unknown };
  } | undefined;
  const status = Number(detail?.statusCode);
  if (Number.isInteger(status) && status > 0) return `HTTP_${status}`;
  const code = String(detail?.code || detail?.cause?.code || '').toUpperCase();
  // PostgreSQL/YSQL SQLSTATEs commonly start with a digit (for example
  // 40001).  Keep that safe, structured diagnostic instead of collapsing it
  // into UNKNOWN_ERROR; the raw database message remains deliberately absent.
  if (/^(?:\d[A-Z0-9]{4}|[A-Z]{2}\d{3})$/.test(code)) return `SQLSTATE_${code}`;
  if (/^[A-Z][A-Z0-9_]{1,47}$/.test(code)) return code;
  const message = String(detail?.message || '').toLowerCase();
  if (message.includes('timeout')) return 'TIMEOUT';
  if (message.includes('receipt') || message.includes('database') || message.includes('ysql')) return 'PERSISTENCE_ERROR';
  return 'UNKNOWN_ERROR';
}

function numericDisplayValue(display: string): string | null {
  const normalized = display.trim().replace(',', '.');
  // Keep the textual display separately. This value exists only for legacy
  // integrations that still inspect `chapters.number`; it is never an id.
  return /^\d+(?:\.\d+)?$/.test(normalized) ? normalized : null;
}

/**
 * Coalesce only post-Telegram durable receipts.  The pre-upload intent stays
 * a per-page fence, so a crash can never make a successful send retryable.
 * A bounded join is short relative to the measured durable write and source
 * latencies, while allowing staggered pages in one bounded wave to share a
 * write. It adds no queue, state, or retry path and a full batch still flushes
 * immediately.
 */
export class UploadReceiptBatcher {
  private pending: Array<{ input: UploadReceiptInput; resolve: () => void; reject: (error: unknown) => void }> = [];
  private flushTimer: NodeJS.Timeout | null = null;
  private flushing = false;
  private readonly stats = new UploadBatchStats();

  constructor(private readonly queue: LiteRepository) {}

  persist(input: UploadReceiptInput): Promise<void> {
    return new Promise((resolve, reject) => {
      this.pending.push({ input, resolve, reject });
      if (this.pending.length >= 48) {
        void this.flush();
      } else if (!this.flushTimer) {
        this.flushTimer = setTimeout(() => void this.flush(), DURABLE_RECEIPT_BATCH_JOIN_MS);
        this.flushTimer.unref();
      }
    });
  }

  snapshot(): UploadBatchSnapshot { return this.stats.snapshot(); }

  private async flush(): Promise<void> {
    if (this.flushTimer) {
      clearTimeout(this.flushTimer);
      this.flushTimer = null;
    }
    // A timer can fire while a previous database flush is still awaiting its
    // reply. Clear that consumed timer before returning, otherwise the first
    // flush sees a non-null (already-fired) handle and leaves later receipts
    // stranded in memory.
    if (this.flushing) return;
    this.flushing = true;
    try {
      if (this.pending.length) {
        const batch = this.pending.splice(0, 48);
        const startedAt = performance.now();
        let writes = 0;
        let succeeded = false;
        try {
          writes++;
          await this.queue.recordUploadReceipts(batch.map((entry) => entry.input));
          for (const entry of batch) entry.resolve();
          succeeded = true;
        } catch (error) {
          // Telegram has already confirmed these exact media ids. Retrying the
          // idempotent YSQL receipt write once is safe and avoids turning a
          // transient pool/serialization failure into an ambiguous re-upload.
          // A second failure still reaches the existing UNKNOWN safety fence.
          try {
            writes++;
            await this.queue.recordUploadReceipts(batch.map((entry) => entry.input));
            for (const entry of batch) entry.resolve();
            succeeded = true;
          } catch (retryError) {
            for (const entry of batch) entry.reject(retryError);
          }
        } finally {
          this.stats.record(batch.length, performance.now() - startedAt, writes, succeeded);
        }
      }
    } finally {
      this.flushing = false;
      // Arrivals collected while YSQL was busy deserve the same bounded join
      // window as the first arrival.  Draining them immediately made the
      // configured join interval ineffective for sustained page waves.
      if (this.pending.length >= 48) {
        void this.flush();
      } else if (this.pending.length && !this.flushTimer) {
        this.flushTimer = setTimeout(() => void this.flush(), DURABLE_RECEIPT_BATCH_JOIN_MS);
        this.flushTimer.unref();
      }
    }
  }
}

/**
 * Coalesce only the durable pre-upload intents.  The batch is committed before
 * any caller receives its media id, so it preserves the crash fence that
 * prevents ambiguous Telegram sends from being replayed.
 */
export class UploadReservationBatcher {
  private pending: Array<{
    input: UploadReservationInput;
    resolve: (value: { mediaId: string; state: string }) => void;
    reject: (error: unknown) => void;
  }> = [];
  private flushTimer: NodeJS.Timeout | null = null;
  private flushing = false;
  private readonly stats = new UploadBatchStats();

  constructor(private readonly queue: LiteRepository) {}

  reserve(input: UploadReservationInput): Promise<{ mediaId: string; state: string }> {
    return new Promise((resolve, reject) => {
      this.pending.push({ input, resolve, reject });
      if (this.pending.length >= 48) {
        void this.flush();
      } else if (!this.flushTimer) {
        this.flushTimer = setTimeout(() => void this.flush(), DURABLE_RESERVATION_BATCH_JOIN_MS);
        this.flushTimer.unref();
      }
    });
  }

  snapshot(): UploadBatchSnapshot { return this.stats.snapshot(); }

  private async flush(): Promise<void> {
    if (this.flushTimer) {
      clearTimeout(this.flushTimer);
      this.flushTimer = null;
    }
    // See UploadReceiptBatcher: a fired timer must not suppress the follow-up
    // batch while an earlier YSQL write is still in flight.
    if (this.flushing) return;
    this.flushing = true;
    try {
      if (this.pending.length) {
        const batch = this.pending.splice(0, 48);
        const startedAt = performance.now();
        let succeeded = false;
        try {
          const reservations = await this.queue.reserveUploads(batch.map((entry) => entry.input));
          if (reservations.length !== batch.length) throw new Error('UPLOAD_RESERVATION_BATCH_INCOMPLETE');
          for (let index = 0; index < batch.length; index++) batch[index].resolve(reservations[index]);
          succeeded = true;
        } catch (error) {
          for (const entry of batch) entry.reject(error);
        } finally {
          this.stats.record(batch.length, performance.now() - startedAt, 1, succeeded);
        }
      }
    } finally {
      this.flushing = false;
      // See UploadReceiptBatcher: queue a bounded next join after an in-flight
      // YSQL write instead of serialising every arriving page immediately.
      if (this.pending.length >= 48) {
        void this.flush();
      } else if (this.pending.length && !this.flushTimer) {
        this.flushTimer = setTimeout(() => void this.flush(), DURABLE_RESERVATION_BATCH_JOIN_MS);
        this.flushTimer.unref();
      }
    }
  }
}

/**
 * The canonical chapter mapping is unique in SQL, but a stale worker can race
 * a recovered worker before either observes that mapping. A stable UUID makes
 * the underlying Reader chapter insert idempotent as well, so that race can
 * never leave an orphan duplicate `chapters` row behind.
 */
function canonicalChapterId(workId: string, chapterKey: string): string {
  const hex = calculateSha256(Buffer.from(`project-nox:lite-chapter:${workId}:${chapterKey}`)).slice(0, 32);
  const variant = ((Number.parseInt(hex[16], 16) & 0x3) | 0x8).toString(16);
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-5${hex.slice(13, 16)}-${variant}${hex.slice(17, 20)}-${hex.slice(20, 32)}`;
}

/**
 * Buffer ownership is intentionally distinct even for minimal/test executor
 * construction. Reusing `downloads` would self-deadlock at capacity one,
 * because a page holds a buffer permit while it acquires a request permit.
 */
function defaultBufferedPages(downloads: AsyncSemaphore): AsyncSemaphore {
  return new AsyncSemaphore(downloads.capacity, 'lite_default_page_buffers');
}

export class LiteExecutor {
  private readonly sourceSlots = new Map<string, AsyncSemaphore>();
  private readonly receiptBatcher: UploadReceiptBatcher;
  private readonly reservationBatcher: UploadReservationBatcher;
  private fairDemandSources = new Set<string>();
  // Held from byte download through Telegram acknowledgement. Receipt
  // persistence retains only scalar metadata, never image bytes, so it can
  // overlap the next bounded page wave without weakening the memory budget.
  // This is distinct from request concurrency: it bounds real image buffers
  // across every chapter worker while Telegram is slow or cooling down.
  constructor(
    private readonly db: Pool,
    private readonly queue: LiteRepository,
    private readonly registry: SourceRegistry,
    private readonly storage: StorageProvider,
    private readonly importerUserId: string,
    private readonly downloads: AsyncSemaphore,
    private readonly perChapterPageConcurrency = 4,
    private readonly bufferedPages: AsyncSemaphore = defaultBufferedPages(downloads),
    private readonly sourceRateLimiter: HostRateLimiter = new HostRateLimiter(2),
    private readonly chapterWorkerCapacity = 1,
    private readonly onRouteSelected?: (taskId: string, source: string) => void,
    private readonly pageBytes = new PageByteBudget(),
  ) {
    this.receiptBatcher = new UploadReceiptBatcher(queue);
    this.reservationBatcher = new UploadReservationBatcher(queue);
  }

  /**
   * A bounded runtime sample of sources with ready imports. It only controls
   * local execution permits; the durable queue remains authority for task
   * existence, priority and work-level round-robin.
   */
  setFairSourceDemand(sources: Iterable<string>): void {
    this.fairDemandSources = new Set([...sources].filter(Boolean));
    for (const [source, slot] of this.sourceSlots) slot.setCapacity(this.sourceCapacity(source));
  }

  getFairSourceDemand(): string[] { return [...this.fairDemandSources].sort(); }

  /** Aggregate-only batch evidence for the existing heartbeat. */
  getUploadBatchSnapshot(): { reservations: UploadBatchSnapshot; receipts: UploadBatchSnapshot } {
    return { reservations: this.reservationBatcher.snapshot(), receipts: this.receiptBatcher.snapshot() };
  }

  private sourceCapacity(source: string): number {
    const configured = SOURCE_CONCURRENCY_LIMITS[source]?.maxChapters ?? 2;
    if (this.fairDemandSources.size <= 1 || !this.fairDemandSources.has(source)) return configured;
    const fairShare = Math.max(1, Math.ceil(Math.max(1, this.chapterWorkerCapacity) /
      Math.min(5, this.fairDemandSources.size)));
    return Math.min(configured, fairShare);
  }

  private hasFairAlternative(source: string): boolean {
    return this.fairDemandSources.size > 1 &&
      [...this.fairDemandSources].some((candidate) => candidate !== source);
  }

  private sourceSlot(source: string): AsyncSemaphore {
    // A confirmed fallback can become the actual producer after the bounded
    // sample was taken. Fold it into the current fair set immediately so it
    // cannot bypass the same per-source cap until the next refresh.
    if (this.fairDemandSources.size > 0 && !this.fairDemandSources.has(source)) {
      this.fairDemandSources.add(source);
      for (const [knownSource, knownSlot] of this.sourceSlots) knownSlot.setCapacity(this.sourceCapacity(knownSource));
    }
    let slot = this.sourceSlots.get(source);
    if (!slot) {
      const configured = SOURCE_CONCURRENCY_LIMITS[source]?.maxChapters ?? 2;
      slot = new AsyncSemaphore(configured, `lite_source_${source}`);
      this.sourceSlots.set(source, slot);
    }
    slot.setCapacity(this.sourceCapacity(source));
    return slot;
  }

  private async routes(task: LiteTask): Promise<ChapterRouteSnapshot> {
    if (!task.workId || !task.chapterKey) return { routes: [], legacy: null };
    const result = await this.db.query<any>(
      `SELECT s.source,s.source_work_id,s.source_chapter_id,s.canonical_chapter_key,s.work_id,s.language,s.edition,s.source_rank,
         s.confirmed,s.has_chapter,s.created_at,COALESCE(h.health,'HEALTHY') health,h.cooldown_until,
         p.enabled,p.status,p.cooldown_until AS source_cooldown,
         COALESCE((to_jsonb(p)->>'chapter_ingestion_enabled')::boolean,true) AS chapter_ingestion_enabled,
         c.id AS legacy_chapter_id,c.published_at AS legacy_published_at
       FROM importer_lite_chapter_sources s
       JOIN importer_sources p ON p.id=s.source
       LEFT JOIN importer_lite_source_health h ON h.source=s.source
       LEFT JOIN importer_chapter_mappings m ON m.source=s.source AND m.source_chapter_id=s.source_chapter_id
       LEFT JOIN chapters c ON c.id=m.chapter_id AND c.work_id=s.work_id
       WHERE s.work_id=$1::uuid AND s.canonical_chapter_key=$2`, [task.workId, task.chapterKey],
    );
    // Every import formerly issued a second, almost-identical indexed lookup
    // solely to find an exact legacy mapping.  Carry that scalar through the
    // route snapshot instead.  It is still constrained to the canonical work
    // and confirmed source identity, and createOrLoadChapter retains its
    // conservative query for callers without this snapshot.
    const legacyRow = result.rows
      .filter((row) => row.confirmed === true && row.legacy_chapter_id)
      .sort((left, right) => Number(left.source_rank || 0) - Number(right.source_rank || 0) ||
        String(left.created_at || '').localeCompare(String(right.created_at || '')))[0];
    const legacy = legacyRow ? {
      chapterId: String(legacyRow.legacy_chapter_id),
      publishedAt: legacyRow.legacy_published_at ? new Date(legacyRow.legacy_published_at) : null,
    } : null;
    const routes = result.rows.map((row) => {
      const status = String(row.status || 'ACTIVE').toUpperCase();
      const healthCooldown = row.cooldown_until ? new Date(row.cooldown_until) : null;
      const sourceCooldown = row.source_cooldown ? new Date(row.source_cooldown) : null;
      // Keep this admission rule exactly aligned with LiteRepository.sourcePolicy:
      // an administrative cooldown with a passed durable deadline resumes the
      // source.  Treating it as disabled here made catalog materialize P1 work
      // which the executor immediately blocked as ALL_ROUTES_UNHEALTHY.
      const administrativeCooldownExpired = status === 'COOLDOWN' && Boolean(sourceCooldown && sourceCooldown <= new Date());
      const sourceEnabled = row.enabled === true && row.chapter_ingestion_enabled !== false &&
        (['ACTIVE', 'DEGRADED', 'RECOVERING'].includes(status) || administrativeCooldownExpired);
      const cooldownUntil = [healthCooldown, sourceCooldown]
        .filter((value): value is Date => Boolean(value && Number.isFinite(value.getTime())))
        .sort((a, b) => b.getTime() - a.getTime())[0] || null;
      return {
      source: row.source, sourceWorkId: row.source_work_id, sourceChapterId: row.source_chapter_id, canonicalChapterKey: row.canonical_chapter_key,
      workId: row.work_id, language: row.language, edition: row.edition, sourceRank: row.source_rank,
      confirmed: row.confirmed, hasChapter: row.has_chapter, health: sourceEnabled ? row.health : 'DISABLED',
      cooldownUntil,
    };
    });
    return { routes, legacy };
  }

  /**
   * Existing legacy mappings are exact source identities, unlike a numeric
   * display value. Reuse them during migration so Lite never overwrites a
   * published editorial chapter merely because its own mapping table is new.
   */
  private async legacyMappedChapter(task: LiteTask, db: Pick<Pool, 'query'> = this.db): Promise<LegacyMappedChapter | null> {
    if (!task.workId || !task.chapterKey) return null;
    const result = await db.query<{ chapter_id: string; published_at: Date | string | null }>(`SELECT m.chapter_id,c.published_at
      FROM importer_lite_chapter_sources s
      JOIN importer_chapter_mappings m ON m.source=s.source AND m.source_chapter_id=s.source_chapter_id
      JOIN chapters c ON c.id=m.chapter_id
      WHERE s.work_id=$1::uuid AND s.canonical_chapter_key=$2 AND s.confirmed=true
        AND m.chapter_id IS NOT NULL AND c.work_id=$1::uuid
      ORDER BY s.source_rank,s.created_at LIMIT 1`, [task.workId, task.chapterKey]);
    const row = result.rows[0];
    if (!row?.chapter_id) return null;
    return { chapterId: row.chapter_id, publishedAt: row.published_at ? new Date(row.published_at) : null };
  }

  private async mapCanonicalChapter(client: Pick<Pool, 'query'>, task: LiteTask, chapterId: string): Promise<string> {
    const display = String(task.payload.displayNumber || '0');
    const mapped = await client.query<{ chapter_id: string }>(
      `INSERT INTO importer_lite_chapters(work_id,canonical_chapter_key,chapter_id,display_number,order_key,edition,language)
       VALUES($1::uuid,$2,$3::uuid,$4,$5,$6,$7)
       ON CONFLICT(work_id,canonical_chapter_key) DO UPDATE SET chapter_id=importer_lite_chapters.chapter_id
       RETURNING chapter_id`,
      [task.workId, task.chapterKey, chapterId, display, String(task.payload.orderKey || display),
        String(task.payload.edition || 'default'), String(task.payload.language || 'und')],
    );
    return mapped.rows[0].chapter_id;
  }

  private async createOrLoadChapter(task: LiteTask, knownLegacy?: LegacyMappedChapter | null): Promise<string> {
    const display = String(task.payload.displayNumber || '0');
    const numeric = numericDisplayValue(display);
    const language = String(task.payload.language || 'und');
    const edition = String(task.payload.edition || 'default');
    const kind = String(task.payload.chapterKind || chapterKind({ displayNumber: display, title: String(task.payload.chapterTitle || '') }));
    const orderKey = String(task.payload.orderKey || chapterOrderKey({ displayNumber: display, title: String(task.payload.chapterTitle || ''), kind }));
    const chapterId = canonicalChapterId(String(task.workId), String(task.chapterKey));
    const client = await this.db.connect();
    try {
      await client.query('BEGIN');
      // importChapter already performs this exact indexed lookup before any
      // source work. Reuse that scalar result so a normal new chapter does
      // not queue a duplicate YSQL read behind the two-connection pool.
      // Callers without that snapshot retain the conservative lookup.
      const legacy = knownLegacy === undefined ? await this.legacyMappedChapter(task, client) : knownLegacy;
      if (legacy) {
        const mapped = await this.mapCanonicalChapter(client, task, legacy.chapterId);
        await client.query('COMMIT');
        return mapped;
      }
      const title = String(task.payload.chapterTitle || '').slice(0, 200);
      let mapped: { rows: Array<{ chapter_id: string }> };
      try {
        // The common fresh-chapter path used three sequential YSQL requests:
        // check Lite mapping, materialize Reader chapter, then create Lite
        // mapping.  The mapping is the durable identity fence, so select it
        // first *inside one statement* and materialize a Reader row only when
        // absent.  This preserves recovery races and avoids holding a client
        // while any network work occurs.
        mapped = await client.query<{ chapter_id: string }>(`WITH existing_mapping AS (
            SELECT chapter_id FROM importer_lite_chapters
            WHERE work_id=$2::uuid AND canonical_chapter_key=$5
          ), durable_chapter AS (
            INSERT INTO chapters(
              id,work_id,number,display_number,canonical_chapter_key,chapter_order_key,chapter_kind,edition,language,title,origin
            )
            SELECT $1::uuid,$2::uuid,$3::numeric,$4,$5,$6,$7,$8,$9,$10,'IMPORTER'
            WHERE NOT EXISTS (SELECT 1 FROM existing_mapping)
            ON CONFLICT (work_id,canonical_chapter_key) WHERE canonical_chapter_key IS NOT NULL
            DO UPDATE SET canonical_chapter_key=EXCLUDED.canonical_chapter_key
            RETURNING id
          ), chosen_chapter AS (
            SELECT chapter_id FROM existing_mapping
            UNION ALL
            SELECT id AS chapter_id FROM durable_chapter
          )
          INSERT INTO importer_lite_chapters(work_id,canonical_chapter_key,chapter_id,display_number,order_key,edition,language)
          SELECT $2::uuid,$5,chapter_id,$4,$6,$8,$9 FROM chosen_chapter
          ON CONFLICT(work_id,canonical_chapter_key) DO UPDATE SET chapter_id=importer_lite_chapters.chapter_id
          RETURNING chapter_id`, [chapterId, task.workId, numeric, display, task.chapterKey, orderKey, kind, edition, language, title]);
      } catch (error: any) {
        // This can only occur when an old Reader numeric unique constraint is
        // still installed. Never merge distinct text identities to bypass it.
        if (error?.code === '23505' || /chapters.*number|unique constraint/i.test(String(error?.message || ''))) {
          throw new Error('CHAPTER_NUMBER_COLLISION_REQUIRES_READER_MIGRATION');
        }
        if (/canonical_chapter_key|display_number|chapter_order_key/i.test(String(error?.message || ''))) {
          throw new Error('READER_CANONICAL_IDENTITY_MIGRATION_REQUIRED');
        }
        throw error;
      }
      const durableChapterId = mapped.rows[0]?.chapter_id;
      if (!durableChapterId) throw new Error('CANONICAL_CHAPTER_UPSERT_DID_NOT_RETURN_ID');
      await client.query('COMMIT');
      return durableChapterId;
    } catch (error) { await client.query('ROLLBACK').catch(() => {}); throw error; } finally { client.release(); }
  }

  async importChapter(task: LiteTask, workerId: string): Promise<void> {
    const routeSnapshot = await this.routes(task);
    const legacy = routeSnapshot.legacy;
    if (legacy?.publishedAt) {
      const chapterId = await this.createOrLoadChapter(task, legacy);
      await this.queue.finish(task, workerId, {
        state: 'DONE', checkpoint: { chapterId, publishedAt: legacy.publishedAt.toISOString(), legacyPublishedReuse: true },
      });
      return;
    }
    // One durable snapshot serves both source pinning and every page.  Fresh
    // chapters have no receipts, so this replaces N position probes with one
    // indexed read without allowing a cache to become authority.
    const uploadReceipts = await this.queue.uploadReceipts(task.id);
    const hasPersistedUpload = [...uploadReceipts.values()].some((receipt) => receipt.state === 'PERSISTED');
    const routeSet = routeSnapshot.routes;
    const selection = selectableChapterSources(routeSet, { language: String(task.payload.language || 'und'), edition: String(task.payload.edition || 'default') });
    if (!selection.routes.length) {
      const until = routeSet.map((route) => route.cooldownUntil?.getTime() || 0).filter(Boolean).sort()[0];
      await this.queue.finish(task, workerId, {
        state: selection.reason === 'ALL_ROUTES_COOLDOWN' ? 'RETRY_WAIT' : 'BLOCKED', reason: selection.reason,
        nextRunAt: until ? new Date(until) : undefined,
      });
      return;
    }
    const pinnedSource = typeof task.checkpoint.selectedSource === 'string' ? task.checkpoint.selectedSource : null;
    const routes = hasPersistedUpload
      ? selection.routes.filter((route) => route.source === pinnedSource)
      : selection.routes;
    if (!routes.length) {
      await this.queue.finish(task, workerId, {
        state: 'BLOCKED',
        reason: hasPersistedUpload ? 'PARTIAL_UPLOAD_SOURCE_ROUTE_UNAVAILABLE' : 'NO_CONFIRMED_ROUTE',
      });
      return;
    }
    let lastError: unknown = null;
    // A failed preferred source must not make a canonical chapter wait for a
    // confirmed, healthy alternative.  This loop is bounded by its routes.
    for (const route of routes) {
      this.onRouteSelected?.(task.id, route.source);
      const adapter = this.registry.get(route.source);
      if (!adapter) { lastError = new Error('SOURCE_ADAPTER_UNAVAILABLE'); continue; }
      if (adapter.isChapterOwnedByWork && (!route.sourceWorkId || !adapter.isChapterOwnedByWork(route.sourceWorkId, route.sourceChapterId))) {
        lastError = new Error('SOURCE_CHAPTER_OWNERSHIP_UNVERIFIED');
        continue;
      }
      const sourceSlot = this.sourceSlot(route.source);
      // A hot source must not keep a worker parked when the bounded runtime
      // sample has a healthy alternative ready. This is a durable yield, not
      // a retry: no provider call has started and the task keeps its priority.
      if (!sourceSlot.tryAcquire()) {
        if (this.hasFairAlternative(route.source)) {
          await this.queue.yieldForSourceFairness(task, workerId);
          return;
        }
        await sourceSlot.acquire();
      }
      try {
        // Telegram has accepted these pages, but their durable receipts still
        // need the small YSQL pool. Do not make every page worker idle behind
        // that write: queue the scalar receipt, continue the bounded wave,
        // and drain every receipt before the chapter can advance. A crash in
        // this interval leaves its pre-Telegram intent durable and recovery
        // treats the result as UNKNOWN rather than sending the page again.
        const deferredReceipts: Promise<void>[] = [];
        const drainDeferredReceipts = async (): Promise<void> => {
          const settled = await Promise.allSettled(deferredReceipts);
          const rejected = settled.find((result): result is PromiseRejectedResult => result.status === 'rejected');
          if (rejected) throw rejected.reason;
        };
        let urls: string[];
        try {
          urls = await adapter.fetchChapterPages(route.sourceChapterId, Number(task.payload.displayNumber));
          if (!urls.length || urls.length > 1000) throw new Error('INVALID_PAGE_MANIFEST');
        } catch (error) { throw sourceFailure(error); }
        const pageManifestHash = calculateSha256(Buffer.from(JSON.stringify(urls)));
        const previousManifestHash = typeof task.checkpoint.pageManifestHash === 'string' ? task.checkpoint.pageManifestHash : null;
        if (hasPersistedUpload && previousManifestHash && previousManifestHash !== pageManifestHash) {
          throw new Error('SOURCE_PAGE_MANIFEST_CHANGED_AFTER_PARTIAL_STORAGE');
        }
        // Persist the chosen source and manifest before storing page bytes.
        // From this point a partial chapter can only resume from the exact
        // same route; otherwise a valid alternative could create a mixed
        // edition without any detectable corruption.
        if (!await this.queue.checkpoint(task, workerId, {
          selectedSource: route.source, sourceChapterId: route.sourceChapterId, pageManifestHash, plannedPageCount: urls.length,
        })) throw new Error('LEASE_LOST_BEFORE_MEDIA_STORAGE');
        const chapterId = await this.createOrLoadChapter(task, legacy);
        let mediaIds: string[];
        try {
          mediaIds = await mapBounded(urls, this.perChapterPageConcurrency, async (url, index) => {
          const position = index + 1;
          const receipt = uploadReceipts.get(position);
          if (receipt?.state === 'PERSISTED' && receipt.storageReady === true && Number(receipt.bytes) > 0 &&
              String(receipt.mime || '').startsWith('image/') && receipt.mediaSha256 === receipt.sha256) return receipt.mediaId;
          if (receipt?.state === 'UNKNOWN') throw new Error(`UPLOAD_OUTCOME_UNKNOWN:${position}`);
          const host = hostFor(url);
          // A count permit owns only the active HTTP body/validation phase.
          // Reserve bounded room before taking the global request slot. A
          // provider can omit Content-Length (MangaFlix does in production),
          // and waiting for bytes after its headers had arrived previously
          // stranded global request permits on non-reading responses. The
          // one-MiB initial reservation is upgraded by readImageBody before
          // an oversized chunk is accepted, with the same 20-MiB terminal
          // headroom as the former header-first admission.
          const pageSignal = AbortSignal.timeout(45_000);
          let pageBufferHeld = false;
          const releasePageBuffer = () => {
            if (!pageBufferHeld) return;
            pageBufferHeld = false;
            this.bufferedPages.release();
          };
          // A holder keeps the pre-admitted reservation visible to the outer
          // cleanup through download, upload, and receipt persistence.
          const byteReservation: { value: PageByteReservation | null } = { value: null };
          try {
            let downloaded: { bytes: Uint8Array; image: ReturnType<typeof inspectImage> };
            try {
              if (host) await this.sourceRateLimiter.acquire(host, pageSignal);
              const initialReservationBytes = unknownPageInitialReservationBytes(this.pageBytes.capacityBytes);
              const reservation = await this.pageBytes.reserve(
                initialReservationBytes,
                pageSignal,
                { completionHeadroomBytes: MAX_IMAGE_BODY_BYTES - initialReservationBytes },
              );
              byteReservation.value = reservation;
              // A task awaiting page bytes owns neither a global HTTP slot
              // nor a page-body count permit. Once admitted, the count gate
              // still bounds active response bodies independently of uploads.
              await this.bufferedPages.acquire(pageSignal);
              pageBufferHeld = true;
              downloaded = await this.downloads.runExclusive(async () => {
                const startedAt = performance.now();
                telemetryCollector.trackActiveDownload(1);
                try {
                  const response = await fetch(url, { headers: await adapter.getImageHeaders?.(url), signal: pageSignal });
                  if (!response.ok) {
                    if (response.status === 429 && host) this.sourceRateLimiter.handle429(host, response.headers.get('retry-after'));
                    const error = sourceFailure(new Error(`PAGE_HTTP_${response.status}`));
                    error.retryAfter = Number(response.headers.get('retry-after') || 0);
                    throw error;
                  }
                  const bytes = await readImageBody(response, { signal: pageSignal, reservation });
                  const image = inspectImage(bytes);
                  if (host) this.sourceRateLimiter.recordSuccess(host);
                  telemetryCollector.recordImageDownload(route.source, performance.now() - startedAt, bytes.length);
                  return { bytes, image };
                } finally {
                  telemetryCollector.trackActiveDownload(-1);
                }
              });
            } catch (error) { throw sourceFailure(error); }
            const { bytes, image } = downloaded;
            const sha256 = calculateSha256(bytes);
            const reserved = await this.reservationBatcher.reserve({ taskId: task.id, pagePosition: position, sha256 });
            if (reserved.state === 'PERSISTED') return reserved.mediaId;
            if (reserved.state === 'UNKNOWN') throw new Error(`UPLOAD_OUTCOME_UNKNOWN:${position}`);
            // The body remains byte-reserved through storage and receipt
            // persistence, so this only frees the count gate for another
            // download; it cannot increase retained media beyond the
            // bounded PageByteBudget.
            releasePageBuffer();
            try {
                const uploadStartedAt = performance.now();
                telemetryCollector.trackActiveTelegramUpload(1);
                let providerKey: string;
                try {
                  providerKey = await this.storage.upload(bytes, image.mime, reserved.mediaId, chapterId);
                  telemetryCollector.recordTelegramUpload(performance.now() - uploadStartedAt, bytes.length);
                } finally {
                  telemetryCollector.trackActiveTelegramUpload(-1);
                }
                const receipt = this.receiptBatcher.persist({
                  taskId: task.id, pagePosition: position, mediaId: reserved.mediaId, sha256,
                  bytes: bytes.length, mime: image.mime, width: image.width, height: image.height, providerKey,
                  botReference: this.storage.getLastBotReference?.(reserved.mediaId) || null,
                  shardId: this.storage.getLastShardId?.(reserved.mediaId) || null,
                  channelId: this.storage.getLastChannelId?.(reserved.mediaId) || null, createdBy: this.importerUserId,
                  messageId: this.storage.getLastMessageId?.(reserved.mediaId) || null,
                }).catch(async (error) => {
                  await this.queue.checkpoint(task, workerId, {
                    lastUploadFailure: {
                      position, phase: 'RECEIPT_PERSISTENCE', class: uploadFailureClass(error), at: new Date().toISOString(),
                    },
                  });
                  await this.queue.markUploadUnknown(task.id, position);
                  throw new Error(`UPLOAD_OUTCOME_UNKNOWN:${position}`);
                });
                // A rejected promise can happen while sibling page work is
                // still running. Attach a no-op handler now; the durable
                // drain below remains the authority that propagates it.
                void receipt.catch(() => {});
                deferredReceipts.push(receipt);
                return reserved.mediaId;
            } catch (error) {
                // A confirmed Telegram success followed by a DB failure is
                // ambiguous. Network failures without an explicit Telegram HTTP
                // rejection are ambiguous too: a response can be lost after the
                // server accepted the upload. Only known HTTP rejections (notably
                // 429) may retry with the reserved media ID.
                if (!(error as any)?.statusCode) {
                  // UNKNOWN is intentionally terminal for this page: retain a
                  // redacted, durable stage/class so production can distinguish
                  // Telegram transport from post-upload receipt persistence
                  // without logging provider responses or credentials.
                  await this.queue.checkpoint(task, workerId, {
                    lastUploadFailure: {
                      position,
                      phase: 'TELEGRAM_TRANSPORT',
                      class: uploadFailureClass(error),
                      at: new Date().toISOString(),
                    },
                  });
                  await this.queue.markUploadUnknown(task.id, position);
                  throw new Error(`UPLOAD_OUTCOME_UNKNOWN:${position}`);
                }
                await this.queue.markUploadRetryable(task.id, position);
                throw error;
            }
          } finally {
            byteReservation.value?.release();
            releasePageBuffer();
          }
        });
        } catch (error) {
          // Do not let source fallback inspect an incomplete durable receipt
          // wave. If a receipt failed, its UNKNOWN fence is more important
          // than the source error that happened alongside it.
          try { await drainDeferredReceipts(); } catch (receiptError) { throw receiptError; }
          throw error;
        }
        await drainDeferredReceipts();
        if (!await this.queue.completeImportAndEnqueuePublish(task, workerId, {
          chapterId, mediaIds, pageCount: urls.length, selectedSource: route.source, mediaStoredAt: new Date().toISOString(),
        })) throw new Error('LEASE_LOST_AFTER_MEDIA_STORAGE');
        return;
      } catch (error) {
        lastError = error;
        if (isSourceFailure(error)) {
          await this.queue.recordSourceFailure(route.source, error);
          // Never blend a partially persisted chapter with an alternative
          // source. The task will retry the pinned route after its cooldown.
          if ([...(await this.queue.uploadReceipts(task.id)).values()].some((receipt) => receipt.state === 'PERSISTED')) break;
          continue;
        }
        break;
      } finally { sourceSlot.release(); }
    }
    const message = String((lastError as Error | null)?.message || 'IMPORT_FAILED');
    const permanent = /UPLOAD_OUTCOME_UNKNOWN|CHAPTER_DISPLAY_NUMBER|CHAPTER_NUMBER_COLLISION|SOURCE_PAGE_MANIFEST_CHANGED_AFTER_PARTIAL_STORAGE|LEASE_LOST_BEFORE_MEDIA_STORAGE|SOURCE_CHAPTER_OWNERSHIP_UNVERIFIED/.test(message);
    const retryAfter = Number((lastError as SourceFailure | null)?.retryAfter || (lastError as any)?.parameters?.retry_after || 0);
    await this.queue.finish(task, workerId, {
      state: permanent || task.attempts >= task.maxAttempts ? 'BLOCKED' : 'RETRY_WAIT', reason: message.slice(0, 500),
      nextRunAt: permanent ? undefined : new Date(Date.now() + (retryAfter > 0
        ? (retryAfter + 2) * 1_000
        : Math.min(30 * 60_000, 30_000 * Math.max(1, task.attempts)))),
    });
  }

  async publish(task: LiteTask, workerId: string): Promise<void> {
    // Normal imports materialize this payload atomically with their durable
    // completion. It is safe only when every value is present and shaped as a
    // scalar reference: LiteRepository.publish still fences the task and
    // validates every media row before anything becomes Reader-visible.
    // Recovery tasks created by older deployments deliberately retain the
    // checkpoint read below, rather than assuming their payload is complete.
    let workId = task.workId;
    let chapterId = typeof task.payload.chapterId === 'string' ? task.payload.chapterId : null;
    let mediaIds = Array.isArray(task.payload.mediaIds) && task.payload.mediaIds.every((id) => typeof id === 'string')
      ? task.payload.mediaIds : null;
    if (!workId || !chapterId || !mediaIds) {
      const importTaskId = String(task.payload.importTaskId || '');
      const imported = await this.db.query<any>('SELECT work_id,checkpoint FROM importer_lite_tasks WHERE id=$1::uuid', [importTaskId]);
      const checkpoint = imported.rows[0]?.checkpoint || {};
      workId = imported.rows[0]?.work_id || null;
      chapterId = typeof checkpoint.chapterId === 'string' ? checkpoint.chapterId : null;
      mediaIds = Array.isArray(checkpoint.mediaIds) && checkpoint.mediaIds.every((id: unknown) => typeof id === 'string')
        ? checkpoint.mediaIds : null;
    }
    if (!workId || !chapterId || !mediaIds) {
      await this.queue.finish(task, workerId, { state: 'BLOCKED', reason: 'IMPORT_CHECKPOINT_INCOMPLETE' });
      return;
    }
    try {
      await this.queue.publish({ task, workerId, chapterId, workId,
        mediaIds, fresh: task.priority === 'P0' || task.priority === 'STAFF' });
    } catch (error: any) {
      const presentation = String(error?.message || '').includes('WORK_PRESENTATION_INCOMPLETE');
      await this.queue.finish(task, workerId, { state: presentation ? 'BLOCKED' : 'RETRY_WAIT',
        reason: String(error?.message || 'PUBLISH_FAILED'), nextRunAt: presentation ? undefined : new Date(Date.now() + 60_000) });
    }
  }
}
