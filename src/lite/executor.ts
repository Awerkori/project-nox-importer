import type { Pool } from 'pg';
import { readImageBody } from '../core/bounded-body.js';
import { calculateSha256, inspectImage } from '../storage/media.js';
import type { StorageProvider } from '../storage/provider.js';
import type { SourceRegistry } from '../sources/registry.js';
import { LiteRepository } from './repository.js';
import { selectableChapterSources } from './source-selector.js';
import type { ChapterSourceRoute, LiteTask } from './types.js';
import { AsyncSemaphore, SOURCE_CONCURRENCY_LIMITS } from '../core/concurrency.js';
import { HostRateLimiter } from '../core/rate-limiter.js';

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
  // Held from byte download through receipt persistence. This is distinct
  // from request concurrency: it bounds real image buffers across every
  // chapter worker while Telegram is slow or cooling down.
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
  ) {}

  private sourceSlot(source: string): AsyncSemaphore {
    let slot = this.sourceSlots.get(source);
    if (!slot) {
      const configured = SOURCE_CONCURRENCY_LIMITS[source]?.maxChapters ?? 2;
      slot = new AsyncSemaphore(configured, `lite_source_${source}`);
      this.sourceSlots.set(source, slot);
    }
    return slot;
  }

  private async routes(task: LiteTask): Promise<ChapterSourceRoute[]> {
    if (!task.workId || !task.chapterKey) return [];
    const result = await this.db.query<any>(
      `SELECT s.source,s.source_work_id,s.source_chapter_id,s.canonical_chapter_key,s.work_id,s.language,s.edition,s.source_rank,
         s.confirmed,s.has_chapter,COALESCE(h.health,'HEALTHY') health,h.cooldown_until,
         p.enabled,p.status,p.cooldown_until AS source_cooldown,
         COALESCE((to_jsonb(p)->>'chapter_ingestion_enabled')::boolean,true) AS chapter_ingestion_enabled
       FROM importer_lite_chapter_sources s
       JOIN importer_sources p ON p.id=s.source
       LEFT JOIN importer_lite_source_health h ON h.source=s.source
       WHERE s.work_id=$1::uuid AND s.canonical_chapter_key=$2`, [task.workId, task.chapterKey],
    );
    return result.rows.map((row) => {
      const status = String(row.status || 'ACTIVE').toUpperCase();
      const sourceEnabled = row.enabled === true && row.chapter_ingestion_enabled !== false && ['ACTIVE', 'DEGRADED', 'RECOVERING'].includes(status);
      const healthCooldown = row.cooldown_until ? new Date(row.cooldown_until) : null;
      const sourceCooldown = row.source_cooldown ? new Date(row.source_cooldown) : null;
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
  }

  /**
   * Existing legacy mappings are exact source identities, unlike a numeric
   * display value. Reuse them during migration so Lite never overwrites a
   * published editorial chapter merely because its own mapping table is new.
   */
  private async legacyMappedChapter(task: LiteTask, db: Pick<Pool, 'query'> = this.db): Promise<{ chapterId: string; publishedAt: Date | null } | null> {
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

  private async createOrLoadChapter(task: LiteTask): Promise<string> {
    const existing = await this.db.query<{ chapter_id: string }>(
      'SELECT chapter_id FROM importer_lite_chapters WHERE work_id=$1::uuid AND canonical_chapter_key=$2', [task.workId, task.chapterKey],
    );
    if (existing.rows[0]) return existing.rows[0].chapter_id;
    const display = String(task.payload.displayNumber || '0');
    const numeric = Number.parseFloat(display.replace(',', '.'));
    if (!Number.isFinite(numeric)) throw new Error('CHAPTER_DISPLAY_NUMBER_NOT_READER_COMPATIBLE');
    const chapterId = canonicalChapterId(String(task.workId), String(task.chapterKey));
    const client = await this.db.connect();
    try {
      await client.query('BEGIN');
      const legacy = await this.legacyMappedChapter(task, client);
      if (legacy) {
        const mapped = await this.mapCanonicalChapter(client, task, legacy.chapterId);
        await client.query('COMMIT');
        return mapped;
      }
      const title = String(task.payload.chapterTitle || '').slice(0, 200);
      const inserted = await client.query<{ id: string }>(`INSERT INTO chapters(id,work_id,number,title,origin) VALUES($1::uuid,$2::uuid,$3,$4,'IMPORTER')
        ON CONFLICT DO NOTHING RETURNING id`, [chapterId, task.workId, numeric, title]);
      let durableChapterId = inserted.rows[0]?.id;
      if (!durableChapterId) {
        // The legacy Reader may still have a numeric uniqueness constraint.
        // Reuse only a clearly identical existing chapter; never silently map
        // a special/edition to a different chapter sharing its number.  In
        // particular, a Reader numeric column can collapse 12.1 and 12.10.
        // The Lite mapping is the durable authority for that distinction, so
        // a row already mapped under another canonical identity is a schema
        // incompatibility to surface, never an excuse to merge chapters.
        const same = await client.query<{ id: string }>(`SELECT id FROM chapters
          WHERE work_id=$1::uuid AND number=$2 AND lower(title)=lower($3) LIMIT 1 FOR UPDATE`, [task.workId, numeric, title]);
        if (!same.rows[0]) throw new Error('CHAPTER_NUMBER_COLLISION_REQUIRES_READER_MIGRATION');
        const mappedIdentity = await client.query<{ canonical_chapter_key: string }>(`SELECT canonical_chapter_key
          FROM importer_lite_chapters WHERE chapter_id=$1::uuid LIMIT 1 FOR UPDATE`, [same.rows[0].id]);
        if (mappedIdentity.rows[0] && mappedIdentity.rows[0].canonical_chapter_key !== task.chapterKey) {
          throw new Error('CHAPTER_NUMBER_COLLISION_REQUIRES_READER_MIGRATION');
        }
        durableChapterId = same.rows[0].id;
      }
      const mapped = await this.mapCanonicalChapter(client, task, durableChapterId);
      await client.query('COMMIT');
      return mapped;
    } catch (error) { await client.query('ROLLBACK').catch(() => {}); throw error; } finally { client.release(); }
  }

  private async hasPersistedUpload(taskId: string): Promise<boolean> {
    const result = await this.db.query<{ persisted: boolean }>(
      `SELECT EXISTS(SELECT 1 FROM importer_lite_uploads WHERE task_id=$1::uuid AND state='PERSISTED') AS persisted`, [taskId],
    );
    return result.rows[0]?.persisted === true;
  }

  async importChapter(task: LiteTask, workerId: string): Promise<void> {
    const legacy = await this.legacyMappedChapter(task);
    if (legacy?.publishedAt) {
      const chapterId = await this.createOrLoadChapter(task);
      await this.queue.finish(task, workerId, {
        state: 'DONE', checkpoint: { chapterId, publishedAt: legacy.publishedAt.toISOString(), legacyPublishedReuse: true },
      });
      return;
    }
    const routeSet = await this.routes(task);
    const selection = selectableChapterSources(routeSet, { language: String(task.payload.language || 'und'), edition: String(task.payload.edition || 'default') });
    if (!selection.routes.length) {
      const until = routeSet.map((route) => route.cooldownUntil?.getTime() || 0).filter(Boolean).sort()[0];
      await this.queue.finish(task, workerId, {
        state: selection.reason === 'ALL_ROUTES_COOLDOWN' ? 'RETRY_WAIT' : 'BLOCKED', reason: selection.reason,
        nextRunAt: until ? new Date(until) : undefined,
      });
      return;
    }
    const hasPersistedUpload = await this.hasPersistedUpload(task.id);
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
      const adapter = this.registry.get(route.source);
      if (!adapter) { lastError = new Error('SOURCE_ADAPTER_UNAVAILABLE'); continue; }
      if (adapter.isChapterOwnedByWork && (!route.sourceWorkId || !adapter.isChapterOwnedByWork(route.sourceWorkId, route.sourceChapterId))) {
        lastError = new Error('SOURCE_CHAPTER_OWNERSHIP_UNVERIFIED');
        continue;
      }
      const sourceSlot = this.sourceSlot(route.source);
      await sourceSlot.acquire();
      try {
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
        const chapterId = await this.createOrLoadChapter(task);
        const mediaIds = await mapBounded(urls, this.perChapterPageConcurrency, async (url, index) => {
          const position = index + 1;
          const already = await this.db.query<{ media_id: string; state: string; storage_ready: boolean | null; bytes: number | null; mime: string | null; media_sha256: string | null; sha256: string }>(
            `SELECT u.media_id,u.state,u.sha256,m.storage_ready,m.bytes,m.mime,m.sha256 AS media_sha256
             FROM importer_lite_uploads u LEFT JOIN media m ON m.id=u.media_id
             WHERE u.task_id=$1::uuid AND u.page_position=$2`, [task.id, position],
          );
          const receipt = already.rows[0];
          if (receipt?.state === 'PERSISTED' && receipt.storage_ready === true && Number(receipt.bytes) > 0 &&
              String(receipt.mime || '').startsWith('image/') && receipt.media_sha256 === receipt.sha256) return receipt.media_id;
          if (receipt?.state === 'UNKNOWN') throw new Error(`UPLOAD_OUTCOME_UNKNOWN:${position}`);
          const host = hostFor(url);
          if (host) await this.sourceRateLimiter.acquire(host);
          return this.bufferedPages.runExclusive(async () => {
            let downloaded: { bytes: Uint8Array; image: ReturnType<typeof inspectImage> };
            try {
              downloaded = await this.downloads.runExclusive(async () => {
                const response = await fetch(url, { headers: await adapter.getImageHeaders?.(url), signal: AbortSignal.timeout(45_000) });
                if (!response.ok) {
                  if (response.status === 429 && host) this.sourceRateLimiter.handle429(host, response.headers.get('retry-after'));
                  const error = sourceFailure(new Error(`PAGE_HTTP_${response.status}`));
                  error.retryAfter = Number(response.headers.get('retry-after') || 0);
                  throw error;
                }
                const bytes = await readImageBody(response, { signal: AbortSignal.timeout(45_000) });
                const image = inspectImage(bytes);
                if (host) this.sourceRateLimiter.recordSuccess(host);
                return { bytes, image };
              });
            } catch (error) { throw sourceFailure(error); }
            const { bytes, image } = downloaded;
            const reserved = await this.queue.reserveUpload(task.id, position, calculateSha256(bytes));
            if (reserved.state === 'PERSISTED') return reserved.mediaId;
            if (reserved.state === 'UNKNOWN') throw new Error(`UPLOAD_OUTCOME_UNKNOWN:${position}`);
            let receiptNeeded = false;
            try {
              const providerKey = await this.storage.upload(bytes, image.mime, reserved.mediaId, chapterId);
              receiptNeeded = true;
              await this.queue.recordUploadReceipt({
                taskId: task.id, pagePosition: position, mediaId: reserved.mediaId, sha256: calculateSha256(bytes),
                bytes: bytes.length, mime: image.mime, width: image.width, height: image.height, providerKey,
                botReference: this.storage.getLastBotReference?.(reserved.mediaId) || null,
                shardId: this.storage.getLastShardId?.(reserved.mediaId) || null,
                channelId: this.storage.getLastChannelId?.(reserved.mediaId) || null, createdBy: this.importerUserId,
                messageId: this.storage.getLastMessageId?.(reserved.mediaId) || null,
              });
              return reserved.mediaId;
            } catch (error) {
              // A confirmed Telegram success followed by a DB failure is
              // ambiguous. Network failures without an explicit Telegram HTTP
              // rejection are ambiguous too: a response can be lost after the
              // server accepted the upload. Only known HTTP rejections (notably
              // 429) may retry with the reserved media ID.
              if (receiptNeeded || !(error as any)?.statusCode) {
                await this.queue.markUploadUnknown(task.id, position);
                throw new Error(`UPLOAD_OUTCOME_UNKNOWN:${position}`);
              }
              await this.queue.markUploadRetryable(task.id, position);
              throw error;
            }
          });
        });
        if (!await this.queue.checkpoint(task, workerId, {
          chapterId, mediaIds, pageCount: urls.length, selectedSource: route.source, mediaStoredAt: new Date().toISOString(),
        })) throw new Error('LEASE_LOST_AFTER_MEDIA_STORAGE');
        if (!await this.queue.finish(task, workerId, {
          state: 'DONE', checkpoint: { chapterId, mediaIds, pageCount: urls.length, selectedSource: route.source },
        })) throw new Error('LEASE_LOST_AFTER_MEDIA_STORAGE');
        await this.queue.recordSourceSuccess(route.source);
        await this.queue.enqueue({ taskType: 'PUBLISH', workId: task.workId, workKey: task.workKey, chapterKey: task.chapterKey,
          priority: task.basePriority === 'STAFF' ? 'P0' : task.basePriority,
          idempotencyKey: `lite:publish:${task.workId}:${task.chapterKey}`, payload: { importTaskId: task.id } });
        return;
      } catch (error) {
        lastError = error;
        if (isSourceFailure(error)) {
          await this.queue.recordSourceFailure(route.source, error);
          // Never blend a partially persisted chapter with an alternative
          // source. The task will retry the pinned route after its cooldown.
          if (await this.hasPersistedUpload(task.id)) break;
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
    const importTaskId = String(task.payload.importTaskId || '');
    const imported = await this.db.query<any>('SELECT work_id,checkpoint FROM importer_lite_tasks WHERE id=$1::uuid', [importTaskId]);
    const checkpoint = imported.rows[0]?.checkpoint || {};
    if (!imported.rows[0]?.work_id || !checkpoint.chapterId || !Array.isArray(checkpoint.mediaIds)) {
      await this.queue.finish(task, workerId, { state: 'BLOCKED', reason: 'IMPORT_CHECKPOINT_INCOMPLETE' });
      return;
    }
    try {
      await this.queue.publish({ task, workerId, chapterId: checkpoint.chapterId, workId: imported.rows[0].work_id,
        mediaIds: checkpoint.mediaIds, fresh: task.priority === 'P0' || task.priority === 'STAFF' });
    } catch (error: any) {
      const presentation = String(error?.message || '').includes('WORK_PRESENTATION_INCOMPLETE');
      await this.queue.finish(task, workerId, { state: presentation ? 'BLOCKED' : 'RETRY_WAIT',
        reason: String(error?.message || 'PUBLISH_FAILED'), nextRunAt: presentation ? undefined : new Date(Date.now() + 60_000) });
    }
  }
}
