import { performance } from 'node:perf_hooks';
import { readImageBody } from '../core/bounded-body.js';
import { calculateSha256, inspectImage } from '../storage/media.js';
import { selectableChapterSources } from './source-selector.js';
import { AsyncSemaphore, SOURCE_CONCURRENCY_LIMITS } from '../core/concurrency.js';
import { HostRateLimiter } from '../core/rate-limiter.js';
import { telemetryCollector } from '../core/telemetry-collector.js';
import { chapterKind, chapterOrderKey } from './chapter-identity.js';
async function mapBounded(items, limit, fn) {
    const result = new Array(items.length);
    let cursor = 0;
    let failed = false;
    let firstError;
    const worker = async () => {
        while (true) {
            // Do not start more network/media work after a sibling failed, but do
            // drain the operations already in flight. Returning immediately from
            // Promise.all would race a late successful receipt against source
            // fallback and could mix two manifests in one durable chapter.
            if (failed)
                return;
            const index = cursor++;
            if (index >= items.length)
                return;
            try {
                result[index] = await fn(items[index], index);
            }
            catch (error) {
                if (!failed) {
                    failed = true;
                    firstError = error;
                }
                return;
            }
        }
    };
    await Promise.all(Array.from({ length: Math.max(1, Math.min(limit, items.length)) }, worker));
    if (failed)
        throw firstError;
    return result;
}
function sourceFailure(error) {
    const normalized = error instanceof Error ? error : new Error(String(error || 'SOURCE_FAILURE'));
    normalized.sourceFailure = true;
    return normalized;
}
function isSourceFailure(error) {
    return Boolean(error?.sourceFailure);
}
function hostFor(url) {
    try {
        return new URL(url).host;
    }
    catch {
        return null;
    }
}
function numericDisplayValue(display) {
    const normalized = display.trim().replace(',', '.');
    // Keep the textual display separately. This value exists only for legacy
    // integrations that still inspect `chapters.number`; it is never an id.
    return /^\d+(?:\.\d+)?$/.test(normalized) ? normalized : null;
}
/**
 * Coalesce only post-Telegram durable receipts.  The pre-upload intent stays
 * a per-page fence, so a crash can never make a successful send retryable.
 * Five milliseconds is enough to join a page wave without adding meaningful
 * latency or a second scheduler/queue.
 */
export class UploadReceiptBatcher {
    queue;
    pending = [];
    flushTimer = null;
    flushing = false;
    constructor(queue) {
        this.queue = queue;
    }
    persist(input) {
        return new Promise((resolve, reject) => {
            this.pending.push({ input, resolve, reject });
            if (this.pending.length >= 48) {
                void this.flush();
            }
            else if (!this.flushTimer) {
                this.flushTimer = setTimeout(() => void this.flush(), 5);
                this.flushTimer.unref();
            }
        });
    }
    async flush() {
        if (this.flushTimer) {
            clearTimeout(this.flushTimer);
            this.flushTimer = null;
        }
        // A timer can fire while a previous database flush is still awaiting its
        // reply. Clear that consumed timer before returning, otherwise the first
        // flush sees a non-null (already-fired) handle and leaves later receipts
        // stranded in memory.
        if (this.flushing)
            return;
        this.flushing = true;
        try {
            while (this.pending.length) {
                const batch = this.pending.splice(0, 48);
                try {
                    await this.queue.recordUploadReceipts(batch.map((entry) => entry.input));
                    for (const entry of batch)
                        entry.resolve();
                }
                catch (error) {
                    for (const entry of batch)
                        entry.reject(error);
                }
            }
        }
        finally {
            this.flushing = false;
            if (this.pending.length && !this.flushTimer) {
                this.flushTimer = setTimeout(() => void this.flush(), 5);
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
    queue;
    pending = [];
    flushTimer = null;
    flushing = false;
    constructor(queue) {
        this.queue = queue;
    }
    reserve(input) {
        return new Promise((resolve, reject) => {
            this.pending.push({ input, resolve, reject });
            if (this.pending.length >= 48) {
                void this.flush();
            }
            else if (!this.flushTimer) {
                this.flushTimer = setTimeout(() => void this.flush(), 5);
                this.flushTimer.unref();
            }
        });
    }
    async flush() {
        if (this.flushTimer) {
            clearTimeout(this.flushTimer);
            this.flushTimer = null;
        }
        // See UploadReceiptBatcher: a fired timer must not suppress the follow-up
        // batch while an earlier YSQL write is still in flight.
        if (this.flushing)
            return;
        this.flushing = true;
        try {
            while (this.pending.length) {
                const batch = this.pending.splice(0, 48);
                try {
                    const reservations = await this.queue.reserveUploads(batch.map((entry) => entry.input));
                    if (reservations.length !== batch.length)
                        throw new Error('UPLOAD_RESERVATION_BATCH_INCOMPLETE');
                    for (let index = 0; index < batch.length; index++)
                        batch[index].resolve(reservations[index]);
                }
                catch (error) {
                    for (const entry of batch)
                        entry.reject(error);
                }
            }
        }
        finally {
            this.flushing = false;
            if (this.pending.length && !this.flushTimer) {
                this.flushTimer = setTimeout(() => void this.flush(), 5);
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
function canonicalChapterId(workId, chapterKey) {
    const hex = calculateSha256(Buffer.from(`project-nox:lite-chapter:${workId}:${chapterKey}`)).slice(0, 32);
    const variant = ((Number.parseInt(hex[16], 16) & 0x3) | 0x8).toString(16);
    return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-5${hex.slice(13, 16)}-${variant}${hex.slice(17, 20)}-${hex.slice(20, 32)}`;
}
/**
 * Buffer ownership is intentionally distinct even for minimal/test executor
 * construction. Reusing `downloads` would self-deadlock at capacity one,
 * because a page holds a buffer permit while it acquires a request permit.
 */
function defaultBufferedPages(downloads) {
    return new AsyncSemaphore(downloads.capacity, 'lite_default_page_buffers');
}
export class LiteExecutor {
    db;
    queue;
    registry;
    storage;
    importerUserId;
    downloads;
    perChapterPageConcurrency;
    bufferedPages;
    sourceRateLimiter;
    sourceSlots = new Map();
    receiptBatcher;
    reservationBatcher;
    // Held from byte download through receipt persistence. This is distinct
    // from request concurrency: it bounds real image buffers across every
    // chapter worker while Telegram is slow or cooling down.
    constructor(db, queue, registry, storage, importerUserId, downloads, perChapterPageConcurrency = 4, bufferedPages = defaultBufferedPages(downloads), sourceRateLimiter = new HostRateLimiter(2)) {
        this.db = db;
        this.queue = queue;
        this.registry = registry;
        this.storage = storage;
        this.importerUserId = importerUserId;
        this.downloads = downloads;
        this.perChapterPageConcurrency = perChapterPageConcurrency;
        this.bufferedPages = bufferedPages;
        this.sourceRateLimiter = sourceRateLimiter;
        this.receiptBatcher = new UploadReceiptBatcher(queue);
        this.reservationBatcher = new UploadReservationBatcher(queue);
    }
    sourceSlot(source) {
        let slot = this.sourceSlots.get(source);
        if (!slot) {
            const configured = SOURCE_CONCURRENCY_LIMITS[source]?.maxChapters ?? 2;
            slot = new AsyncSemaphore(configured, `lite_source_${source}`);
            this.sourceSlots.set(source, slot);
        }
        return slot;
    }
    async routes(task) {
        if (!task.workId || !task.chapterKey)
            return [];
        const result = await this.db.query(`SELECT s.source,s.source_work_id,s.source_chapter_id,s.canonical_chapter_key,s.work_id,s.language,s.edition,s.source_rank,
         s.confirmed,s.has_chapter,COALESCE(h.health,'HEALTHY') health,h.cooldown_until,
         p.enabled,p.status,p.cooldown_until AS source_cooldown,
         COALESCE((to_jsonb(p)->>'chapter_ingestion_enabled')::boolean,true) AS chapter_ingestion_enabled
       FROM importer_lite_chapter_sources s
       JOIN importer_sources p ON p.id=s.source
       LEFT JOIN importer_lite_source_health h ON h.source=s.source
       WHERE s.work_id=$1::uuid AND s.canonical_chapter_key=$2`, [task.workId, task.chapterKey]);
        return result.rows.map((row) => {
            const status = String(row.status || 'ACTIVE').toUpperCase();
            const sourceEnabled = row.enabled === true && row.chapter_ingestion_enabled !== false && ['ACTIVE', 'DEGRADED', 'RECOVERING'].includes(status);
            const healthCooldown = row.cooldown_until ? new Date(row.cooldown_until) : null;
            const sourceCooldown = row.source_cooldown ? new Date(row.source_cooldown) : null;
            const cooldownUntil = [healthCooldown, sourceCooldown]
                .filter((value) => Boolean(value && Number.isFinite(value.getTime())))
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
    async legacyMappedChapter(task, db = this.db) {
        if (!task.workId || !task.chapterKey)
            return null;
        const result = await db.query(`SELECT m.chapter_id,c.published_at
      FROM importer_lite_chapter_sources s
      JOIN importer_chapter_mappings m ON m.source=s.source AND m.source_chapter_id=s.source_chapter_id
      JOIN chapters c ON c.id=m.chapter_id
      WHERE s.work_id=$1::uuid AND s.canonical_chapter_key=$2 AND s.confirmed=true
        AND m.chapter_id IS NOT NULL AND c.work_id=$1::uuid
      ORDER BY s.source_rank,s.created_at LIMIT 1`, [task.workId, task.chapterKey]);
        const row = result.rows[0];
        if (!row?.chapter_id)
            return null;
        return { chapterId: row.chapter_id, publishedAt: row.published_at ? new Date(row.published_at) : null };
    }
    async mapCanonicalChapter(client, task, chapterId) {
        const display = String(task.payload.displayNumber || '0');
        const mapped = await client.query(`INSERT INTO importer_lite_chapters(work_id,canonical_chapter_key,chapter_id,display_number,order_key,edition,language)
       VALUES($1::uuid,$2,$3::uuid,$4,$5,$6,$7)
       ON CONFLICT(work_id,canonical_chapter_key) DO UPDATE SET chapter_id=importer_lite_chapters.chapter_id
       RETURNING chapter_id`, [task.workId, task.chapterKey, chapterId, display, String(task.payload.orderKey || display),
            String(task.payload.edition || 'default'), String(task.payload.language || 'und')]);
        return mapped.rows[0].chapter_id;
    }
    async createOrLoadChapter(task) {
        const existing = await this.db.query('SELECT chapter_id FROM importer_lite_chapters WHERE work_id=$1::uuid AND canonical_chapter_key=$2', [task.workId, task.chapterKey]);
        if (existing.rows[0])
            return existing.rows[0].chapter_id;
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
            const legacy = await this.legacyMappedChapter(task, client);
            if (legacy) {
                const mapped = await this.mapCanonicalChapter(client, task, legacy.chapterId);
                await client.query('COMMIT');
                return mapped;
            }
            const title = String(task.payload.chapterTitle || '').slice(0, 200);
            let inserted;
            try {
                inserted = await client.query(`INSERT INTO chapters(
            id,work_id,number,display_number,canonical_chapter_key,chapter_order_key,chapter_kind,edition,language,title,origin
          ) VALUES($1::uuid,$2::uuid,$3::numeric,$4,$5,$6,$7,$8,$9,$10,'IMPORTER')
          ON CONFLICT (work_id,canonical_chapter_key) WHERE canonical_chapter_key IS NOT NULL
          DO UPDATE SET canonical_chapter_key=EXCLUDED.canonical_chapter_key
          RETURNING id`, [chapterId, task.workId, numeric, display, task.chapterKey, orderKey, kind, edition, language, title]);
            }
            catch (error) {
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
            let durableChapterId = inserted.rows[0]?.id;
            if (!durableChapterId)
                throw new Error('CANONICAL_CHAPTER_UPSERT_DID_NOT_RETURN_ID');
            const mapped = await this.mapCanonicalChapter(client, task, durableChapterId);
            await client.query('COMMIT');
            return mapped;
        }
        catch (error) {
            await client.query('ROLLBACK').catch(() => { });
            throw error;
        }
        finally {
            client.release();
        }
    }
    async importChapter(task, workerId) {
        const legacy = await this.legacyMappedChapter(task);
        if (legacy?.publishedAt) {
            const chapterId = await this.createOrLoadChapter(task);
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
        let lastError = null;
        // A failed preferred source must not make a canonical chapter wait for a
        // confirmed, healthy alternative.  This loop is bounded by its routes.
        for (const route of routes) {
            const adapter = this.registry.get(route.source);
            if (!adapter) {
                lastError = new Error('SOURCE_ADAPTER_UNAVAILABLE');
                continue;
            }
            if (adapter.isChapterOwnedByWork && (!route.sourceWorkId || !adapter.isChapterOwnedByWork(route.sourceWorkId, route.sourceChapterId))) {
                lastError = new Error('SOURCE_CHAPTER_OWNERSHIP_UNVERIFIED');
                continue;
            }
            const sourceSlot = this.sourceSlot(route.source);
            await sourceSlot.acquire();
            try {
                let urls;
                try {
                    urls = await adapter.fetchChapterPages(route.sourceChapterId, Number(task.payload.displayNumber));
                    if (!urls.length || urls.length > 1000)
                        throw new Error('INVALID_PAGE_MANIFEST');
                }
                catch (error) {
                    throw sourceFailure(error);
                }
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
                }))
                    throw new Error('LEASE_LOST_BEFORE_MEDIA_STORAGE');
                const chapterId = await this.createOrLoadChapter(task);
                const mediaIds = await mapBounded(urls, this.perChapterPageConcurrency, async (url, index) => {
                    const position = index + 1;
                    const receipt = uploadReceipts.get(position);
                    if (receipt?.state === 'PERSISTED' && receipt.storageReady === true && Number(receipt.bytes) > 0 &&
                        String(receipt.mime || '').startsWith('image/') && receipt.mediaSha256 === receipt.sha256)
                        return receipt.mediaId;
                    if (receipt?.state === 'UNKNOWN')
                        throw new Error(`UPLOAD_OUTCOME_UNKNOWN:${position}`);
                    const host = hostFor(url);
                    if (host)
                        await this.sourceRateLimiter.acquire(host);
                    return this.bufferedPages.runExclusive(async () => {
                        let downloaded;
                        try {
                            const startedAt = performance.now();
                            telemetryCollector.trackActiveDownload(1);
                            try {
                                downloaded = await this.downloads.runExclusive(async () => {
                                    const response = await fetch(url, { headers: await adapter.getImageHeaders?.(url), signal: AbortSignal.timeout(45_000) });
                                    if (!response.ok) {
                                        if (response.status === 429 && host)
                                            this.sourceRateLimiter.handle429(host, response.headers.get('retry-after'));
                                        const error = sourceFailure(new Error(`PAGE_HTTP_${response.status}`));
                                        error.retryAfter = Number(response.headers.get('retry-after') || 0);
                                        throw error;
                                    }
                                    const bytes = await readImageBody(response, { signal: AbortSignal.timeout(45_000) });
                                    const image = inspectImage(bytes);
                                    if (host)
                                        this.sourceRateLimiter.recordSuccess(host);
                                    return { bytes, image };
                                });
                                telemetryCollector.recordImageDownload(route.source, performance.now() - startedAt, downloaded.bytes.length);
                            }
                            finally {
                                telemetryCollector.trackActiveDownload(-1);
                            }
                        }
                        catch (error) {
                            throw sourceFailure(error);
                        }
                        const { bytes, image } = downloaded;
                        const sha256 = calculateSha256(bytes);
                        const reserved = await this.reservationBatcher.reserve({ taskId: task.id, pagePosition: position, sha256 });
                        if (reserved.state === 'PERSISTED')
                            return reserved.mediaId;
                        if (reserved.state === 'UNKNOWN')
                            throw new Error(`UPLOAD_OUTCOME_UNKNOWN:${position}`);
                        let receiptNeeded = false;
                        try {
                            const uploadStartedAt = performance.now();
                            telemetryCollector.trackActiveTelegramUpload(1);
                            let providerKey;
                            try {
                                providerKey = await this.storage.upload(bytes, image.mime, reserved.mediaId, chapterId);
                                telemetryCollector.recordTelegramUpload(performance.now() - uploadStartedAt, bytes.length);
                            }
                            finally {
                                telemetryCollector.trackActiveTelegramUpload(-1);
                            }
                            receiptNeeded = true;
                            await this.receiptBatcher.persist({
                                taskId: task.id, pagePosition: position, mediaId: reserved.mediaId, sha256,
                                bytes: bytes.length, mime: image.mime, width: image.width, height: image.height, providerKey,
                                botReference: this.storage.getLastBotReference?.(reserved.mediaId) || null,
                                shardId: this.storage.getLastShardId?.(reserved.mediaId) || null,
                                channelId: this.storage.getLastChannelId?.(reserved.mediaId) || null, createdBy: this.importerUserId,
                                messageId: this.storage.getLastMessageId?.(reserved.mediaId) || null,
                            });
                            return reserved.mediaId;
                        }
                        catch (error) {
                            // A confirmed Telegram success followed by a DB failure is
                            // ambiguous. Network failures without an explicit Telegram HTTP
                            // rejection are ambiguous too: a response can be lost after the
                            // server accepted the upload. Only known HTTP rejections (notably
                            // 429) may retry with the reserved media ID.
                            if (receiptNeeded || !error?.statusCode) {
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
                }))
                    throw new Error('LEASE_LOST_AFTER_MEDIA_STORAGE');
                if (!await this.queue.finish(task, workerId, {
                    state: 'DONE', checkpoint: { chapterId, mediaIds, pageCount: urls.length, selectedSource: route.source },
                }))
                    throw new Error('LEASE_LOST_AFTER_MEDIA_STORAGE');
                await this.queue.recordSourceSuccess(route.source);
                await this.queue.enqueue({ taskType: 'PUBLISH', workId: task.workId, workKey: task.workKey, chapterKey: task.chapterKey,
                    priority: task.basePriority === 'STAFF' ? 'P0' : task.basePriority,
                    idempotencyKey: `lite:publish:${task.workId}:${task.chapterKey}`, payload: { importTaskId: task.id } });
                return;
            }
            catch (error) {
                lastError = error;
                if (isSourceFailure(error)) {
                    await this.queue.recordSourceFailure(route.source, error);
                    // Never blend a partially persisted chapter with an alternative
                    // source. The task will retry the pinned route after its cooldown.
                    if ([...(await this.queue.uploadReceipts(task.id)).values()].some((receipt) => receipt.state === 'PERSISTED'))
                        break;
                    continue;
                }
                break;
            }
            finally {
                sourceSlot.release();
            }
        }
        const message = String(lastError?.message || 'IMPORT_FAILED');
        const permanent = /UPLOAD_OUTCOME_UNKNOWN|CHAPTER_DISPLAY_NUMBER|CHAPTER_NUMBER_COLLISION|SOURCE_PAGE_MANIFEST_CHANGED_AFTER_PARTIAL_STORAGE|LEASE_LOST_BEFORE_MEDIA_STORAGE|SOURCE_CHAPTER_OWNERSHIP_UNVERIFIED/.test(message);
        const retryAfter = Number(lastError?.retryAfter || lastError?.parameters?.retry_after || 0);
        await this.queue.finish(task, workerId, {
            state: permanent || task.attempts >= task.maxAttempts ? 'BLOCKED' : 'RETRY_WAIT', reason: message.slice(0, 500),
            nextRunAt: permanent ? undefined : new Date(Date.now() + (retryAfter > 0
                ? (retryAfter + 2) * 1_000
                : Math.min(30 * 60_000, 30_000 * Math.max(1, task.attempts)))),
        });
    }
    async publish(task, workerId) {
        const importTaskId = String(task.payload.importTaskId || '');
        const imported = await this.db.query('SELECT work_id,checkpoint FROM importer_lite_tasks WHERE id=$1::uuid', [importTaskId]);
        const checkpoint = imported.rows[0]?.checkpoint || {};
        if (!imported.rows[0]?.work_id || !checkpoint.chapterId || !Array.isArray(checkpoint.mediaIds)) {
            await this.queue.finish(task, workerId, { state: 'BLOCKED', reason: 'IMPORT_CHECKPOINT_INCOMPLETE' });
            return;
        }
        try {
            await this.queue.publish({ task, workerId, chapterId: checkpoint.chapterId, workId: imported.rows[0].work_id,
                mediaIds: checkpoint.mediaIds, fresh: task.priority === 'P0' || task.priority === 'STAFF' });
        }
        catch (error) {
            const presentation = String(error?.message || '').includes('WORK_PRESENTATION_INCOMPLETE');
            await this.queue.finish(task, workerId, { state: presentation ? 'BLOCKED' : 'RETRY_WAIT',
                reason: String(error?.message || 'PUBLISH_FAILED'), nextRunAt: presentation ? undefined : new Date(Date.now() + 60_000) });
        }
    }
}
