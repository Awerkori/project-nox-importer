import { randomUUID } from 'node:crypto';
import { readImageBody } from '../core/bounded-body.js';
import { calculateSha256, inspectImage } from '../storage/media.js';
import { selectableChapterSources } from './source-selector.js';
import { AsyncSemaphore, SOURCE_CONCURRENCY_LIMITS } from '../core/concurrency.js';
import { HostRateLimiter } from '../core/rate-limiter.js';
async function mapBounded(items, limit, fn) {
    const result = new Array(items.length);
    let cursor = 0;
    const worker = async () => {
        while (true) {
            const index = cursor++;
            if (index >= items.length)
                return;
            result[index] = await fn(items[index], index);
        }
    };
    await Promise.all(Array.from({ length: Math.max(1, Math.min(limit, items.length)) }, worker));
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
    // Held from byte download through receipt persistence. This is distinct
    // from request concurrency: it bounds real image buffers across every
    // chapter worker while Telegram is slow or cooling down.
    constructor(db, queue, registry, storage, importerUserId, downloads, perChapterPageConcurrency = 4, bufferedPages = downloads, sourceRateLimiter = new HostRateLimiter(2)) {
        this.db = db;
        this.queue = queue;
        this.registry = registry;
        this.storage = storage;
        this.importerUserId = importerUserId;
        this.downloads = downloads;
        this.perChapterPageConcurrency = perChapterPageConcurrency;
        this.bufferedPages = bufferedPages;
        this.sourceRateLimiter = sourceRateLimiter;
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
    async createOrLoadChapter(task) {
        const existing = await this.db.query('SELECT chapter_id FROM importer_lite_chapters WHERE work_id=$1::uuid AND canonical_chapter_key=$2', [task.workId, task.chapterKey]);
        if (existing.rows[0])
            return existing.rows[0].chapter_id;
        const display = String(task.payload.displayNumber || '0');
        const numeric = Number.parseFloat(display.replace(',', '.'));
        if (!Number.isFinite(numeric))
            throw new Error('CHAPTER_DISPLAY_NUMBER_NOT_READER_COMPATIBLE');
        const chapterId = randomUUID();
        const client = await this.db.connect();
        try {
            await client.query('BEGIN');
            const title = String(task.payload.chapterTitle || '').slice(0, 200);
            const inserted = await client.query(`INSERT INTO chapters(id,work_id,number,title,origin) VALUES($1::uuid,$2::uuid,$3,$4,'IMPORTER')
        ON CONFLICT DO NOTHING RETURNING id`, [chapterId, task.workId, numeric, title]);
            let durableChapterId = inserted.rows[0]?.id;
            if (!durableChapterId) {
                // The legacy Reader may still have a numeric uniqueness constraint.
                // Reuse only a clearly identical existing chapter; never silently map
                // a special/edition to a different chapter sharing its number.
                const same = await client.query(`SELECT id FROM chapters
          WHERE work_id=$1::uuid AND number=$2 AND lower(title)=lower($3) LIMIT 1 FOR UPDATE`, [task.workId, numeric, title]);
                if (!same.rows[0])
                    throw new Error('CHAPTER_NUMBER_COLLISION_REQUIRES_READER_MIGRATION');
                durableChapterId = same.rows[0].id;
            }
            const mapped = await client.query(`INSERT INTO importer_lite_chapters(work_id,canonical_chapter_key,chapter_id,display_number,order_key,edition,language)
         VALUES($1::uuid,$2,$3::uuid,$4,$5,$6,$7)
         ON CONFLICT(work_id,canonical_chapter_key) DO UPDATE SET chapter_id=importer_lite_chapters.chapter_id
         RETURNING chapter_id`, [task.workId, task.chapterKey, durableChapterId, display, String(task.payload.orderKey || display),
                String(task.payload.edition || 'default'), String(task.payload.language || 'und')]);
            await client.query('COMMIT');
            return mapped.rows[0].chapter_id;
        }
        catch (error) {
            await client.query('ROLLBACK').catch(() => { });
            throw error;
        }
        finally {
            client.release();
        }
    }
    async hasPersistedUpload(taskId) {
        const result = await this.db.query(`SELECT EXISTS(SELECT 1 FROM importer_lite_uploads WHERE task_id=$1::uuid AND state='PERSISTED') AS persisted`, [taskId]);
        return result.rows[0]?.persisted === true;
    }
    async importChapter(task, workerId) {
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
                    const already = await this.db.query('SELECT media_id,state FROM importer_lite_uploads WHERE task_id=$1::uuid AND page_position=$2', [task.id, position]);
                    if (already.rows[0]?.state === 'PERSISTED')
                        return already.rows[0].media_id;
                    if (already.rows[0]?.state === 'UNKNOWN')
                        throw new Error(`UPLOAD_OUTCOME_UNKNOWN:${position}`);
                    const host = hostFor(url);
                    if (host)
                        await this.sourceRateLimiter.acquire(host);
                    return this.bufferedPages.runExclusive(async () => {
                        let downloaded;
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
                        }
                        catch (error) {
                            throw sourceFailure(error);
                        }
                        const { bytes, image } = downloaded;
                        const reserved = await this.queue.reserveUpload(task.id, position, calculateSha256(bytes));
                        if (reserved.state === 'PERSISTED')
                            return reserved.mediaId;
                        if (reserved.state === 'UNKNOWN')
                            throw new Error(`UPLOAD_OUTCOME_UNKNOWN:${position}`);
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
                await this.queue.checkpoint(task, workerId, { chapterId, mediaIds, pageCount: urls.length, selectedSource: route.source, mediaStoredAt: new Date().toISOString() });
                await this.queue.finish(task, workerId, { state: 'DONE', checkpoint: { chapterId, mediaIds, pageCount: urls.length, selectedSource: route.source } });
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
                    if (await this.hasPersistedUpload(task.id))
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
