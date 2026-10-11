import { randomUUID } from 'node:crypto';
import { readImageBody } from '../core/bounded-body.js';
import { calculateSha256, inspectImage } from '../storage/media.js';
import { selectChapterSource } from './source-selector.js';
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
export class LiteExecutor {
    db;
    queue;
    registry;
    storage;
    importerUserId;
    downloads;
    perChapterPageConcurrency;
    constructor(db, queue, registry, storage, importerUserId, downloads, perChapterPageConcurrency = 4) {
        this.db = db;
        this.queue = queue;
        this.registry = registry;
        this.storage = storage;
        this.importerUserId = importerUserId;
        this.downloads = downloads;
        this.perChapterPageConcurrency = perChapterPageConcurrency;
    }
    async routes(task) {
        if (!task.workId || !task.chapterKey)
            return [];
        const result = await this.db.query(`SELECT s.source,s.source_chapter_id,s.canonical_chapter_key,s.work_id,s.language,s.edition,s.source_rank,
         s.confirmed,s.has_chapter,COALESCE(h.health,'HEALTHY') health,h.cooldown_until
       FROM importer_lite_chapter_sources s LEFT JOIN importer_lite_source_health h ON h.source=s.source
       WHERE s.work_id=$1::uuid AND s.canonical_chapter_key=$2`, [task.workId, task.chapterKey]);
        return result.rows.map((row) => ({
            source: row.source, sourceChapterId: row.source_chapter_id, canonicalChapterKey: row.canonical_chapter_key,
            workId: row.work_id, language: row.language, edition: row.edition, sourceRank: row.source_rank,
            confirmed: row.confirmed, hasChapter: row.has_chapter, health: row.health,
            cooldownUntil: row.cooldown_until ? new Date(row.cooldown_until) : null,
        }));
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
    async recordSourceFailure(source, error) {
        const retryAfter = Number(error?.retryAfter || error?.parameters?.retry_after || 0);
        const cooldownSeconds = retryAfter > 0 ? Math.min(3600, retryAfter + 2) : 60;
        const health = retryAfter > 0 ? 'RATE_LIMITED' : 'DEGRADED';
        await this.db.query(`INSERT INTO importer_lite_source_health(source,health,cooldown_until,consecutive_failures,last_error)
      VALUES($1,$2,now()+($3 || ' seconds')::interval,1,$4)
      ON CONFLICT(source) DO UPDATE SET health=excluded.health,cooldown_until=excluded.cooldown_until,
        consecutive_failures=importer_lite_source_health.consecutive_failures+1,last_error=excluded.last_error,updated_at=now()`, [source, health, String(cooldownSeconds), String(error?.message || 'SOURCE_FAILURE').slice(0, 500)]).catch(() => { });
    }
    async importChapter(task, workerId) {
        const routeSet = await this.routes(task);
        const choice = selectChapterSource(routeSet, { language: String(task.payload.language || 'und'), edition: String(task.payload.edition || 'default') });
        if (!choice.route) {
            const until = routeSet.map((route) => route.cooldownUntil?.getTime() || 0).filter(Boolean).sort()[0];
            await this.queue.finish(task, workerId, {
                state: choice.reason === 'ALL_ROUTES_COOLDOWN' ? 'RETRY_WAIT' : 'BLOCKED', reason: choice.reason,
                nextRunAt: until ? new Date(until) : undefined,
            });
            return;
        }
        const route = choice.route;
        const adapter = this.registry.get(route.source);
        if (!adapter) {
            await this.queue.finish(task, workerId, { state: 'BLOCKED', reason: 'SOURCE_ADAPTER_UNAVAILABLE' });
            return;
        }
        try {
            const urls = await adapter.fetchChapterPages(route.sourceChapterId, Number(task.payload.displayNumber));
            if (!urls.length || urls.length > 1000)
                throw new Error('INVALID_PAGE_MANIFEST');
            const chapterId = await this.createOrLoadChapter(task);
            const mediaIds = await mapBounded(urls, this.perChapterPageConcurrency, async (url, index) => {
                const position = index + 1;
                const already = await this.db.query('SELECT media_id,state FROM importer_lite_uploads WHERE task_id=$1::uuid AND page_position=$2', [task.id, position]);
                if (already.rows[0]?.state === 'PERSISTED')
                    return already.rows[0].media_id;
                if (already.rows[0]?.state === 'UNKNOWN')
                    throw new Error(`UPLOAD_OUTCOME_UNKNOWN:${position}`);
                const { bytes, image } = await this.downloads.runExclusive(async () => {
                    const response = await fetch(url, { headers: await adapter.getImageHeaders?.(url), signal: AbortSignal.timeout(45_000) });
                    if (!response.ok)
                        throw Object.assign(new Error(`PAGE_HTTP_${response.status}`), { status: response.status });
                    const bytes = await readImageBody(response, { signal: AbortSignal.timeout(45_000) });
                    return { bytes, image: inspectImage(bytes) };
                });
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
                    });
                    return reserved.mediaId;
                }
                catch (error) {
                    // Only a confirmed Telegram success followed by DB failure is not
                    // safely retryable. A plain request failure remains retryable.
                    if (receiptNeeded)
                        await this.queue.markUploadUnknown(task.id, position);
                    throw error;
                }
            });
            await this.queue.checkpoint(task, workerId, { chapterId, mediaIds, pageCount: urls.length, selectedSource: route.source, mediaStoredAt: new Date().toISOString() });
            await this.queue.finish(task, workerId, { state: 'DONE', checkpoint: { chapterId, mediaIds, pageCount: urls.length, selectedSource: route.source } });
            await this.queue.enqueue({ taskType: 'PUBLISH', workId: task.workId, workKey: task.workKey, chapterKey: task.chapterKey,
                priority: task.basePriority === 'STAFF' ? 'P0' : task.basePriority,
                idempotencyKey: `lite:publish:${task.workId}:${task.chapterKey}`, payload: { importTaskId: task.id } });
        }
        catch (error) {
            await this.recordSourceFailure(route.source, error);
            const permanent = /INVALID_PAGE_MANIFEST|UPLOAD_OUTCOME_UNKNOWN|CHAPTER_DISPLAY_NUMBER/.test(String(error?.message));
            const retryAfter = Number(error?.retryAfter || error?.parameters?.retry_after || 0);
            await this.queue.finish(task, workerId, {
                state: permanent || task.attempts >= task.maxAttempts ? 'BLOCKED' : 'RETRY_WAIT',
                reason: String(error?.message || 'IMPORT_FAILED').slice(0, 500),
                nextRunAt: permanent ? undefined : new Date(Date.now() + (retryAfter > 0
                    ? (retryAfter + 2) * 1_000
                    : Math.min(30 * 60_000, 30_000 * Math.max(1, task.attempts)))),
            });
        }
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
