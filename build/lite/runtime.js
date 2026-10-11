import { DeduplicationEngine } from '../core/deduplication.js';
import { canonicalChapterKey, chapterOrderKey } from './chapter-identity.js';
import { LiteExecutor } from './executor.js';
import { LiteRepository } from './repository.js';
import { Logger } from '../core/logger.js';
import { readImageBody } from '../core/bounded-body.js';
import { processAndStoreMedia } from '../storage/media.js';
import { AsyncSemaphore } from '../core/concurrency.js';
import { HostRateLimiter } from '../core/rate-limiter.js';
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const rankForSource = (source) => source === 'mangaflix' ? 10 : source === 'manhastro' ? 20 : 100;
/** One process, one executor pool, one bounded reconciler. */
export class ImporterLiteRuntime {
    pool;
    catalogClient;
    registry;
    storage;
    config;
    sourceRateLimiter;
    stopped = false;
    timers = [];
    queue;
    executor;
    dedupe;
    logger = new Logger('ImporterLite');
    downloads;
    bufferedPages;
    claimGate = new AsyncSemaphore(1, 'lite_claim_gate');
    sitePausedUntil = 0;
    idleClaimUntil = 0;
    constructor(pool, catalogClient, registry, storage, config, sourceRateLimiter = new HostRateLimiter(2)) {
        this.pool = pool;
        this.catalogClient = catalogClient;
        this.registry = registry;
        this.storage = storage;
        this.config = config;
        this.sourceRateLimiter = sourceRateLimiter;
        if (!config.IMPORTER_USER_ID)
            throw new Error('IMPORTER_USER_ID is required when IMPORTER_RUNTIME_MODE=lite');
        this.queue = new LiteRepository(pool);
        this.downloads = new AsyncSemaphore(config.DOWNLOAD_INFLIGHT_CONCURRENCY, 'lite_global_page_downloads');
        this.bufferedPages = new AsyncSemaphore(config.BUFFERED_PAGE_CONCURRENCY, 'lite_global_page_buffers');
        this.executor = new LiteExecutor(pool, this.queue, registry, storage, config.IMPORTER_USER_ID, this.downloads, Math.max(1, Math.min(6, config.BATCH_PAGE_DOWNLOAD_CONCURRENCY)), this.bufferedPages, sourceRateLimiter);
        this.dedupe = new DeduplicationEngine(catalogClient);
    }
    async start() {
        await this.seedDiscovery();
        const workers = Math.max(1, Math.min(this.config.MAX_CONCURRENT_CHAPTERS, this.config.TESTED_CONCURRENCY_CEILING));
        for (let i = 0; i < workers; i++)
            void this.worker(`${this.config.WORKER_ID}:lite:${i + 1}`);
        const timer = setInterval(() => void this.queue.reconcile(50).catch(() => { }), 30_000);
        timer.unref();
        this.timers.push(timer);
        const healthTimer = setInterval(() => void this.probeSharedResources(), 30_000);
        healthTimer.unref();
        this.timers.push(healthTimer);
        await this.probeSharedResources();
    }
    stop() { this.stopped = true; for (const timer of this.timers)
        clearInterval(timer); this.timers = []; }
    async seedDiscovery() {
        const sources = await this.pool.query(`SELECT id FROM importer_sources
      WHERE enabled=true AND COALESCE(catalog_discovery_enabled,true)=true ORDER BY id`);
        for (const { id } of sources.rows) {
            await this.queue.enqueue({ taskType: 'DISCOVER', workKey: `source:${id}`, priority: 'P2',
                idempotencyKey: `lite:discover:${id}`, payload: { source: id }, checkpoint: { cursor: null } });
        }
    }
    async worker(workerId) {
        while (!this.stopped) {
            try {
                if (Date.now() < this.sitePausedUntil || this.pool.waitingCount > 0) {
                    await sleep(250);
                    continue;
                }
                const task = await this.claimNext(workerId);
                if (!task) {
                    await sleep(750);
                    continue;
                }
                await this.handle(task, workerId);
            }
            catch {
                await sleep(1_000);
            }
        }
    }
    /** One empty claim probe per idle interval, shared by every worker. */
    async claimNext(workerId) {
        return this.claimGate.runExclusive(async () => {
            if (Date.now() < this.idleClaimUntil)
                return null;
            const task = await this.queue.claim(workerId, this.config.QUEUE_LEASE_DURATION_SECONDS);
            this.idleClaimUntil = task ? 0 : Date.now() + 750;
            return task;
        });
    }
    /** A small protective gate, deliberately not another scheduler. */
    async probeSharedResources() {
        const started = Date.now();
        try {
            const response = await fetch(`${this.config.NOX_MANGA_URL.replace(/\/$/, '')}/api/health`, { signal: AbortSignal.timeout(3_000) });
            await response.body?.cancel().catch(() => { });
            const elapsed = Date.now() - started;
            if (!response.ok || elapsed > 1_500) {
                this.sitePausedUntil = Date.now() + 30_000;
                this.logger.warn('Pausing new Lite claims to protect shared site resources', { status: response.status, elapsed });
            }
        }
        catch {
            // A probe outage should reduce pressure, not cause retries or a restart.
            this.sitePausedUntil = Date.now() + 30_000;
        }
    }
    async handle(task, workerId) {
        if (task.taskType === 'DISCOVER')
            return this.discover(task, workerId);
        if (task.taskType === 'CATALOG')
            return this.catalog(task, workerId);
        if (task.taskType === 'IMPORT_CHAPTER')
            return this.executor.importChapter(task, workerId);
        if (task.taskType === 'PUBLISH')
            return this.executor.publish(task, workerId);
        await this.queue.finish(task, workerId, { state: 'BLOCKED', reason: 'UNKNOWN_TASK_TYPE' });
    }
    async discover(task, workerId) {
        const source = String(task.payload.source || '');
        const adapter = this.registry.get(source);
        if (!adapter)
            return void await this.queue.finish(task, workerId, { state: 'BLOCKED', reason: 'SOURCE_ADAPTER_UNAVAILABLE' });
        try {
            const page = await adapter.fetchUpdatedWorks(String(task.checkpoint.cursor || '') || null, { mode: 'maintenance' });
            await this.queue.recordSourceSuccess(source);
            for (const work of page.works.slice(0, 100)) {
                await this.queue.enqueue({ taskType: 'CATALOG', workKey: `source:${source}:${work.sourceWorkId}`, priority: 'P2',
                    idempotencyKey: `lite:catalog:${source}:${work.sourceWorkId}`, payload: { source, sourceWorkId: work.sourceWorkId } });
            }
            // The durable task itself holds the cursor; no new cursor rows or sweep.
            await this.queue.finish(task, workerId, { state: 'RETRY_WAIT', priority: 'P2', nextRunAt: new Date(Date.now() + (page.nextCursor ? 1_000 : 10 * 60_000)),
                checkpoint: { cursor: page.nextCursor } });
        }
        catch (error) {
            await this.queue.recordSourceFailure(source, error);
            await this.queue.finish(task, workerId, { state: 'RETRY_WAIT', reason: String(error?.message || 'DISCOVERY_FAILED'), nextRunAt: new Date(Date.now() + 60_000) });
        }
    }
    async catalog(task, workerId) {
        const source = String(task.payload.source || '');
        const sourceWorkId = String(task.payload.sourceWorkId || '');
        const adapter = this.registry.get(source);
        if (!adapter || !sourceWorkId)
            return void await this.queue.finish(task, workerId, { state: 'BLOCKED', reason: 'CATALOG_INPUT_INVALID' });
        let fetchedSnapshot = false;
        try {
            const snapshot = adapter.fetchWorkSnapshot ? await adapter.fetchWorkSnapshot(sourceWorkId) : {
                details: await adapter.fetchWorkDetails(sourceWorkId), chapters: await adapter.fetchChapters(sourceWorkId),
            };
            fetchedSnapshot = true;
            const resolved = await this.dedupe.resolveWork({ source, sourceWorkId, title: snapshot.details.title, slug: snapshot.details.slug,
                aliases: snapshot.details.alternativeTitles, genres: snapshot.details.genres, author: snapshot.details.author,
                artist: snapshot.details.artist, synopsis: snapshot.details.synopsis, rawMetadata: snapshot.details.raw,
                loadCoverId: snapshot.details.coverUrl ? () => this.loadCover(adapter, snapshot.details.coverUrl) : undefined });
            if (!resolved.workId || resolved.status === 'AMBIGUOUS') {
                await this.queue.finish(task, workerId, { state: 'BLOCKED', reason: resolved.reason || 'AMBIGUOUS_CATALOG_MATCH' });
                return;
            }
            const language = String(snapshot.details.language || snapshot.details.raw?.language || 'pt-BR');
            const edition = String(snapshot.details.edition || snapshot.details.raw?.edition || 'default');
            // P0 only accelerates releases beyond the durable manifest frontier.
            // A source can reveal historical chapters later; those are P1 repairs,
            // not P0, without inferring that every numeric gap is a missing chapter.
            const p0Monitoring = task.priority === 'P0' || task.priority === 'STAFF';
            const knownFrontier = p0Monitoring
                ? await this.pool.query(`SELECT max(order_key) AS order_key
          FROM importer_lite_chapter_sources WHERE work_id=$1::uuid AND order_key LIKE 'n|%'`, [resolved.workId])
                : { rows: [{ order_key: null }] };
            const knownNormalOrder = knownFrontier.rows[0]?.order_key || null;
            for (const chapter of snapshot.chapters) {
                const displayNumber = chapter.displayNumber || String(chapter.number);
                const chapterKey = canonicalChapterKey({ displayNumber, title: chapter.title, language, edition });
                const orderKey = chapterOrderKey({ displayNumber, title: chapter.title });
                const historicalRepair = p0Monitoring && orderKey.startsWith('n|') && Boolean(knownNormalOrder && orderKey <= knownNormalOrder);
                const chapterPriority = p0Monitoring && !historicalRepair ? 'P0' : 'P1';
                await this.pool.query(`INSERT INTO importer_lite_chapter_sources(work_id,canonical_chapter_key,source,source_chapter_id,language,edition,order_key,source_rank,confirmed,has_chapter,last_verified_at)
          VALUES($1::uuid,$2,$3,$4,$5,$6,$7,$8,true,true,now())
          ON CONFLICT(source,source_chapter_id) DO UPDATE SET canonical_chapter_key=excluded.canonical_chapter_key,
            language=excluded.language,edition=excluded.edition,order_key=excluded.order_key,source_rank=excluded.source_rank,
            confirmed=true,has_chapter=true,last_verified_at=now(),updated_at=now()`, [resolved.workId, chapterKey, source, chapter.sourceChapterId, language, edition, orderKey, rankForSource(source)]);
                await this.queue.enqueue({ taskType: 'IMPORT_CHAPTER', workId: resolved.workId, workKey: `work:${resolved.workId}`,
                    chapterKey, priority: chapterPriority, idempotencyKey: `lite:chapter:${resolved.workId}:${chapterKey}`,
                    payload: { displayNumber, chapterTitle: chapter.title || '', orderKey, language, edition } });
            }
            // A newly confirmed mapping (or repaired presentation metadata) is
            // evidence worth re-evaluating. This indexed, per-work wake-up replaces
            // the legacy global blocked-task sweeps.
            await this.queue.wakeCatalogBlocked(resolved.workId, `work:${resolved.workId}`);
            // Ongoing works retain one cheap, persistent P0 check; completed works
            // stay catalogued and are revisited only through discovery/repair.
            const ongoing = snapshot.details.status === 'ONGOING';
            await this.queue.finish(task, workerId, ongoing ? { state: 'RETRY_WAIT', priority: 'P0', nextRunAt: new Date(Date.now() + 15 * 60_000), checkpoint: { workId: resolved.workId } } : { state: 'DONE', checkpoint: { workId: resolved.workId } });
            await this.queue.recordSourceSuccess(source);
        }
        catch (error) {
            if (!fetchedSnapshot)
                await this.queue.recordSourceFailure(source, error);
            await this.queue.finish(task, workerId, { state: task.attempts >= task.maxAttempts ? 'BLOCKED' : 'RETRY_WAIT',
                reason: String(error?.message || 'CATALOG_FAILED').slice(0, 500), nextRunAt: new Date(Date.now() + 60_000) });
        }
    }
    /** Invalid/blocked covers remain absent; they never overwrite a known cover. */
    async loadCover(adapter, url) {
        try {
            const bytes = await this.downloads.runExclusive(async () => {
                let host = null;
                try {
                    host = new URL(url).host;
                }
                catch { }
                if (host)
                    await this.sourceRateLimiter.acquire(host);
                const response = await fetch(url, { headers: await adapter.getImageHeaders?.(url), signal: AbortSignal.timeout(30_000) });
                if (!response.ok) {
                    if (response.status === 429 && host)
                        this.sourceRateLimiter.handle429(host, response.headers.get('retry-after'));
                    throw new Error(`COVER_HTTP_${response.status}`);
                }
                const bytes = await readImageBody(response, { signal: AbortSignal.timeout(30_000), maxBytes: 8 * 1024 * 1024 });
                if (host)
                    this.sourceRateLimiter.recordSuccess(host);
                return bytes;
            });
            const stored = await processAndStoreMedia(this.catalogClient, this.storage, bytes, this.config.IMPORTER_USER_ID, 'cover');
            return stored.mediaId;
        }
        catch {
            return null;
        }
    }
}
