import { DeduplicationEngine } from '../core/deduplication.js';
import { canonicalChapterKey, chapterOrderKey } from './chapter-identity.js';
import { LiteExecutor } from './executor.js';
import { LiteRepository } from './repository.js';
import { Logger } from '../core/logger.js';
import { readImageBody } from '../core/bounded-body.js';
import { processAndStoreMedia } from '../storage/media.js';
import { AsyncSemaphore } from '../core/concurrency.js';
import { HostRateLimiter } from '../core/rate-limiter.js';
import { telemetryCollector } from '../core/telemetry-collector.js';
import { sanitizeSynopsis } from './metadata.js';
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
    // Observation is intentionally a one-slot lane. It may create P0 chapter
    // work, but an empty observation must never compete with chapter workers.
    monitorGate = new AsyncSemaphore(1, 'lite_p0_monitor');
    sitePausedUntil = 0;
    idleClaimUntil = 0;
    idleMonitorUntil = 0;
    lastSiteProbe = {
        state: 'UNKNOWN', checkedAt: new Date(0).toISOString(), latencyMs: null,
    };
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
        telemetryCollector.configureChapterSlots(workers, () => workers);
        for (let i = 0; i < workers; i++)
            void this.worker(`${this.config.WORKER_ID}:lite:${i + 1}`, i);
        const timer = setInterval(() => void this.queue.reconcile(50).catch(() => { }), 30_000);
        timer.unref();
        this.timers.push(timer);
        const monitorTimer = setInterval(() => void this.runMonitorTurn().catch(() => { }), 1_000);
        monitorTimer.unref();
        this.timers.push(monitorTimer);
        const healthTimer = setInterval(() => void this.probeSharedResources(), 30_000);
        healthTimer.unref();
        this.timers.push(healthTimer);
        const metricsTimer = setInterval(() => void this.emitOperationalSnapshot().catch(() => { }), 60_000);
        metricsTimer.unref();
        this.timers.push(metricsTimer);
        await this.probeSharedResources();
    }
    stop() { this.stopped = true; for (const timer of this.timers)
        clearInterval(timer); this.timers = []; }
    async seedDiscovery() {
        const sources = await this.pool.query(`SELECT id,sync_interval_minutes FROM importer_sources
      WHERE enabled=true AND COALESCE(status,'ACTIVE') IN ('ACTIVE','DEGRADED','RECOVERING') ORDER BY id`);
        for (const { id, sync_interval_minutes } of sources.rows) {
            await this.queue.enqueue({ taskType: 'DISCOVER', workKey: `source:${id}`, priority: 'P2',
                idempotencyKey: `lite:discover:${id}`, payload: { source: id, syncIntervalMinutes: sync_interval_minutes }, checkpoint: { cursor: null } });
        }
    }
    async upsertChapterSources(rows) {
        for (let offset = 0; offset < rows.length; offset += 500) {
            const batch = rows.slice(offset, offset + 500);
            const values = [];
            const tuples = batch.map((row, index) => {
                const start = index * 9;
                values.push(row.workId, row.chapterKey, row.source, row.sourceWorkId, row.sourceChapterId, row.language, row.edition, row.orderKey, row.sourceRank);
                return `($${start + 1}::uuid,$${start + 2},$${start + 3},$${start + 4},$${start + 5},$${start + 6},$${start + 7},$${start + 8},$${start + 9},true,true,now())`;
            });
            await this.pool.query(`INSERT INTO importer_lite_chapter_sources
        (work_id,canonical_chapter_key,source,source_work_id,source_chapter_id,language,edition,order_key,source_rank,confirmed,has_chapter,last_verified_at)
        VALUES ${tuples.join(',')}
        ON CONFLICT(source,source_chapter_id) DO UPDATE SET canonical_chapter_key=excluded.canonical_chapter_key,
          source_work_id=excluded.source_work_id,language=excluded.language,edition=excluded.edition,order_key=excluded.order_key,source_rank=excluded.source_rank,
          confirmed=true,has_chapter=true,last_verified_at=now(),updated_at=now()`, values);
        }
    }
    async worker(workerId, slotIndex) {
        telemetryCollector.registerSlot(slotIndex);
        while (!this.stopped) {
            try {
                if (Date.now() < this.sitePausedUntil || this.pool.waitingCount > 0) {
                    telemetryCollector.setSlotState(slotIndex, 'WAITING_BARRIER', 'site-or-pool-protection');
                    await sleep(250);
                    continue;
                }
                telemetryCollector.setSlotState(slotIndex, 'WAITING_CLAIM_DB');
                const task = await this.claimNext(workerId);
                if (!task) {
                    telemetryCollector.setSlotState(slotIndex, 'IDLE');
                    await sleep(750);
                    continue;
                }
                telemetryCollector.setSlotState(slotIndex, task.taskType === 'IMPORT_CHAPTER' ? 'ACTIVE_DOWNLOAD' : task.taskType === 'PUBLISH' ? 'ACTIVE_DB' : 'ACTIVE_SOURCE', task.taskType);
                await this.handle(task, workerId);
                telemetryCollector.setSlotState(slotIndex, 'IDLE');
            }
            catch {
                telemetryCollector.setSlotState(slotIndex, 'IDLE');
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
    /**
     * Claim at most one due P0 observation. A failed/empty probe backs off for
     * fifteen seconds, so thousands of watched works do not turn into empty DB
     * claims. This is a resource budget, not a second priority scheduler.
     */
    async runMonitorTurn() {
        if (this.stopped || Date.now() < this.sitePausedUntil || this.pool.waitingCount > 0)
            return;
        if (!this.monitorGate.tryAcquire())
            return;
        try {
            if (Date.now() < this.idleMonitorUntil)
                return;
            const task = await this.queue.claimMonitor(`${this.config.WORKER_ID}:lite-monitor`, this.config.QUEUE_LEASE_DURATION_SECONDS);
            this.idleMonitorUntil = task ? 0 : Date.now() + 15_000;
            if (task)
                await this.handle(task, `${this.config.WORKER_ID}:lite-monitor`);
        }
        finally {
            this.monitorGate.release();
        }
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
                this.lastSiteProbe = { state: 'PAUSED', checkedAt: new Date().toISOString(), latencyMs: elapsed };
                this.logger.warn('Pausing new Lite claims to protect shared site resources', { status: response.status, elapsed });
            }
            else {
                this.lastSiteProbe = { state: 'HEALTHY', checkedAt: new Date().toISOString(), latencyMs: elapsed };
            }
        }
        catch {
            // A probe outage should reduce pressure, not cause retries or a restart.
            this.sitePausedUntil = Date.now() + 30_000;
            this.lastSiteProbe = { state: 'PAUSED', checkedAt: new Date().toISOString(), latencyMs: null };
        }
    }
    /** A small diagnostic snapshot; it deliberately skips contention and scans. */
    async emitOperationalSnapshot() {
        if (this.pool.waitingCount > 0)
            return;
        const [tasks, progress, blocked, sourceHealth] = await Promise.all([
            this.queue.operationalSnapshot(), this.queue.progressSnapshot(),
            this.queue.blockedReasonSnapshot(), this.queue.sourceHealthSnapshot(),
        ]);
        const memory = process.memoryUsage();
        const storageMetrics = this.storage.getMetricsSummary?.();
        const telegram = storageMetrics ? {
            uploads: Number(storageMetrics.totalBotUploads || 0),
            shardUploads: Number(storageMetrics.totalShardUploads || 0),
            active: (storageMetrics.bots || []).reduce((sum, bot) => sum + Number(bot.active || 0), 0),
            rateLimits429: (storageMetrics.bots || []).reduce((sum, bot) => sum + Number(bot.rateLimits429 || 0), 0),
        } : null;
        this.logger.info('Lite operational progress', {
            publishedLastMinute: progress.publishedLastMinute,
            oldestReadyAt: progress.oldestReadyAt?.toISOString() || null,
            oldestStoredUnpublishedAt: progress.oldestStoredUnpublishedAt?.toISOString() || null,
            nextP0MonitorAt: progress.nextMonitorAt?.toISOString() || null,
            tasks, blocked, sourceHealth, slots: telemetryCollector.getSlotProductivitySnapshot(),
            memoryMiB: {
                rss: Math.round(memory.rss / 1024 / 1024), heapUsed: Math.round(memory.heapUsed / 1024 / 1024),
                external: Math.round(memory.external / 1024 / 1024), arrayBuffers: Math.round(memory.arrayBuffers / 1024 / 1024),
            },
            ysql: { poolTotal: this.pool.totalCount, poolIdle: this.pool.idleCount, poolWaiting: this.pool.waitingCount },
            telegram, site: this.lastSiteProbe,
        });
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
        const policy = await this.queue.sourcePolicy(source);
        if (!policy?.runnable) {
            const nextRunAt = policy?.cooldownUntil && policy.cooldownUntil > new Date()
                ? policy.cooldownUntil : new Date(Date.now() + (policy?.syncIntervalMs || 60 * 60_000));
            await this.queue.finish(task, workerId, { state: 'RETRY_WAIT', reason: `SOURCE_NOT_READY:${policy?.status || 'MISSING'}`, nextRunAt });
            return;
        }
        try {
            const page = await adapter.fetchUpdatedWorks(String(task.checkpoint.cursor || '') || null, { mode: 'maintenance' });
            await this.queue.recordSourceSuccess(source);
            for (const work of page.works.slice(0, 100)) {
                await this.queue.enqueue({ taskType: 'CATALOG', workKey: `source:${source}:${work.sourceWorkId}`, priority: 'P2',
                    idempotencyKey: `lite:catalog:${source}:${work.sourceWorkId}`, payload: { source, sourceWorkId: work.sourceWorkId } });
            }
            // The durable task itself holds the cursor; no new cursor rows or sweep.
            await this.queue.finish(task, workerId, { state: 'RETRY_WAIT', priority: 'P2', nextRunAt: new Date(Date.now() + (page.nextCursor ? 1_000 : policy.syncIntervalMs)),
                checkpoint: { cursor: page.nextCursor }, resetAttempts: true });
        }
        catch (error) {
            await this.queue.recordSourceFailure(source, error);
            // Discovery is one durable recurring observation per source. A long
            // upstream outage must not create an unbounded task set or make that
            // source permanently invisible; its retry cadence is capped instead.
            await this.queue.finish(task, workerId, { state: 'RETRY_WAIT', reason: String(error?.message || 'DISCOVERY_FAILED'),
                nextRunAt: new Date(Date.now() + this.recurringRetryDelayMs(task.attempts)) });
        }
    }
    async catalog(task, workerId) {
        const source = String(task.payload.source || '');
        const sourceWorkId = String(task.payload.sourceWorkId || '');
        const adapter = this.registry.get(source);
        if (!adapter || !sourceWorkId)
            return void await this.queue.finish(task, workerId, { state: 'BLOCKED', reason: 'CATALOG_INPUT_INVALID' });
        const policy = await this.queue.sourcePolicy(source);
        if (!policy?.runnable) {
            const nextRunAt = policy?.cooldownUntil && policy.cooldownUntil > new Date()
                ? policy.cooldownUntil : new Date(Date.now() + (policy?.syncIntervalMs || 60 * 60_000));
            await this.queue.finish(task, workerId, {
                state: task.attempts >= task.maxAttempts ? 'BLOCKED' : 'RETRY_WAIT',
                reason: `SOURCE_NOT_READY:${policy?.status || 'MISSING'}`, nextRunAt,
                priority: task.monitoring ? 'P2' : undefined, monitoring: task.monitoring || undefined,
            });
            return;
        }
        let fetchedSnapshot = false;
        try {
            const snapshot = adapter.fetchWorkSnapshot ? await adapter.fetchWorkSnapshot(sourceWorkId) : {
                details: await adapter.fetchWorkDetails(sourceWorkId), chapters: await adapter.fetchChapters(sourceWorkId),
            };
            fetchedSnapshot = true;
            const rawContentRating = String(snapshot.details.raw?.contentRating || snapshot.details.raw?.content_rating || '').toUpperCase();
            const resolved = await this.dedupe.resolveWork({ source, sourceWorkId, title: snapshot.details.title, slug: snapshot.details.slug,
                aliases: snapshot.details.alternativeTitles, genres: snapshot.details.genres, author: snapshot.details.author,
                artist: snapshot.details.artist, synopsis: sanitizeSynopsis(snapshot.details.synopsis) || undefined, kind: snapshot.details.kind,
                status: snapshot.details.status, year: snapshot.details.year, ageRating: snapshot.details.ageRating,
                contentRating: rawContentRating === 'ADULT_18' || Number(snapshot.details.ageRating || 0) >= 18 ? 'ADULT_18' : undefined,
                rawMetadata: snapshot.details.raw,
                loadCoverId: snapshot.details.coverUrl ? () => this.loadCover(adapter, snapshot.details.coverUrl) : undefined });
            if (!resolved.workId || resolved.status === 'AMBIGUOUS') {
                await this.queue.finish(task, workerId, { state: 'BLOCKED', reason: resolved.reason || 'AMBIGUOUS_CATALOG_MATCH' });
                return;
            }
            const language = String(snapshot.details.language || snapshot.details.raw?.language || 'pt-BR');
            const edition = String(snapshot.details.edition || snapshot.details.raw?.edition || 'default');
            // A recurring monitor is intentionally separate from P0 *execution*.
            // P0 only accelerates releases beyond the durable manifest frontier.
            // A source can reveal historical chapters later; those are P1 repairs,
            // not P0, without inferring that every numeric gap is a missing chapter.
            const p0Monitoring = task.monitoring || task.priority === 'P0' || task.priority === 'STAFF';
            const knownFrontier = p0Monitoring
                ? await this.pool.query(`SELECT max(order_key) AS order_key
          FROM importer_lite_chapter_sources WHERE work_id=$1::uuid AND order_key LIKE 'n|%'`, [resolved.workId])
                : { rows: [{ order_key: null }] };
            const knownNormalOrder = knownFrontier.rows[0]?.order_key || null;
            const sourceRows = [];
            const chapterTasks = [];
            const sourceChapterIds = new Set();
            for (const chapter of snapshot.chapters) {
                // A duplicate manifest entry cannot be allowed to make one batched
                // UPSERT fail and delay every other chapter in this catalog pass.
                if (sourceChapterIds.has(chapter.sourceChapterId))
                    continue;
                sourceChapterIds.add(chapter.sourceChapterId);
                const displayNumber = chapter.displayNumber || String(chapter.number);
                const chapterKey = canonicalChapterKey({ displayNumber, title: chapter.title, language, edition });
                const orderKey = chapterOrderKey({ displayNumber, title: chapter.title });
                const historicalRepair = p0Monitoring && orderKey.startsWith('n|') && Boolean(knownNormalOrder && orderKey <= knownNormalOrder);
                const chapterPriority = p0Monitoring && !historicalRepair ? 'P0' : 'P1';
                sourceRows.push({ workId: resolved.workId, chapterKey, source, sourceWorkId, sourceChapterId: chapter.sourceChapterId,
                    language, edition, orderKey, sourceRank: rankForSource(source) });
                chapterTasks.push({ taskType: 'IMPORT_CHAPTER', workId: resolved.workId, workKey: `work:${resolved.workId}`,
                    chapterKey, priority: chapterPriority, idempotencyKey: `lite:chapter:${resolved.workId}:${chapterKey}`,
                    payload: { displayNumber, chapterTitle: chapter.title || '', orderKey, language, edition } });
            }
            await this.upsertChapterSources(sourceRows);
            const materialized = await this.queue.enqueueMany(chapterTasks);
            const newReleaseCount = chapterTasks.filter((candidate) => candidate.priority === 'P0' && materialized.insertedKeys.has(candidate.idempotencyKey)).length;
            // A newly confirmed mapping (or repaired presentation metadata) is
            // evidence worth re-evaluating. This indexed, per-work wake-up replaces
            // the legacy global blocked-task sweeps.
            await this.queue.wakeCatalogBlocked(resolved.workId, `work:${resolved.workId}`);
            // Ongoing/hiatus works retain one cheap persistent monitor. The monitor
            // remains base P2 and outside normal claims; only its newly inserted
            // chapters are P0. Completed works remain available to discovery/repair.
            const status = String(snapshot.details.status || 'UNKNOWN').toUpperCase();
            const monitored = status === 'ONGOING' || status === 'HIATUS';
            await this.queue.finish(task, workerId, monitored ? {
                state: 'RETRY_WAIT', priority: 'P2', monitoring: true,
                nextRunAt: new Date(Date.now() + this.monitorIntervalMs(task, status, newReleaseCount)),
                workId: resolved.workId,
                checkpoint: {
                    workId: resolved.workId, lastCheckedAt: new Date().toISOString(),
                    lastNewReleaseCount: newReleaseCount,
                    emptyChecks: newReleaseCount ? 0 : Number(task.checkpoint.emptyChecks || 0) + 1,
                },
                resetAttempts: true,
            } : { state: 'DONE', workId: resolved.workId, checkpoint: { workId: resolved.workId }, resetAttempts: true });
            await this.queue.recordSourceSuccess(source);
        }
        catch (error) {
            if (!fetchedSnapshot)
                await this.queue.recordSourceFailure(source, error);
            const monitorRetry = task.monitoring;
            const retryDelay = monitorRetry ? this.recurringRetryDelayMs(task.attempts) : 60_000;
            // A monitoring row is an ongoing P0 observation, not a failed chapter.
            // Keep exactly one durable row with a capped cadence; its successful
            // next observation resets the failure window. A manually-awakened
            // STAFF catalog task is no longer marked monitoring and still obtains a
            // visible terminal block after its normal bounded attempts.
            await this.queue.finish(task, workerId, { state: monitorRetry ? 'RETRY_WAIT' : task.attempts >= task.maxAttempts ? 'BLOCKED' : 'RETRY_WAIT',
                reason: String(error?.message || 'CATALOG_FAILED').slice(0, 500), nextRunAt: new Date(Date.now() + retryDelay),
                priority: monitorRetry ? 'P2' : undefined, monitoring: monitorRetry || undefined,
                checkpoint: monitorRetry ? { monitorFailures: Number(task.checkpoint.monitorFailures || 0) + 1 } : undefined });
        }
    }
    /** Release bursts get a quick recheck; quiet works exponentially back off. */
    monitorIntervalMs(task, status, newReleaseCount) {
        if (status === 'HIATUS')
            return 6 * 60 * 60_000;
        if (newReleaseCount > 0)
            return 5 * 60_000;
        const emptyChecks = Math.max(0, Number(task.checkpoint.emptyChecks || 0));
        return Math.min(6 * 60 * 60_000, 15 * 60_000 * 2 ** Math.min(emptyChecks, 5));
    }
    /** Bounded health recovery for the two recurring observation task types. */
    recurringRetryDelayMs(attempts) {
        return Math.min(6 * 60 * 60_000, 60_000 * 2 ** Math.min(Math.max(0, attempts), 8));
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
