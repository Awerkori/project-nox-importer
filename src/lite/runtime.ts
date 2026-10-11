import type { Pool } from 'pg';
import { DeduplicationEngine } from '../core/deduplication.js';
import type { Config } from '../config.js';
import type { SourceRegistry } from '../sources/registry.js';
import type { StorageProvider } from '../storage/provider.js';
import { canonicalChapterKey, chapterOrderKey } from './chapter-identity.js';
import { LiteExecutor } from './executor.js';
import { LiteRepository } from './repository.js';
import type { LiteTask } from './types.js';
import { Logger } from '../core/logger.js';
import { readImageBody } from '../core/bounded-body.js';
import { processAndStoreMedia } from '../storage/media.js';
import { AsyncSemaphore } from '../core/concurrency.js';
import { HostRateLimiter } from '../core/rate-limiter.js';
import { telemetryCollector } from '../core/telemetry-collector.js';

const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));
const rankForSource = (source: string) => source === 'mangaflix' ? 10 : source === 'manhastro' ? 20 : 100;

/** One process, one executor pool, one bounded reconciler. */
export class ImporterLiteRuntime {
  private stopped = false;
  private timers: NodeJS.Timeout[] = [];
  private readonly queue: LiteRepository;
  private readonly executor: LiteExecutor;
  private readonly dedupe: DeduplicationEngine;
  private readonly logger = new Logger('ImporterLite');
  private readonly downloads: AsyncSemaphore;
  private readonly bufferedPages: AsyncSemaphore;
  private readonly claimGate = new AsyncSemaphore(1, 'lite_claim_gate');
  private sitePausedUntil = 0;
  private idleClaimUntil = 0;
  private lastSiteProbe: { state: 'UNKNOWN' | 'HEALTHY' | 'PAUSED'; checkedAt: string; latencyMs: number | null } = {
    state: 'UNKNOWN', checkedAt: new Date(0).toISOString(), latencyMs: null,
  };

  constructor(
    private readonly pool: Pool,
    private readonly catalogClient: any,
    private readonly registry: SourceRegistry,
    private readonly storage: StorageProvider,
    private readonly config: Config,
    private readonly sourceRateLimiter: HostRateLimiter = new HostRateLimiter(2),
  ) {
    if (!config.IMPORTER_USER_ID) throw new Error('IMPORTER_USER_ID is required when IMPORTER_RUNTIME_MODE=lite');
    this.queue = new LiteRepository(pool);
    this.downloads = new AsyncSemaphore(config.DOWNLOAD_INFLIGHT_CONCURRENCY, 'lite_global_page_downloads');
    this.bufferedPages = new AsyncSemaphore(config.BUFFERED_PAGE_CONCURRENCY, 'lite_global_page_buffers');
    this.executor = new LiteExecutor(pool, this.queue, registry, storage, config.IMPORTER_USER_ID, this.downloads,
      Math.max(1, Math.min(6, config.BATCH_PAGE_DOWNLOAD_CONCURRENCY)), this.bufferedPages, sourceRateLimiter);
    this.dedupe = new DeduplicationEngine(catalogClient);
  }

  async start(): Promise<void> {
    await this.seedDiscovery();
    const workers = Math.max(1, Math.min(this.config.MAX_CONCURRENT_CHAPTERS, this.config.TESTED_CONCURRENCY_CEILING));
    telemetryCollector.configureChapterSlots(workers, () => workers);
    for (let i = 0; i < workers; i++) void this.worker(`${this.config.WORKER_ID}:lite:${i + 1}`, i);
    const timer = setInterval(() => void this.queue.reconcile(50).catch(() => {}), 30_000);
    timer.unref(); this.timers.push(timer);
    const healthTimer = setInterval(() => void this.probeSharedResources(), 30_000);
    healthTimer.unref(); this.timers.push(healthTimer);
    const metricsTimer = setInterval(() => void this.emitOperationalSnapshot().catch(() => {}), 60_000);
    metricsTimer.unref(); this.timers.push(metricsTimer);
    await this.probeSharedResources();
  }

  stop(): void { this.stopped = true; for (const timer of this.timers) clearInterval(timer); this.timers = []; }

  private async seedDiscovery(): Promise<void> {
    const sources = await this.pool.query<{ id: string }>(`SELECT id FROM importer_sources
      WHERE enabled=true AND COALESCE(catalog_discovery_enabled,true)=true ORDER BY id`);
    for (const { id } of sources.rows) {
      await this.queue.enqueue({ taskType: 'DISCOVER', workKey: `source:${id}`, priority: 'P2',
        idempotencyKey: `lite:discover:${id}`, payload: { source: id }, checkpoint: { cursor: null } });
    }
  }

  private async worker(workerId: string, slotIndex: number): Promise<void> {
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
        telemetryCollector.setSlotState(slotIndex,
          task.taskType === 'IMPORT_CHAPTER' ? 'ACTIVE_DOWNLOAD' : task.taskType === 'PUBLISH' ? 'ACTIVE_DB' : 'ACTIVE_SOURCE',
          task.taskType);
        await this.handle(task, workerId);
        telemetryCollector.setSlotState(slotIndex, 'IDLE');
      } catch {
        telemetryCollector.setSlotState(slotIndex, 'IDLE');
        await sleep(1_000);
      }
    }
  }

  /** One empty claim probe per idle interval, shared by every worker. */
  private async claimNext(workerId: string): Promise<LiteTask | null> {
    return this.claimGate.runExclusive(async () => {
      if (Date.now() < this.idleClaimUntil) return null;
      const task = await this.queue.claim(workerId, this.config.QUEUE_LEASE_DURATION_SECONDS);
      this.idleClaimUntil = task ? 0 : Date.now() + 750;
      return task;
    });
  }

  /** A small protective gate, deliberately not another scheduler. */
  private async probeSharedResources(): Promise<void> {
    const started = Date.now();
    try {
      const response = await fetch(`${this.config.NOX_MANGA_URL.replace(/\/$/, '')}/api/health`, { signal: AbortSignal.timeout(3_000) });
      await response.body?.cancel().catch(() => {});
      const elapsed = Date.now() - started;
      if (!response.ok || elapsed > 1_500) {
        this.sitePausedUntil = Date.now() + 30_000;
        this.lastSiteProbe = { state: 'PAUSED', checkedAt: new Date().toISOString(), latencyMs: elapsed };
        this.logger.warn('Pausing new Lite claims to protect shared site resources', { status: response.status, elapsed });
      } else {
        this.lastSiteProbe = { state: 'HEALTHY', checkedAt: new Date().toISOString(), latencyMs: elapsed };
      }
    } catch {
      // A probe outage should reduce pressure, not cause retries or a restart.
      this.sitePausedUntil = Date.now() + 30_000;
      this.lastSiteProbe = { state: 'PAUSED', checkedAt: new Date().toISOString(), latencyMs: null };
    }
  }

  /** A small diagnostic snapshot; it deliberately skips contention and scans. */
  private async emitOperationalSnapshot(): Promise<void> {
    if (this.pool.waitingCount > 0) return;
    const [tasks, progress, blocked, sourceHealth] = await Promise.all([
      this.queue.operationalSnapshot(), this.queue.progressSnapshot(),
      this.queue.blockedReasonSnapshot(), this.queue.sourceHealthSnapshot(),
    ]);
    const memory = process.memoryUsage();
    const storageMetrics = this.storage.getMetricsSummary?.() as any;
    const telegram = storageMetrics ? {
      uploads: Number(storageMetrics.totalBotUploads || 0),
      shardUploads: Number(storageMetrics.totalShardUploads || 0),
      active: (storageMetrics.bots || []).reduce((sum: number, bot: any) => sum + Number(bot.active || 0), 0),
      rateLimits429: (storageMetrics.bots || []).reduce((sum: number, bot: any) => sum + Number(bot.rateLimits429 || 0), 0),
    } : null;
    this.logger.info('Lite operational progress', {
      publishedLastMinute: progress.publishedLastMinute,
      oldestReadyAt: progress.oldestReadyAt?.toISOString() || null,
      oldestStoredUnpublishedAt: progress.oldestStoredUnpublishedAt?.toISOString() || null,
      tasks, blocked, sourceHealth, slots: telemetryCollector.getSlotProductivitySnapshot(),
      memoryMiB: {
        rss: Math.round(memory.rss / 1024 / 1024), heapUsed: Math.round(memory.heapUsed / 1024 / 1024),
        external: Math.round(memory.external / 1024 / 1024), arrayBuffers: Math.round(memory.arrayBuffers / 1024 / 1024),
      },
      ysql: { poolTotal: this.pool.totalCount, poolIdle: this.pool.idleCount, poolWaiting: this.pool.waitingCount },
      telegram, site: this.lastSiteProbe,
    });
  }

  private async handle(task: LiteTask, workerId: string): Promise<void> {
    if (task.taskType === 'DISCOVER') return this.discover(task, workerId);
    if (task.taskType === 'CATALOG') return this.catalog(task, workerId);
    if (task.taskType === 'IMPORT_CHAPTER') return this.executor.importChapter(task, workerId);
    if (task.taskType === 'PUBLISH') return this.executor.publish(task, workerId);
    await this.queue.finish(task, workerId, { state: 'BLOCKED', reason: 'UNKNOWN_TASK_TYPE' });
  }

  private async discover(task: LiteTask, workerId: string): Promise<void> {
    const source = String(task.payload.source || ''); const adapter = this.registry.get(source);
    if (!adapter) return void await this.queue.finish(task, workerId, { state: 'BLOCKED', reason: 'SOURCE_ADAPTER_UNAVAILABLE' });
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
    } catch (error: any) {
      await this.queue.recordSourceFailure(source, error);
      await this.queue.finish(task, workerId, { state: 'RETRY_WAIT', reason: String(error?.message || 'DISCOVERY_FAILED'), nextRunAt: new Date(Date.now() + 60_000) });
    }
  }

  private async catalog(task: LiteTask, workerId: string): Promise<void> {
    const source = String(task.payload.source || ''); const sourceWorkId = String(task.payload.sourceWorkId || '');
    const adapter = this.registry.get(source);
    if (!adapter || !sourceWorkId) return void await this.queue.finish(task, workerId, { state: 'BLOCKED', reason: 'CATALOG_INPUT_INVALID' });
    let fetchedSnapshot = false;
    try {
      const snapshot = adapter.fetchWorkSnapshot ? await adapter.fetchWorkSnapshot(sourceWorkId) : {
        details: await adapter.fetchWorkDetails(sourceWorkId), chapters: await adapter.fetchChapters(sourceWorkId),
      };
      fetchedSnapshot = true;
      const resolved = await this.dedupe.resolveWork({ source, sourceWorkId, title: snapshot.details.title, slug: snapshot.details.slug,
        aliases: snapshot.details.alternativeTitles, genres: snapshot.details.genres, author: snapshot.details.author,
        artist: snapshot.details.artist, synopsis: snapshot.details.synopsis, rawMetadata: snapshot.details.raw,
        loadCoverId: snapshot.details.coverUrl ? () => this.loadCover(adapter, snapshot.details.coverUrl!) : undefined } as any);
      if (!resolved.workId || resolved.status === 'AMBIGUOUS') {
        await this.queue.finish(task, workerId, { state: 'BLOCKED', reason: resolved.reason || 'AMBIGUOUS_CATALOG_MATCH' }); return;
      }
      const language = String(snapshot.details.language || (snapshot.details.raw as any)?.language || 'pt-BR');
      const edition = String(snapshot.details.edition || (snapshot.details.raw as any)?.edition || 'default');
      // P0 only accelerates releases beyond the durable manifest frontier.
      // A source can reveal historical chapters later; those are P1 repairs,
      // not P0, without inferring that every numeric gap is a missing chapter.
      const p0Monitoring = task.priority === 'P0' || task.priority === 'STAFF';
      const knownFrontier = p0Monitoring
        ? await this.pool.query<{ order_key: string | null }>(`SELECT max(order_key) AS order_key
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
            confirmed=true,has_chapter=true,last_verified_at=now(),updated_at=now()`,
          [resolved.workId,chapterKey,source,chapter.sourceChapterId,language,edition,orderKey,rankForSource(source)]);
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
    } catch (error: any) {
      if (!fetchedSnapshot) await this.queue.recordSourceFailure(source, error);
      await this.queue.finish(task, workerId, { state: task.attempts >= task.maxAttempts ? 'BLOCKED' : 'RETRY_WAIT',
        reason: String(error?.message || 'CATALOG_FAILED').slice(0,500), nextRunAt: new Date(Date.now() + 60_000) });
    }
  }

  /** Invalid/blocked covers remain absent; they never overwrite a known cover. */
  private async loadCover(adapter: NonNullable<ReturnType<SourceRegistry['get']>>, url: string): Promise<string | null> {
    try {
      const bytes = await this.downloads.runExclusive(async () => {
        let host: string | null = null;
        try { host = new URL(url).host; } catch {}
        if (host) await this.sourceRateLimiter.acquire(host);
        const response = await fetch(url, { headers: await adapter.getImageHeaders?.(url), signal: AbortSignal.timeout(30_000) });
        if (!response.ok) {
          if (response.status === 429 && host) this.sourceRateLimiter.handle429(host, response.headers.get('retry-after'));
          throw new Error(`COVER_HTTP_${response.status}`);
        }
        const bytes = await readImageBody(response, { signal: AbortSignal.timeout(30_000), maxBytes: 8 * 1024 * 1024 });
        if (host) this.sourceRateLimiter.recordSuccess(host);
        return bytes;
      });
      const stored = await processAndStoreMedia(this.catalogClient, this.storage, bytes, this.config.IMPORTER_USER_ID!, 'cover');
      return stored.mediaId;
    } catch { return null; }
  }
}
