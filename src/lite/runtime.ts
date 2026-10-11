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
  private sitePausedUntil = 0;

  constructor(
    private readonly pool: Pool,
    private readonly catalogClient: any,
    private readonly registry: SourceRegistry,
    private readonly storage: StorageProvider,
    private readonly config: Config,
  ) {
    if (!config.IMPORTER_USER_ID) throw new Error('IMPORTER_USER_ID is required when IMPORTER_RUNTIME_MODE=lite');
    this.queue = new LiteRepository(pool);
    this.downloads = new AsyncSemaphore(config.DOWNLOAD_INFLIGHT_CONCURRENCY, 'lite_global_page_downloads');
    this.executor = new LiteExecutor(pool, this.queue, registry, storage, config.IMPORTER_USER_ID, this.downloads,
      Math.max(1, Math.min(6, config.BATCH_PAGE_DOWNLOAD_CONCURRENCY)));
    this.dedupe = new DeduplicationEngine(catalogClient);
  }

  async start(): Promise<void> {
    await this.seedDiscovery();
    const workers = Math.max(1, Math.min(this.config.MAX_CONCURRENT_CHAPTERS, this.config.TESTED_CONCURRENCY_CEILING));
    for (let i = 0; i < workers; i++) void this.worker(`${this.config.WORKER_ID}:lite:${i + 1}`);
    const timer = setInterval(() => void this.queue.reconcile(50).catch(() => {}), 30_000);
    timer.unref(); this.timers.push(timer);
    const healthTimer = setInterval(() => void this.probeSharedResources(), 30_000);
    healthTimer.unref(); this.timers.push(healthTimer);
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

  private async worker(workerId: string): Promise<void> {
    while (!this.stopped) {
      try {
        if (Date.now() < this.sitePausedUntil || this.pool.waitingCount > 0) { await sleep(250); continue; }
        const task = await this.queue.claim(workerId, this.config.QUEUE_LEASE_DURATION_SECONDS);
        if (!task) { await sleep(750); continue; }
        await this.handle(task, workerId);
      } catch { await sleep(1_000); }
    }
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
        this.logger.warn('Pausing new Lite claims to protect shared site resources', { status: response.status, elapsed });
      }
    } catch {
      // A probe outage should reduce pressure, not cause retries or a restart.
      this.sitePausedUntil = Date.now() + 30_000;
    }
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
      for (const work of page.works.slice(0, 100)) {
        await this.queue.enqueue({ taskType: 'CATALOG', workKey: `source:${source}:${work.sourceWorkId}`, priority: 'P2',
          idempotencyKey: `lite:catalog:${source}:${work.sourceWorkId}`, payload: { source, sourceWorkId: work.sourceWorkId } });
      }
      // The durable task itself holds the cursor; no new cursor rows or sweep.
      await this.queue.finish(task, workerId, { state: 'RETRY_WAIT', priority: 'P2', nextRunAt: new Date(Date.now() + (page.nextCursor ? 1_000 : 10 * 60_000)),
        checkpoint: { cursor: page.nextCursor } });
    } catch (error: any) {
      await this.queue.finish(task, workerId, { state: 'RETRY_WAIT', reason: String(error?.message || 'DISCOVERY_FAILED'), nextRunAt: new Date(Date.now() + 60_000) });
    }
  }

  private async catalog(task: LiteTask, workerId: string): Promise<void> {
    const source = String(task.payload.source || ''); const sourceWorkId = String(task.payload.sourceWorkId || '');
    const adapter = this.registry.get(source);
    if (!adapter || !sourceWorkId) return void await this.queue.finish(task, workerId, { state: 'BLOCKED', reason: 'CATALOG_INPUT_INVALID' });
    try {
      const snapshot = adapter.fetchWorkSnapshot ? await adapter.fetchWorkSnapshot(sourceWorkId) : {
        details: await adapter.fetchWorkDetails(sourceWorkId), chapters: await adapter.fetchChapters(sourceWorkId),
      };
      const resolved = await this.dedupe.resolveWork({ source, sourceWorkId, title: snapshot.details.title, slug: snapshot.details.slug,
        aliases: snapshot.details.alternativeTitles, genres: snapshot.details.genres, author: snapshot.details.author,
        artist: snapshot.details.artist, synopsis: snapshot.details.synopsis, rawMetadata: snapshot.details.raw,
        loadCoverId: snapshot.details.coverUrl ? () => this.loadCover(adapter, snapshot.details.coverUrl!) : undefined } as any);
      if (!resolved.workId || resolved.status === 'AMBIGUOUS') {
        await this.queue.finish(task, workerId, { state: 'BLOCKED', reason: resolved.reason || 'AMBIGUOUS_CATALOG_MATCH' }); return;
      }
      const language = String(snapshot.details.language || (snapshot.details.raw as any)?.language || 'pt-BR');
      const edition = String(snapshot.details.edition || (snapshot.details.raw as any)?.edition || 'default');
      for (const chapter of snapshot.chapters) {
        const displayNumber = chapter.displayNumber || String(chapter.number);
        const chapterKey = canonicalChapterKey({ displayNumber, title: chapter.title, language, edition });
        const orderKey = chapterOrderKey({ displayNumber, title: chapter.title });
        await this.pool.query(`INSERT INTO importer_lite_chapter_sources(work_id,canonical_chapter_key,source,source_chapter_id,language,edition,source_rank,confirmed,has_chapter,last_verified_at)
          VALUES($1::uuid,$2,$3,$4,$5,$6,$7,true,true,now())
          ON CONFLICT(source,source_chapter_id) DO UPDATE SET canonical_chapter_key=excluded.canonical_chapter_key,confirmed=true,has_chapter=true,last_verified_at=now(),updated_at=now()`,
          [resolved.workId,chapterKey,source,chapter.sourceChapterId,language,edition,rankForSource(source)]);
        await this.queue.enqueue({ taskType: 'IMPORT_CHAPTER', workId: resolved.workId, workKey: `work:${resolved.workId}`,
          chapterKey, priority: 'P1', idempotencyKey: `lite:chapter:${resolved.workId}:${chapterKey}`,
          payload: { displayNumber, chapterTitle: chapter.title || '', orderKey, language, edition } });
      }
      // Ongoing works retain one cheap, persistent P0 check; completed works
      // stay catalogued and are revisited only through discovery/repair.
      const ongoing = snapshot.details.status === 'ONGOING';
      await this.queue.finish(task, workerId, ongoing ? { state: 'RETRY_WAIT', priority: 'P0', nextRunAt: new Date(Date.now() + 15 * 60_000), checkpoint: { workId: resolved.workId } } : { state: 'DONE', checkpoint: { workId: resolved.workId } });
    } catch (error: any) {
      await this.queue.finish(task, workerId, { state: task.attempts >= task.maxAttempts ? 'BLOCKED' : 'RETRY_WAIT',
        reason: String(error?.message || 'CATALOG_FAILED').slice(0,500), nextRunAt: new Date(Date.now() + 60_000) });
    }
  }

  /** Invalid/blocked covers remain absent; they never overwrite a known cover. */
  private async loadCover(adapter: NonNullable<ReturnType<SourceRegistry['get']>>, url: string): Promise<string | null> {
    try {
      const bytes = await this.downloads.runExclusive(async () => {
        const response = await fetch(url, { headers: await adapter.getImageHeaders?.(url), signal: AbortSignal.timeout(30_000) });
        if (!response.ok) throw new Error(`COVER_HTTP_${response.status}`);
        return readImageBody(response, { signal: AbortSignal.timeout(30_000), maxBytes: 8 * 1024 * 1024 });
      });
      const stored = await processAndStoreMedia(this.catalogClient, this.storage, bytes, this.config.IMPORTER_USER_ID!, 'cover');
      return stored.mediaId;
    } catch { return null; }
  }
}
