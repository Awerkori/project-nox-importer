import { randomUUID } from 'node:crypto';
import type { Pool } from 'pg';
import { DeduplicationEngine } from '../core/deduplication.js';
import type { Config } from '../config.js';
import type { SourceRegistry } from '../sources/registry.js';
import type { StorageProvider } from '../storage/provider.js';
import { canonicalChapterKey, chapterKind, chapterOrderKey } from './chapter-identity.js';
import { LiteExecutor } from './executor.js';
import { LiteRepository, type EnqueueLiteTask } from './repository.js';
import type { LiteTask } from './types.js';
import { Logger } from '../core/logger.js';
import { readImageBody } from '../core/bounded-body.js';
import { processAndStoreMedia } from '../storage/media.js';
import { AsyncSemaphore } from '../core/concurrency.js';
import { HostRateLimiter } from '../core/rate-limiter.js';
import { telemetryCollector } from '../core/telemetry-collector.js';
import { sanitizeSynopsis } from './metadata.js';

const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));
const withTimeout = async <T>(operation: Promise<T>, timeoutMs: number, code: string): Promise<T> => {
  let timer: NodeJS.Timeout | undefined;
  try {
    return await Promise.race([
      operation,
      new Promise<never>((_, reject) => {
        timer = setTimeout(() => reject(new Error(code)), timeoutMs);
        timer.unref();
      }),
    ]);
  } finally {
    if (timer) clearTimeout(timer);
  }
};
const rankForSource = (source: string) => source === 'mangaflix' ? 10 : source === 'manhastro' ? 20 : 100;
// Deliberately shared with the legacy runtime.  This fences a rolling
// cutover: Lite must stand by while v1 owns the producer lease (and vice
// versa), rather than merely avoiding duplicate Lite claims.
const RUNTIME_LEADER_KEY = 'importer_runtime_leader';
const RUNTIME_LEASE_SECONDS = 45;
const RUNTIME_LEASE_RENEW_MS = 15_000;

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
  // Observation is intentionally a one-slot lane. It may create P0 chapter
  // work, but an empty observation must never compete with chapter workers.
  private readonly monitorGate = new AsyncSemaphore(1, 'lite_p0_monitor');
  private readonly runtimeInstanceId: string;
  private readonly runtimeBootedAt = new Date();
  private isRuntimeLeader = false;
  private runtimeLeadershipTimer: NodeJS.Timeout | null = null;
  private runtimeLeadershipRenewing = false;
  private sitePausedUntil = 0;
  private idleClaimUntil = 0;
  private idleMonitorUntil = 0;
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
    this.runtimeInstanceId = `${config.WORKER_ID}@${process.pid}-${randomUUID().slice(0, 8)}`;
    this.queue = new LiteRepository(pool);
    this.downloads = new AsyncSemaphore(config.DOWNLOAD_INFLIGHT_CONCURRENCY, 'lite_global_page_downloads');
    this.bufferedPages = new AsyncSemaphore(config.BUFFERED_PAGE_CONCURRENCY, 'lite_global_page_buffers');
    this.executor = new LiteExecutor(pool, this.queue, registry, storage, config.IMPORTER_USER_ID, this.downloads,
      Math.max(1, Math.min(6, config.BATCH_PAGE_DOWNLOAD_CONCURRENCY)), this.bufferedPages, sourceRateLimiter);
    this.dedupe = new DeduplicationEngine(catalogClient);
  }

  async start(): Promise<void> {
    // Do this before taking the shared producer lease or creating a task. A
    // Lite process must fail closed against an old Reader schema, rather than
    // discover a numeric collision after it has downloaded/uploaded media.
    await this.assertReaderCanonicalIdentitySchema();
    if (!(await this.waitForRuntimeLeadership())) return;
    await this.seedDiscovery();
    // A former runtime may have left every source observation due at once.
    // Stagger only discovery rows; executable catalog/chapter work remains
    // immediately eligible under the normal strict priority contract.
    await this.queue.staggerReadyDiscoveries(60_000, 64);
    // Do not inherit an unmeasured legacy MAX_CONCURRENT_CHAPTERS during a
    // Lite cutover.  The Lite-specific cap begins safely and can only rise
    // through an explicit, benchmarked staging configuration.
    const workers = Math.max(1, Math.min(this.config.LITE_CHAPTER_WORKERS ?? 1,
      this.config.MAX_CONCURRENT_CHAPTERS, this.config.TESTED_CONCURRENCY_CEILING));
    telemetryCollector.configureChapterSlots(workers, () => workers);
    // Queue lease owners include the per-boot leader identity. A process that
    // lost leadership can never checkpoint or finish a lease reclaimed by a
    // successor which happens to use the same configured WORKER_ID.
    for (let i = 0; i < workers; i++) void this.worker(`${this.runtimeInstanceId}:lite:${i + 1}`, i);
    const timer = setInterval(() => {
      if (this.isRuntimeLeader) void this.queue.reconcile(50).catch(() => {});
    }, 30_000);
    timer.unref(); this.timers.push(timer);
    const monitorTimer = setInterval(() => void this.runMonitorTurn().catch(() => {}), 1_000);
    monitorTimer.unref(); this.timers.push(monitorTimer);
    const healthTimer = setInterval(() => void this.probeSharedResources(), 30_000);
    healthTimer.unref(); this.timers.push(healthTimer);
    const metricsTimer = setInterval(() => void this.emitOperationalSnapshot().catch(() => {}), 60_000);
    metricsTimer.unref(); this.timers.push(metricsTimer);
    await this.probeSharedResources();
    await this.emitOperationalSnapshot();
  }

  private async assertReaderCanonicalIdentitySchema(): Promise<void> {
    const required = ['canonical_chapter_key', 'display_number', 'chapter_order_key', 'chapter_kind', 'edition', 'language'];
    const columns = await this.pool.query<{ column_name: string; is_nullable: string }>(
      `SELECT column_name,is_nullable
       FROM information_schema.columns
       WHERE table_schema='public' AND table_name='chapters'
         AND column_name IN ('canonical_chapter_key','display_number','chapter_order_key','chapter_kind','edition','language','number')`,
    );
    const present = new Map(columns.rows.map((row) => [row.column_name, row.is_nullable]));
    const missing = required.filter((column) => !present.has(column));
    const numberNullable = present.get('number') === 'YES';
    const canonicalIndex = await this.pool.query<{ exists: boolean }>(
      `SELECT EXISTS(
         SELECT 1 FROM pg_indexes
         WHERE schemaname='public' AND tablename='chapters'
           AND indexname='chapters_work_canonical_chapter_key_uq'
       ) AS exists`,
    );
    const numericConstraint = await this.pool.query<{ exists: boolean }>(
      `SELECT EXISTS(
         SELECT 1 FROM pg_indexes
         WHERE schemaname='public' AND tablename='chapters'
           AND indexname='chapters_work_id_number_key'
       ) AS exists`,
    );
    if (missing.length || !numberNullable || canonicalIndex.rows[0]?.exists !== true || numericConstraint.rows[0]?.exists === true) {
      const detail = [
        missing.length ? `missing=${missing.join(',')}` : null,
        !numberNullable ? 'number_is_still_required' : null,
        canonicalIndex.rows[0]?.exists !== true ? 'canonical_unique_index_missing' : null,
        numericConstraint.rows[0]?.exists === true ? 'legacy_numeric_uniqueness_active' : null,
      ].filter(Boolean).join(';');
      throw new Error(`READER_CANONICAL_IDENTITY_MIGRATION_REQUIRED:${detail}`);
    }
  }

  stop(): void {
    this.stopped = true;
    for (const timer of this.timers) clearInterval(timer);
    this.timers = [];
    void this.releaseRuntimeLeadership();
  }

  /**
   * The shared settings lease is the only cross-runtime coordination point.
   * It gates admission only: workers which already hold a task lease may
   * finish their durable write while a new STAFF request or a successor
   * runtime waits to claim new work.
   */
  private async tryAcquireRuntimeLeadership(): Promise<boolean> {
    try {
      const result = await this.pool.query(
        `INSERT INTO settings AS leader (key,value)
         VALUES($1,json_build_object('owner',$2::text,'started_at',$4::timestamptz,
           'expires_at',now()+($3::int * interval '1 second'))::text)
         ON CONFLICT(key) DO UPDATE SET value=json_build_object('owner',$2::text,
           'started_at',$4::timestamptz,'expires_at',now()+($3::int * interval '1 second'))::text
         WHERE (leader.value::jsonb->>'owner')=$2::text
            OR COALESCE((leader.value::jsonb->>'expires_at')::timestamptz,'epoch'::timestamptz)<=now()
         RETURNING value`,
        [RUNTIME_LEADER_KEY, this.runtimeInstanceId, RUNTIME_LEASE_SECONDS, this.runtimeBootedAt.toISOString()],
      );
      const acquired = result.rows.length > 0;
      if (acquired && !this.isRuntimeLeader) {
        this.isRuntimeLeader = true;
        this.logger.info('Acquired Lite runtime leadership', { instanceId: this.runtimeInstanceId, leaseSeconds: RUNTIME_LEASE_SECONDS });
      }
      return acquired;
    } catch (error: any) {
      this.logger.warn('Failed to acquire Lite runtime leadership', { error: error?.message });
      return false;
    }
  }

  private startRuntimeLeadershipRenewal(): void {
    if (this.runtimeLeadershipTimer) return;
    this.runtimeLeadershipTimer = setInterval(async () => {
      if (!this.isRuntimeLeader || this.runtimeLeadershipRenewing || this.stopped) return;
      this.runtimeLeadershipRenewing = true;
      try {
        const result = await this.pool.query(
          `UPDATE settings SET value=json_build_object('owner',$2::text,'started_at',$4::timestamptz,
             'expires_at',now()+($3::int * interval '1 second'))::text
           WHERE key=$1 AND (value::jsonb->>'owner')=$2::text RETURNING value`,
          [RUNTIME_LEADER_KEY, this.runtimeInstanceId, RUNTIME_LEASE_SECONDS, this.runtimeBootedAt.toISOString()],
        );
        if (result.rows.length === 0) {
          this.isRuntimeLeader = false;
          this.logger.error('Lost Lite runtime leadership; new claims are gated.', { instanceId: this.runtimeInstanceId });
        }
      } catch (error: any) {
        // The durable lease remains valid for its remaining window. Retrying
        // renewal is safer than treating a transient YSQL fault as permission
        // for another runtime to publish immediately.
        this.logger.warn('Lite runtime leadership renewal failed', { error: error?.message });
      } finally {
        this.runtimeLeadershipRenewing = false;
      }
    }, RUNTIME_LEASE_RENEW_MS);
    this.runtimeLeadershipTimer.unref();
  }

  private async waitForRuntimeLeadership(): Promise<boolean> {
    let lastStandbyLog = 0;
    while (!this.stopped) {
      if (await this.tryAcquireRuntimeLeadership()) {
        this.startRuntimeLeadershipRenewal();
        return true;
      }
      if (Date.now() - lastStandbyLog >= 30_000) {
        this.logger.warn('Lite is on standby; another importer owns the runtime lease.');
        lastStandbyLog = Date.now();
      }
      await sleep(5_000);
    }
    return false;
  }

  private async releaseRuntimeLeadership(): Promise<void> {
    if (this.runtimeLeadershipTimer) {
      clearInterval(this.runtimeLeadershipTimer);
      this.runtimeLeadershipTimer = null;
    }
    if (!this.isRuntimeLeader) return;
    this.isRuntimeLeader = false;
    try {
      await this.pool.query(
        `UPDATE settings SET value=json_build_object('owner',$2::text,'expires_at',now())::text
         WHERE key=$1 AND (value::jsonb->>'owner')=$2::text`,
        [RUNTIME_LEADER_KEY, this.runtimeInstanceId],
      );
    } catch (error: any) {
      this.logger.warn('Failed to release Lite runtime leadership', { error: error?.message });
    }
  }

  private async seedDiscovery(): Promise<void> {
    const sources = await this.pool.query<{ id: string; sync_interval_minutes: number }>(`SELECT id,sync_interval_minutes FROM importer_sources
      WHERE enabled=true AND COALESCE(status,'ACTIVE') IN ('ACTIVE','DEGRADED','RECOVERING')
        AND COALESCE((to_jsonb(importer_sources)->>'catalog_discovery_enabled')::boolean,true)
      ORDER BY id`);
    const firstRun = Date.now();
    for (const [index, { id, sync_interval_minutes }] of sources.rows.entries()) {
      await this.queue.enqueue({ taskType: 'DISCOVER', workKey: `source:${id}`, priority: 'P2',
        idempotencyKey: `lite:discover:${id}`, payload: { source: id, syncIntervalMinutes: sync_interval_minutes },
        checkpoint: { cursor: null }, nextRunAt: new Date(firstRun + index * 60_000) });
    }
  }

  private async upsertChapterSources(rows: Array<{
    workId: string; chapterKey: string; source: string; sourceWorkId: string; sourceChapterId: string;
    language: string; edition: string; orderKey: string; sourceRank: number;
  }>): Promise<void> {
    for (let offset = 0; offset < rows.length; offset += 500) {
      const batch = rows.slice(offset, offset + 500);
      const values: unknown[] = [];
      const tuples = batch.map((row, index) => {
        const start = index * 9;
        values.push(row.workId, row.chapterKey, row.source, row.sourceWorkId, row.sourceChapterId,
          row.language, row.edition, row.orderKey, row.sourceRank);
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

  private async worker(workerId: string, slotIndex: number): Promise<void> {
    telemetryCollector.registerSlot(slotIndex);
    while (!this.stopped) {
      try {
        if (!this.isRuntimeLeader) {
          telemetryCollector.setSlotState(slotIndex, 'WAITING_BARRIER', 'runtime-standby');
          await sleep(250);
          continue;
        }
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
      if (!this.isRuntimeLeader) return null;
      if (Date.now() < this.idleClaimUntil) return null;
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
  private async runMonitorTurn(): Promise<void> {
    if (this.stopped || !this.isRuntimeLeader || Date.now() < this.sitePausedUntil || this.pool.waitingCount > 0) return;
    if (!this.monitorGate.tryAcquire()) return;
    try {
      if (Date.now() < this.idleMonitorUntil) return;
      const task = await this.queue.claimMonitor(`${this.runtimeInstanceId}:lite-monitor`, this.config.QUEUE_LEASE_DURATION_SECONDS);
      this.idleMonitorUntil = task ? 0 : Date.now() + 15_000;
      if (task) await this.handle(task, `${this.runtimeInstanceId}:lite-monitor`);
    } finally {
      this.monitorGate.release();
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
    const snapshot = {
      runtime: 'lite' as const,
      runtimeInstanceId: this.runtimeInstanceId,
      leader: this.isRuntimeLeader,
      chapterWorkers: Math.max(1, Math.min(this.config.LITE_CHAPTER_WORKERS ?? 1,
        this.config.MAX_CONCURRENT_CHAPTERS, this.config.TESTED_CONCURRENCY_CEILING)),
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
    };
    this.logger.info('Lite operational progress', snapshot);
    // A compact durable marker makes a native deploy auditable even when its
    // dashboard only exposes a rolling log window. It is diagnostic state,
    // never an authority for leadership or task eligibility.
    await this.pool.query(
      `INSERT INTO settings(key,value) VALUES('importer_heartbeat',$1)
       ON CONFLICT(key) DO UPDATE SET value=excluded.value`,
      [JSON.stringify({ ...snapshot, observedAt: new Date().toISOString() })],
    );
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
    const policy = await this.queue.sourcePolicy(source);
    if (!policy?.runnable || !policy.catalogDiscoveryEnabled) {
      const nextRunAt = policy?.cooldownUntil && policy.cooldownUntil > new Date()
        ? policy.cooldownUntil : new Date(Date.now() + (policy?.syncIntervalMs || 60 * 60_000));
      await this.queue.finish(task, workerId, { state: 'RETRY_WAIT',
        reason: !policy?.catalogDiscoveryEnabled ? 'SOURCE_DISCOVERY_DISABLED' : `SOURCE_NOT_READY:${policy?.status || 'MISSING'}`, nextRunAt });
      return;
    }
    try {
      const page = await withTimeout(
        adapter.fetchUpdatedWorks(String(task.checkpoint.cursor || '') || null, { mode: 'maintenance' }),
        this.config.SOURCE_OPERATION_TIMEOUT_MS,
        'DISCOVERY_SOURCE_TIMEOUT',
      );
      await this.queue.recordSourceSuccess(source);
      for (const work of page.works.slice(0, 100)) {
        await this.queue.enqueue({ taskType: 'CATALOG', workKey: `source:${source}:${work.sourceWorkId}`, priority: 'P2',
          idempotencyKey: `lite:catalog:${source}:${work.sourceWorkId}`, payload: { source, sourceWorkId: work.sourceWorkId } });
      }
      // The durable task itself holds the cursor; no new cursor rows or sweep.
      // A cursor asks for a later small batch, not an immediate unbounded
      // sweep. This leaves a one-worker cutover free to catalog materialized
      // works and prevents one source from filling every P2 admission slot.
      const nextDelay = page.nextCursor
        ? Math.max(60_000, Math.min(policy.syncIntervalMs, 5 * 60_000))
        : policy.syncIntervalMs;
      await this.queue.finish(task, workerId, { state: 'RETRY_WAIT', priority: 'P2', nextRunAt: new Date(Date.now() + nextDelay),
        checkpoint: { cursor: page.nextCursor }, resetAttempts: true });
    } catch (error: any) {
      await this.queue.recordSourceFailure(source, error);
      // Discovery is one durable recurring observation per source. A long
      // upstream outage must not create an unbounded task set or make that
      // source permanently invisible; its retry cadence is capped instead.
      await this.queue.finish(task, workerId, { state: 'RETRY_WAIT', reason: String(error?.message || 'DISCOVERY_FAILED'),
        nextRunAt: new Date(Date.now() + this.recurringRetryDelayMs(task.attempts)) });
    }
  }

  private async catalog(task: LiteTask, workerId: string): Promise<void> {
    const source = String(task.payload.source || ''); const sourceWorkId = String(task.payload.sourceWorkId || '');
    const adapter = this.registry.get(source);
    if (!adapter || !sourceWorkId) return void await this.queue.finish(task, workerId, { state: 'BLOCKED', reason: 'CATALOG_INPUT_INVALID' });
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
      const snapshot = await withTimeout(adapter.fetchWorkSnapshot
        ? adapter.fetchWorkSnapshot(sourceWorkId)
        : Promise.all([adapter.fetchWorkDetails(sourceWorkId), adapter.fetchChapters(sourceWorkId)])
          .then(([details, chapters]) => ({ details, chapters })),
      this.config.SOURCE_OPERATION_TIMEOUT_MS,
      'CATALOG_SOURCE_TIMEOUT');
      fetchedSnapshot = true;
      const rawContentRating = String((snapshot.details.raw as any)?.contentRating || (snapshot.details.raw as any)?.content_rating || '').toUpperCase();
      const resolved = await this.dedupe.resolveWork({ source, sourceWorkId, title: snapshot.details.title, slug: snapshot.details.slug,
        aliases: snapshot.details.alternativeTitles, genres: snapshot.details.genres, author: snapshot.details.author,
        artist: snapshot.details.artist, synopsis: sanitizeSynopsis(snapshot.details.synopsis) || undefined, kind: snapshot.details.kind,
        status: snapshot.details.status, year: snapshot.details.year, ageRating: snapshot.details.ageRating,
        contentRating: rawContentRating === 'ADULT_18' || Number(snapshot.details.ageRating || 0) >= 18 ? 'ADULT_18' : undefined,
        rawMetadata: snapshot.details.raw,
        loadCoverId: snapshot.details.coverUrl ? () => this.loadCover(adapter, snapshot.details.coverUrl!) : undefined } as any);
      if (!resolved.workId || resolved.status === 'AMBIGUOUS') {
        await this.queue.finish(task, workerId, { state: 'BLOCKED', reason: resolved.reason || 'AMBIGUOUS_CATALOG_MATCH' }); return;
      }
      const language = String(snapshot.details.language || (snapshot.details.raw as any)?.language || 'pt-BR');
      const edition = String(snapshot.details.edition || (snapshot.details.raw as any)?.edition || 'default');
      // A recurring monitor is intentionally separate from P0 *execution*.
      // P0 only accelerates releases beyond the durable manifest frontier.
      // A source can reveal historical chapters later; those are P1 repairs,
      // not P0, without inferring that every numeric gap is a missing chapter.
      const p0Monitoring = task.monitoring || task.priority === 'P0' || task.priority === 'STAFF';
      const knownFrontier = p0Monitoring
        ? await this.pool.query<{ order_key: string | null }>(`SELECT max(order_key) AS order_key
          FROM importer_lite_chapter_sources WHERE work_id=$1::uuid AND order_key LIKE 'n|%'`, [resolved.workId])
        : { rows: [{ order_key: null }] };
      const knownNormalOrder = knownFrontier.rows[0]?.order_key || null;
      const sourceRows: Array<{
        workId: string; chapterKey: string; source: string; sourceWorkId: string; sourceChapterId: string;
        language: string; edition: string; orderKey: string; sourceRank: number;
      }> = [];
      const chapterTasks: EnqueueLiteTask[] = [];
      const sourceChapterIds = new Set<string>();
      for (const chapter of snapshot.chapters) {
        // A duplicate manifest entry cannot be allowed to make one batched
        // UPSERT fail and delay every other chapter in this catalog pass.
        if (sourceChapterIds.has(chapter.sourceChapterId)) continue;
        sourceChapterIds.add(chapter.sourceChapterId);
        const displayNumber = chapter.displayNumber || String(chapter.number);
        const chapterKindValue = chapterKind({ displayNumber, title: chapter.title });
        const chapterKey = canonicalChapterKey({ displayNumber, title: chapter.title, kind: chapterKindValue, language, edition });
        const orderKey = chapterOrderKey({ displayNumber, title: chapter.title, kind: chapterKindValue });
        const historicalRepair = p0Monitoring && orderKey.startsWith('n|') && Boolean(knownNormalOrder && orderKey <= knownNormalOrder);
        const chapterPriority = p0Monitoring && !historicalRepair ? 'P0' : 'P1';
        sourceRows.push({ workId: resolved.workId, chapterKey, source, sourceWorkId, sourceChapterId: chapter.sourceChapterId,
          language, edition, orderKey, sourceRank: rankForSource(source) });
        if (policy.chapterIngestionEnabled) {
          chapterTasks.push({ taskType: 'IMPORT_CHAPTER', workId: resolved.workId, workKey: `work:${resolved.workId}`,
            chapterKey, priority: chapterPriority, idempotencyKey: `lite:chapter:${resolved.workId}:${chapterKey}`,
            payload: { displayNumber, chapterTitle: chapter.title || '', chapterKind: chapterKindValue, orderKey, language, edition } });
        }
      }
      await this.upsertChapterSources(sourceRows);
      const materialized = await this.queue.enqueueMany(chapterTasks);
      const newReleaseCount = chapterTasks.filter((candidate) =>
        candidate.priority === 'P0' && materialized.insertedKeys.has(candidate.idempotencyKey),
      ).length;
      // A newly confirmed mapping (or repaired presentation metadata) is
      // evidence worth re-evaluating. This indexed, per-work wake-up replaces
      // the legacy global blocked-task sweeps.
      if (policy.chapterIngestionEnabled) await this.queue.wakeCatalogBlocked(resolved.workId, `work:${resolved.workId}`);
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
    } catch (error: any) {
      if (!fetchedSnapshot) await this.queue.recordSourceFailure(source, error);
      const monitorRetry = task.monitoring;
      const retryDelay = monitorRetry ? this.recurringRetryDelayMs(task.attempts) : 60_000;
      // A monitoring row is an ongoing P0 observation, not a failed chapter.
      // Keep exactly one durable row with a capped cadence; its successful
      // next observation resets the failure window. A manually-awakened
      // STAFF catalog task is no longer marked monitoring and still obtains a
      // visible terminal block after its normal bounded attempts.
      await this.queue.finish(task, workerId, { state: monitorRetry ? 'RETRY_WAIT' : task.attempts >= task.maxAttempts ? 'BLOCKED' : 'RETRY_WAIT',
        reason: String(error?.message || 'CATALOG_FAILED').slice(0,500), nextRunAt: new Date(Date.now() + retryDelay),
        priority: monitorRetry ? 'P2' : undefined, monitoring: monitorRetry || undefined,
        checkpoint: monitorRetry ? { monitorFailures: Number(task.checkpoint.monitorFailures || 0) + 1 } : undefined });
    }
  }

  /** Release bursts get a quick recheck; quiet works exponentially back off. */
  private monitorIntervalMs(task: LiteTask, status: string, newReleaseCount: number): number {
    if (status === 'HIATUS') return 6 * 60 * 60_000;
    if (newReleaseCount > 0) return 5 * 60_000;
    const emptyChecks = Math.max(0, Number(task.checkpoint.emptyChecks || 0));
    return Math.min(6 * 60 * 60_000, 15 * 60_000 * 2 ** Math.min(emptyChecks, 5));
  }

  /** Bounded health recovery for the two recurring observation task types. */
  private recurringRetryDelayMs(attempts: number): number {
    return Math.min(6 * 60 * 60_000, 60_000 * 2 ** Math.min(Math.max(0, attempts), 8));
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
