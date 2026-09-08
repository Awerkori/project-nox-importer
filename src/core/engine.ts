import type { SupabaseClient } from '@supabase/supabase-js';
import { SourceRegistry } from '../sources/registry.js';
import { StorageProvider } from '../storage/provider.js';
import { ImporterQueue, QueueJob } from './queue.js';
import { DeduplicationEngine, CandidateWork } from './deduplication.js';
import { CheckpointManager } from './checkpoint.js';
import { HostRateLimiter } from './rate-limiter.js';
import { processAndStoreMedia } from '../storage/media.js';
import { Logger } from './logger.js';
import { Config } from '../config.js';
import { diagnostics } from './diagnostics.js';
import { AdaptiveAutotuner, AsyncSemaphore } from './concurrency.js';

export class ImporterEngine {
  private logger = new Logger('Engine');
  private queue: ImporterQueue;
  private deduplication: DeduplicationEngine;
  private checkpoints: CheckpointManager;
  private autotuner: AdaptiveAutotuner;
  private isRunning = false;
  private stopSignal = false;
  private abortController = new AbortController();

  constructor(
    private supabase: SupabaseClient,
    private storage: StorageProvider,
    private registry: SourceRegistry,
    private rateLimiter: HostRateLimiter,
    private config: Config
  ) {
    this.queue = new ImporterQueue(supabase, config.WORKER_ID);
    this.deduplication = new DeduplicationEngine(supabase);
    this.checkpoints = new CheckpointManager(supabase);
    this.autotuner = new AdaptiveAutotuner({
      initialConcurrency: Math.min(3, config.MAX_CONCURRENT_CHAPTERS || 3),
      maxConcurrency: Math.max(3, config.MAX_CONCURRENT_CHAPTERS || 6),
    });
  }

  getAutotuner(): AdaptiveAutotuner {
    return this.autotuner;
  }

  async start(): Promise<void> {
    this.isRunning = true;
    this.stopSignal = false;
    this.abortController = new AbortController();

    this.logger.info('Importer Engine daemon started with multi-source concurrent runners', {
      workerId: this.config.WORKER_ID,
      storageProvider: this.storage.getProviderKey(),
      initialConcurrency: this.autotuner.getCurrentConcurrency(),
    });

    // 1. Run startup recovery for stalled jobs from crashed instances
    await this.runStartupRecovery();

    // 2. Launch background autotuner telemetry loop (every 30s)
    this.runAutotunerLoop();

    // 3. Launch background discovery scheduler loop
    this.runDiscoveryLoop();

    // 4. Launch independent concurrent worker loops for each registered source
    const activeWorkers: Promise<void>[] = [];
    for (const adapter of this.registry.getAll()) {
      activeWorkers.push(this.runSourceWorker(adapter.id));
    }
    // General worker to process any unassigned or balancing jobs
    activeWorkers.push(this.runGeneralWorker());

    // Wait until all workers finish upon stop signal
    await Promise.all(activeWorkers);

    this.isRunning = false;
    this.logger.info('Importer Engine stopped gracefully');
  }

  async runStartupRecovery(): Promise<void> {
    try {
      const { data: stalled, error } = await this.supabase
        .from('importer_queue')
        .select('id, task_type, source, attempts, locked_by, lease_expires_at')
        .eq('status', 'IMPORTING')
        .lt('lease_expires_at', new Date().toISOString());

      if (error) {
        this.logger.warn('Failed to query stalled jobs during startup recovery', { error: error.message });
        return;
      }

      if (stalled && stalled.length > 0) {
        this.logger.warn(`Startup recovery detected ${stalled.length} interrupted job(s) from previous worker crash`, {
          count: stalled.length,
          stalledJobs: stalled.map((j) => ({
            id: j.id,
            taskType: j.task_type,
            source: j.source,
            attempts: j.attempts,
            previousWorker: j.locked_by,
          })),
        });
      } else {
        this.logger.info('Startup recovery check passed: no stalled jobs detected');
      }
    } catch (err: any) {
      this.logger.warn('Error during startup recovery check', { error: err?.message });
    }
  }

  stop(): void {
    this.stopSignal = true;
    this.abortController.abort();
  }

  /**
   * Periodic discovery scheduler running in the background
   */
  private async runDiscoveryLoop(): Promise<void> {
    while (!this.stopSignal) {
      try {
        await this.scheduleSources();
      } catch (err: any) {
        this.logger.error('Error during source discovery scheduling', { error: err?.message });
      }

      // Check discovery every 30 seconds
      await this.sleep(30_000);
    }
  }

  /**
   * Periodic autotuner telemetry & evaluation loop (every 30s)
   */
  private async runAutotunerLoop(): Promise<void> {
    while (!this.stopSignal) {
      await this.sleep(30_000);
      if (this.stopSignal) break;

      try {
        const mem = diagnostics.getMemorySnapshot();
        const evaluation = this.autotuner.evaluateCycle();
        const activeJobs = diagnostics.getActiveJobsCount();

        this.logger.info(
          `[Autotuner Telemetry] Action: ${evaluation.action} | Concurrency: ${evaluation.concurrency} | Active Jobs: ${activeJobs} | Mem: ${mem.heapUsedMb}MB heap / ${mem.rssMb}MB rss (512MB RAM) | Reason: ${evaluation.reason}`
        );
      } catch (err: any) {
        this.logger.error('Error during autotuner evaluation loop', { error: err?.message });
      }
    }
  }

  /**
   * Dedicated worker loop for a specific source
   */
  private async runSourceWorker(source: string): Promise<void> {
    this.logger.info(`Starting dedicated runner for source: ${source}`);

    while (!this.stopSignal) {
      try {
        // Verify source status before attempting to acquire
        const { data: src } = await this.supabase
          .from('importer_sources')
          .select('status, enabled, cooldown_until')
          .eq('id', source)
          .maybeSingle();

        if (src) {
          if (!src.enabled || src.status === 'PAUSED' || src.status === 'DISABLED') {
            await this.sleep(10_000);
            continue;
          }

          if (src.status === 'COOLDOWN') {
            const cooldownUntil = src.cooldown_until ? new Date(src.cooldown_until).getTime() : 0;
            if (Date.now() < cooldownUntil) {
              await this.sleep(10_000);
              continue;
            }
          }
        }

        // Acquire job for this source
        const job = await this.queue.acquireNextJob(
          Math.ceil(this.config.QUEUE_LEASE_DURATION_SECONDS / 60),
          source
        );

        if (!job) {
          // Queue empty for this source: idle sleep 5 seconds
          await this.sleep(5_000);
          continue;
        }

        // Process job with concurrency semaphores
        await this.executeJobWithLimits(job);

        // Continuous drain: immediately check for next job without delay
        await this.sleep(50);
      } catch (err: any) {
        this.logger.error(`Error in worker loop for source ${source}`, { error: err?.message });
        await this.sleep(5_000);
      }
    }
  }

  /**
   * General worker loop to process jobs with no source filter
   */
  private async runGeneralWorker(): Promise<void> {
    this.logger.info('Starting general fallback runner');

    while (!this.stopSignal) {
      try {
        const job = await this.queue.acquireNextJob(
          Math.ceil(this.config.QUEUE_LEASE_DURATION_SECONDS / 60)
        );

        if (!job) {
          await this.sleep(10_000);
          continue;
        }

        await this.executeJobWithLimits(job);
        await this.sleep(50);
      } catch (err: any) {
        this.logger.error('Error in general worker loop', { error: err?.message });
        await this.sleep(5_000);
      }
    }
  }

  /**
   * Executes a job respecting global and per-source concurrency semaphores
   */
  private async executeJobWithLimits(job: QueueJob): Promise<void> {
    if (job.task_type === 'IMPORT_CHAPTER') {
      const globalSem = this.autotuner.getGlobalChapterSemaphore();
      const sourceSem = this.autotuner.getSourceSemaphore(job.source, 2);

      await globalSem.runExclusive(async () => {
        await sourceSem.runExclusive(async () => {
          await this.processJob(job);
        });
      });
    } else {
      await this.processJob(job);
    }
  }

  /**
   * Discrete step method preserved for unit tests & single iterations
   */
  async step(source?: string): Promise<boolean> {
    await this.scheduleSources();

    const job = await this.queue.acquireNextJob(
      Math.ceil(this.config.QUEUE_LEASE_DURATION_SECONDS / 60),
      source
    );

    if (!job) {
      this.logger.debug('No pending jobs in queue', { source });
      return false;
    }

    await this.processJob(job);
    return true;
  }

  private async scheduleSources(): Promise<void> {
    const { data: sources, error } = await this.supabase
      .from('importer_sources')
      .select('*');

    if (error || !sources) return;

    const now = Date.now();

    for (const src of sources) {
      if (src.enabled === false || src.status === 'DISABLED' || src.status === 'PAUSED') {
        continue;
      }

      if (src.status === 'COOLDOWN') {
        const cooldownUntil = src.cooldown_until ? new Date(src.cooldown_until).getTime() : 0;
        if (now < cooldownUntil) {
          continue;
        }

        this.logger.info(`Source ${src.id} cooldown expired. Transitioning back to ACTIVE`, { source: src.id });
        src.status = 'ACTIVE';
        src.cooldown_until = null;
        await this.supabase
          .from('importer_sources')
          .update({ status: 'ACTIVE', cooldown_until: null, updated_at: new Date().toISOString() })
          .eq('id', src.id);
      }

      if (src.status !== 'ACTIVE') {
        continue;
      }

      if (src.base_url && src.rate_limit_per_second) {
        try {
          const host = new URL(src.base_url).host;
          this.rateLimiter.setHostRate(host, Number(src.rate_limit_per_second) || 2.0);
        } catch {}
      }

      const lastSync = src.last_sync_at ? new Date(src.last_sync_at).getTime() : 0;
      const intervalMs = (src.sync_interval_minutes || 30) * 60 * 1000;

      if (now - lastSync >= intervalMs) {
        const dedupeKey = `${src.id}:discover:${Math.floor(now / intervalMs)}`;
        await this.queue.enqueue('DISCOVER_WORKS', src.id, dedupeKey, {}, 10);

        await this.supabase
          .from('importer_sources')
          .update({ last_sync_at: new Date().toISOString() })
          .eq('id', src.id);
      }
    }
  }

  private async processJob(job: QueueJob): Promise<void> {
    const heartbeat = this.queue.startHeartbeat(
      job.id,
      this.config.QUEUE_HEARTBEAT_INTERVAL_SECONDS
    );

    try {
      const { data: sourceRec } = await this.supabase
        .from('importer_sources')
        .select('id, status, cooldown_until, enabled')
        .eq('id', job.source)
        .maybeSingle();

      if (sourceRec) {
        if (sourceRec.status === 'PAUSED' || sourceRec.status === 'DISABLED' || !sourceRec.enabled) {
          this.logger.info(`Postponing job ${job.id}: source ${job.source} is ${sourceRec.status}`);
          heartbeat.stop();
          await this.queue.releaseJob(job.id, 'RETRY', `Source ${job.source} is ${sourceRec.status}`, 15);
          return;
        }

        if (sourceRec.status === 'COOLDOWN') {
          const cooldownUntil = sourceRec.cooldown_until ? new Date(sourceRec.cooldown_until).getTime() : 0;
          if (Date.now() < cooldownUntil) {
            const waitMinutes = Math.max(1, Math.ceil((cooldownUntil - Date.now()) / 60000));
            heartbeat.stop();
            await this.queue.releaseJob(job.id, 'RETRY', `Source in COOLDOWN until ${sourceRec.cooldown_until}`, waitMinutes);
            return;
          } else {
            await this.supabase
              .from('importer_sources')
              .update({ status: 'ACTIVE', cooldown_until: null, updated_at: new Date().toISOString() })
              .eq('id', job.source);
          }
        }
      }

      this.logger.info('Processing job', {
        jobId: job.id,
        taskType: job.task_type,
        source: job.source,
        chapterSortKey: job.chapter_sort_key,
      });

      switch (job.task_type) {
        case 'DISCOVER_WORKS':
          await this.handleDiscoverWorks(job);
          break;
        case 'SYNC_WORK':
          await this.handleSyncWork(job);
          break;
        case 'IMPORT_CHAPTER':
          await this.handleImportChapter(job);
          break;
        default:
          throw new Error(`Unknown task type: ${job.task_type}`);
      }

      heartbeat.stop();
      await this.queue.releaseJob(job.id, 'COMPLETED');
    } catch (err: any) {
      heartbeat.stop();
      const errorMessage = err?.message || String(err);
      this.logger.error('Job execution failed', {
        jobId: job.id,
        taskType: job.task_type,
        error: errorMessage,
        attempts: job.attempts,
      });

      const is429 = err?.status === 429 || err?.statusCode === 429 || /429|rate\s*limit/i.test(errorMessage);
      const isTimeout = /timeout|aborted|ETIMEDOUT/i.test(errorMessage);

      if (is429) {
        this.autotuner.recordError('ratelimit');
        let waitSeconds = 60;
        const retryAfter = err?.retryAfter || err?.headers?.get?.('retry-after');
        if (retryAfter) {
          const parsed = parseInt(retryAfter, 10);
          if (!isNaN(parsed) && parsed > 0) waitSeconds = Math.min(3600, parsed);
        }
        const cooldownUntil = new Date(Date.now() + waitSeconds * 1000).toISOString();
        this.logger.warn(`Source ${job.source} entered COOLDOWN due to rate limit for ${waitSeconds}s`);

        await this.supabase
          .from('importer_sources')
          .update({
            status: 'COOLDOWN',
            cooldown_until: cooldownUntil,
            updated_at: new Date().toISOString(),
          })
          .eq('id', job.source);

        const backoffMinutes = Math.max(1, Math.ceil(waitSeconds / 60));
        await this.queue.releaseJob(job.id, 'RETRY', `Rate limit triggered: COOLDOWN until ${cooldownUntil}`, backoffMinutes);
        return;
      }

      if (isTimeout) {
        this.autotuner.recordError('timeout');
      } else {
        this.autotuner.recordError('error');
      }

      const nextStatus = job.attempts >= job.max_attempts ? 'FAILED' : 'RETRY';
      const backoffMinutes = Math.min(60, Math.pow(2, job.attempts) * 2);
      await this.queue.releaseJob(job.id, nextStatus, errorMessage, backoffMinutes);
    }
  }

  private async handleDiscoverWorks(job: QueueJob): Promise<void> {
    const adapter = this.registry.get(job.source);
    if (!adapter) throw new Error(`Source adapter not registered: ${job.source}`);

    const checkpoint = await this.checkpoints.getCheckpoint(job.source);
    const isCompleted = Boolean(checkpoint?.metadata?.catalog_completed);
    const mode: 'bootstrap' | 'maintenance' = isCompleted ? 'maintenance' : 'bootstrap';

    this.logger.info(`Running ${mode} discovery for source ${job.source}`, {
      source: job.source,
      mode,
      cursor: checkpoint?.cursor_value,
    });

    const { works, nextCursor } = await adapter.fetchUpdatedWorks(checkpoint?.cursor_value, { mode });

    this.logger.info('Discovered updated works', {
      source: job.source,
      mode,
      count: works.length,
      nextCursor,
    });

    for (const work of works) {
      const dedupeKey = `${job.source}:work:${work.sourceWorkId}`;
      await this.queue.enqueue(
        'SYNC_WORK',
        job.source,
        dedupeKey,
        {
          sourceWorkId: work.sourceWorkId,
          slug: work.slug,
          title: work.title,
        },
        20
      );
    }

    if (mode === 'bootstrap') {
      if (!nextCursor || works.length === 0) {
        await this.checkpoints.markCatalogCompleted(job.source, null, {
          lastDiscoveredCount: works.length,
          lastBootstrapCursor: checkpoint?.cursor_value,
        });
      } else {
        await this.checkpoints.saveCheckpoint(job.source, nextCursor, {
          ...(checkpoint?.metadata || {}),
          catalog_completed: false,
          lastDiscoveredCount: works.length,
        });
      }
    } else {
      const updatedCursor = (nextCursor ?? checkpoint?.cursor_value) ?? null;
      await this.checkpoints.saveCheckpoint(job.source, updatedCursor, {
        ...(checkpoint?.metadata || {}),
        catalog_completed: true,
        lastMaintenanceCheckAt: new Date().toISOString(),
        lastDiscoveredCount: works.length,
      });
    }
  }

  private async handleSyncWork(job: QueueJob): Promise<void> {
    const { sourceWorkId } = job.payload;
    if (!sourceWorkId) throw new Error('Missing sourceWorkId in payload');

    const adapter = this.registry.get(job.source);
    if (!adapter) throw new Error(`Source adapter not registered: ${job.source}`);

    const details = await adapter.fetchWorkDetails(sourceWorkId);
    const botUserId = await this.resolveBotUserId();

    let coverMediaId: string | null = null;
    if (details.coverUrl) {
      try {
        coverMediaId = await this.downloadAndRegisterImage(details.coverUrl, botUserId, 'editorial');
      } catch (coverErr: any) {
        this.logger.warn('Failed to import cover image, proceeding without cover', {
          error: coverErr?.message,
          coverUrl: details.coverUrl,
        });
      }
    }

    const candidate: CandidateWork = {
      source: job.source,
      sourceWorkId: details.sourceWorkId,
      title: details.title,
      slug: details.slug,
      synopsis: details.synopsis,
      author: details.author,
      artist: details.artist,
      kind: details.kind,
      status: details.status,
      year: details.year,
      ageRating: details.ageRating,
      coverId: coverMediaId,
      aliases: details.alternativeTitles,
      rawMetadata: details.raw,
    };

    const result = await this.deduplication.resolveWork(candidate);

    if (result.status === 'AMBIGUOUS') {
      this.logger.warn('Skipping chapter sync for ambiguous work', {
        source: job.source,
        sourceWorkId,
        title: details.title,
      });
      return;
    }

    if (!result.workId) {
      throw new Error(`Failed to obtain valid workId for ${details.title}`);
    }

    const chapters = await adapter.fetchChapters(sourceWorkId);
    this.logger.info('Found chapters for work', {
      title: details.title,
      chapterCount: chapters.length,
    });

    if (chapters.length === 0) return;

    // Batch query to find already COMPLETED chapter mappings in ONE query instead of N queries
    const allSourceChapterIds = chapters.map((ch) => ch.sourceChapterId);
    const { data: existingMappings } = await this.supabase
      .from('importer_chapter_mappings')
      .select('source_chapter_id, status')
      .eq('source', job.source)
      .in('source_chapter_id', allSourceChapterIds);

    const completedIds = new Set(
      (existingMappings || [])
        .filter((m) => m.status === 'COMPLETED')
        .map((m) => m.source_chapter_id)
    );

    // Also check published chapters in public.chapters for this work
    const { data: publishedChapters } = await this.supabase
      .from('chapters')
      .select('number')
      .eq('work_id', result.workId)
      .not('published_at', 'is', null);

    const publishedNumbers = new Set((publishedChapters || []).map((c) => c.number));

    // Filter out already imported chapters
    const missingChapters = chapters.filter(
      (ch) => !completedIds.has(ch.sourceChapterId) && !publishedNumbers.has(ch.number)
    );

    // Sort strictly ASCENDING by chapter number: 1 -> 2 -> 3 ... -> 100
    missingChapters.sort((a, b) => a.number - b.number);

    this.logger.info(`Enqueuing ${missingChapters.length} missing chapters in strict ascending order`, {
      source: job.source,
      workId: result.workId,
      totalChapters: chapters.length,
      missingChapters: missingChapters.length,
    });

    for (const ch of missingChapters) {
      const dedupeKey = `${job.source}:chapter:${ch.sourceChapterId}`;
      const sortKey = this.computeChapterSortKey(ch.number);

      await this.queue.enqueue(
        'IMPORT_CHAPTER',
        job.source,
        dedupeKey,
        {
          sourceWorkId,
          sourceChapterId: ch.sourceChapterId,
          workId: result.workId,
          workMappingId: result.mappingId,
          chapterNumber: ch.number,
          chapterTitle: ch.title || '',
          expectedPageCount: ch.pageCount,
        },
        30,
        sortKey
      );
    }
  }

  private computeChapterSortKey(chapterNumber: number | string): number {
    const num = typeof chapterNumber === 'number' ? chapterNumber : parseFloat(String(chapterNumber));
    if (isNaN(num) || num < 0) return 999999;
    return Number(num.toFixed(4));
  }

  private async handleImportChapter(job: QueueJob): Promise<void> {
    const {
      sourceWorkId,
      sourceChapterId,
      workId,
      workMappingId,
      chapterNumber,
      chapterTitle,
    } = job.payload;

    if (!sourceChapterId || !workId || chapterNumber === undefined) {
      throw new Error('Incomplete chapter import payload');
    }

    const adapter = this.registry.get(job.source);
    if (!adapter) throw new Error(`Source adapter not registered: ${job.source}`);

    // Register active job in forensics tracker
    diagnostics.registerJob({
      jobId: job.id,
      taskType: job.task_type,
      source: job.source,
      workId,
      chapterNumber,
      completedPages: 0,
    });

    try {
      const pageUrls = await adapter.fetchChapterPages(sourceChapterId, chapterNumber);
      if (!pageUrls || pageUrls.length === 0) {
        throw new Error(`Source returned 0 pages for chapter ${chapterNumber} (${sourceChapterId})`);
      }

      const expectedCount = pageUrls.length;
      this.logger.info('Importing chapter pages with bounded pipeline', {
        workId,
        chapterNumber,
        pageCount: expectedCount,
      });

      const botUserId = await this.resolveBotUserId();
      const storedPages: Array<{ mediaId: string; width: number; height: number } | null> = new Array(
        expectedCount
      ).fill(null);

      // Bounded parallel page pipeline
      const pageConcurrency = this.config.BATCH_PAGE_DOWNLOAD_CONCURRENCY || 3;
      const pageSemaphore = new AsyncSemaphore(pageConcurrency);
      const globalMediaSemaphore = this.autotuner.getGlobalMediaSemaphore();

      let completedPagesCount = 0;

      const pageTasks = pageUrls.map((pageUrl, idx) =>
        pageSemaphore.runExclusive(async () => {
          if (this.stopSignal) {
            throw new Error('Process shutdown requested during chapter page download');
          }

          return globalMediaSemaphore.runExclusive(async () => {
            const parsedUrl = new URL(pageUrl);
            await this.rateLimiter.acquire(parsedUrl.host);

            // Retry page download up to 3 times before failing
            let attempts = 0;
            let lastErr: any;
            while (attempts < 3 && !this.stopSignal) {
              attempts++;
              try {
                const pageBytes = await this.fetchImageBytes(pageUrl);
                const res = await processAndStoreMedia(
                  this.supabase,
                  this.storage,
                  pageBytes,
                  botUserId,
                  'editorial'
                );

                storedPages[idx] = {
                  mediaId: res.mediaId,
                  width: res.width,
                  height: res.height,
                };

                completedPagesCount++;
                diagnostics.updateJobProgress(job.id, completedPagesCount);
                return;
              } catch (err: any) {
                lastErr = err;
                if (attempts < 3 && !this.stopSignal) {
                  await this.sleep(1000 * attempts);
                }
              }
            }

            throw new Error(
              `Failed to process page ${idx + 1}/${expectedCount} after 3 attempts: ${lastErr?.message}`
            );
          });
        })
      );

      await Promise.all(pageTasks);

      // SAFEGUARD 1: Strict integrity check (expectedPages === validUploadedPages)
      const validPages: Array<{ mediaId: string; width: number; height: number }> = [];
      for (let i = 0; i < expectedCount; i++) {
        const p = storedPages[i];
        if (!p || !p.mediaId) {
          throw new Error(`Page ${i + 1} failed or has missing mediaId`);
        }
        validPages.push(p);
      }

      if (validPages.length !== expectedCount) {
        const err = `Verification failed: expected ${expectedCount} pages, but successfully processed ${validPages.length}`;
        await this.supabase.from('importer_chapter_mappings').upsert(
          {
            source: job.source,
            source_chapter_id: sourceChapterId,
            work_mapping_id: workMappingId,
            chapter_number: chapterNumber,
            page_count: validPages.length,
            status: 'VERIFICATION_FAILED',
            last_error: err,
          },
          { onConflict: 'source,source_chapter_id' }
        );
        throw new Error(err);
      }

      // Ensure work has a valid cover with storage_ready = true before publishing chapter
      const { data: workRecord } = await this.supabase
        .from('works')
        .select('cover_id')
        .eq('id', workId)
        .single();

      if (!workRecord?.cover_id && validPages.length > 0) {
        await this.supabase
          .from('works')
          .update({ cover_id: validPages[0].mediaId })
          .eq('id', workId);
      }

      // Find or create chapter record in public.chapters
      let chapterId: string;
      const { data: existingChapter } = await this.supabase
        .from('chapters')
        .select('id, published_at')
        .eq('work_id', workId)
        .eq('number', chapterNumber)
        .maybeSingle();

      if (existingChapter) {
        chapterId = existingChapter.id;
        if (chapterTitle) {
          await this.supabase
            .from('chapters')
            .update({ title: chapterTitle.slice(0, 200) })
            .eq('id', chapterId);
        }
      } else {
        chapterId = crypto.randomUUID();
        const { error: chErr } = await this.supabase.from('chapters').insert({
          id: chapterId,
          work_id: workId,
          number: chapterNumber,
          title: (chapterTitle || '').slice(0, 200),
        });
        if (chErr) throw chErr;
      }

      // SAFEGUARD 1: Batch upsert into public.pages ONLY after ALL pages are verified
      const pagesToUpsert = validPages.map((p, idx) => ({
        chapter_id: chapterId,
        position: idx + 1,
        media_id: p.mediaId,
        width: p.width,
        height: p.height,
      }));

      const { error: pageErr } = await this.supabase
        .from('pages')
        .upsert(pagesToUpsert, { onConflict: 'chapter_id,position' });

      if (pageErr) throw pageErr;

      // Publish chapter now that all pages are confirmed stored
      const { error: pubErr } = await this.supabase
        .from('chapters')
        .update({ published_at: new Date().toISOString() })
        .eq('id', chapterId);

      if (pubErr) {
        this.logger.error('Failed to mark chapter as published', { error: pubErr.message, chapterId });
        throw pubErr;
      }

      // Mark work as published if it was in draft
      await this.supabase
        .from('works')
        .update({ published: true, updated_at: new Date().toISOString() })
        .eq('id', workId)
        .eq('published', false);

      // Record chapter mapping as COMPLETED
      await this.supabase.from('importer_chapter_mappings').upsert(
        {
          source: job.source,
          source_chapter_id: sourceChapterId,
          chapter_id: chapterId,
          work_mapping_id: workMappingId,
          chapter_number: chapterNumber,
          page_count: validPages.length,
          status: 'COMPLETED',
          last_error: null,
        },
        { onConflict: 'source,source_chapter_id' }
      );

      this.logger.info('Successfully imported and published chapter', {
        workId,
        chapterNumber,
        pageCount: validPages.length,
      });
    } finally {
      diagnostics.unregisterJob(job.id);
    }
  }

  private async downloadAndRegisterImage(
    url: string,
    userId: string,
    purpose: string = 'editorial'
  ): Promise<string> {
    const parsedUrl = new URL(url);
    await this.rateLimiter.acquire(parsedUrl.host);
    const bytes = await this.fetchImageBytes(url);
    const res = await processAndStoreMedia(this.supabase, this.storage, bytes, userId, purpose);
    return res.mediaId;
  }

  private async fetchImageBytes(url: string): Promise<Uint8Array> {
    const res = await fetch(url, {
      headers: {
        'User-Agent':
          'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
        Referer: `${new URL(url).origin}/`,
      },
      signal: AbortSignal.timeout(45_000),
    });

    if (!res.ok) {
      throw new Error(`Failed to download image from ${url}: HTTP ${res.status}`);
    }

    const arrayBuf = await res.arrayBuffer();
    return new Uint8Array(arrayBuf);
  }

  private cachedBotUserId: string | null = null;

  private async resolveBotUserId(): Promise<string> {
    if (this.config.IMPORTER_USER_ID) {
      return this.config.IMPORTER_USER_ID;
    }
    if (this.cachedBotUserId) {
      return this.cachedBotUserId;
    }

    const { data: adminRole } = await this.supabase
      .from('access_roles')
      .select('user_id')
      .eq('role', 'ADMIN')
      .limit(1)
      .maybeSingle();

    if (adminRole?.user_id) {
      this.cachedBotUserId = adminRole.user_id;
      return adminRole.user_id;
    }

    const { data: anyMember } = await this.supabase
      .from('members')
      .select('id')
      .limit(1)
      .maybeSingle();

    if (anyMember?.id) {
      this.cachedBotUserId = anyMember.id;
      return anyMember.id;
    }

    throw new Error('No valid member found in public.members to attribute imported media.');
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}
