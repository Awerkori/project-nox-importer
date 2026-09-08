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

export class ImporterEngine {
  private logger = new Logger('Engine');
  private queue: ImporterQueue;
  private deduplication: DeduplicationEngine;
  private checkpoints: CheckpointManager;
  private isRunning = false;
  private stopSignal = false;

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
  }

  async start(): Promise<void> {
    this.isRunning = true;
    this.stopSignal = false;
    this.logger.info('Importer Engine daemon started', {
      workerId: this.config.WORKER_ID,
      pollInterval: this.config.POLL_INTERVAL_SECONDS,
      storageProvider: this.storage.getProviderKey(),
    });

    // 1. Run startup recovery for stalled jobs from crashed instances
    await this.runStartupRecovery();

    let lastTelemetryTime = 0;
    const telemetryIntervalMs = 5 * 60 * 1000; // 5 minutes

    while (!this.stopSignal) {
      try {
        const now = Date.now();
        if (now - lastTelemetryTime >= telemetryIntervalMs) {
          lastTelemetryTime = now;
          const mem = process.memoryUsage();
          this.logger.info(
            `[Daemon Telemetry] Memory: ${Math.round(mem.heapUsed / 1024 / 1024)}MB heap / ${Math.round(mem.rss / 1024 / 1024)}MB rss (512MB RAM) | Worker: ${this.config.WORKER_ID}`
          );
        }

        await this.step();
      } catch (err: any) {
        this.logger.error('Unexpected error in engine step', { error: err?.message, stack: err?.stack });
      }

      if (!this.stopSignal) {
        await this.sleep(this.config.POLL_INTERVAL_SECONDS * 1000);
      }
    }

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
  }

  /**
   * Run a single discrete engine iteration (also used in tests)
   */
  async step(): Promise<boolean> {
    // 1. Check sources scheduling
    await this.scheduleSources();

    // 2. Acquire and execute next job from queue
    const job = await this.queue.acquireNextJob(Math.ceil(this.config.QUEUE_LEASE_DURATION_SECONDS / 60));
    if (!job) {
      this.logger.debug('No pending jobs in queue');
      return false;
    }

    await this.processJob(job);
    return true;
  }

  private async scheduleSources(): Promise<void> {
    const { data: sources, error } = await this.supabase
      .from('importer_sources')
      .select('*')
      .eq('enabled', true);

    if (error || !sources) return;

    for (const src of sources) {
      // Dynamically apply host rate limit from database if configured
      if (src.base_url && src.rate_limit_per_second) {
        try {
          const host = new URL(src.base_url).host;
          this.rateLimiter.setHostRate(host, Number(src.rate_limit_per_second) || 2.0);
        } catch {}
      }

      const lastSync = src.last_sync_at ? new Date(src.last_sync_at).getTime() : 0;
      const intervalMs = (src.sync_interval_minutes || 30) * 60 * 1000;
      const now = Date.now();

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
      this.logger.info('Processing job', {
        jobId: job.id,
        taskType: job.task_type,
        source: job.source,
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
        // Historical backlog completely traversed -> mark catalog as completed!
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
      // Maintenance mode: preserve completed status and update newest release cursor
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

    // Bot user ID in members table for media uploads
    const botUserId = await this.resolveBotUserId();

    // Process cover if present
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

    // Fetch chapters from source
    const chapters = await adapter.fetchChapters(sourceWorkId);
    this.logger.info('Found chapters for work', {
      title: details.title,
      chapterCount: chapters.length,
    });

    for (const ch of chapters) {
      // Check if already completed in importer_chapter_mappings
      const { data: existingMap } = await this.supabase
        .from('importer_chapter_mappings')
        .select('id, status')
        .eq('source', job.source)
        .eq('source_chapter_id', ch.sourceChapterId)
        .maybeSingle();

      if (existingMap && existingMap.status === 'COMPLETED') {
        continue; // Already successfully imported and verified
      }

      const dedupeKey = `${job.source}:chapter:${ch.sourceChapterId}`;
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
        30
      );
    }
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

    // Fetch page URLs
    const pageUrls = await adapter.fetchChapterPages(sourceChapterId, chapterNumber);
    if (!pageUrls || pageUrls.length === 0) {
      throw new Error(`Source returned 0 pages for chapter ${chapterNumber} (${sourceChapterId})`);
    }

    const expectedCount = pageUrls.length;
    this.logger.info('Importing chapter pages', {
      workId,
      chapterNumber,
      pageCount: expectedCount,
    });

    const botUserId = await this.resolveBotUserId();
    const storedPages: Array<{ mediaId: string; width: number; height: number }> = [];

    // Download, validate, and store each page in sequence with concurrency bounds
    for (let i = 0; i < pageUrls.length; i++) {
      const pageUrl = pageUrls[i];
      const parsedUrl = new URL(pageUrl);
      await this.rateLimiter.acquire(parsedUrl.host);

      const pageBytes = await this.fetchImageBytes(pageUrl);
      const res = await processAndStoreMedia(
        this.supabase,
        this.storage,
        pageBytes,
        botUserId,
        'editorial'
      );

      storedPages.push({
        mediaId: res.mediaId,
        width: res.width,
        height: res.height,
      });
    }

    // MANDATORY INTEGRITY VERIFICATION:
    // validPages == expectedPages strictly enforced
    if (storedPages.length !== expectedCount) {
      const err = `Verification failed: expected ${expectedCount} pages, but successfully processed ${storedPages.length}`;
      await this.supabase.from('importer_chapter_mappings').upsert(
        {
          source: job.source,
          source_chapter_id: sourceChapterId,
          work_mapping_id: workMappingId,
          chapter_number: chapterNumber,
          page_count: storedPages.length,
          status: 'VERIFICATION_FAILED',
          last_error: err,
        },
        { onConflict: 'source,source_chapter_id' }
      );
      throw new Error(err);
    }

    // Ensure work has a valid cover with storage_ready = true before publishing chapter
    // (Required by public.verify_publication_media database trigger!)
    const { data: workRecord } = await this.supabase
      .from('works')
      .select('cover_id')
      .eq('id', workId)
      .single();

    if (!workRecord?.cover_id && storedPages.length > 0) {
      // Use page 1 as cover if work has no cover
      await this.supabase
        .from('works')
        .update({ cover_id: storedPages[0].mediaId })
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
      // Update title if empty
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

    // Insert or update public.pages
    for (let idx = 0; idx < storedPages.length; idx++) {
      const pos = idx + 1;
      const p = storedPages[idx];
      const { error: pageErr } = await this.supabase.from('pages').upsert(
        {
          chapter_id: chapterId,
          position: pos,
          media_id: p.mediaId,
          width: p.width,
          height: p.height,
        },
        { onConflict: 'chapter_id,position' }
      );
      if (pageErr) throw pageErr;
    }

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
        page_count: storedPages.length,
        status: 'COMPLETED',
        last_error: null,
      },
      { onConflict: 'source,source_chapter_id' }
    );

    this.logger.info('Successfully imported and published chapter', {
      workId,
      chapterNumber,
      pageCount: storedPages.length,
    });
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

    // Lookup first ADMIN member in access_roles
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

    // Fallback: look in members
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
