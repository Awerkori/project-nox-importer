import { ImporterQueue } from './queue.js';
import { DeduplicationEngine } from './deduplication.js';
import { CheckpointManager } from './checkpoint.js';
import { processAndStoreMedia } from '../storage/media.js';
import { Logger } from './logger.js';
export class ImporterEngine {
    supabase;
    storage;
    registry;
    rateLimiter;
    config;
    logger = new Logger('Engine');
    queue;
    deduplication;
    checkpoints;
    isRunning = false;
    stopSignal = false;
    constructor(supabase, storage, registry, rateLimiter, config) {
        this.supabase = supabase;
        this.storage = storage;
        this.registry = registry;
        this.rateLimiter = rateLimiter;
        this.config = config;
        this.queue = new ImporterQueue(supabase, config.WORKER_ID);
        this.deduplication = new DeduplicationEngine(supabase);
        this.checkpoints = new CheckpointManager(supabase);
    }
    async start() {
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
                    this.logger.info(`[Daemon Telemetry] Memory: ${Math.round(mem.heapUsed / 1024 / 1024)}MB heap / ${Math.round(mem.rss / 1024 / 1024)}MB rss (512MB RAM) | Worker: ${this.config.WORKER_ID}`);
                }
                await this.step();
            }
            catch (err) {
                this.logger.error('Unexpected error in engine step', { error: err?.message, stack: err?.stack });
            }
            if (!this.stopSignal) {
                await this.sleep(this.config.POLL_INTERVAL_SECONDS * 1000);
            }
        }
        this.isRunning = false;
        this.logger.info('Importer Engine stopped gracefully');
    }
    async runStartupRecovery() {
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
            }
            else {
                this.logger.info('Startup recovery check passed: no stalled jobs detected');
            }
        }
        catch (err) {
            this.logger.warn('Error during startup recovery check', { error: err?.message });
        }
    }
    stop() {
        this.stopSignal = true;
    }
    /**
     * Run a single discrete engine iteration (also used in tests)
     */
    async step() {
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
    async scheduleSources() {
        const { data: sources, error } = await this.supabase
            .from('importer_sources')
            .select('*');
        if (error || !sources)
            return;
        const now = Date.now();
        for (const src of sources) {
            // 1. Skip disabled or paused sources
            if (src.enabled === false || src.status === 'DISABLED' || src.status === 'PAUSED') {
                continue;
            }
            // 2. Handle COOLDOWN status
            if (src.status === 'COOLDOWN') {
                const cooldownUntil = src.cooldown_until ? new Date(src.cooldown_until).getTime() : 0;
                if (now < cooldownUntil) {
                    this.logger.debug(`Source ${src.id} is in COOLDOWN until ${src.cooldown_until}, skipping scheduling`);
                    continue;
                }
                // Cooldown expired: automatically transition back to ACTIVE
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
            // Dynamically apply host rate limit from database if configured
            if (src.base_url && src.rate_limit_per_second) {
                try {
                    const host = new URL(src.base_url).host;
                    this.rateLimiter.setHostRate(host, Number(src.rate_limit_per_second) || 2.0);
                }
                catch { }
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
    async processJob(job) {
        const heartbeat = this.queue.startHeartbeat(job.id, this.config.QUEUE_HEARTBEAT_INTERVAL_SECONDS);
        try {
            // Check if the source is PAUSED, DISABLED, or actively in COOLDOWN
            const { data: sourceRec } = await this.supabase
                .from('importer_sources')
                .select('id, status, cooldown_until, enabled')
                .eq('id', job.source)
                .maybeSingle();
            if (sourceRec) {
                if (sourceRec.status === 'PAUSED' || sourceRec.status === 'DISABLED' || !sourceRec.enabled) {
                    this.logger.info(`Postponing job ${job.id}: source ${job.source} is ${sourceRec.status}`, {
                        jobId: job.id,
                        source: job.source,
                        status: sourceRec.status,
                    });
                    heartbeat.stop();
                    await this.queue.releaseJob(job.id, 'RETRY', `Source ${job.source} is ${sourceRec.status}`, 15);
                    return;
                }
                if (sourceRec.status === 'COOLDOWN') {
                    const cooldownUntil = sourceRec.cooldown_until ? new Date(sourceRec.cooldown_until).getTime() : 0;
                    if (Date.now() < cooldownUntil) {
                        const waitMinutes = Math.max(1, Math.ceil((cooldownUntil - Date.now()) / 60000));
                        this.logger.info(`Postponing job ${job.id}: source ${job.source} is in COOLDOWN for ${waitMinutes}m`, {
                            jobId: job.id,
                            source: job.source,
                            cooldownUntil: sourceRec.cooldown_until,
                        });
                        heartbeat.stop();
                        await this.queue.releaseJob(job.id, 'RETRY', `Source in COOLDOWN until ${sourceRec.cooldown_until}`, waitMinutes);
                        return;
                    }
                    else {
                        // Expired cooldown -> flip to ACTIVE
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
        }
        catch (err) {
            heartbeat.stop();
            const errorMessage = err?.message || String(err);
            this.logger.error('Job execution failed', {
                jobId: job.id,
                taskType: job.task_type,
                error: errorMessage,
                attempts: job.attempts,
            });
            // Handle 429 / Rate Limit detection
            const is429 = err?.status === 429 || err?.statusCode === 429 || /429|rate\s*limit/i.test(errorMessage);
            if (is429) {
                let waitSeconds = 60;
                const retryAfter = err?.retryAfter || err?.headers?.get?.('retry-after');
                if (retryAfter) {
                    const parsed = parseInt(retryAfter, 10);
                    if (!isNaN(parsed) && parsed > 0)
                        waitSeconds = Math.min(3600, parsed);
                }
                const cooldownUntil = new Date(Date.now() + waitSeconds * 1000).toISOString();
                this.logger.warn(`Source ${job.source} entered COOLDOWN due to rate limit for ${waitSeconds}s`, {
                    source: job.source,
                    cooldownUntil,
                });
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
            const nextStatus = job.attempts >= job.max_attempts ? 'FAILED' : 'RETRY';
            const backoffMinutes = Math.min(60, Math.pow(2, job.attempts) * 2);
            await this.queue.releaseJob(job.id, nextStatus, errorMessage, backoffMinutes);
        }
    }
    async handleDiscoverWorks(job) {
        const adapter = this.registry.get(job.source);
        if (!adapter)
            throw new Error(`Source adapter not registered: ${job.source}`);
        const checkpoint = await this.checkpoints.getCheckpoint(job.source);
        const isCompleted = Boolean(checkpoint?.metadata?.catalog_completed);
        const mode = isCompleted ? 'maintenance' : 'bootstrap';
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
            await this.queue.enqueue('SYNC_WORK', job.source, dedupeKey, {
                sourceWorkId: work.sourceWorkId,
                slug: work.slug,
                title: work.title,
            }, 20);
        }
        if (mode === 'bootstrap') {
            if (!nextCursor || works.length === 0) {
                // Historical backlog completely traversed -> mark catalog as completed!
                await this.checkpoints.markCatalogCompleted(job.source, null, {
                    lastDiscoveredCount: works.length,
                    lastBootstrapCursor: checkpoint?.cursor_value,
                });
            }
            else {
                await this.checkpoints.saveCheckpoint(job.source, nextCursor, {
                    ...(checkpoint?.metadata || {}),
                    catalog_completed: false,
                    lastDiscoveredCount: works.length,
                });
            }
        }
        else {
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
    async handleSyncWork(job) {
        const { sourceWorkId } = job.payload;
        if (!sourceWorkId)
            throw new Error('Missing sourceWorkId in payload');
        const adapter = this.registry.get(job.source);
        if (!adapter)
            throw new Error(`Source adapter not registered: ${job.source}`);
        const details = await adapter.fetchWorkDetails(sourceWorkId);
        // Bot user ID in members table for media uploads
        const botUserId = await this.resolveBotUserId();
        // Process cover if present
        let coverMediaId = null;
        if (details.coverUrl) {
            try {
                coverMediaId = await this.downloadAndRegisterImage(details.coverUrl, botUserId, 'editorial');
            }
            catch (coverErr) {
                this.logger.warn('Failed to import cover image, proceeding without cover', {
                    error: coverErr?.message,
                    coverUrl: details.coverUrl,
                });
            }
        }
        const candidate = {
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
            await this.queue.enqueue('IMPORT_CHAPTER', job.source, dedupeKey, {
                sourceWorkId,
                sourceChapterId: ch.sourceChapterId,
                workId: result.workId,
                workMappingId: result.mappingId,
                chapterNumber: ch.number,
                chapterTitle: ch.title || '',
                expectedPageCount: ch.pageCount,
            }, 30);
        }
    }
    async handleImportChapter(job) {
        const { sourceWorkId, sourceChapterId, workId, workMappingId, chapterNumber, chapterTitle, } = job.payload;
        if (!sourceChapterId || !workId || chapterNumber === undefined) {
            throw new Error('Incomplete chapter import payload');
        }
        const adapter = this.registry.get(job.source);
        if (!adapter)
            throw new Error(`Source adapter not registered: ${job.source}`);
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
        const storedPages = [];
        // Download, validate, and store each page in sequence with concurrency bounds
        for (let i = 0; i < pageUrls.length; i++) {
            const pageUrl = pageUrls[i];
            const parsedUrl = new URL(pageUrl);
            await this.rateLimiter.acquire(parsedUrl.host);
            const pageBytes = await this.fetchImageBytes(pageUrl);
            const res = await processAndStoreMedia(this.supabase, this.storage, pageBytes, botUserId, 'editorial');
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
            await this.supabase.from('importer_chapter_mappings').upsert({
                source: job.source,
                source_chapter_id: sourceChapterId,
                work_mapping_id: workMappingId,
                chapter_number: chapterNumber,
                page_count: storedPages.length,
                status: 'VERIFICATION_FAILED',
                last_error: err,
            }, { onConflict: 'source,source_chapter_id' });
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
        let chapterId;
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
        }
        else {
            chapterId = crypto.randomUUID();
            const { error: chErr } = await this.supabase.from('chapters').insert({
                id: chapterId,
                work_id: workId,
                number: chapterNumber,
                title: (chapterTitle || '').slice(0, 200),
            });
            if (chErr)
                throw chErr;
        }
        // Insert or update public.pages
        for (let idx = 0; idx < storedPages.length; idx++) {
            const pos = idx + 1;
            const p = storedPages[idx];
            const { error: pageErr } = await this.supabase.from('pages').upsert({
                chapter_id: chapterId,
                position: pos,
                media_id: p.mediaId,
                width: p.width,
                height: p.height,
            }, { onConflict: 'chapter_id,position' });
            if (pageErr)
                throw pageErr;
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
        await this.supabase.from('importer_chapter_mappings').upsert({
            source: job.source,
            source_chapter_id: sourceChapterId,
            chapter_id: chapterId,
            work_mapping_id: workMappingId,
            chapter_number: chapterNumber,
            page_count: storedPages.length,
            status: 'COMPLETED',
            last_error: null,
        }, { onConflict: 'source,source_chapter_id' });
        this.logger.info('Successfully imported and published chapter', {
            workId,
            chapterNumber,
            pageCount: storedPages.length,
        });
    }
    async downloadAndRegisterImage(url, userId, purpose = 'editorial') {
        const parsedUrl = new URL(url);
        await this.rateLimiter.acquire(parsedUrl.host);
        const bytes = await this.fetchImageBytes(url);
        const res = await processAndStoreMedia(this.supabase, this.storage, bytes, userId, purpose);
        return res.mediaId;
    }
    async fetchImageBytes(url) {
        const res = await fetch(url, {
            headers: {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
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
    cachedBotUserId = null;
    async resolveBotUserId() {
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
    sleep(ms) {
        return new Promise((resolve) => setTimeout(resolve, ms));
    }
}
