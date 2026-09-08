import { ImporterQueue } from './queue.js';
import { DeduplicationEngine } from './deduplication.js';
import { CheckpointManager } from './checkpoint.js';
import { processAndStoreMedia } from '../storage/media.js';
import { Logger } from './logger.js';
import { diagnostics } from './diagnostics.js';
import { AdaptiveAutotuner, AsyncSemaphore } from './concurrency.js';
export function computeCanonicalChapterKey(chapterNumber, chapterTitle) {
    const num = typeof chapterNumber === 'number' ? chapterNumber : parseFloat(String(chapterNumber));
    const normalizedNumber = isNaN(num) || num < 0 ? 0 : Number(num.toFixed(4));
    const titleLower = (chapterTitle || '').toLowerCase();
    const hasSpecialKeywords = /especial|special|extra|omake|side|spin-off/i.test(titleLower);
    const hasPrologueKeywords = /pr[oó]logo|prologue/i.test(titleLower);
    const isPrologue = hasPrologueKeywords || (normalizedNumber === 0 && !hasSpecialKeywords);
    const isSpecial = hasSpecialKeywords || isPrologue;
    let specialCategory;
    if (isPrologue)
        specialCategory = 'prologue';
    else if (/extra/i.test(titleLower))
        specialCategory = 'extra';
    else if (/side/i.test(titleLower))
        specialCategory = 'side';
    else if (hasSpecialKeywords)
        specialCategory = 'special';
    let sortKey = normalizedNumber;
    if (hasSpecialKeywords && normalizedNumber === 0) {
        sortKey = 0.0001;
    }
    return {
        normalizedNumber,
        sortKey: Number(sortKey.toFixed(4)),
        isSpecial,
        specialCategory,
    };
}
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
    autotuner;
    isRunning = false;
    stopSignal = false;
    abortController = new AbortController();
    constructor(supabase, storage, registry, rateLimiter, config) {
        this.supabase = supabase;
        this.storage = storage;
        this.registry = registry;
        this.rateLimiter = rateLimiter;
        this.config = config;
        this.queue = new ImporterQueue(supabase, config.WORKER_ID);
        this.deduplication = new DeduplicationEngine(supabase);
        this.checkpoints = new CheckpointManager(supabase);
        this.autotuner = new AdaptiveAutotuner({
            initialConcurrency: Math.min(3, config.MAX_CONCURRENT_CHAPTERS || 3),
            maxConcurrency: Math.max(3, config.MAX_CONCURRENT_CHAPTERS || 6),
        });
    }
    getAutotuner() {
        return this.autotuner;
    }
    async start() {
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
        const activeWorkers = [];
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
        this.abortController.abort();
    }
    /**
     * Periodic discovery scheduler running in the background
     */
    async runDiscoveryLoop() {
        while (!this.stopSignal) {
            try {
                await this.scheduleSources();
            }
            catch (err) {
                this.logger.error('Error during source discovery scheduling', { error: err?.message });
            }
            // Check discovery every 30 seconds
            await this.sleep(30_000);
        }
    }
    autotunerCycleCount = 0;
    /**
     * Periodic autotuner telemetry & evaluation loop (every 30s)
     */
    async runAutotunerLoop() {
        while (!this.stopSignal) {
            await this.sleep(30_000);
            if (this.stopSignal)
                break;
            try {
                this.autotunerCycleCount++;
                const mem = diagnostics.getMemorySnapshot();
                const evaluation = this.autotuner.evaluateCycle();
                const activeJobs = diagnostics.getActiveJobsCount();
                const lagMetrics = diagnostics.lagMonitor?.getMetrics?.() || { avgLagMs: 0 };
                this.logger.info(`[Autotuner Telemetry] Action: ${evaluation.action} | Concurrency: ${evaluation.concurrency} | Active Jobs: ${activeJobs} | Mem: ${mem.heapUsedMb}MB heap / ${mem.rssMb}MB rss (512MB RAM) | Reason: ${evaluation.reason}`);
                // Record async telemetry snapshot without blocking the loop
                void this.recordTelemetrySnapshot({
                    workerId: this.config.WORKER_ID,
                    rssMb: mem.rssMb,
                    heapUsedMb: mem.heapUsedMb,
                    heapTotalMb: mem.heapTotalMb,
                    externalMb: mem.externalMb,
                    arrayBuffersMb: mem.arrayBuffersMb,
                    eventLoopLagMs: lagMetrics.avgLagMs,
                    concurrency: evaluation.concurrency,
                    activeJobs,
                    cycleAction: evaluation.action,
                    cycleReason: evaluation.reason,
                });
                // Periodic pruning of old telemetry every 120 cycles (~1 hour)
                if (this.autotunerCycleCount % 120 === 0) {
                    void this.pruneTelemetry();
                }
            }
            catch (err) {
                this.logger.error('Error during autotuner evaluation loop', { error: err?.message });
            }
        }
    }
    /**
     * Dedicated worker loop for a specific source
     */
    async runSourceWorker(source) {
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
                const job = await this.queue.acquireNextJob(Math.ceil(this.config.QUEUE_LEASE_DURATION_SECONDS / 60), source);
                if (!job) {
                    // Queue empty for this source: idle sleep 5 seconds
                    await this.sleep(5_000);
                    continue;
                }
                // Process job with concurrency semaphores
                await this.executeJobWithLimits(job);
                // Continuous drain: immediately check for next job without delay
                await this.sleep(50);
            }
            catch (err) {
                this.logger.error(`Error in worker loop for source ${source}`, { error: err?.message });
                await this.sleep(5_000);
            }
        }
    }
    /**
     * General worker loop to process jobs with no source filter
     */
    async runGeneralWorker() {
        this.logger.info('Starting general fallback runner');
        while (!this.stopSignal) {
            try {
                const job = await this.queue.acquireNextJob(Math.ceil(this.config.QUEUE_LEASE_DURATION_SECONDS / 60));
                if (!job) {
                    await this.sleep(10_000);
                    continue;
                }
                await this.executeJobWithLimits(job);
                await this.sleep(50);
            }
            catch (err) {
                this.logger.error('Error in general worker loop', { error: err?.message });
                await this.sleep(5_000);
            }
        }
    }
    /**
     * Executes a job respecting global and per-source concurrency semaphores
     */
    async executeJobWithLimits(job) {
        if (job.task_type === 'IMPORT_CHAPTER') {
            const globalSem = this.autotuner.getGlobalChapterSemaphore();
            const sourceSem = this.autotuner.getSourceSemaphore(job.source, 2);
            await globalSem.runExclusive(async () => {
                await sourceSem.runExclusive(async () => {
                    await this.processJob(job);
                });
            });
        }
        else {
            await this.processJob(job);
        }
    }
    /**
     * Discrete step method preserved for unit tests & single iterations
     */
    async step(source) {
        await this.scheduleSources();
        const job = await this.queue.acquireNextJob(Math.ceil(this.config.QUEUE_LEASE_DURATION_SECONDS / 60), source);
        if (!job) {
            this.logger.debug('No pending jobs in queue', { source });
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
                    }
                    else {
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
            const is429 = err?.status === 429 || err?.statusCode === 429 || /429|rate\s*limit/i.test(errorMessage);
            const isTimeout = /timeout|aborted|ETIMEDOUT/i.test(errorMessage);
            if (is429) {
                this.autotuner.recordError('ratelimit');
                let waitSeconds = 60;
                const retryAfter = err?.retryAfter || err?.headers?.get?.('retry-after');
                if (retryAfter) {
                    const parsed = parseInt(retryAfter, 10);
                    if (!isNaN(parsed) && parsed > 0)
                        waitSeconds = Math.min(3600, parsed);
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
            }
            else {
                this.autotuner.recordError('error');
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
        const botUserId = await this.resolveBotUserId();
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
        const chapters = await adapter.fetchChapters(sourceWorkId);
        this.logger.info('Found chapters for work', {
            title: details.title,
            chapterCount: chapters.length,
        });
        if (chapters.length === 0)
            return;
        // Batch query to find already COMPLETED chapter mappings in ONE query instead of N queries
        const allSourceChapterIds = chapters.map((ch) => ch.sourceChapterId);
        const { data: existingMappings } = await this.supabase
            .from('importer_chapter_mappings')
            .select('source_chapter_id, status')
            .eq('source', job.source)
            .in('source_chapter_id', allSourceChapterIds);
        const completedIds = new Set((existingMappings || [])
            .filter((m) => m.status === 'COMPLETED')
            .map((m) => m.source_chapter_id));
        // Also check published chapters in public.chapters for this work
        const { data: publishedChapters } = await this.supabase
            .from('chapters')
            .select('id, number, title')
            .eq('work_id', result.workId)
            .not('published_at', 'is', null);
        // Match each candidate chapter against published chapters
        const missingChapters = [];
        for (const ch of chapters) {
            if (completedIds.has(ch.sourceChapterId)) {
                continue;
            }
            const chKey = this.computeCanonicalChapterKey(ch.number, ch.title);
            // Find if an existing published chapter matches canonical key
            const matchedPublished = (publishedChapters || []).find((pub) => {
                const pubKey = this.computeCanonicalChapterKey(pub.number, pub.title);
                if (pubKey.normalizedNumber !== chKey.normalizedNumber)
                    return false;
                // Do not merge specials with regular chapters
                if (pubKey.isSpecial !== chKey.isSpecial)
                    return false;
                if (chKey.specialCategory && pubKey.specialCategory && chKey.specialCategory !== pubKey.specialCategory)
                    return false;
                return true;
            });
            if (matchedPublished) {
                // Chapter is ALREADY published: link mapping to canonical chapter, zero re-download!
                await this.supabase.from('importer_chapter_mappings').upsert({
                    source: job.source,
                    source_chapter_id: ch.sourceChapterId,
                    chapter_id: matchedPublished.id,
                    work_mapping_id: result.mappingId,
                    chapter_number: ch.number,
                    page_count: ch.pageCount || 0,
                    status: 'COMPLETED',
                    last_error: null,
                }, { onConflict: 'source,source_chapter_id' });
                continue;
            }
            missingChapters.push(ch);
        }
        // For missing chapters, check if another source already has an active job in queue
        const { data: activeJobs } = await this.supabase
            .from('importer_queue')
            .select('payload, source, status')
            .eq('task_type', 'IMPORT_CHAPTER')
            .in('status', ['QUEUED', 'IMPORTING', 'RETRY']);
        const activeJobsForWork = (activeJobs || []).filter((j) => j.payload?.workId === result.workId);
        const chaptersToEnqueue = missingChapters.filter((ch) => {
            const chKey = this.computeCanonicalChapterKey(ch.number, ch.title);
            const activeJob = activeJobsForWork.find((j) => {
                const jobNum = Number(j.payload?.chapterNumber);
                return Number(jobNum.toFixed(4)) === chKey.normalizedNumber;
            });
            if (activeJob) {
                // If Kuro already has active job, secondary source defers
                if (activeJob.source === 'kuro' && job.source !== 'kuro') {
                    return false;
                }
                // If current source already has active job, do not duplicate
                if (activeJob.source === job.source) {
                    return false;
                }
            }
            return true;
        });
        // Sort strictly ASCENDING by canonical sort key: 0 -> 1 -> 1.5 -> 2 ...
        chaptersToEnqueue.sort((a, b) => {
            const keyA = this.computeCanonicalChapterKey(a.number, a.title).sortKey;
            const keyB = this.computeCanonicalChapterKey(b.number, b.title).sortKey;
            return keyA - keyB;
        });
        this.logger.info(`Enqueuing ${chaptersToEnqueue.length} missing chapters in strict canonical ascending order`, {
            source: job.source,
            workId: result.workId,
            totalChapters: chapters.length,
            alreadyPublished: chapters.length - missingChapters.length,
            enqueued: chaptersToEnqueue.length,
        });
        for (const ch of chaptersToEnqueue) {
            const dedupeKey = `${job.source}:chapter:${ch.sourceChapterId}`;
            const chKey = this.computeCanonicalChapterKey(ch.number, ch.title);
            await this.queue.enqueue('IMPORT_CHAPTER', job.source, dedupeKey, {
                sourceWorkId,
                sourceChapterId: ch.sourceChapterId,
                workId: result.workId,
                workMappingId: result.mappingId,
                chapterNumber: ch.number,
                chapterTitle: ch.title || '',
                expectedPageCount: ch.pageCount,
            }, 30, chKey.sortKey);
        }
    }
    computeCanonicalChapterKey(chapterNumber, chapterTitle) {
        return computeCanonicalChapterKey(chapterNumber, chapterTitle);
    }
    computeChapterSortKey(chapterNumber, chapterTitle) {
        return this.computeCanonicalChapterKey(chapterNumber, chapterTitle).sortKey;
    }
    async handleImportChapter(job) {
        const { sourceWorkId, sourceChapterId, workId, workMappingId, chapterNumber, chapterTitle, } = job.payload;
        if (!sourceChapterId || !workId || chapterNumber === undefined) {
            throw new Error('Incomplete chapter import payload');
        }
        // Pre-flight check: if already published by concurrent worker, skip download
        const { data: alreadyPub } = await this.supabase
            .from('chapters')
            .select('id, number, title')
            .eq('work_id', workId)
            .eq('number', chapterNumber)
            .not('published_at', 'is', null)
            .maybeSingle();
        if (alreadyPub) {
            this.logger.info('Chapter already published by concurrent source, linking mapping and skipping duplicate download', {
                workId,
                chapterNumber,
                source: job.source,
                canonicalChapterId: alreadyPub.id,
            });
            await this.supabase.from('importer_chapter_mappings').upsert({
                source: job.source,
                source_chapter_id: sourceChapterId,
                chapter_id: alreadyPub.id,
                work_mapping_id: workMappingId,
                chapter_number: chapterNumber,
                status: 'COMPLETED',
                last_error: null,
            }, { onConflict: 'source,source_chapter_id' });
            return;
        }
        const adapter = this.registry.get(job.source);
        if (!adapter)
            throw new Error(`Source adapter not registered: ${job.source}`);
        // Register active job in forensics tracker
        diagnostics.registerJob({
            jobId: job.id,
            taskType: job.task_type,
            source: job.source,
            workId,
            chapterNumber,
            completedPages: 0,
        });
        const tStart = Date.now();
        let tDownload = 0;
        let tUpload = 0;
        let tDb = 0;
        let totalBytes = 0;
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
            const storedPages = new Array(expectedCount).fill(null);
            // Bounded parallel page pipeline
            const pageConcurrency = this.config.BATCH_PAGE_DOWNLOAD_CONCURRENCY || 3;
            const pageSemaphore = new AsyncSemaphore(pageConcurrency);
            const globalMediaSemaphore = this.autotuner.getGlobalMediaSemaphore();
            let completedPagesCount = 0;
            const pageTasks = pageUrls.map((pageUrl, idx) => pageSemaphore.runExclusive(async () => {
                if (this.stopSignal) {
                    throw new Error('Process shutdown requested during chapter page download');
                }
                return globalMediaSemaphore.runExclusive(async () => {
                    const parsedUrl = new URL(pageUrl);
                    await this.rateLimiter.acquire(parsedUrl.host);
                    // Retry page download up to 3 times before failing
                    let attempts = 0;
                    let lastErr;
                    while (attempts < 3 && !this.stopSignal) {
                        attempts++;
                        try {
                            const d0 = Date.now();
                            const pageBytes = await this.fetchImageBytes(pageUrl);
                            tDownload += Date.now() - d0;
                            totalBytes += pageBytes.length;
                            const u0 = Date.now();
                            const res = await processAndStoreMedia(this.supabase, this.storage, pageBytes, botUserId, 'editorial');
                            tUpload += Date.now() - u0;
                            storedPages[idx] = {
                                mediaId: res.mediaId,
                                width: res.width,
                                height: res.height,
                            };
                            completedPagesCount++;
                            diagnostics.updateJobProgress(job.id, completedPagesCount);
                            return;
                        }
                        catch (err) {
                            lastErr = err;
                            if (attempts < 3 && !this.stopSignal) {
                                await this.sleep(1000 * attempts);
                            }
                        }
                    }
                    throw new Error(`Failed to process page ${idx + 1}/${expectedCount} after 3 attempts: ${lastErr?.message}`);
                });
            }));
            await Promise.all(pageTasks);
            // SAFEGUARD 1: Strict integrity check (expectedPages === validUploadedPages)
            const validPages = [];
            for (let i = 0; i < expectedCount; i++) {
                const p = storedPages[i];
                if (!p || !p.mediaId) {
                    throw new Error(`Page ${i + 1} failed or has missing mediaId`);
                }
                validPages.push(p);
            }
            if (validPages.length !== expectedCount) {
                const err = `Verification failed: expected ${expectedCount} pages, but successfully processed ${validPages.length}`;
                await this.supabase.from('importer_chapter_mappings').upsert({
                    source: job.source,
                    source_chapter_id: sourceChapterId,
                    work_mapping_id: workMappingId,
                    chapter_number: chapterNumber,
                    page_count: validPages.length,
                    status: 'VERIFICATION_FAILED',
                    last_error: err,
                }, { onConflict: 'source,source_chapter_id' });
                throw new Error(err);
            }
            const db0 = Date.now();
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
            let chapterId;
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
            if (pageErr)
                throw pageErr;
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
            // Record chapter mapping as COMPLETED with is_page_provider: true
            await this.supabase.from('importer_chapter_mappings').upsert({
                source: job.source,
                source_chapter_id: sourceChapterId,
                chapter_id: chapterId,
                work_mapping_id: workMappingId,
                chapter_number: chapterNumber,
                page_count: validPages.length,
                status: 'COMPLETED',
                last_error: null,
            }, { onConflict: 'source,source_chapter_id' });
            tDb = Date.now() - db0;
            // Record fine-grained chapter job metric asynchronously
            void this.recordJobMetric({
                workerId: this.config.WORKER_ID,
                source: job.source,
                workId,
                chapterId,
                chapterNumber,
                pageCount: validPages.length,
                totalBytes,
                durationMs: Date.now() - tStart,
                downloadMs: tDownload,
                uploadMs: tUpload,
                dbMs: tDb,
                status: 'COMPLETED',
            });
            this.logger.info('Successfully imported and published chapter', {
                workId,
                chapterNumber,
                pageCount: validPages.length,
            });
        }
        catch (err) {
            // Record failed chapter metric asynchronously with sanitized error message
            void this.recordJobMetric({
                workerId: this.config.WORKER_ID,
                source: job.source,
                workId,
                chapterId: null,
                chapterNumber,
                pageCount: 0,
                totalBytes,
                durationMs: Date.now() - tStart,
                downloadMs: tDownload,
                uploadMs: tUpload,
                dbMs: tDb,
                status: 'FAILED',
                errorMessage: this.sanitizeErrorMessage(err?.message || String(err)),
            });
            throw err;
        }
        finally {
            diagnostics.unregisterJob(job.id);
        }
    }
    async recordJobMetric(metric) {
        try {
            await this.supabase.from('importer_job_metrics').insert({
                worker_id: metric.workerId,
                source: metric.source,
                work_id: metric.workId,
                chapter_id: metric.chapterId,
                chapter_number: metric.chapterNumber,
                page_count: metric.pageCount,
                total_bytes: metric.totalBytes,
                duration_ms: metric.durationMs,
                download_ms: metric.downloadMs,
                upload_ms: metric.uploadMs,
                db_ms: metric.dbMs,
                status: metric.status,
                error_message: metric.errorMessage ? this.sanitizeErrorMessage(metric.errorMessage) : null,
            });
        }
        catch (err) {
            this.logger.warn('Failed to record job metric to Supabase (non-fatal)', { error: err?.message });
        }
    }
    async recordTelemetrySnapshot(snapshot) {
        try {
            await this.supabase.from('importer_telemetry').insert({
                worker_id: snapshot.workerId,
                rss_mb: snapshot.rssMb,
                heap_used_mb: snapshot.heapUsedMb,
                heap_total_mb: snapshot.heapTotalMb,
                external_mb: snapshot.externalMb,
                array_buffers_mb: snapshot.arrayBuffersMb,
                event_loop_lag_ms: snapshot.eventLoopLagMs,
                concurrency: snapshot.concurrency,
                active_jobs: snapshot.activeJobs,
                cycle_action: snapshot.cycleAction,
                cycle_reason: snapshot.cycleReason,
            });
        }
        catch (err) {
            this.logger.warn('Failed to record telemetry snapshot to Supabase (non-fatal)', { error: err?.message });
        }
    }
    async pruneTelemetry() {
        try {
            await this.supabase.rpc('importer_prune_telemetry', {
                p_telemetry_hours: 24,
                p_job_metrics_days: 7,
            });
            this.logger.info('Pruned old importer telemetry and job metrics');
        }
        catch (err) {
            this.logger.warn('Failed to prune telemetry (non-fatal)', { error: err?.message });
        }
    }
    sanitizeErrorMessage(raw) {
        if (!raw)
            return '';
        return raw
            .replace(/Bearer\s+[A-Za-z0-9._~+/-]+=*/gi, 'Bearer [REDACTED]')
            .replace(/(token|access_token|refresh_token|secret|password|key)=([^\s&]+)/gi, '$1=[REDACTED]')
            .replace(/https?:\/\/[^:\/\s]+:[^@\/\s]+@/gi, 'https://[CREDENTIALS_REDACTED]@')
            .replace(/Cookie:\s*[^\r\n]+/gi, 'Cookie: [REDACTED]')
            .replace(/eyJ[a-zA-Z0-9_-]{10,}\.[a-zA-Z0-9_-]{10,}\.[a-zA-Z0-9_-]{10,}/g, '[JWT_REDACTED]')
            .slice(0, 1000);
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
    sleep(ms) {
        return new Promise((resolve) => setTimeout(resolve, ms));
    }
}
