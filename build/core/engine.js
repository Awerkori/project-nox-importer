import { ImporterQueue } from './queue.js';
import { DeduplicationEngine, computeCanonicalChapterKey } from './deduplication.js';
import { CheckpointManager } from './checkpoint.js';
import { processAndStoreMedia } from '../storage/media.js';
import { Logger } from './logger.js';
import { diagnostics } from './diagnostics.js';
import { AdaptiveAutotuner } from './concurrency.js';
import { PublicationBarrier } from './publication.js';
import { RetryPolicy, ProviderDownloadError } from './retry-policy.js';
import { ExistingWorksReconciler } from './reconciliation.js';
export { computeCanonicalChapterKey };
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
    publicationBarrier;
    reconciler;
    isRunning = false;
    stopSignal = false;
    abortController = new AbortController();
    // Ready Queue bounded buffer control in RAM (< 40MB max)
    static activeBufferedBytes = 0;
    static MAX_BUFFERED_BYTES = 40 * 1024 * 1024;
    constructor(supabase, storage, registry, rateLimiter, config) {
        this.supabase = supabase;
        this.storage = storage;
        this.registry = registry;
        this.rateLimiter = rateLimiter;
        this.config = config;
        this.queue = new ImporterQueue(supabase, config.WORKER_ID);
        this.deduplication = new DeduplicationEngine(supabase);
        this.checkpoints = new CheckpointManager(supabase);
        this.publicationBarrier = new PublicationBarrier(supabase);
        this.reconciler = new ExistingWorksReconciler(supabase, this.queue, registry);
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
        // 4. Launch background publication sweep loop (every 10s)
        this.runPublicationSweepLoop();
        // 5. Launch background lease recovery loop (every 60s)
        this.runLeaseRecoveryLoop();
        // 6. Launch periodic existing works reconciliation loop (every 15 min)
        this.runReconciliationLoop();
        // 7. Launch independent concurrent worker loops for each registered source
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
            this.logger.info('Starting generic lease recovery for stalled jobs...');
            const { recovered, failed } = await this.queue.recoverExpiredLeases();
            if (recovered > 0 || failed > 0) {
                this.logger.warn(`Startup recovery processed stalled jobs: ${recovered} requeued to QUEUED, ${failed} marked as FAILED`, { recovered, failed });
            }
            else {
                this.logger.info('Startup recovery check passed: no stalled jobs detected');
            }
            // 2. Sweep any staged publications left over from previous instance
            await this.publicationBarrier.sweepStagedPublications();
            // 3. Recover stalled 502 retries with long delays from previous exponential backoff policy
            await this.recoverStalled502Retries();
        }
        catch (err) {
            this.logger.warn('Error during startup recovery check', { error: err?.message });
        }
    }
    /**
     * Recalculates next_run_at for legacy retry jobs that were given long exponential backoffs (16-32 min)
     * due to transient 502/503 errors, rescheduling them for quick execution (10-35s).
     */
    async recoverStalled502Retries() {
        try {
            const now = new Date();
            let query = this.supabase
                .from('importer_queue')
                .select('id, attempts, next_run_at, last_error, source')
                .eq('status', 'RETRY');
            if (typeof query.is === 'function') {
                query = query.is('locked_by', null);
            }
            const { data: retries, error } = await query;
            if (error || !retries || retries.length === 0)
                return 0;
            let count = 0;
            for (const job of retries) {
                const err = job.last_error || '';
                // Only recover jobs that are demonstrably 502/503/timeout and scheduled into the future
                const isStorageErr = /502|503|timeout|aborted|ETIMEDOUT|Internal storage upload failed/i.test(err);
                const isPausedKuro = job.source === 'kuro' && /PAUSED/i.test(err);
                const isFuture = job.next_run_at && new Date(job.next_run_at) > now;
                if (isStorageErr && !isPausedKuro && isFuture) {
                    // Reschedule for quick execution (10 to 35 seconds with jitter)
                    const delaySec = 10 + Math.floor(Math.random() * 25);
                    const newNextRun = new Date(Date.now() + delaySec * 1000).toISOString();
                    await this.supabase
                        .from('importer_queue')
                        .update({
                        next_run_at: newNextRun,
                        updated_at: new Date().toISOString(),
                    })
                        .eq('id', job.id);
                    count++;
                }
            }
            if (count > 0) {
                this.logger.info(`Startup recovery rescheduled ${count} stalled 502/503 retry job(s) for immediate execution.`);
            }
            return count;
        }
        catch (err) {
            this.logger.warn('Error during recoverStalled502Retries', { error: err?.message });
            return 0;
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
    /**
     * Periodic publication sweep loop (every 10s) to unblock STAGED chapters
     */
    async runPublicationSweepLoop() {
        while (!this.stopSignal) {
            await this.sleep(10_000);
            if (this.stopSignal)
                break;
            try {
                await this.publicationBarrier.sweepStagedPublications();
            }
            catch (err) {
                this.logger.error('Error during publication sweep loop', { error: err?.message });
            }
        }
    }
    /**
     * Periodic lease recovery loop (every 60s) to rescue stalled jobs from crashed instances
     */
    async runLeaseRecoveryLoop() {
        while (!this.stopSignal) {
            await this.sleep(60_000);
            if (this.stopSignal)
                break;
            try {
                await this.queue.recoverExpiredLeases();
            }
            catch (err) {
                this.logger.warn('Error during periodic lease recovery loop', { error: err?.message });
            }
        }
    }
    /**
     * Periodic existing works reconciliation loop
     * Handles high-priority staff requests, on-demand admin reconciliations, and periodic catalog health batches.
     */
    async runReconciliationLoop() {
        await this.sleep(3000); // Quick startup delay
        let lastFullBatch = 0;
        while (!this.stopSignal) {
            const now = Date.now();
            try {
                // 1. Process active Prioridade Absoluta staff requests immediately
                const staffQuery = this.supabase.from('importer_staff_requests');
                if (staffQuery && typeof staffQuery.select === 'function') {
                    const { data: activeStaff } = await staffQuery
                        .select('work_id')
                        .in('status', ['QUEUED', 'IMPORTING', 'RETRYING']);
                    for (const req of activeStaff || []) {
                        if (this.stopSignal)
                            break;
                        try {
                            this.logger.info(`Running cross-provider reconciliation for Prioridade Absoluta work ${req.work_id}`);
                            await this.reconciler.reconcileWorkManifest(req.work_id, { priority: 100 });
                        }
                        catch (err) {
                            this.logger.warn(`Failed reconciling Prioridade Absoluta work ${req.work_id}`, { error: err?.message });
                        }
                    }
                }
                // 2. Process works explicitly requested for reconciliation from Admin UI
                const healthQuery = this.supabase.from('importer_work_health');
                if (healthQuery && typeof healthQuery.select === 'function') {
                    const { data: requestedWorks } = await healthQuery
                        .select('work_id')
                        .eq('health_status', 'RECONCILING')
                        .limit(5);
                    for (const req of requestedWorks || []) {
                        if (this.stopSignal)
                            break;
                        try {
                            this.logger.info(`Running requested reconciliation for work ${req.work_id}`);
                            await this.reconciler.reconcileWorkManifest(req.work_id);
                        }
                        catch (err) {
                            this.logger.warn(`Failed reconciling requested work ${req.work_id}`, { error: err?.message });
                        }
                    }
                }
                // 3. Periodic full catalog batch every 15 minutes
                if (now - lastFullBatch >= 15 * 60 * 1000) {
                    lastFullBatch = now;
                    this.logger.info('Starting periodic existing works reconciliation batch...');
                    await this.reconciler.reconcileExistingWorks(20);
                }
            }
            catch (err) {
                this.logger.error('Error during periodic reconciliation loop', { error: err?.message });
            }
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
            // Prioridade Absoluta Guard: if an active focus work exists, ONLY jobs for that work may run
            try {
                let reqQuery = this.supabase
                    .from('importer_staff_requests')
                    .select('id, work_id');
                if (typeof reqQuery?.in === 'function') {
                    reqQuery = reqQuery.in('status', ['QUEUED', 'IMPORTING', 'RETRYING']);
                }
                if (typeof reqQuery?.maybeSingle === 'function') {
                    const { data: activeFocus } = await reqQuery.maybeSingle();
                    if (activeFocus?.work_id && job.payload?.workId && job.payload.workId !== activeFocus.work_id) {
                        this.logger.info(`Focus Mode active for work ${activeFocus.work_id}. Deferring non-priority job for ${job.payload?.workId}`, {
                            jobId: job.id,
                            focusWorkId: activeFocus.work_id,
                            jobWorkId: job.payload?.workId,
                        });
                        heartbeat.stop();
                        await this.queue.releaseJob(job.id, 'RETRY', `Focus mode active for work ${activeFocus.work_id}`, 15);
                        return;
                    }
                }
            }
            catch {
                // Safe fallback in test harnesses where importer_staff_requests is unmocked
            }
            if (job.payload?.workId) {
                try {
                    await this.supabase
                        .from('importer_staff_requests')
                        .update({
                        status: 'IMPORTING',
                        last_attempt_at: new Date().toISOString(),
                        updated_at: new Date().toISOString(),
                    })
                        .eq('work_id', job.payload.workId)
                        .in('status', ['QUEUED', 'RETRYING']);
                }
                catch {
                    // Non-blocking telemetry
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
            const classification = RetryPolicy.classify(err);
            const isStaffPriority = Boolean(job.payload?.staffRequested) || (job.priority >= 100);
            const decision = RetryPolicy.decide(classification, job.attempts, job.max_attempts, { isStaffPriority });
            if (classification.retryClass === 'QUEUE_RETRY_429') {
                if (classification.sourceStage === 'storage') {
                    // Storage rate limit (Telegram / Storage Bridge):
                    // Throttles ONLY the Storage rate limiter; DO NOT scale down general job concurrency!
                    if (typeof this.storage.getRateLimiter === 'function') {
                        this.storage.getRateLimiter().recordRateLimit(classification.retryAfterSeconds);
                    }
                }
                else {
                    // Source provider rate limit: place the source in COOLDOWN
                    const waitSeconds = classification.retryAfterSeconds || 60;
                    const cooldownUntil = new Date(Date.now() + waitSeconds * 1000).toISOString();
                    this.logger.warn(`Source ${job.source} entered COOLDOWN due to provider rate limit for ${waitSeconds}s`);
                    await this.supabase
                        .from('importer_sources')
                        .update({
                        status: 'COOLDOWN',
                        cooldown_until: cooldownUntil,
                        updated_at: new Date().toISOString(),
                    })
                        .eq('id', job.source);
                }
            }
            else if (classification.retryClass === 'QUEUE_RETRY_STORAGE_502' || classification.retryClass === 'QUEUE_RETRY_STORAGE_503') {
                if (typeof this.storage.getRateLimiter === 'function') {
                    this.storage.getRateLimiter().recordTransientError();
                }
            }
            else if (classification.sourceStage === 'provider' && /rate\s*limit/i.test(errorMessage)) {
                // Source provider rate limit fallback: place the source in COOLDOWN
                const waitSeconds = classification.retryAfterSeconds || 60;
                const cooldownUntil = new Date(Date.now() + waitSeconds * 1000).toISOString();
                this.logger.warn(`Source ${job.source} entered COOLDOWN due to provider rate limit for ${waitSeconds}s`);
                await this.supabase
                    .from('importer_sources')
                    .update({
                    status: 'COOLDOWN',
                    cooldown_until: cooldownUntil,
                    updated_at: new Date().toISOString(),
                })
                    .eq('id', job.source);
            }
            else if (classification.retryClass === 'QUEUE_RETRY_TIMEOUT') {
                this.autotuner.recordError('timeout');
            }
            else if (classification.sourceStage === 'system') {
                this.autotuner.recordError('error');
            }
            this.logger.warn(`Job ${job.id} retry decision: ${decision.status} (delay: ${decision.delaySeconds}s, class: ${classification.retryClass})`, {
                jobId: job.id,
                source: job.source,
                workId: job.payload?.workId,
                chapterSortKey: job.chapter_sort_key,
                retryClass: classification.retryClass,
                attempt: job.attempts,
                delaySeconds: decision.delaySeconds,
                reason: decision.reason,
            });
            await this.queue.releaseJob(job.id, decision.status, this.sanitizeErrorMessage(errorMessage), decision.delaySeconds, classification.retryClass);
            if (job.payload?.workId && decision.status === 'RETRY') {
                const nextAttemptIso = new Date(Date.now() + decision.delaySeconds * 1000).toISOString();
                try {
                    await this.supabase
                        .from('importer_staff_requests')
                        .update({
                        status: 'RETRYING',
                        last_error: this.sanitizeErrorMessage(errorMessage),
                        last_attempt_at: new Date().toISOString(),
                        next_attempt_at: nextAttemptIso,
                        attempt_count: job.attempts,
                        updated_at: new Date().toISOString(),
                    })
                        .eq('work_id', job.payload.workId)
                        .in('status', ['QUEUED', 'IMPORTING', 'RETRYING']);
                }
                catch {
                    // Non-blocking telemetry
                }
            }
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
            }, 60);
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
        let { sourceWorkId } = job.payload;
        if (!sourceWorkId && job.payload?.workId) {
            const { data: mapping } = await this.supabase
                .from('importer_work_mappings')
                .select('source_work_id, source')
                .eq('work_id', job.payload.workId)
                .eq('source', job.source)
                .maybeSingle();
            if (mapping?.source_work_id) {
                sourceWorkId = mapping.source_work_id;
            }
            else {
                const { data: anyMapping } = await this.supabase
                    .from('importer_work_mappings')
                    .select('source_work_id, source')
                    .eq('work_id', job.payload.workId)
                    .order('confidence', { ascending: false })
                    .limit(1)
                    .maybeSingle();
                if (anyMapping?.source_work_id) {
                    sourceWorkId = anyMapping.source_work_id;
                }
            }
        }
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
                    is_page_provider: false,
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
        if (chaptersToEnqueue.length > 0) {
            let isStaffPriority = Boolean(job.payload?.staffRequested);
            if (!isStaffPriority) {
                try {
                    let reqQuery = this.supabase
                        .from('importer_staff_requests')
                        .select('id')
                        .eq('work_id', result.workId);
                    if (typeof reqQuery?.in === 'function') {
                        reqQuery = reqQuery.in('status', ['QUEUED', 'IMPORTING', 'RETRYING']);
                    }
                    if (typeof reqQuery?.maybeSingle === 'function') {
                        const { data: staffReq } = await reqQuery.maybeSingle();
                        if (staffReq)
                            isStaffPriority = true;
                    }
                }
                catch {
                    // Safe fallback
                }
            }
            const chapterPriority = isStaffPriority ? 100 : 30;
            // 1. Batch pre-register in importer_chapter_mappings in chunks
            const mappingsToUpsert = chaptersToEnqueue.map((ch) => {
                const chKey = this.computeCanonicalChapterKey(ch.number, ch.title);
                return {
                    source: job.source,
                    source_chapter_id: ch.sourceChapterId,
                    work_id: result.workId,
                    work_mapping_id: result.mappingId,
                    chapter_number: ch.number,
                    chapter_sort_key: chKey.sortKey,
                    page_count: ch.pageCount || 0,
                    status: 'PENDING',
                    is_gap: false,
                    last_error: null,
                };
            });
            const CHUNK_SIZE = 50;
            for (let i = 0; i < mappingsToUpsert.length; i += CHUNK_SIZE) {
                const chunk = mappingsToUpsert.slice(i, i + CHUNK_SIZE);
                await this.supabase
                    .from('importer_chapter_mappings')
                    .upsert(chunk, { onConflict: 'source,source_chapter_id' });
            }
            // 2. Batch enqueue tasks to importer_queue
            const queueJobs = chaptersToEnqueue.map((ch) => {
                const dedupeKey = `${job.source}:chapter:${ch.sourceChapterId}`;
                const chKey = this.computeCanonicalChapterKey(ch.number, ch.title);
                return {
                    taskType: 'IMPORT_CHAPTER',
                    source: job.source,
                    dedupeKey,
                    payload: {
                        sourceWorkId,
                        sourceChapterId: ch.sourceChapterId,
                        workId: result.workId,
                        workMappingId: result.mappingId,
                        chapterNumber: ch.number,
                        chapterTitle: ch.title || '',
                        expectedPageCount: ch.pageCount,
                        staffRequested: isStaffPriority,
                    },
                    priority: chapterPriority,
                    chapterSortKey: chKey.sortKey,
                };
            });
            await this.queue.enqueueBatch(queueJobs);
        }
        // Discovery complete for this work sync cycle: sweep any STAGED chapters that were waiting on discovery
        await this.publicationBarrier.sweepStagedPublications();
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
            const sortKey = this.computeChapterSortKey(chapterNumber, chapterTitle);
            await this.supabase.from('importer_chapter_mappings').upsert({
                source: job.source,
                source_chapter_id: sourceChapterId,
                chapter_id: alreadyPub.id,
                work_id: workId,
                work_mapping_id: workMappingId,
                chapter_number: chapterNumber,
                chapter_sort_key: sortKey,
                is_page_provider: false,
                status: 'COMPLETED',
                last_error: null,
            }, { onConflict: 'source,source_chapter_id' });
            return;
        }
        const adapter = this.registry.get(job.source);
        if (!adapter)
            throw new Error(`Source adapter not registered: ${job.source}`);
        // Pre-flight check 2: Check if chapter record already exists in database
        const { data: existingChapter } = await this.supabase
            .from('chapters')
            .select('id, published_at')
            .eq('work_id', workId)
            .eq('number', chapterNumber)
            .maybeSingle();
        // Mark chapter mapping as IMPORTING
        await this.supabase
            .from('importer_chapter_mappings')
            .update({ status: 'IMPORTING', updated_at: new Date().toISOString() })
            .eq('source', job.source)
            .eq('source_chapter_id', sourceChapterId);
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
            this.logger.info('Importing chapter pages with high-performance decoupled pipeline', {
                workId,
                chapterNumber,
                pageCount: expectedCount,
            });
            const botUserId = await this.resolveBotUserId();
            let validPages = [];
            let skipDownloadDueToExistingPages = false;
            // Pre-download deduplication: check if existing chapter already has all pages stored
            if (existingChapter) {
                const { data: existingPages } = await this.supabase
                    .from('pages')
                    .select('position, media_id, width, height')
                    .eq('chapter_id', existingChapter.id)
                    .order('position', { ascending: true });
                if (existingPages &&
                    existingPages.length === expectedCount &&
                    existingPages.every((p) => Boolean(p.media_id))) {
                    this.logger.info('Deduplication: chapter already has all pages in storage/db, skipping download', {
                        workId,
                        chapterNumber,
                        pageCount: expectedCount,
                    });
                    validPages = existingPages.map((p) => ({
                        mediaId: p.media_id,
                        width: p.width || 800,
                        height: p.height || 1200,
                    }));
                    skipDownloadDueToExistingPages = true;
                }
            }
            if (!skipDownloadDueToExistingPages) {
                const isPriority = Boolean(job.payload?.staffRequested);
                if (isPriority) {
                    this.rateLimiter.setTurboMode(true);
                }
                const storedPages = new Array(expectedCount).fill(null);
                // Download pool concurrency: 8 in priority mode, default from config (6)
                const downloadConcurrency = isPriority
                    ? Math.min(8, Math.max(6, (this.config.BATCH_PAGE_DOWNLOAD_CONCURRENCY || 3) * 2))
                    : (this.config.BATCH_PAGE_DOWNLOAD_CONCURRENCY || 6);
                // Upload pool concurrency: up to 4, bounded by autotuner and globalMediaSemaphore
                const uploadConcurrency = Math.min(4, Math.max(2, this.autotuner.getCurrentConcurrency()));
                const globalMediaSemaphore = this.autotuner.getGlobalMediaSemaphore();
                const fallbacks = job.payload?.fallbackSources;
                const readyQueue = [];
                let nextDownloadIndex = 0;
                let allDownloadsFinished = false;
                let pipelineError = null;
                const consumerResolvers = [];
                const notifyConsumer = () => {
                    while (consumerResolvers.length > 0) {
                        const resolve = consumerResolvers.shift();
                        if (resolve)
                            resolve();
                    }
                };
                const waitForPage = () => {
                    if (readyQueue.length > 0 || allDownloadsFinished || pipelineError || this.stopSignal) {
                        return Promise.resolve();
                    }
                    return new Promise((resolve) => {
                        consumerResolvers.push(resolve);
                    });
                };
                // Producer: downloads pages from provider CDN concurrently into bounded buffer
                const producer = async () => {
                    while (!this.stopSignal && !pipelineError) {
                        // Memory backpressure check: wait if in-flight active buffer >= MAX_BUFFERED_BYTES (40MB)
                        while (ImporterEngine.activeBufferedBytes >= ImporterEngine.MAX_BUFFERED_BYTES &&
                            !this.stopSignal &&
                            !pipelineError) {
                            await this.sleep(30);
                        }
                        const idx = nextDownloadIndex++;
                        if (idx >= expectedCount) {
                            break;
                        }
                        const pageUrl = pageUrls[idx];
                        const parsedUrl = new URL(pageUrl);
                        await this.rateLimiter.acquire(parsedUrl.host);
                        let attempts = 0;
                        let pageBytes = null;
                        let lastErr = null;
                        const d0 = Date.now();
                        while (attempts < 3 && !this.stopSignal && !pipelineError) {
                            attempts++;
                            try {
                                pageBytes = await this.fetchImageBytes(pageUrl, job.source);
                                tDownload += Date.now() - d0;
                                totalBytes += pageBytes.length;
                                ImporterEngine.activeBufferedBytes += pageBytes.length;
                                break;
                            }
                            catch (err) {
                                lastErr = err;
                                if (attempts < 3 && !this.stopSignal && !pipelineError) {
                                    await this.sleep(500 * attempts);
                                }
                            }
                        }
                        // Multi-source fallback support if primary source fails
                        if (!pageBytes && fallbacks && fallbacks.length > 0) {
                            this.logger.warn(`Primary source ${job.source} failed on page ${idx + 1}. Attempting multi-source fallback...`, {
                                workId,
                                chapterNumber,
                                fallbackCount: fallbacks.length,
                            });
                            for (const fb of fallbacks) {
                                try {
                                    const { data: srcCheck } = await this.supabase
                                        .from('importer_sources')
                                        .select('status, enabled, cooldown_until')
                                        .eq('id', fb.source)
                                        .maybeSingle();
                                    if (srcCheck) {
                                        if (!srcCheck.enabled || srcCheck.status === 'PAUSED' || srcCheck.status === 'DISABLED') {
                                            continue;
                                        }
                                        if (srcCheck.status === 'COOLDOWN') {
                                            const cd = srcCheck.cooldown_until ? new Date(srcCheck.cooldown_until).getTime() : 0;
                                            if (Date.now() < cd)
                                                continue;
                                        }
                                    }
                                    const fbAdapter = this.registry.get(fb.source);
                                    if (!fbAdapter)
                                        continue;
                                    const fbPages = await fbAdapter.fetchChapterPages(fb.sourceChapterId, chapterNumber);
                                    if (fbPages && fbPages[idx]) {
                                        pageBytes = await this.fetchImageBytes(fbPages[idx], fb.source);
                                        tDownload += Date.now() - d0;
                                        totalBytes += pageBytes.length;
                                        ImporterEngine.activeBufferedBytes += pageBytes.length;
                                        this.logger.info(`Successfully rescued page ${idx + 1} using fallback source ${fb.source}`);
                                        break;
                                    }
                                }
                                catch (fbErr) {
                                    this.logger.warn(`Fallback source ${fb.source} failed for page ${idx + 1}`, { error: fbErr?.message });
                                }
                            }
                        }
                        if (!pageBytes) {
                            pipelineError = new Error(`Failed to process page ${idx + 1}/${expectedCount} after 3 attempts: ${lastErr?.message}`);
                            notifyConsumer();
                            break;
                        }
                        readyQueue.push({ index: idx, pageBytes });
                        notifyConsumer();
                    }
                };
                let completedUploadsCount = 0;
                // Consumer: uploads downloaded pages to Storage Bridge / Telegram concurrently
                const consumer = async () => {
                    while (!this.stopSignal && !pipelineError) {
                        while (readyQueue.length === 0) {
                            if (allDownloadsFinished || pipelineError || this.stopSignal) {
                                return;
                            }
                            await waitForPage();
                        }
                        const item = readyQueue.shift();
                        if (!item)
                            continue;
                        let pageBytes = item.pageBytes;
                        try {
                            const u0 = Date.now();
                            const res = await globalMediaSemaphore.runExclusive(async () => {
                                return await processAndStoreMedia(this.supabase, this.storage, pageBytes, botUserId, 'editorial');
                            });
                            const uploadDuration = Date.now() - u0;
                            tUpload += uploadDuration;
                            if (typeof this.storage.getRateLimiter === 'function') {
                                const limiter = this.storage.getRateLimiter();
                                if (typeof limiter.recordSuccess === 'function') {
                                    limiter.recordSuccess(pageBytes.length, uploadDuration);
                                }
                            }
                            storedPages[item.index] = {
                                mediaId: res.mediaId,
                                width: res.width,
                                height: res.height,
                            };
                            completedUploadsCount++;
                            diagnostics.updateJobProgress(job.id, completedUploadsCount);
                        }
                        catch (err) {
                            pipelineError = err;
                            this.logger.error(`Failed to upload page ${item.index + 1}/${expectedCount}`, { error: err?.message });
                            notifyConsumer();
                            break;
                        }
                        finally {
                            if (pageBytes) {
                                ImporterEngine.activeBufferedBytes = Math.max(0, ImporterEngine.activeBufferedBytes - pageBytes.length);
                                pageBytes = null;
                            }
                        }
                    }
                };
                const producerPromises = Array.from({ length: downloadConcurrency }, () => producer());
                const consumerPromises = Array.from({ length: uploadConcurrency }, () => consumer());
                await Promise.all(producerPromises);
                allDownloadsFinished = true;
                notifyConsumer();
                await Promise.all(consumerPromises);
                if (pipelineError) {
                    throw pipelineError;
                }
                // SAFEGUARD 1: Strict integrity check
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
                    origin: 'IMPORTER',
                });
                if (chErr)
                    throw chErr;
            }
            // SAFEGUARD 1: Batch upsert into public.pages ONLY after ALL pages are verified
            if (!skipDownloadDueToExistingPages) {
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
            }
            // SAFEGUARD 2: Stage chapter with published_at = NULL in public.chapters
            const chKey = this.computeCanonicalChapterKey(chapterNumber, chapterTitle);
            await this.publicationBarrier.stageChapter({
                workId,
                chapterId,
                chapterNumber,
                sortKey: chKey.sortKey,
                source: job.source,
                sourceChapterId,
                workMappingId,
                pageCount: validPages.length,
                isPageProvider: true,
            });
            // SAFEGUARD 3: Try to publish immediately 1x via barrier. If blocked, release worker slot immediately!
            const pubResult = await this.publicationBarrier.tryPublish(workId, chKey.sortKey, chapterId);
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
                status: pubResult.published ? 'COMPLETED' : 'STAGED',
            });
            if (pubResult.published) {
                this.logger.info('Successfully imported and published chapter in canonical order', {
                    workId,
                    chapterNumber,
                    sortKey: chKey.sortKey,
                    pageCount: validPages.length,
                });
                try {
                    const manQuery = this.supabase.from('importer_chapter_manifest');
                    if (manQuery && typeof manQuery.update === 'function') {
                        await manQuery
                            .update({
                            status: 'PUBLISHED',
                            last_checked_at: new Date().toISOString(),
                        })
                            .eq('work_id', workId)
                            .eq('chapter_sort_key', chKey.sortKey);
                    }
                }
                catch {
                    // Non-blocking
                }
                await this.checkStaffRequestCompletion(workId);
            }
            else {
                this.logger.info('Successfully imported and staged chapter. Waiting for preceding chapter(s) to publish', {
                    workId,
                    chapterNumber,
                    sortKey: chKey.sortKey,
                    reason: pubResult.reason,
                    pageCount: validPages.length,
                });
                try {
                    const manQuery = this.supabase.from('importer_chapter_manifest');
                    if (manQuery && typeof manQuery.update === 'function') {
                        await manQuery
                            .update({
                            status: 'STAGED',
                            last_checked_at: new Date().toISOString(),
                        })
                            .eq('work_id', workId)
                            .eq('chapter_sort_key', chKey.sortKey);
                    }
                }
                catch {
                    // Non-blocking
                }
            }
        }
        catch (err) {
            // If chapter failed definitively after exhausting all attempts, handle fallback / register gap
            if (job.attempts + 1 >= job.max_attempts) {
                const chKey = this.computeCanonicalChapterKey(chapterNumber, chapterTitle);
                await this.publicationBarrier.handleDefiniteFailure(workId, chapterNumber, chKey.sortKey, job.source);
            }
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
            if (Boolean(job.payload?.staffRequested)) {
                this.rateLimiter.setTurboMode(false);
            }
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
    async fetchImageBytes(url, source = 'unknown') {
        const parsedUrl = new URL(url);
        const res = await fetch(url, {
            headers: {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
                Referer: `${parsedUrl.origin}/`,
            },
            signal: AbortSignal.timeout(45_000),
        });
        if (!res.ok) {
            if (res.status === 429) {
                const retryAfter = res.headers.get('Retry-After');
                this.rateLimiter.handle429(parsedUrl.host, retryAfter);
            }
            throw new ProviderDownloadError(res.status, url, source, `Failed to download image from ${url}: HTTP ${res.status}`);
        }
        this.rateLimiter.recordSuccess(parsedUrl.host);
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
    /**
     * Verifies if all chapters in the canonical manifest for a prioritized work are accounted for
     * (either PUBLISHED or marked as UNRESOLVED_GAP). If no chapters remain in QUEUED or STAGED,
     * marks the staff request as COMPLETED.
     */
    async checkStaffRequestCompletion(workId) {
        try {
            const staffQuery = this.supabase.from('importer_staff_requests');
            if (!staffQuery || typeof staffQuery.select !== 'function')
                return;
            const { data: activeRequests } = await staffQuery
                .select('id, status')
                .eq('work_id', workId)
                .in('status', ['QUEUED', 'IMPORTING', 'RETRYING']);
            if (!activeRequests || activeRequests.length === 0)
                return;
            // 1. Check if active jobs remain in importer_queue
            const qQuery = this.supabase.from('importer_queue');
            if (qQuery && typeof qQuery.select === 'function') {
                const { count } = await qQuery
                    .select('id', { count: 'exact', head: true })
                    .eq('payload->>workId', workId)
                    .in('status', ['QUEUED', 'IMPORTING', 'RETRY']);
                if (count && count > 0)
                    return;
            }
            // 2. Check if chapters remain uncompleted in importer_chapter_manifest
            const manQuery = this.supabase.from('importer_chapter_manifest');
            if (manQuery && typeof manQuery.select === 'function') {
                const { data: pendingChapters } = await manQuery
                    .select('chapter_sort_key, status')
                    .eq('work_id', workId)
                    .in('status', ['QUEUED', 'STAGED']);
                if (pendingChapters && pendingChapters.length > 0)
                    return;
            }
            // 3. Complete staff request
            await staffQuery
                .update({
                status: 'COMPLETED',
                completed_at: new Date().toISOString(),
                updated_at: new Date().toISOString(),
            })
                .eq('work_id', workId)
                .in('status', ['QUEUED', 'IMPORTING', 'RETRYING']);
            this.logger.info(`Prioridade Absoluta completed for work ${workId}: all manifest chapters resolved!`);
        }
        catch (err) {
            this.logger.warn(`Failed to check staff request completion for work ${workId}`, { error: err?.message });
        }
    }
    sleep(ms) {
        return new Promise((resolve) => setTimeout(resolve, ms));
    }
}
