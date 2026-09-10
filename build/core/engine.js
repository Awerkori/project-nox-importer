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
export class JobCancelledByStaffError extends Error {
    jobId;
    constructor(jobId, message = 'Job cancelado pela Staff no checkpoint seguro') {
        super(message);
        this.jobId = jobId;
        this.name = 'JobCancelledByStaffError';
    }
}
export function classifyPageUrl(url, index, total) {
    const clean = decodeURIComponent(url).toLowerCase();
    if (/credito|crédito|credit|credits/i.test(clean))
        return 'CREDIT_PAGE';
    if (/recrut|recrutamento|recruit/i.test(clean))
        return 'RECRUITMENT_PAGE';
    if (/aviso|warning|notice/i.test(clean))
        return 'WARNING_PAGE';
    if (/fanservice|apoie|doacao|doação|donate|discord|parceria/i.test(clean))
        return 'PROMO_PAGE';
    if (index === 0 && (/capa|cover|front|\(0+\)|\b000?\b|_00\./i.test(clean) || /\[.+\]\s*.+\(0\)/i.test(clean))) {
        return 'CREDIT_PAGE';
    }
    if ((index === 0 || index === total - 1) && /scan|staff|discord|padrim|apoia/i.test(clean)) {
        return 'PROMO_PAGE';
    }
    return 'CONTENT_PAGE';
}
export class NarrativePageUnavailableError extends Error {
    source;
    pageIndex;
    totalPages;
    originalError;
    constructor(source, pageIndex, totalPages, originalError) {
        super(`Narrative story page ${pageIndex + 1}/${totalPages} unavailable on source ${source}: ${originalError}`);
        this.source = source;
        this.pageIndex = pageIndex;
        this.totalPages = totalPages;
        this.originalError = originalError;
        this.name = 'NarrativePageUnavailableError';
    }
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
        // 7. Launch background upstream provider health check loop (every 5 min)
        this.runUpstreamHealthLoop();
        // 8. Launch independent concurrent worker loops for each registered source
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
    /**
     * Periodic upstream provider health check loop (every 5 min)
     * Evaluates UPSTREAM_BLOCKED, RECOVERING, and DEGRADED sources.
     * If Cloudflare lifts 403 on datacenter egress, stages safe recovery:
     * UPSTREAM_BLOCKED -> RECOVERING -> ACTIVE (only after validating Search, Chapters, Pages, and Download)
     */
    async runUpstreamHealthLoop() {
        await this.sleep(15_000);
        while (!this.stopSignal) {
            try {
                await this.checkBlockedSourcesHealth();
            }
            catch (err) {
                this.logger.error('Error during upstream sources health check loop', { error: err?.message });
            }
            await this.sleep(5 * 60_000);
        }
    }
    async checkBlockedSourcesHealth() {
        const { data: blockedSources, error } = await this.supabase
            .from('importer_sources')
            .select('id, name, status, base_url, blocked_reason, blocked_details')
            .in('status', ['UPSTREAM_BLOCKED', 'RECOVERING', 'DEGRADED']);
        if (error || !blockedSources || blockedSources.length === 0)
            return;
        for (const src of blockedSources) {
            if (this.stopSignal)
                break;
            await this.probeSourceHealth(src);
        }
    }
    async probeSourceHealth(src) {
        const nowIso = new Date().toISOString();
        const adapter = this.registry.get(src.id);
        if (!adapter)
            return;
        try {
            this.logger.info(`Probing health for ${src.id}...`);
            let searchResults = [];
            try {
                searchResults = await adapter.searchWorks('Solo');
            }
            catch (sErr) {
                this.logger.warn(`Search probe failed for ${src.id}`, { error: sErr?.message });
            }
            if (!searchResults || searchResults.length === 0) {
                this.logger.info(`Source ${src.id} search probe returned no results. Retaining UPSTREAM_BLOCKED.`);
                await this.supabase
                    .from('importer_sources')
                    .update({
                    status: 'UPSTREAM_BLOCKED',
                    blocked_reason: 'CLOUDFLARE_DATACENTER_BLOCK',
                    blocked_details: {
                        message: 'Cloudflare bloqueia o ambiente atual do Importer (DIScloud / OVH). Local/Mihon: funcional; DIScloud: HTTP 403.',
                        local_status: 200,
                        discloud_status: 403,
                        last_checked_at: nowIso,
                    },
                    last_health_check_at: nowIso,
                    updated_at: nowIso,
                })
                    .eq('id', src.id);
                return;
            }
            this.logger.info(`Source ${src.id} Search probe succeeded (${searchResults.length} works). Transitioning to RECOVERING to validate full pipeline.`);
            await this.supabase
                .from('importer_sources')
                .update({
                status: 'RECOVERING',
                last_health_check_at: nowIso,
                updated_at: nowIso,
            })
                .eq('id', src.id);
            try {
                // Stage 2: Chapters
                const chapters = await adapter.fetchChapters(searchResults[0].sourceWorkId).catch(() => []);
                if (!chapters || chapters.length === 0) {
                    throw new Error('Health check stage 2 failed: 0 chapters returned');
                }
                const testChapter = chapters[0];
                // Stage 3: Pages
                const pages = await adapter.fetchChapterPages(testChapter.sourceChapterId);
                if (!pages || pages.length === 0) {
                    throw new Error('Health check stage 3 failed: 0 pages returned');
                }
                // Stage 4: Download 1 image
                const firstPageUrl = typeof pages[0] === 'string' ? pages[0] : pages[0]?.imageUrl;
                const imgBytes = await this.fetchImageBytes(firstPageUrl, src.id);
                if (!imgBytes || imgBytes.byteLength === 0) {
                    throw new Error('Health check stage 4 failed: image download returned 0 bytes');
                }
                // Passed all 4 stages! Transition to ACTIVE
                this.logger.info(`Source ${src.id} passed all 4 validation stages! Transitioning to ACTIVE.`);
                await this.supabase
                    .from('importer_sources')
                    .update({
                    status: 'ACTIVE',
                    enabled: true,
                    blocked_reason: null,
                    blocked_details: {},
                    last_health_check_at: nowIso,
                    updated_at: nowIso,
                })
                    .eq('id', src.id);
                // Unpark held jobs for this source back to QUEUED
                try {
                    const { error: unparkErr } = await this.supabase
                        .from('importer_queue')
                        .update({
                        status: 'QUEUED',
                        last_error: null,
                        updated_at: nowIso,
                    })
                        .eq('source', src.id)
                        .eq('status', 'BLOCKED_BY_UPSTREAM');
                    if (!unparkErr) {
                        this.logger.info(`Unparked held jobs for ${src.id} back to QUEUED now that source is ACTIVE`);
                    }
                }
                catch (unparkErr) {
                    this.logger.warn(`Failed to unpark jobs for ${src.id}`, { error: unparkErr?.message });
                }
            }
            catch (valErr) {
                this.logger.warn(`Source ${src.id} recovery validation failed`, { error: valErr?.message });
                await this.supabase
                    .from('importer_sources')
                    .update({
                    status: 'UPSTREAM_BLOCKED',
                    blocked_reason: 'RECOVERY_VALIDATION_FAILED',
                    blocked_details: {
                        error: valErr?.message,
                        last_checked_at: nowIso,
                    },
                    last_health_check_at: nowIso,
                    updated_at: nowIso,
                })
                    .eq('id', src.id);
            }
        }
        catch (err) {
            this.logger.error(`Error probing health for source ${src.id}`, { error: err?.message });
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
                    if (!src.enabled || src.status === 'PAUSED' || src.status === 'DISABLED' || src.status === 'UPSTREAM_BLOCKED') {
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
            if (src.enabled === false || src.status === 'DISABLED' || src.status === 'PAUSED' || src.status === 'UPSTREAM_BLOCKED') {
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
                // Prevent duplicate DISCOVER_WORKS jobs from piling up if one is already active or in retry
                let hasActive = false;
                try {
                    const q = this.supabase
                        .from('importer_queue')
                        .select('id, status')
                        .eq('task_type', 'DISCOVER_WORKS')
                        .eq('source', src.id);
                    const { data: existingActive } = typeof q.in === 'function'
                        ? await q.in('status', ['QUEUED', 'IMPORTING', 'RETRY']).limit(1)
                        : await q.limit(10);
                    if (existingActive && Array.isArray(existingActive)) {
                        hasActive = existingActive.some((j) => ['QUEUED', 'IMPORTING', 'RETRY'].includes(j.status));
                    }
                }
                catch { }
                if (hasActive) {
                    continue;
                }
                const dedupeKey = `${src.id}:discover:${Math.floor(now / intervalMs)}`;
                await this.queue.enqueue('DISCOVER_WORKS', src.id, dedupeKey, {
                    workTitle: `Varredura de Catálogo (${src.name || src.id})`,
                }, 10);
                await this.supabase
                    .from('importer_sources')
                    .update({ last_sync_at: new Date().toISOString() })
                    .eq('id', src.id);
            }
        }
    }
    async processJob(job) {
        let cancelSignalTriggered = false;
        const heartbeat = this.queue.startHeartbeat(job.id, this.config.QUEUE_HEARTBEAT_INTERVAL_SECONDS, () => {
            cancelSignalTriggered = true;
        });
        try {
            // Checkpoint 0: Staff cancellation pre-flight check
            if (job.cancel_requested || (await this.queue.isCancelRequested(job.id))) {
                heartbeat.stop();
                this.logger.info(`Job ${job.id} cancelled by staff prior to execution.`);
                await this.queue.releaseJob(job.id, 'CANCELLED_BY_STAFF');
                return;
            }
            const { data: sourceRec } = await this.supabase
                .from('importer_sources')
                .select('id, status, cooldown_until, enabled')
                .eq('id', job.source)
                .maybeSingle();
            if (sourceRec) {
                if (sourceRec.status === 'UPSTREAM_BLOCKED') {
                    // If this is a chapter import job, check if other healthy sources exist for the work
                    let hasHealthyFallback = false;
                    if (job.task_type === 'IMPORT_CHAPTER' && job.payload?.workId && job.payload?.chapterNumber) {
                        const candidateFallbacks = await this.resolveDynamicCandidateFallbacks(job.payload.workId, job.payload.chapterNumber, job.source, job.payload?.fallbackSources || []);
                        if (candidateFallbacks.length > 0) {
                            hasHealthyFallback = true;
                        }
                    }
                    if (!hasHealthyFallback) {
                        this.logger.warn(`Parking job ${job.id}: source ${job.source} is UPSTREAM_BLOCKED (retaining safely in BLOCKED_BY_UPSTREAM)`);
                        heartbeat.stop();
                        await this.queue.releaseJob(job.id, 'BLOCKED_BY_UPSTREAM', `Bloqueado a montante: upstream_blocked (${sourceRec.status})`);
                        return;
                    }
                }
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
                    await this.handleImportChapter(job, () => cancelSignalTriggered);
                    break;
                default:
                    throw new Error(`Unknown task type: ${job.task_type}`);
            }
            heartbeat.stop();
            await this.queue.releaseJob(job.id, 'COMPLETED');
        }
        catch (err) {
            heartbeat.stop();
            // Check if job was cancelled by staff at safe checkpoint
            if (err instanceof JobCancelledByStaffError || cancelSignalTriggered) {
                this.logger.info(`Job ${job.id} safely cancelled by staff at checkpoint`);
                if (job.payload?.sourceChapterId && job.source) {
                    try {
                        await this.supabase
                            .from('importer_chapter_mappings')
                            .update({ status: 'QUEUED', updated_at: new Date().toISOString() })
                            .eq('source', job.source)
                            .eq('source_chapter_id', job.payload.sourceChapterId);
                    }
                    catch { }
                }
                await this.queue.releaseJob(job.id, 'CANCELLED_BY_STAFF');
                return;
            }
            const errorMessage = err?.message || String(err);
            this.logger.error('Job execution failed', {
                jobId: job.id,
                taskType: job.task_type,
                error: errorMessage,
                attempts: job.attempts,
            });
            // If error is from an upstream provider blocked by Cloudflare (HTTP 403 / Cloudflare Challenge on datacenter),
            // transition source to UPSTREAM_BLOCKED and park the job safely in BLOCKED_BY_UPSTREAM instead of retry loop
            if (/403.*cloudflare|cloudflare.*403|upstream_blocked/i.test(errorMessage) && job.attempts >= 3) {
                this.logger.warn(`Source ${job.source} detected upstream block (HTTP 403 / Cloudflare). Transitioning source to UPSTREAM_BLOCKED and parking job.`);
                const nowIso = new Date().toISOString();
                try {
                    await this.supabase
                        .from('importer_sources')
                        .update({
                        status: 'UPSTREAM_BLOCKED',
                        blocked_reason: 'CLOUDFLARE_DATACENTER_BLOCK',
                        blocked_details: {
                            message: 'Cloudflare bloqueia o ambiente atual do Importer (DIScloud / OVH). Local/Mihon: funcional; DIScloud: HTTP 403.',
                            local_status: 200,
                            discloud_status: 403,
                            last_checked_at: nowIso,
                        },
                        updated_at: nowIso,
                    })
                        .eq('id', job.source);
                }
                catch { }
                await this.queue.releaseJob(job.id, 'BLOCKED_BY_UPSTREAM', `Bloqueado a montante: upstream_blocked (CLOUDFLARE_DATACENTER_BLOCK)`);
                return;
            }
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
                        expectedPageCount: ch.pageCount || null,
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
    async handleImportChapter(job, isCancelled) {
        const { sourceWorkId, sourceChapterId, workId, workMappingId, chapterNumber, chapterTitle, } = job.payload;
        if (!sourceChapterId || !workId || chapterNumber === undefined) {
            throw new Error('Incomplete chapter import payload');
        }
        // Checkpoint 1: Pre-flight check for staff cancellation
        if (isCancelled?.() || (await this.queue.isCancelRequested(job.id))) {
            throw new JobCancelledByStaffError(job.id);
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
        let effectiveSource = job.source;
        let effectiveSourceChapterId = sourceChapterId;
        let effectiveWorkMappingId = workMappingId;
        const initialSource = job.source;
        // Detect Source / ID format mismatch
        // (e.g. MangaFlix job with numeric Manhastro/Kuro chapter ID)
        if (effectiveSource === 'mangaflix' && /^\d+$/.test(effectiveSourceChapterId)) {
            const { data: realMapping } = await this.supabase
                .from('importer_chapter_mappings')
                .select('source, source_chapter_id, work_mapping_id')
                .eq('work_id', workId)
                .eq('source_chapter_id', effectiveSourceChapterId)
                .maybeSingle();
            if (realMapping?.source) {
                this.logger.warn(`Source mismatch detected for job ${job.id}: job.source is ${job.source} but chapter ID ${effectiveSourceChapterId} belongs to ${realMapping.source}. Auto-correcting source.`, {
                    originalSource: job.source,
                    correctedSource: realMapping.source,
                    chapterNumber,
                });
                effectiveSource = realMapping.source;
                if (realMapping.work_mapping_id) {
                    effectiveWorkMappingId = realMapping.work_mapping_id;
                }
                await this.supabase
                    .from('importer_queue')
                    .update({ source: effectiveSource, updated_at: new Date().toISOString() })
                    .eq('id', job.id);
            }
        }
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
            .eq('source', effectiveSource)
            .eq('source_chapter_id', effectiveSourceChapterId);
        // Register active job in forensics tracker
        diagnostics.registerJob({
            jobId: job.id,
            taskType: job.task_type,
            source: effectiveSource,
            workId,
            chapterNumber,
            completedPages: 0,
        });
        const tStart = Date.now();
        let tDownload = 0;
        let tUpload = 0;
        let tDb = 0;
        let totalBytes = 0;
        let successfulExecution = false;
        let lastRescuedError = null;
        let validPages = [];
        let skipDownloadDueToExistingPages = false;
        // Dynamic candidate fallbacks resolution across payload, mappings, manifest, and work sources
        const candidateFallbacks = await this.resolveDynamicCandidateFallbacks(workId, chapterNumber, effectiveSource, job.payload?.fallbackSources || []);
        let allSourceCandidates = [
            { source: effectiveSource, sourceChapterId: effectiveSourceChapterId, mappingId: effectiveWorkMappingId },
            ...candidateFallbacks,
        ];
        // If primary source is UPSTREAM_BLOCKED, skip directly to first healthy fallback
        const { data: primarySrc } = await this.supabase
            .from('importer_sources')
            .select('status')
            .eq('id', effectiveSource)
            .maybeSingle();
        if (primarySrc?.status === 'UPSTREAM_BLOCKED' && candidateFallbacks.length > 0) {
            this.logger.info(`CROSS_PROVIDER_RESCUE: Primary source ${effectiveSource} is UPSTREAM_BLOCKED. Routing directly to fallback source ${candidateFallbacks[0].source}`, {
                workId,
                chapterNumber,
                fallbackSource: candidateFallbacks[0].source,
            });
            allSourceCandidates = candidateFallbacks;
        }
        try {
            for (let candidateIdx = 0; candidateIdx < allSourceCandidates.length; candidateIdx++) {
                const candidate = allSourceCandidates[candidateIdx];
                effectiveSource = candidate.source;
                effectiveSourceChapterId = candidate.sourceChapterId;
                if (candidate.mappingId) {
                    effectiveWorkMappingId = candidate.mappingId;
                }
                const adapter = this.registry.get(effectiveSource);
                if (!adapter) {
                    this.logger.warn(`Source adapter not registered: ${effectiveSource}, skipping candidate`);
                    continue;
                }
                if (candidateIdx > 0) {
                    this.logger.info(`CROSS_PROVIDER_RESCUE: Rescuing chapter ${chapterNumber} using fallback source ${effectiveSource} (${effectiveSourceChapterId}) instead of ${initialSource}`, {
                        workId,
                        chapterNumber,
                        rescueSource: effectiveSource,
                        previousError: lastRescuedError,
                    });
                }
                let pageUrls = [];
                let primaryError = null;
                try {
                    pageUrls = await adapter.fetchChapterPages(effectiveSourceChapterId, chapterNumber);
                }
                catch (adapterErr) {
                    primaryError = adapterErr instanceof Error ? adapterErr : new Error(String(adapterErr));
                    this.logger.warn(`Source ${effectiveSource} failed fetchChapterPages for ch ${chapterNumber}`, {
                        error: primaryError.message,
                    });
                }
                if (!pageUrls || pageUrls.length === 0) {
                    lastRescuedError = primaryError?.message || `Source ${effectiveSource} returned 0 pages`;
                    if (candidateIdx === allSourceCandidates.length - 1) {
                        if (allSourceCandidates.length === 1) {
                            const detail = primaryError?.message ? `: ${primaryError.message}` : '';
                            throw new Error(`Source ${effectiveSource} failed to return pages for chapter ${chapterNumber} (${effectiveSourceChapterId})${detail}`);
                        }
                        else {
                            const detail = primaryError?.message ? ` (Primary error: ${primaryError.message})` : '';
                            throw new Error(`Failed to obtain pages for chapter ${chapterNumber} (${effectiveSourceChapterId}) across primary and fallback sources [${allSourceCandidates.map(c => c.source).join(', ')}]${detail}`);
                        }
                    }
                    continue;
                }
                const expectedCount = pageUrls.length;
                this.supabase.from('importer_queue').update({
                    progress_total: expectedCount,
                    progress_stage: 'DOWNLOADING',
                    progress_current: 0,
                }).eq('id', job.id).then(() => { }, () => { });
                this.logger.info('Importing chapter pages with high-performance decoupled pipeline', {
                    workId,
                    chapterNumber,
                    pageCount: expectedCount,
                    source: effectiveSource,
                });
                const botUserId = await this.resolveBotUserId();
                validPages = [];
                skipDownloadDueToExistingPages = false;
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
                        successfulExecution = true;
                        break;
                    }
                }
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
                const readyQueue = [];
                let nextDownloadIndex = 0;
                let allDownloadsFinished = false;
                let pipelineError = null;
                let manifestRefreshed = false;
                let failed404Count = 0;
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
                // Producer: downloads raw page bytes from source CDN into memory
                const producer = async () => {
                    while (!this.stopSignal && !pipelineError) {
                        // Memory backpressure check: wait if in-flight active buffer >= MAX_BUFFERED_BYTES (40MB)
                        while (ImporterEngine.activeBufferedBytes >= ImporterEngine.MAX_BUFFERED_BYTES &&
                            !this.stopSignal &&
                            !pipelineError) {
                            if (isCancelled?.()) {
                                pipelineError = new JobCancelledByStaffError(job.id);
                                notifyConsumer();
                                break;
                            }
                            await this.sleep(30);
                        }
                        // Safe Checkpoint: cancellation check
                        if (isCancelled?.() || (nextDownloadIndex % 3 === 0 && (await this.queue.isCancelRequested(job.id)))) {
                            pipelineError = new JobCancelledByStaffError(job.id);
                            notifyConsumer();
                            break;
                        }
                        const idx = nextDownloadIndex++;
                        if (idx >= expectedCount) {
                            break;
                        }
                        let pageUrl = pageUrls[idx];
                        const parsedUrl = new URL(pageUrl);
                        await this.rateLimiter.acquire(parsedUrl.host);
                        let pageBytes = null;
                        let attempts = 0;
                        let lastErr = null;
                        const d0 = Date.now();
                        while (attempts < 3 && !this.stopSignal && !pipelineError) {
                            attempts++;
                            try {
                                pageBytes = await this.fetchImageBytes(pageUrl, effectiveSource);
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
                        if (!pageBytes) {
                            const errMsg = (lastErr instanceof Error && lastErr.message) ? lastErr.message : (lastErr ? String(lastErr) : 'Erro desconhecido');
                            const is404 = errMsg.includes('HTTP 404') || errMsg.includes('status: 404');
                            let currentUrl = pageUrls[idx] || '';
                            // MANIFEST REFRESH: Before blind failure, query upstream to see if URLs were updated
                            if (is404 && !manifestRefreshed) {
                                manifestRefreshed = true;
                                try {
                                    const refreshAdapter = this.registry.get(effectiveSource);
                                    if (refreshAdapter) {
                                        const refreshedUrls = await refreshAdapter.fetchChapterPages(effectiveSourceChapterId, chapterNumber);
                                        if (refreshedUrls && refreshedUrls.length === expectedCount) {
                                            const isDifferent = refreshedUrls.some((u, i) => u !== pageUrls[i]);
                                            if (isDifferent) {
                                                this.logger.info(`MANIFEST_REFRESH: Upstream manifest refreshed with updated URLs for chapter ${chapterNumber} on ${effectiveSource}`, {
                                                    workId,
                                                    chapterNumber,
                                                    oldUrl: currentUrl,
                                                    newUrl: refreshedUrls[idx],
                                                });
                                                pageUrls = refreshedUrls;
                                                currentUrl = pageUrls[idx] || '';
                                                // Retry downloading with the fresh URL
                                                try {
                                                    pageBytes = await this.fetchImageBytes(currentUrl, effectiveSource);
                                                    tDownload += Date.now() - d0;
                                                    totalBytes += pageBytes.length;
                                                    ImporterEngine.activeBufferedBytes += pageBytes.length;
                                                }
                                                catch (freshErr) {
                                                    lastErr = freshErr;
                                                }
                                            }
                                            else {
                                                this.logger.info(`MANIFEST_REFRESH: Upstream manifest verified, URLs unchanged for chapter ${chapterNumber} on ${effectiveSource}`);
                                            }
                                        }
                                    }
                                }
                                catch (refreshErr) {
                                    this.logger.warn(`MANIFEST_REFRESH failed for ${effectiveSource} ch ${chapterNumber}`, { error: refreshErr?.message });
                                }
                            }
                            // If recovered by manifest refresh, proceed!
                            if (pageBytes) {
                                readyQueue.push({ index: idx, pageBytes });
                                notifyConsumer();
                                continue;
                            }
                            // Page Classification
                            const pageSemantic = classifyPageUrl(currentUrl, idx, expectedCount);
                            // Non-content pages (credits, recruitment, promo, warning) can be skipped with telemetry
                            if (is404 && pageSemantic !== 'CONTENT_PAGE') {
                                failed404Count++;
                                this.logger.warn(`SKIPPED_NON_CONTENT_PAGE: Skipping non-content 404 page ${idx + 1}/${expectedCount} (${pageSemantic}): ${currentUrl}`, {
                                    workId,
                                    chapterNumber,
                                    pageSemantic,
                                    currentUrl,
                                    failed404Count,
                                });
                                storedPages[idx] = { mediaId: '__SKIPPED_NON_CONTENT_PAGE__', width: 0, height: 0 };
                                continue;
                            }
                            // Narrative story page or persistent error: CANNOT be skipped!
                            pipelineError = new NarrativePageUnavailableError(effectiveSource, idx, expectedCount, errMsg);
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
                        if (isCancelled?.()) {
                            pipelineError = new JobCancelledByStaffError(job.id);
                            break;
                        }
                        while (readyQueue.length === 0) {
                            if (allDownloadsFinished || pipelineError || this.stopSignal) {
                                return;
                            }
                            await waitForPage();
                        }
                        if (isCancelled?.()) {
                            pipelineError = new JobCancelledByStaffError(job.id);
                            break;
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
                            if (completedUploadsCount % 2 === 0 || completedUploadsCount === expectedCount) {
                                this.supabase.from('importer_queue').update({
                                    progress_current: completedUploadsCount,
                                    progress_total: expectedCount,
                                    progress_stage: 'UPLOADING',
                                }).eq('id', job.id).then(() => { }, () => { });
                            }
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
                    // Type assertion: TS can't track mutations from async closures (producer/consumer)
                    const resolvedError = pipelineError;
                    // If narrative page was unavailable and we have remaining candidate sources, rescue entire chapter!
                    if (resolvedError instanceof NarrativePageUnavailableError) {
                        if (candidateIdx < allSourceCandidates.length - 1) {
                            lastRescuedError = resolvedError.message;
                            this.logger.warn(`CROSS_PROVIDER_RESCUE: Narrative page failed on ${effectiveSource} for ch ${chapterNumber}. Rescuing entire chapter cleanly from ${allSourceCandidates[candidateIdx + 1].source}.`, {
                                failedPage: resolvedError.pageIndex + 1,
                                reason: resolvedError.message,
                                nextCandidateSource: allSourceCandidates[candidateIdx + 1].source,
                            });
                            continue;
                        }
                        else {
                            // No candidate left to rescue! If permanent 404 or multiple attempts, mark as permanent gap
                            if (/404|not found/i.test(resolvedError.message) || job.attempts >= 2) {
                                const gapReason = '404 em imagem narrativa da fonte sem fallback disponível';
                                this.logger.error(`PERMANENT_NARRATIVE_GAP: Chapter ${chapterNumber} of work ${workId} has permanent 404 on story pages with no viable fallback. Marking as terminal gap.`, {
                                    workId,
                                    chapterNumber,
                                    source: effectiveSource,
                                    failedPage: resolvedError.pageIndex + 1,
                                });
                                // Update chapter mapping
                                try {
                                    await this.supabase
                                        .from('importer_chapter_mappings')
                                        .update({
                                        status: 'FAILED',
                                        is_gap: true,
                                        last_error: gapReason,
                                        updated_at: new Date().toISOString(),
                                    })
                                        .eq('work_id', workId)
                                        .eq('chapter_number', chapterNumber);
                                }
                                catch { }
                                // Update chapter manifest
                                try {
                                    await this.supabase
                                        .from('importer_chapter_manifest')
                                        .update({
                                        status: 'UNRESOLVED_GAP',
                                        is_gap: true,
                                        gap_reason: 'PERMANENT_404_UNRESOLVED',
                                        last_error: gapReason,
                                        last_checked_at: new Date().toISOString(),
                                    })
                                        .eq('work_id', workId)
                                        .eq('chapter_number', chapterNumber);
                                }
                                catch { }
                                throw new Error(`[PERMANENT_404_UNRESOLVED] ${gapReason}: ${resolvedError.message}`);
                            }
                        }
                    }
                    throw resolvedError;
                }
                this.supabase.from('importer_queue').update({
                    progress_current: expectedCount,
                    progress_total: expectedCount,
                    progress_stage: 'VALIDATING',
                }).eq('id', job.id).then(() => { }, () => { });
                // SAFEGUARD 1: Strict integrity check
                for (let i = 0; i < expectedCount; i++) {
                    const p = storedPages[i];
                    if (p?.mediaId === '__SKIPPED_NON_CONTENT_PAGE__') {
                        continue;
                    }
                    if (!p || !p.mediaId) {
                        throw new Error(`Incomplete chapter import: page ${i + 1}/${expectedCount} failed storage`);
                    }
                }
                validPages = storedPages.filter((p) => Boolean(p && p.mediaId && p.mediaId !== '__SKIPPED_NON_CONTENT_PAGE__'));
                if (validPages.length === 0) {
                    throw new Error(`Chapter ${chapterNumber} contains 0 valid content pages`);
                }
                successfulExecution = true;
                break;
            }
            if (!successfulExecution) {
                throw new Error(`Failed to import chapter ${chapterNumber} across all candidates${lastRescuedError ? ': ' + lastRescuedError : ''}`);
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
                if (chErr) {
                    if (chErr.code === '23505' || chErr.message?.includes('violates unique constraint')) {
                        const { data: raceCh } = await this.supabase
                            .from('chapters')
                            .select('id')
                            .eq('work_id', workId)
                            .eq('number', chapterNumber)
                            .maybeSingle();
                        if (raceCh) {
                            chapterId = raceCh.id;
                        }
                        else {
                            throw chErr;
                        }
                    }
                    else {
                        throw chErr;
                    }
                }
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
                source: effectiveSource,
                sourceChapterId: effectiveSourceChapterId,
                workMappingId: effectiveWorkMappingId,
                pageCount: validPages.length,
                isPageProvider: true,
            });
            // Update queue row with progress and recovery info
            const queueProgressUpdate = {
                progress_current: validPages.length,
                progress_total: validPages.length,
                progress_stage: 'STAGED',
                source: effectiveSource,
            };
            if (lastRescuedError) {
                queueProgressUpdate.last_recovered_error = `CROSS_PROVIDER_RESCUE: Rescued cleanly via ${effectiveSource} (${lastRescuedError})`;
                queueProgressUpdate.recovered_at = new Date().toISOString();
                queueProgressUpdate.last_error = null;
            }
            await this.supabase.from('importer_queue').update(queueProgressUpdate).eq('id', job.id);
            // If source was switched via rescue, update the failed source chapter mapping
            if (effectiveSource !== initialSource) {
                await this.supabase
                    .from('importer_chapter_mappings')
                    .update({
                    is_page_provider: false,
                    last_error: `Replaced by ${effectiveSource} via CROSS_PROVIDER_RESCUE`,
                    updated_at: new Date().toISOString(),
                })
                    .eq('source', initialSource)
                    .eq('work_id', workId)
                    .eq('chapter_number', chapterNumber);
            }
            // SAFEGUARD 3: Try to publish immediately 1x via barrier. If blocked, release worker slot immediately!
            const pubResult = await this.publicationBarrier.tryPublish(workId, chKey.sortKey, chapterId);
            tDb = Date.now() - db0;
            // Record fine-grained chapter job metric asynchronously
            void this.recordJobMetric({
                workerId: this.config.WORKER_ID,
                source: effectiveSource,
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
                await this.publicationBarrier.handleDefiniteFailure(workId, chapterNumber, chKey.sortKey, effectiveSource);
            }
            // Record failed chapter metric asynchronously with sanitized error message
            void this.recordJobMetric({
                workerId: this.config.WORKER_ID,
                source: effectiveSource,
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
        if (!raw || raw.trim() === '' || raw === 'undefined')
            return '';
        return raw
            .replace(/Bearer\s+[A-Za-z0-9._~+/-]+=*/gi, 'Bearer [REDACTED]')
            .replace(/(token|access_token|refresh_token|secret|password|key)=([^\s&]+)/gi, '$1=[REDACTED]')
            .replace(/https?:\/\/[^:\/\s]+:[^@\/\s]+@/gi, 'https://[CREDENTIALS_REDACTED]@')
            .replace(/Cookie:\s*[^\r\n]+/gi, 'Cookie: [REDACTED]')
            .replace(/eyJ[a-zA-Z0-9_-]{10,}\.[a-zA-Z0-9_-]{10,}\.[a-zA-Z0-9_-]{10,}/g, '[JWT_REDACTED]')
            .replace(/:\s*undefined\b/gi, ': erro desconhecido')
            .replace(/\bundefined\b/gi, 'erro desconhecido')
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
        const isKuro = parsedUrl.host.includes('kuromangas.com');
        const referer = isKuro
            ? 'https://kuromangas.com/'
            : `${parsedUrl.origin}/`;
        let res = null;
        let fetchError = null;
        try {
            res = await fetch(url, {
                headers: {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
                    Referer: referer,
                },
                signal: AbortSignal.timeout(45_000),
            });
        }
        catch (err) {
            fetchError = err;
        }
        // If Cloudflare 403 or network error occurred and internal bridge is configured, fallback to bridge
        if ((fetchError || res?.status === 403) && this.config.NOX_STORAGE_BRIDGE_TOKEN && isKuro) {
            try {
                const bridgeUrl = `${(this.config.NOX_MANGA_URL || 'https://manga.project-nox-awerkori.workers.dev').replace(/\/$/, '')}/api/internal/importer/kuro-bridge`;
                const bridgeRes = await fetch(bridgeUrl, {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                        Authorization: `Bearer ${this.config.NOX_STORAGE_BRIDGE_TOKEN}`,
                    },
                    body: JSON.stringify({
                        url,
                        headers: {
                            Referer: referer,
                        },
                    }),
                    signal: AbortSignal.timeout(45_000),
                });
                if (bridgeRes.ok) {
                    this.rateLimiter.recordSuccess(parsedUrl.host);
                    const arrayBuf = await bridgeRes.arrayBuffer();
                    return new Uint8Array(arrayBuf);
                }
            }
            catch (bridgeErr) {
                this.logger.warn(`Failed image download via bridge for ${url}`, { error: bridgeErr?.message });
            }
        }
        if (fetchError) {
            throw fetchError;
        }
        if (!res || !res.ok) {
            const status = res?.status || 500;
            if (status === 429) {
                const retryAfter = res?.headers?.get('Retry-After');
                this.rateLimiter.handle429(parsedUrl.host, retryAfter);
            }
            throw new ProviderDownloadError(status, url, source, `Failed to download image from ${url}: HTTP ${status}`);
        }
        this.rateLimiter.recordSuccess(parsedUrl.host);
        const arrayBuf = await res.arrayBuffer();
        return new Uint8Array(arrayBuf);
    }
    async resolveDynamicCandidateFallbacks(workId, chapterNumber, excludeSource, payloadFallbacks = []) {
        const candidates = [];
        const seen = new Set();
        const addCandidate = (s, id, mapId) => {
            if (!s || !id || s === excludeSource)
                return;
            const key = `${s}:${id}`;
            if (!seen.has(key)) {
                seen.add(key);
                candidates.push({ source: s, sourceChapterId: id, mappingId: mapId });
            }
        };
        // 1. Initial payload fallbacks
        for (const fb of payloadFallbacks) {
            if (fb?.source && fb?.sourceChapterId) {
                addCandidate(fb.source, fb.sourceChapterId, fb.mappingId);
            }
        }
        // 2. Alternative mappings in importer_chapter_mappings
        try {
            const { data: chMappings } = await this.supabase
                .from('importer_chapter_mappings')
                .select('source, source_chapter_id, work_mapping_id')
                .eq('work_id', workId)
                .eq('chapter_number', chapterNumber)
                .neq('source', excludeSource);
            if (chMappings) {
                for (const m of chMappings) {
                    addCandidate(m.source, m.source_chapter_id, m.work_mapping_id);
                }
            }
        }
        catch { }
        // 3. Alternative available sources in importer_chapter_manifest
        try {
            const { data: manifestCh } = await this.supabase
                .from('importer_chapter_manifest')
                .select('available_sources')
                .eq('work_id', workId)
                .eq('chapter_number', chapterNumber)
                .maybeSingle();
            if (manifestCh?.available_sources && Array.isArray(manifestCh.available_sources)) {
                for (const s of manifestCh.available_sources) {
                    if (s?.source && s?.source_chapter_id) {
                        addCandidate(s.source, s.source_chapter_id);
                    }
                }
            }
        }
        catch { }
        // 4. Discover active sources in importer_work_mappings not yet in candidates
        try {
            const { data: workMappings } = await this.supabase
                .from('importer_work_mappings')
                .select('id, source, source_work_id')
                .eq('work_id', workId)
                .neq('source', excludeSource)
                .neq('sync_status', 'UNMATCHED');
            if (workMappings) {
                for (const wm of workMappings) {
                    if (candidates.some((c) => c.source === wm.source))
                        continue;
                    const { data: altSrcCheck } = await this.supabase
                        .from('importer_sources')
                        .select('status, enabled')
                        .eq('id', wm.source)
                        .maybeSingle();
                    if (altSrcCheck && (!altSrcCheck.enabled || altSrcCheck.status === 'UPSTREAM_BLOCKED' || altSrcCheck.status === 'DISABLED' || altSrcCheck.status === 'PAUSED')) {
                        continue;
                    }
                    const altAdapter = this.registry.get(wm.source);
                    if (altAdapter && typeof altAdapter.fetchChapters === 'function') {
                        try {
                            const altChapters = await altAdapter.fetchChapters(wm.source_work_id);
                            const matched = altChapters.find((c) => c.number === chapterNumber);
                            if (matched && matched.sourceChapterId) {
                                addCandidate(wm.source, matched.sourceChapterId, wm.id);
                            }
                        }
                        catch { }
                    }
                }
            }
        }
        catch { }
        // 5. Filter candidates against operational sources (exclude disabled, paused, or cooling down)
        const healthyCandidates = [];
        for (const c of candidates) {
            try {
                const { data: srcCheck } = await this.supabase
                    .from('importer_sources')
                    .select('status, enabled, cooldown_until')
                    .eq('id', c.source)
                    .maybeSingle();
                if (srcCheck) {
                    if (!srcCheck.enabled || srcCheck.status === 'PAUSED' || srcCheck.status === 'DISABLED' || srcCheck.status === 'UPSTREAM_BLOCKED') {
                        continue;
                    }
                    if (srcCheck.status === 'COOLDOWN') {
                        const cd = srcCheck.cooldown_until ? new Date(srcCheck.cooldown_until).getTime() : 0;
                        if (Date.now() < cd)
                            continue;
                    }
                }
                healthyCandidates.push(c);
            }
            catch {
                healthyCandidates.push(c);
            }
        }
        return healthyCandidates;
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
