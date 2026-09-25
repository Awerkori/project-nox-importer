import { callProvider } from './retry-policy.js';
import type { SupabaseClient } from '@supabase/supabase-js';
import { getYugabytePool, recoverStalledLeasesDirect } from '../db/yugabyte-direct.js';
import { SourceRegistry } from '../sources/registry.js';
import { StorageProvider } from '../storage/provider.js';
import { ImporterQueue, QueueJob, TaskType } from './queue.js';
import { DeduplicationEngine, CandidateWork, computeCanonicalChapterKey, ADULT_SOURCES } from './deduplication.js';
import { CheckpointManager } from './checkpoint.js';
import { HostRateLimiter } from './rate-limiter.js';
import { processAndStoreMedia } from '../storage/media.js';
import { Logger } from './logger.js';
import { Config } from '../config.js';
import { withSourceChapterPermits } from './concurrency.js';
import { readImageBody } from './bounded-body.js';
import { diagnostics } from './diagnostics.js';
import { AdaptiveAutotuner, AsyncSemaphore, BufferReservation, SOURCE_CONCURRENCY_LIMITS } from './concurrency.js';
import { PublicationBarrier } from './publication.js';
import { NoxWorkerStorageError } from '../storage/worker.js';
import { RetryPolicy, ProviderDownloadError, InvalidMediaError } from './retry-policy.js';
import { ExistingWorksReconciler } from './reconciliation.js';
import { CloudflareClassifier, CloudflareClassification } from './cloudflare-classifier.js';
import { SourceCircuitBreaker } from './circuit-breaker.js';
import { SharedNetworkDetector } from './shared-network-detector.js';
import { SourceAdmissionGate } from './source-admission-gate.js';
import { PublicationSafetyBarrier } from './publication-safety-barrier.js';
import { ProtectiveSentinel } from './protective-sentinel.js';
import { telemetryCollector } from './telemetry-collector.js';
import { WorkAffinityScheduler, SchedulerStateStore, AdmissionController } from './scheduler/index.js';
import { performance } from 'node:perf_hooks';

export { computeCanonicalChapterKey };

export type InternalLivenessState = 'HEALTHY_IDLE' | 'HEALTHY_WORKING' | 'BACKPRESSURED' | 'STALLED';
export type ExternalLivenessState = InternalLivenessState | 'DEAD';

export function computeInternalLivenessState(params: {
  isStopActive: boolean;
  rssMb: number;
  rssTripwireMb?: number;
  eligibleCount: number;
  importingCount: number;
  activeJobsCount: number;
  minutesSinceProgress: number;
}): InternalLivenessState {
  const tripwire = params.rssTripwireMb ?? 380;
  if (params.isStopActive || params.rssMb >= tripwire) {
    return 'BACKPRESSURED';
  }
  if ((params.eligibleCount > 0 || params.importingCount > 0 || params.activeJobsCount > 0) && params.minutesSinceProgress >= 15) {
    return 'STALLED';
  }
  if (params.eligibleCount === 0 && params.importingCount === 0 && params.activeJobsCount === 0) {
    return 'HEALTHY_IDLE';
  }
  return 'HEALTHY_WORKING';
}

export function computeExternalLivenessState(params: {
  lastHeartbeatTimestamp: number;
  now?: number;
  heartbeatTimeoutMs?: number;
  internalState: InternalLivenessState;
}): ExternalLivenessState {
  const now = params.now ?? Date.now();
  const timeout = params.heartbeatTimeoutMs ?? (3 * 60 * 1000);
  if (now - params.lastHeartbeatTimestamp > timeout) {
    return 'DEAD';
  }
  return params.internalState;
}

export class JobCancelledByStaffError extends Error {
  constructor(public readonly jobId: string, message: string = 'Job cancelado pela Staff no checkpoint seguro') {
    super(message);
    this.name = 'JobCancelledByStaffError';
  }
}

export type PageSemanticType = 'CONTENT_PAGE' | 'CREDIT_PAGE' | 'PROMO_PAGE' | 'RECRUITMENT_PAGE' | 'WARNING_PAGE';

export function classifyPageUrl(url: string, index: number, total: number): PageSemanticType {
  const clean = decodeURIComponent(url).toLowerCase();
  if (/credito|crédito|credit|credits/i.test(clean)) return 'CREDIT_PAGE';
  if (/recrut|recrutamento|recruit/i.test(clean)) return 'RECRUITMENT_PAGE';
  if (/aviso|warning|notice/i.test(clean)) return 'WARNING_PAGE';
  if (/fanservice|apoie|doacao|doação|donate|discord|parceria/i.test(clean)) return 'PROMO_PAGE';
  if (index === 0 && (/capa|cover|front|\(0+\)|\b000?\b|_00\./i.test(clean) || /\[.+\]\s*.+\(0\)/i.test(clean))) {
    return 'CREDIT_PAGE';
  }
  if ((index === 0 || index === total - 1) && /scan|staff|discord|padrim|apoia/i.test(clean)) {
    return 'PROMO_PAGE';
  }
  return 'CONTENT_PAGE';
}

export class NarrativePageUnavailableError extends Error {
  constructor(
    public source: string,
    public pageIndex: number,
    public totalPages: number,
    public originalError: string
  ) {
    super(`Narrative story page ${pageIndex + 1}/${totalPages} unavailable on source ${source}: ${originalError}`);
    this.name = 'NarrativePageUnavailableError';
  }
}


export class ImporterEngine {
  private logger = new Logger('Engine');
  private queue: ImporterQueue;
  private deduplication: DeduplicationEngine;
  private checkpoints: CheckpointManager;
  private autotuner: AdaptiveAutotuner;
  private publicationBarrier: PublicationBarrier;
  private safetyBarrier: PublicationSafetyBarrier;
  private protectiveSentinel: ProtectiveSentinel;
  private reconciler: ExistingWorksReconciler;
  public schedulerStateStore: SchedulerStateStore;
  public admissionController: AdmissionController;
  public scheduler: WorkAffinityScheduler;
  private circuitBreaker = new SourceCircuitBreaker();
  private sharedNetworkDetector = new SharedNetworkDetector();
  private admissionGate = new SourceAdmissionGate();
  private isRunning = false;
  private stopSignal = false;
  private abortController = new AbortController();
  private chapterClaimMutex = new AsyncSemaphore(1, 'chapter_claim_mutex');
  private activeSourcesCache: { sources: string[]; cachedAt: number } = { sources: [], cachedAt: 0 };
  private knownCoveredWorks = new Set<string>();
  private lastProgressTimestamp: number = Date.now();
  private lastAutoRecoveryTimestamp: number = 0;
  private lastNewWorkAutoRecoveryTimestamp: number = 0;

  // Actual retained image bytes; bounded globally by page permits and per-image size.
  public static activeBufferedBytes = 0;

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
    this.publicationBarrier = new PublicationBarrier(supabase);
    this.safetyBarrier = new PublicationSafetyBarrier(supabase);
    this.protectiveSentinel = new ProtectiveSentinel(supabase, undefined, config.NOX_MANGA_URL);
    this.reconciler = new ExistingWorksReconciler(supabase, this.queue, registry, config.NOX_MANGA_URL);
    this.schedulerStateStore = new SchedulerStateStore();
    let dbPool: any = (supabase as any)?.getPool?.() || (supabase as any)?.pool || (supabase as any)?.db;
    if (dbPool && typeof dbPool.connect !== 'function') {
      const dbObj = dbPool;
      dbPool = {
        query: (text: string, params?: any[]) => dbObj.query(text, params),
        connect: async () => ({
          query: (text: string, params?: any[]) => dbObj.query(text, params),
          release: () => {},
        }),
      };
    }
    this.admissionController = new AdmissionController(this.schedulerStateStore, this.protectiveSentinel, dbPool);
    this.scheduler = new WorkAffinityScheduler(this.schedulerStateStore, this.admissionController, this.protectiveSentinel, dbPool);
    this.publicationBarrier.onPublished = (isFreshRelease: boolean) => {
      this.scheduler.recordPublication(isFreshRelease);
    };
    const requestedMax = Math.min(
      config.MAX_CONCURRENT_CHAPTERS || 8,
      config.TESTED_CONCURRENCY_CEILING || 18
    );
    this.autotuner = new AdaptiveAutotuner({
      initialConcurrency: Math.min(requestedMax, 8),
      maxConcurrency: requestedMax,
      maxRssMb: 350,
      maxHeapMb: 200,
      maxExternalAndBuffersMb: 100,
      maxEventLoopLagMs: 250,
      requiredStableCycles: 3,
      cooldownPeriodMs: 15 * 1000,
      rssSoftLimitMb: 330,
      rssHardLimitMb: 380,
      rssEmergencyLimitMb: 410,
      maxBufferedBytes: 64 * 1024 * 1024,
    });
  }

  getAutotuner(): AdaptiveAutotuner {
    return this.autotuner;
  }

  getSafetyBarrier(): PublicationSafetyBarrier {
    return this.safetyBarrier;
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

    // 1b. Initialize WorkAffinityScheduler & AdmissionController
    await this.scheduler.initialize();

    // 2. Launch background autotuner telemetry loop (every 30s)
    this.runAutotunerLoop();

    // 3. Launch background discovery scheduler loop
    this.runDiscoveryLoop();

    // 3b. Launch continuous catalog backfill loop (expands catalog across pages 1..N)
    this.runCatalogBackfillLoop();

    // 4. Launch background publication sweep loop (every 10s)
    this.runPublicationSweepLoop();

    // 5. Launch background lease recovery loop (every 60s)
    this.runLeaseRecoveryLoop();

    // 5b. Launch background cooldown source auto-probe loop (every 30s)
    this.runSourceCooldownProbeLoop();

    // 5c. Launch background controlled redundant job cleanup loop (every 30s)
    this.runRedundantJobCleanupLoop();

    // 6. Launch periodic existing works reconciliation loop (every 15 min)
    this.runReconciliationLoop();

    // 7. Launch background upstream provider health check loop (every 60s)
    this.runUpstreamHealthLoop();

    // 8. Launch Pre-SLA Sentinel watchdog loop (every 15s)
    this.protectiveSentinel.startWatchdogLoop();

    // 9. Launch Liveness Watchdog and Auto-Recovery loop (every 30s)
    this.runLivenessWatchdogLoop();

    // A bounded shared runner pool claims by queue priority. Per-source semaphores
    // still enforce source limits, without hundreds of idle claimers ahead of fresh jobs.
    const activeWorkers: Promise<void>[] = [];
    // Dedicated discovery worker lane to guarantee DISCOVER_WORKS is NEVER starved
    activeWorkers.push(this.runDiscoveryWorker());
    // Dedicated sync worker lane to guarantee SYNC_WORK runs steadily alongside discovery
    activeWorkers.push(this.runCatalogSyncWorker());
    // General worker to process any unassigned or balancing jobs
    activeWorkers.push(this.runGeneralWorker());

    // Wait until all workers finish upon stop signal
    await Promise.all(activeWorkers);

    this.admissionController.stop();
    this.isRunning = false;
    this.logger.info('Importer Engine stopped gracefully');
  }

  async runStartupRecovery(): Promise<void> {
    try {
      this.logger.info('Starting generic lease recovery for stalled jobs...');
      let { recovered, failed } = await this.queue.recoverExpiredLeases();
      if (recovered > 0 || failed > 0) {
        this.logger.warn(
          `Startup recovery processed stalled jobs: ${recovered} requeued to QUEUED, ${failed} marked as FAILED`,
          { recovered, failed }
        );
      } else {
        this.logger.info('Startup recovery check passed: no stalled jobs detected');
      }

      // 2. Sweep any staged publications left over from previous instance
      await this.publicationBarrier.sweepStagedPublications();

      // 3. Recover stalled 502 retries with long delays from previous exponential backoff policy
      await this.recoverStalled502Retries();
    } catch (err: any) {
      this.logger.warn('Error during startup recovery check', { error: err?.message });
    }
  }

  /**
   * Recalculates next_run_at for legacy retry jobs that were given long exponential backoffs (16-32 min)
   * due to transient 502/503 errors, rescheduling them for quick execution (10-35s).
   */
  async recoverStalled502Retries(): Promise<number> {
    try {
      const now = new Date();
      let query: any = this.supabase
        .from('importer_queue')
        .select('id, attempts, next_run_at, last_error, source')
        .eq('status', 'RETRY');

      if (typeof query.is === 'function') {
        query = query.is('locked_by', null);
      }

      let { data: retries, error } = await query;

      if (error || !retries || retries.length === 0) return 0;

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
    } catch (err: any) {
      this.logger.warn('Error during recoverStalled502Retries', { error: err?.message });
      return 0;
    }
  }

  stop(): void {
    this.stopSignal = true;
    this.abortController.abort();
  }

  private discoveryAllowedCache = false;
  private discoveryAllowedCachedAt = 0;

  /**
   * Checks whether catalog discovery and backfill are globally enabled in system settings.
   * Cached for 5s to eliminate unnecessary database calls on tight loops.
   */
  async isDiscoveryAllowed(): Promise<boolean> {
    const now = Date.now();
    if (now - this.discoveryAllowedCachedAt < 5_000) {
      return this.discoveryAllowedCache;
    }
    try {
      const { data } = await this.supabase
        .from('settings')
        .select('value')
        .eq('key', 'catalog_discovery_enabled')
        .maybeSingle();

      if (!data || !data.value) {
        this.discoveryAllowedCache = false;
        this.discoveryAllowedCachedAt = now;
        return false;
      }
      const val = String(data.value).trim().toUpperCase();
      this.discoveryAllowedCache = (val === 'ENABLED');
      this.discoveryAllowedCachedAt = now;
      return this.discoveryAllowedCache;
    } catch {
      this.discoveryAllowedCache = false;
      return false;
    }
  }

  /**
   * Periodic discovery scheduler running in the background
   */
  private async runDiscoveryLoop(): Promise<void> {
    while (!this.stopSignal) {
      try {
        const allowed = await this.isDiscoveryAllowed();
        if (allowed) {
          await this.scheduleSources();
        } else {
          this.logger.debug('Catalog discovery is globally DISABLED. Discovery loop idle.');
        }
      } catch (err: any) {
        this.logger.error('Error during source discovery scheduling', { error: err?.message });
      }

      // Check discovery every 30 seconds
      await this.sleep(30_000);
    }
  }

  /**
   * Continuous catalog backfill loop (expands catalog from ~100 to thousands of works).
   * Traverses pages 1..N of active sources using persistent checkpoints.
   */
  private async runCatalogBackfillLoop(): Promise<void> {
    await this.sleep(5_000); // 5s initial warmup

    while (!this.stopSignal) {
      try {
        await this.scheduleCatalogBackfill();
      } catch (err: any) {
        this.logger.error('Error during catalog backfill scheduling', { error: err?.message });
      }

      // Check backfill opportunities every 20 seconds
      await this.sleep(20_000);
    }
  }

  private async scheduleCatalogBackfill(): Promise<void> {
    const isDiscoveryAllowed = await this.isDiscoveryAllowed();
    if (!isDiscoveryAllowed) return;

    const isAllowed = await this.safetyBarrier.isBackfillAllowed();
    if (!isAllowed) return;

    let { data: sources, error } = await this.supabase
      .from('importer_sources')
      .select('*');

    if (error || !sources) return;

    for (const src of sources) {
      if (!src.enabled || (src as any).catalog_discovery_enabled === false || (src as any).catalog_discovery_enabled === 0 || src.status !== 'ACTIVE') continue;

      const checkpoint = await this.checkpoints.getCheckpoint(src.id);
      // If completed pass, allow re-scan only after 12 hours
      if (checkpoint?.metadata?.catalog_completed) {
        const completedAt = checkpoint.metadata.catalog_completed_at
          ? new Date(checkpoint.metadata.catalog_completed_at).getTime()
          : 0;
        const twelveHoursMs = 12 * 60 * 60 * 1000;
        if (Date.now() - completedAt < twelveHoursMs) {
          continue;
        }
      }

      // Backpressure check: throttle backfill if there are already 10+ discovery/sync jobs queued for this source
      try {
        let { data: activeJobs, error: activeJobsError } = await this.supabase
          .from('importer_queue')
          .select('id')
          .eq('source', src.id)
          .in('task_type', ['DISCOVER_WORKS', 'SYNC_WORK'])
          .in('status', ['QUEUED', 'IMPORTING'])
          .limit(10);

        if (activeJobsError || (activeJobs?.length ?? 0) >= 10) {
          continue;
        }
      } catch {}

      const currentCursor = checkpoint?.cursor_value || null;
      const dedupeKey = `${src.id}:backfill:${currentCursor || 'page1'}:${Math.floor(Date.now() / 60000)}`;

      await this.queue.enqueue(
        'DISCOVER_WORKS',
        src.id,
        dedupeKey,
        {
          workTitle: `Varredura Contínua de Catálogo (${src.name || src.id})`,
          mode: 'bootstrap',
          cursor: currentCursor,
        },
        50
      );
    }
  }

  /**
   * Periodic publication sweep loop (every 10s) to unblock STAGED chapters
   */
  private async runPublicationSweepLoop(): Promise<void> {
    while (!this.stopSignal) {
      await this.sleep(10_000);
      if (this.stopSignal) break;

      try {
        const count = await this.publicationBarrier.sweepStagedPublications();
        if (count > 0) {
          this.lastProgressTimestamp = Date.now();
        }
      } catch (err: any) {
        this.logger.error('Error during publication sweep loop', { error: err?.message });
      }
    }
  }

  /**
   * Periodic lease recovery loop (every 60s) to rescue stalled jobs from crashed instances
   */
  private async runLeaseRecoveryLoop(): Promise<void> {
    while (!this.stopSignal) {
      await this.sleep(60_000);
      if (this.stopSignal) break;

      try {
        await this.queue.recoverExpiredLeases();
      } catch (err: any) {
        this.logger.warn('Error during periodic lease recovery loop', { error: err?.message });
      }
    }
  }

  /**
   * Periodic auto-probe and auto-healing loop for sources in COOLDOWN / DEGRADED (runs every 30s).
   * Restores expired cooldowns immediately through production admission probe.
   */
  private async runSourceCooldownProbeLoop(): Promise<void> {
    await this.sleep(3_000);
    while (!this.stopSignal) {
      if (this.stopSignal) break;

      try {
        const { data: cooldownSources, error } = await this.supabase
          .from('importer_sources')
          .select('id, name, status, base_url, cooldown_until, blocked_reason')
          .in('status', ['COOLDOWN', 'DEGRADED', 'PROBING']);

        if (error || !cooldownSources || cooldownSources.length === 0) continue;

        const now = Date.now();
        for (const src of cooldownSources) {
          if (this.stopSignal) break;

          const cooldownUntil = src.cooldown_until ? new Date(src.cooldown_until).getTime() : 0;
          if (src.status === 'COOLDOWN' && now < cooldownUntil) {
            // Still within cooldown period, skip until expiry
            continue;
          }

          // Cooldown expired or DEGRADED: probe for auto-healing to ACTIVE
          await this.probeSourceHealth(src);
        }
      } catch (err: any) {
        this.logger.warn('Error during source cooldown auto-probe loop', { error: err?.message });
      }
      await this.sleep(30_000);
    }
  }

  /**
   * Periodic atomic background cleanup of redundant queue jobs (runs every 30s).
   * Safely marks queued/retrying jobs as COMPLETED with CANONICAL_ALREADY_SATISFIED
   * if their canonical chapter has already been published in chapters table.
   * Full worker slots wasted = 0.
   */
  private async runRedundantJobCleanupLoop(): Promise<void> {
    await this.sleep(5_000);
    while (!this.stopSignal) {
      if (this.stopSignal) break;

      try {
        if (typeof (this.scheduler as any).runControlledRedundantJobCleanup === 'function') {
          await (this.scheduler as any).runControlledRedundantJobCleanup(200);
        }
      } catch (err: any) {
        this.logger.warn('Error during redundant job cleanup loop', { error: err?.message });
      }
      await this.sleep(30_000);
    }
  }

  /**
   * Periodic existing works reconciliation loop
   * Handles high-priority staff requests, on-demand admin reconciliations, and periodic catalog health batches.
   */
  private async runReconciliationLoop(): Promise<void> {
    await this.sleep(3000); // Quick startup delay

    let lastFullBatch = 0;

    while (!this.stopSignal) {
      const now = Date.now();

      try {
        // 1. Process active Prioridade Absoluta staff requests immediately
        const staffQuery = this.supabase.from('importer_staff_requests');
        if (staffQuery && typeof staffQuery.select === 'function') {
          let { data: activeStaff } = await staffQuery
            .select('work_id')
            .in('status', ['QUEUED', 'IMPORTING', 'RETRYING']);

          for (const req of activeStaff || []) {
            if (this.stopSignal) break;
            try {
              this.logger.info(`Running cross-provider reconciliation for Prioridade Absoluta work ${req.work_id}`);
              await this.reconciler.reconcileWorkManifest(req.work_id, { priority: 100 });
            } catch (err: any) {
              this.logger.warn(`Failed reconciling Prioridade Absoluta work ${req.work_id}`, { error: err?.message });
            }
          }
        }

        // 2. Process works explicitly requested for reconciliation from Admin UI
        const healthQuery = this.supabase.from('importer_work_health');
        if (healthQuery && typeof healthQuery.select === 'function') {
          let { data: requestedWorks } = await healthQuery
            .select('work_id')
            .eq('health_status', 'RECONCILING')
            .limit(5);

          for (const req of requestedWorks || []) {
            if (this.stopSignal) break;
            try {
              this.logger.info(`Running requested reconciliation for work ${req.work_id}`);
              await this.reconciler.reconcileWorkManifest(req.work_id);
            } catch (err: any) {
              this.logger.warn(`Failed reconciling requested work ${req.work_id}`, { error: err?.message });
            }
          }
        }

        // 3. Periodic micro-batch every 3-4 minutes with randomized jitter (0-60s)
        const jitterMs = Math.floor(Math.random() * 60_000);
        const intervalMs = (3 * 60 * 1000) + jitterMs;
        if (now - lastFullBatch >= intervalMs) {
          lastFullBatch = now;
          this.logger.info('Starting periodic existing works reconciliation micro-batch (3 works)...');
          await this.reconciler.reconcileExistingWorks(3);
        }
      } catch (err: any) {
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
  private async runUpstreamHealthLoop(): Promise<void> {
    await this.sleep(15_000);

    while (!this.stopSignal) {
      try {
        await this.checkBlockedSourcesHealth();
      } catch (err: any) {
        this.logger.error('Error during upstream sources health check loop', { error: err?.message });
      }

      await this.sleep(60_000);
    }
  }

  /**
   * Periodic Liveness Watchdog and Auto-Recovery Loop (Section 7, 8 & 16)
   * Evaluates pipeline liveness against 5 distinct operational states:
   * HEALTHY_IDLE, HEALTHY_WORKING, BACKPRESSURED, STALLED, DEAD.
   * Emits operational WARNING (>=15m) and CRITICAL (>=30m) alerts and runs auto-recovery.
   */
  private async runLivenessWatchdogLoop(): Promise<void> {
    await this.sleep(30_000);

    while (!this.stopSignal) {
      if (this.stopSignal) break;

      try {
        const now = Date.now();
        const mem = diagnostics.getMemorySnapshot();
        const activeJobsCount = diagnostics.getActiveJobsCount();
        const isStopActive = await this.protectiveSentinel.isProtectiveStopActive();

        // Query queue for eligible and importing work counts
        const pool = getYugabytePool();
        const qRes = await pool.query(
          `SELECT 
             COUNT(CASE WHEN status IN ('QUEUED', 'RETRY') AND (next_run_at IS NULL OR next_run_at <= NOW()) THEN 1 END) as eligible_cnt,
             COUNT(CASE WHEN status = 'IMPORTING' THEN 1 END) as importing_cnt,
             COUNT(CASE WHEN status = 'IMPORTING' AND (
               lease_expires_at <= NOW()
               OR (lease_expires_at IS NULL AND (locked_at <= NOW() - INTERVAL '5 minutes' OR updated_at <= NOW() - INTERVAL '5 minutes'))
               OR (locked_at <= NOW() - INTERVAL '15 minutes' AND updated_at <= NOW() - INTERVAL '15 minutes')
             ) THEN 1 END) as stale_importing_cnt
           FROM importer_queue WHERE task_type = 'IMPORT_CHAPTER'`
        );
        const eligibleCount = parseInt(qRes.rows[0]?.eligible_cnt || '0', 10);
        const importingCount = parseInt(qRes.rows[0]?.importing_cnt || '0', 10);
        const staleImportingCount = parseInt(qRes.rows[0]?.stale_importing_cnt || '0', 10);

        const timeSinceProgressMs = now - this.lastProgressTimestamp;
        const minutesSinceProgress = Math.floor(timeSinceProgressMs / 60_000);

        // Check if there are stale IMPORTING jobs with expired or unrenewed leases
        if (staleImportingCount > 0) {
          this.logger.warn(`⚠️ [Liveness] Detected ${staleImportingCount} stale IMPORTING job(s) with expired or unrenewed leases. Rescuing immediately.`);
          try {
            const { recoveredCount } = await recoverStalledLeasesDirect(0);
            if (recoveredCount > 0) {
              this.logger.info(`[Liveness Auto-Recovery] Requeued ${recoveredCount} stale IMPORTING jobs back to QUEUED`);
            }
          } catch (e: any) {
            this.logger.warn('Direct stale lease recovery failed', { error: e?.message });
          }
        }

        const livenessState = computeInternalLivenessState({
          isStopActive,
          rssMb: mem.rssMb,
          eligibleCount,
          importingCount,
          activeJobsCount,
          minutesSinceProgress,
        });

        // New Work Pipeline Telemetry & Liveness
        let lastNewWorkCreatedAt: Date | null = null;
        let lastNewWorkAdmittedAt: Date | null = null;
        let waitingAdmissionCount = 0;
        try {
          const nwRes = await pool.query(`
            SELECT 
              (SELECT MAX(created_at) FROM works) as last_created,
              (SELECT MAX(updated_at) FROM importer_work_mappings WHERE sync_status = 'ACTIVE') as last_admitted,
              (SELECT COUNT(*) FROM importer_work_mappings WHERE sync_status = 'WAITING_ADMISSION') as waiting_admission_cnt
          `);
          if (nwRes.rows[0]) {
            lastNewWorkCreatedAt = nwRes.rows[0].last_created ? new Date(nwRes.rows[0].last_created) : null;
            lastNewWorkAdmittedAt = nwRes.rows[0].last_admitted ? new Date(nwRes.rows[0].last_admitted) : null;
            waitingAdmissionCount = parseInt(nwRes.rows[0].waiting_admission_cnt || '0', 10);
          }
        } catch (nwErr: any) {
          this.logger.warn('Failed to query new work pipeline metrics', { error: nwErr?.message });
        }

        const latestNewWorkActivityMs = Math.max(
          lastNewWorkCreatedAt?.getTime() || 0,
          lastNewWorkAdmittedAt?.getTime() || 0
        );
        const minutesSinceLastNewWork = latestNewWorkActivityMs > 0
          ? Math.floor((now - latestNewWorkActivityMs) / 60_000)
          : 999;

        let newWorkPipeline: 'NEW_WORK_PIPELINE_HEALTHY' | 'NEW_WORK_PIPELINE_IDLE' | 'NEW_WORK_PIPELINE_STALLED' | 'NEW_WORK_PIPELINE_BACKPRESSURED' = 'NEW_WORK_PIPELINE_HEALTHY';

        if (isStopActive || mem.rssMb > 450) {
          newWorkPipeline = 'NEW_WORK_PIPELINE_BACKPRESSURED';
        } else if (waitingAdmissionCount === 0) {
          newWorkPipeline = 'NEW_WORK_PIPELINE_IDLE';
        } else if (minutesSinceLastNewWork >= 30) {
          newWorkPipeline = 'NEW_WORK_PIPELINE_STALLED';
        } else {
          newWorkPipeline = 'NEW_WORK_PIPELINE_HEALTHY';
        }

        // Write atomic heartbeat to database settings table for external watchdog / supervisor monitoring
        try {
          const hbPayload = JSON.stringify({
            state: livenessState,
            chapterPipeline: livenessState === 'STALLED' ? 'STALLED' : 'WORKING',
            newWorkPipeline,
            minutesSinceLastNewWork,
            waitingAdmissionCount,
            lastNewWorkCreatedAt: lastNewWorkCreatedAt ? lastNewWorkCreatedAt.toISOString() : null,
            lastNewWorkAdmittedAt: lastNewWorkAdmittedAt ? lastNewWorkAdmittedAt.toISOString() : null,
            timestamp: new Date().toISOString(),
            pid: process.pid,
            workerId: this.config.WORKER_ID,
            rssMb: mem.rssMb,
            eligibleCount,
            importingCount,
            staleImportingCount,
            minutesSinceProgress,
          });
          await pool.query(
            `INSERT INTO settings (key, value)
             VALUES ('importer_heartbeat', $1)
             ON CONFLICT (key) DO UPDATE SET value = $1`,
            [hbPayload]
          );
        } catch (hbErr: any) {
          this.logger.warn('Failed to record importer_heartbeat to settings', { error: hbErr?.message });
        }

        // Section 6 Auto-Recovery for New Work Pipeline Stalls
        if (newWorkPipeline === 'NEW_WORK_PIPELINE_STALLED') {
          const timeSinceLastNewWorkRecovery = now - this.lastNewWorkAutoRecoveryTimestamp;
          if (timeSinceLastNewWorkRecovery >= 3 * 60 * 1000) {
            this.lastNewWorkAutoRecoveryTimestamp = now;
            this.logger.warn(`⚠️ [NEW_WORK_PIPELINE_STALLED] No new work created or admitted for ${minutesSinceLastNewWork} minutes with ${waitingAdmissionCount} candidates waiting. Triggering auto-recovery...`);
            try {
              await this.admissionController.runAdmissionCycle();
            } catch (admErr: any) {
              this.logger.warn('New work auto-recovery admission cycle failed', { error: admErr?.message });
            }
          }
        }

        if (livenessState === 'STALLED') {
          if (minutesSinceProgress >= 30) {
            this.logger.error(
              `🚨 [LIVENESS CRITICAL] Importer STALLED: ${eligibleCount} eligible jobs, ${importingCount} in-flight jobs, but NO progress for ${minutesSinceProgress} minutes! Triggering auto-recovery...`
            );
          } else {
            this.logger.warn(
              `⚠️ [LIVENESS WARNING] Importer STALLED: ${eligibleCount} eligible jobs, ${importingCount} in-flight jobs, but NO progress for ${minutesSinceProgress} minutes. Triggering auto-recovery...`
            );
          }

          // Throttle auto-recovery actions to run at most once every 3 minutes while still STALLED
          const timeSinceLastAutoRecovery = now - this.lastAutoRecoveryTimestamp;
          if (timeSinceLastAutoRecovery >= 3 * 60 * 1000) {
            this.lastAutoRecoveryTimestamp = now;

            // Auto-recovery actions:
            // 1. Recover stale leases
            try {
              const { recoveredCount } = await recoverStalledLeasesDirect(0);
              if (recoveredCount > 0) {
                this.logger.info(`[Liveness Auto-Recovery] Direct YSQL recovered ${recoveredCount} stalled jobs back to QUEUED.`);
              } else {
                const { recovered } = await this.queue.recoverExpiredLeases();
                if (recovered > 0) {
                  this.logger.info(`[Liveness Auto-Recovery] Queue recovered ${recovered} stalled jobs back to QUEUED.`);
                }
              }
            } catch (e: any) {
              this.logger.warn('Liveness lease recovery failed', { error: e?.message });
            }

            // 2. Clear old hung jobs in diagnostics (>15m)
            const allJobs = diagnostics.getActiveJobs();
            for (const j of allJobs) {
              if (now - j.startedAt > 15 * 60 * 1000) {
                this.logger.warn(`[Liveness Auto-Recovery] Evicting orphaned in-memory job ${j.jobId} running for ${Math.floor((now - j.startedAt)/1000)}s`);
                diagnostics.unregisterJob(j.jobId);
              }
            }

            // 3. Clear protective stop if site is healthy
            if (isStopActive) {
              await this.protectiveSentinel.evaluateAutoResume();
            }

            // 4. Re-run admission cycle to admit eligible works
            try {
              await this.admissionController.runAdmissionCycle();
            } catch (e: any) {
              this.logger.warn('Liveness admission replenishment failed', { error: e?.message });
            }
          }
          // Note: Notice that this.lastProgressTimestamp is NEVER reset artificially.
          // It only updates when REAL progress occurs (chapter published/staged or job completed).
        } else {
          this.logger.info(
            `[Liveness Watchdog] State: ${livenessState} | Eligible: ${eligibleCount} | InFlight: ${importingCount} | LastProgress: ${minutesSinceProgress}m ago`
          );
        }
      } catch (err: any) {
        this.logger.warn('Error during liveness watchdog cycle', { error: err?.message });
      }

      await this.sleep(30_000);
    }
  }

  public async checkBlockedSourcesHealth(): Promise<void> {
    let { data: blockedSources, error } = await this.supabase
      .from('importer_sources')
      .select('id, name, status, base_url, blocked_reason, blocked_details, cooldown_until')
      .in('status', ['UPSTREAM_BLOCKED', 'RECOVERING', 'PROBING', 'DEGRADED', 'COOLDOWN']);

    if (error || !blockedSources || blockedSources.length === 0) return;

    for (const src of blockedSources) {
      if (this.stopSignal) break;
      await this.probeSourceHealth(src);
    }
  }

  public async probeSourceHealth(src: {
    id: string;
    name: string;
    status: string;
    base_url?: string;
    blocked_reason?: string | null;
    blocked_details?: any;
    cooldown_until?: string | null;
  }): Promise<void> {
    const nowIso = new Date().toISOString();
    const adapter = this.registry.get(src.id);
    if (!adapter) return;

    // If shared network incident is active, suppress probe storms
    if (this.sharedNetworkDetector.isSharedBlockActive()) {
      this.logger.warn(`Shared network block is currently active on datacenter network. Suppressing probe for ${src.id}.`);
      return;
    }

    try {
      this.logger.info(`Executing Production Admission Probe for source: ${src.id}...`);
      await this.supabase
        .from('importer_sources')
        .update({
          status: 'PROBING',
          last_health_check_at: nowIso,
          updated_at: nowIso,
        })
        .eq('id', src.id);

      const report = await this.admissionGate.executeProdProbe(adapter);

      if (report.overallStatus !== 'PASS') {
        const primaryReason: CloudflareClassification = report.classification || 'CLOUDFLARE_DATACENTER_BLOCK';
        this.logger.info(`Source ${src.id} failed production admission probe (${primaryReason}).`, { stages: report.stages });

        this.circuitBreaker.recordFailure(src.id, primaryReason);
        this.sharedNetworkDetector.recordBlockEvent({
          sourceId: src.id,
          classification: primaryReason,
          cfRay: report.cfRay,
        });

        if (src.status === 'COOLDOWN' || src.status === 'DEGRADED' || src.status === 'PROBING') {
          const { cooldownMs } = this.circuitBreaker.recordFailure(src.id, primaryReason);
          const nextCooldownIso = new Date(Date.now() + (cooldownMs || 60_000)).toISOString();
          await this.supabase
            .from('importer_sources')
            .update({
              status: 'COOLDOWN',
              cooldown_until: nextCooldownIso,
              blocked_reason: primaryReason,
              blocked_details: {
                stages: report.stages,
                cf_ray: report.cfRay,
                last_checked_at: nowIso,
              },
              last_health_check_at: nowIso,
              updated_at: nowIso,
            })
            .eq('id', src.id);
          return;
        }

        await this.supabase
          .from('importer_sources')
          .update({
            status: 'UPSTREAM_BLOCKED',
            blocked_reason: primaryReason,
            blocked_details: {
              message:
                'Cloudflare bloqueia o ambiente atual do Importer (DIScloud / OVH ASN 16276). Local/Mihon: funcional; DIScloud: HTTP 403.',
              local_status: 200,
              discloud_status: 403,
              last_checked_at: nowIso,
              stages: report.stages,
              cf_ray: report.cfRay,
            },
            last_health_check_at: nowIso,
            updated_at: nowIso,
          })
          .eq('id', src.id);
        return;
      }

      // Passed all 6 stages! Transition to ACTIVE
      this.logger.info(`Source ${src.id} passed all 6 stages of Admission Probe! Transitioning to ACTIVE.`);
      this.circuitBreaker.recordSuccess(src.id);
      this.sourceStatusCache.delete(src.id);

      await this.supabase
        .from('importer_sources')
        .update({
          status: 'ACTIVE',
          enabled: true,
          blocked_reason: null,
          blocked_details: {
            recovered_at: nowIso,
            probe_success: true,
            stages: report.stages,
          },
          last_health_check_at: nowIso,
          updated_at: nowIso,
        })
        .eq('id', src.id);

      // Unpark held jobs for this source back to QUEUED
      try {
        let { error: unparkErr } = await this.supabase
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
      } catch (unparkErr: any) {
        this.logger.warn(`Failed to unpark jobs for ${src.id}`, { error: unparkErr?.message });
      }
    } catch (err: any) {
      this.logger.error(`Error probing health for source ${src.id}`, { error: err?.message });
    }
  }

  private autotunerCycleCount = 0;

  /**
   * Periodic autotuner telemetry & evaluation loop (every 30s)
   */
  private async runAutotunerLoop(): Promise<void> {
    while (!this.stopSignal) {
      await this.sleep(30_000);
      if (this.stopSignal) break;

      try {
        this.autotunerCycleCount++;
        const mem = diagnostics.getMemorySnapshot();
        const evaluation = this.autotuner.evaluateCycle();
        const activeJobs = diagnostics.getActiveJobsCount();
        const uploads = this.autotuner.getGlobalMediaSemaphore();
        const buffers = this.autotuner.getBufferedPageSemaphore();
        this.logger.info('Pipeline capacity', {
          chapterConcurrency: evaluation.concurrency,
          testedChapterCeiling: this.config.TESTED_CONCURRENCY_CEILING || 32,
          mediaConcurrency: uploads.capacity,
          activeMediaUploads: uploads.active,
          bufferedPages: buffers.active,
          queuedBufferWaiters: buffers.queued,
          bufferedBytes: ImporterEngine.activeBufferedBytes,
          reservedBytes: this.autotuner.getReservedBytes(),
          committedBytes: this.autotuner.getCommittedBytes(),
          bufferBudgetMaxObserved: this.autotuner.getMaxCommittedBytesObserved(),
          activeJobs,
          rssMb: mem.rssMb,
        });
        const lagMetrics = (diagnostics as any).lagMonitor?.getMetrics?.() || { avgLagMs: 0 };

        this.logger.info(
          `[Autotuner Telemetry] Action: ${evaluation.action} | Concurrency: ${evaluation.concurrency} | Active Jobs: ${activeJobs} | Mem: ${mem.heapUsedMb}MB heap / ${mem.rssMb}MB rss (512MB RAM) | Reason: ${evaluation.reason}`
        );

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
      } catch (err: any) {
        this.logger.error('Error during autotuner evaluation loop', { error: err?.message });
      }
    }
  }

  private sourceEmptyCooldown = new Map<string, number>();
  private sourceStatusCache = new Map<string, {
    enabled: boolean;
    chapterIngestionEnabled: boolean;
    catalogDiscoveryEnabled: boolean;
    status: string;
    cooldownUntil: number;
    cachedAt: number;
  }>();

  private async getEligibleChapterSources(): Promise<string[]> {
    const now = Date.now();
    if (now - this.activeSourcesCache.cachedAt > 5_000) {
      try {
        let { data: srcs } = await this.supabase
          .from('importer_sources')
          .select('id, enabled, chapter_ingestion_enabled, status, cooldown_until')
          .eq('chapter_ingestion_enabled', true);

        if (srcs && srcs.length > 0) {
          const activeIds = srcs
            .filter((s: any) => {
              if (s.enabled === false) return false;
              if (s.status === 'ACTIVE') return true;
              if (['COOLDOWN', 'PROBING', 'DEGRADED'].includes(s.status)) {
                if (!s.cooldown_until) return true;
                return new Date(s.cooldown_until).getTime() <= now;
              }
              return false;
            })
            .map((s: any) => s.id);
          this.activeSourcesCache = { sources: activeIds, cachedAt: now };
        }
      } catch (err: any) {
        // Retain existing cache on transient failure
      }
    }

    const candidateSources = this.activeSourcesCache.sources.length > 0
      ? this.activeSourcesCache.sources
      : Object.keys(SOURCE_CONCURRENCY_LIMITS);

    const eligible: string[] = [];
    for (const src of candidateSources) {
      // 1. In-memory circuit breaker check
      if (!this.circuitBreaker.canExecute(src)) continue;
      // 2. Source semaphore available permit check
      const sem = this.autotuner.getSourceSemaphore(src);
      if (sem.available <= 0) continue;
      eligible.push(src);
    }

    // Sort eligible sources by available permits descending to prioritize highest available headroom
    eligible.sort((a, b) => {
      const availA = this.autotuner.getSourceSemaphore(a).available;
      const availB = this.autotuner.getSourceSemaphore(b).available;
      return availB - availA;
    });

    return eligible;
  }

  private async checkSourceAvailability(source: string, taskType: TaskType = 'IMPORT_CHAPTER'): Promise<boolean> {
    // 1. Check local circuit breaker first (zero-cost in-memory check)
    if (!this.circuitBreaker.canExecute(source)) {
      return false;
    }

    const now = Date.now();
    let cached = this.sourceStatusCache.get(source);
    if (!cached || now - cached.cachedAt > 10_000) {
      let { data: src } = await this.supabase
        .from('importer_sources')
        .select('status, enabled, cooldown_until, chapter_ingestion_enabled, catalog_discovery_enabled')
        .eq('id', source)
        .maybeSingle();

      if (src) {
        cached = {
          enabled: src.enabled !== false,
          chapterIngestionEnabled: (src as any).chapter_ingestion_enabled !== false,
          catalogDiscoveryEnabled: (src as any).catalog_discovery_enabled === true || (src as any).catalog_discovery_enabled === 1,
          status: src.status || 'ACTIVE',
          cooldownUntil: src.cooldown_until ? new Date(src.cooldown_until).getTime() : 0,
          cachedAt: now,
        };
        this.sourceStatusCache.set(source, cached);
      }
    }

    if (!cached) return true;
    if (!cached.enabled) return false;

    if (taskType === 'IMPORT_CHAPTER') {
      if (!cached.chapterIngestionEnabled) return false;
      if (
        cached.status === 'PAUSED' ||
        cached.status === 'DISABLED' ||
        cached.status === 'UPSTREAM_BLOCKED' ||
        cached.status === 'EXCLUDED_BY_POLICY'
      ) {
        return false;
      }
      if (cached.status === 'COOLDOWN' && now < cached.cooldownUntil) {
        return false;
      }
    } else {
      if (!cached.catalogDiscoveryEnabled) return false;
      if (cached.status !== 'ACTIVE') return false;
    }

    return true;
  }

  /**
   * Dedicated multi-slot concurrent runner for a specific source.
   * Runs up to sourceLimits.maxChapters parallel worker slots, acquiring jobs atomically.
   */
  private async runSourceWorker(source: string): Promise<void> {
    const limits = this.autotuner.getSourceLimits(source);
    const concurrencySlots = Math.max(1, limits.maxChapters);
    this.logger.info(`Starting dedicated runner pool for source: ${source} (${concurrencySlots} concurrent chapter slots)`);

    const slotPromises = Array.from({ length: concurrencySlots }, (_, slotIndex) =>
      this.runSourceSlot(source, slotIndex)
    );

    await Promise.all(slotPromises);
  }

  private async runSourceSlot(source: string, slotIndex: number): Promise<void> {
    // 1. Initial runner startup stagger (50-150ms per slot index)
    if (slotIndex > 0) {
      const initialStaggerMs = slotIndex * 80 + Math.floor(Math.random() * 50);
      await this.sleep(initialStaggerMs);
    }

    const sourceSem = this.autotuner.getSourceSemaphore(source);
    const globalSem = this.autotuner.getGlobalChapterSemaphore();

    while (!this.stopSignal) {
      try {
        // 0. Enforce PublicationSafetyBarrier: if CLOSED or RECOVERING, hold 0 permits, 0 worker slots
        const canAcquire = await this.safetyBarrier.canAcquireChapters();
        if (!canAcquire) {
          await this.sleep(3000);
          continue;
        }

        // 1. Jitter between acquisitions (40-120ms) to avoid simultaneous claims on Gateway/Hyperdrive
        const claimJitterMs = 40 + Math.floor(Math.random() * 80);
        await this.sleep(claimJitterMs);

        // 1. Check if source had no jobs recently (backoff to avoid spin)
        const emptyUntil = this.sourceEmptyCooldown.get(source) || 0;
        if (Date.now() < emptyUntil) {
          await this.sleep(1500);
          continue;
        }

        // 2. Check source enabled & not in cooldown
        const isAvailable = await this.checkSourceAvailability(source);
        if (!isAvailable) {
          await this.sleep(5000);
          continue;
        }

        // 3. Acquire source permit first
        await sourceSem.acquire();

        // 4. Acquire global chapter permit
        try {
          await globalSem.acquire();
        } catch (semErr) {
          sourceSem.release();
          throw semErr;
        }

        // We hold BOTH permits! Now atomically acquire next job for this source from queue
        let job: QueueJob | null = null;
        try {
          job = await this.queue.acquireNextJob(
            Math.ceil(this.config.QUEUE_LEASE_DURATION_SECONDS / 60),
            source,
            'IMPORT_CHAPTER'
          );
        } catch (acquireErr: any) {
          this.logger.warn(`Error acquiring job for source ${source}: ${acquireErr?.message}`);
          globalSem.release();
          sourceSem.release();
          await this.sleep(2000);
          continue;
        }

        if (!job) {
          // No job available for this source: back off sibling slots for 4s
          this.sourceEmptyCooldown.set(source, Date.now() + 4000);
          globalSem.release();
          sourceSem.release();
          await this.sleep(2000);
          continue;
        }

        // Job found: clear empty cooldown
        this.sourceEmptyCooldown.delete(source);

        // Process job with lease heartbeat and timeout protection
        try {
          await this.executeJobDirectly(job);
        } finally {
          globalSem.release();
          sourceSem.release();
        }

        // Brief yield
        await this.sleep(50);
      } catch (err: any) {
        this.logger.error(`Error in worker slot ${slotIndex} for source ${source}`, { error: err?.message });
        await this.sleep(3000);
      }
    }
  }

  /**
   * General fallback worker runner running multiple concurrent slots
   */
  private async runGeneralWorker(): Promise<void> {
    const slotsCount = Math.max(
      1,
      Math.min(this.config.MAX_CONCURRENT_CHAPTERS || 5, this.config.TESTED_CONCURRENCY_CEILING || 32)
    );
    this.logger.info(`Starting shared chapter runner pool (${slotsCount} slots for target concurrency ${slotsCount})`);
    const slots = Array.from({ length: slotsCount }, (_, i) => this.runGeneralSlot(i));
    await Promise.all(slots);
  }

  private async runGeneralSlot(slotIndex: number): Promise<void> {
    telemetryCollector.registerSlot(slotIndex);

    // 1. Initial runner startup stagger (30-80ms per slot index) to prevent all runners opening simultaneously
    if (slotIndex > 0) {
      telemetryCollector.setSlotState(slotIndex, 'IDLE');
      const initialStaggerMs = slotIndex * 50 + Math.floor(Math.random() * 30);
      await this.sleep(initialStaggerMs);
    }

    const globalSem = this.autotuner.getGlobalChapterSemaphore();

    while (!this.stopSignal) {
      try {
        // 0. Enforce PublicationSafetyBarrier: if CLOSED or RECOVERING, hold 0 permits, 0 slots
        telemetryCollector.setSlotState(slotIndex, 'WAITING_FOR_PUBLICATION_BARRIER');
        const canAcquire = await this.safetyBarrier.canAcquireChapters();
        if (!canAcquire) {
          await this.sleep(3000);
          continue;
        }

        // 0b. Enforce ProtectiveSentinel: if PROTECTIVE_STOP active, hold 0 permits, 0 new claims
        if (await this.protectiveSentinel.isProtectiveStopActive()) {
          telemetryCollector.setSlotState(slotIndex, 'PROTECTIVE_STOP');
          await this.sleep(3000);
          continue;
        }

        // 1. Jitter between iterations (10-30ms)
        telemetryCollector.setSlotState(slotIndex, 'IDLE');
        const claimJitterMs = 10 + Math.floor(Math.random() * 20);
        await this.sleep(claimJitterMs);

        telemetryCollector.setSlotState(slotIndex, 'WAITING_FOR_JOB');
        const claimT0 = performance.now();

        const claimResult = await this.chapterClaimMutex.runExclusive(async () => {
          // A. Check global chapter semaphore capacity first
          if (!globalSem.tryAcquire()) {
            return null;
          }

          // B. Find sources that currently have available capacity
          const eligibleSources = await this.getEligibleChapterSources();
          if (eligibleSources.length === 0) {
            globalSem.release();
            return null;
          }

          // C. Query Yugabyte for the highest priority job among ELIGIBLE sources via WorkAffinityScheduler
          let candidateJob: QueueJob | null = null;
          try {
            candidateJob = await this.scheduler.acquireNextChapterJob({
              workerId: this.config.WORKER_ID,
              leaseDurationMinutes: Math.ceil(this.config.QUEUE_LEASE_DURATION_SECONDS / 60),
              allowedSources: eligibleSources,
            });
          } catch (acquireErr: any) {
            this.logger.warn(`Error acquiring chapter job: ${acquireErr?.message}`);
            globalSem.release();
            return null;
          }

          if (!candidateJob) {
            globalSem.release();
            return null;
          }

          // D. Atomically acquire the source permit
          const sourceSem = this.autotuner.getSourceSemaphore(candidateJob.source);
          if (sourceSem.tryAcquire()) {
            return {
              job: candidateJob,
              sourcePermitAcquired: true,
              globalPermitAcquired: true,
            };
          } else {
            // In the rare race where source filled, immediately release back to QUEUED (zero blocking)
            this.logger.warn(`Source ${candidateJob.source} filled concurrently, releasing job ${candidateJob.id} back to QUEUED`);
            if (candidateJob.payload?.workId) {
              this.scheduler.onJobFinished(candidateJob.payload.workId, candidateJob.chapter_sort_key);
            }
            await this.queue.releaseJob(candidateJob.id, 'QUEUED', 'Source concurrency full, released immediately', 0);
            globalSem.release();
            return null;
          }
        });

        if (!claimResult || !claimResult.job) {
          telemetryCollector.setSlotState(slotIndex, 'WAITING_FOR_SOURCE', 'waiting_for_eligible_source');
          const emptyBackoffMs = 1000 + Math.floor(Math.random() * 1000);
          await this.sleep(emptyBackoffMs);
          continue;
        }

        const job = claimResult.job;
        const sourcePermitAcquired = claimResult.sourcePermitAcquired;
        const globalPermitAcquired = claimResult.globalPermitAcquired;

        const claimDurationMs = performance.now() - claimT0;
        const sourceSem = this.autotuner.getSourceSemaphore(job.source);

        telemetryCollector.setSlotState(slotIndex, 'ACTIVE_PROCESSING', `${job.source} ch ${job.payload?.chapterNumber}`);
        try {
          await this.executeJobDirectly(job, { claimDurationMs, semWaitMs: 0 });
        } finally {
          if (job.payload?.workId) {
            this.scheduler.onJobFinished(job.payload.workId, job.chapter_sort_key);
          }
          this.scheduler.recordJobCompletion();
          if (sourcePermitAcquired) {
            sourceSem.release();
          }
          if (globalPermitAcquired) {
            globalSem.release();
          }
          telemetryCollector.setSlotState(slotIndex, 'IDLE');
        }

        await this.sleep(30);
      } catch (err: any) {
        telemetryCollector.setSlotState(slotIndex, 'IDLE');
        this.logger.error(`Error in general worker slot ${slotIndex}`, { error: err?.message });
        await this.sleep(2000);
      }
    }
  }

  /**
   * Dedicated discovery worker loop to guarantee catalog scanning is NEVER starved by chapter backlog.
   * Continuously claims DISCOVER_WORKS jobs from the queue.
   */
  private async runDiscoveryWorker(): Promise<void> {
    this.logger.info('Starting dedicated discovery lane runner');

    while (!this.stopSignal) {
      try {
        if (await this.protectiveSentinel.isProtectiveStopActive()) {
          await this.sleep(3000);
          continue;
        }

        const isDiscoveryAllowed = await this.isDiscoveryAllowed();
        if (!isDiscoveryAllowed) {
          await this.sleep(5_000);
          continue;
        }

        const job = await this.queue.acquireNextJob(
          Math.ceil(this.config.QUEUE_LEASE_DURATION_SECONDS / 60),
          undefined,
          'DISCOVER_WORKS'
        );

        if (!job) {
          await this.sleep(5_000);
          continue;
        }

        this.logger.info(`[Discovery Lane] Acquired ${job.task_type} for source ${job.source} (Job: ${job.id})`);
        await this.executeJobDirectly(job);
        await this.sleep(100);
      } catch (err: any) {
        this.logger.error('Error in discovery worker lane', { error: err?.message });
        await this.sleep(5_000);
      }
    }
  }

  /**
   * Dedicated catalog sync worker loop to guarantee work metadata / chapter discovery runs steadily.
   * Continuously claims SYNC_WORK jobs from the queue.
   */
  private async runCatalogSyncWorker(): Promise<void> {
    this.logger.info('Starting dedicated catalog sync lane runner');

    while (!this.stopSignal) {
      try {
        if (await this.protectiveSentinel.isProtectiveStopActive()) {
          await this.sleep(3000);
          continue;
        }

        const isDiscoveryAllowed = await this.isDiscoveryAllowed();
        if (!isDiscoveryAllowed) {
          await this.sleep(5_000);
          continue;
        }

        const job = await this.queue.acquireNextJob(
          Math.ceil(this.config.QUEUE_LEASE_DURATION_SECONDS / 60),
          undefined,
          'SYNC_WORK'
        );

        if (!job) {
          await this.sleep(5_000);
          continue;
        }

        this.logger.info(`[Sync Lane] Acquired ${job.task_type} for source ${job.source} (Job: ${job.id})`);
        await this.executeJobDirectly(job);
        await this.sleep(100);
      } catch (err: any) {
        this.logger.error('Error in catalog sync worker lane', { error: err?.message });
        await this.sleep(5_000);
      }
    }
  }

  /**
   * Executes a job with active lease heartbeat and hard timeout watchdog.
   */
  private async executeJobDirectly(job: QueueJob, extraTiming?: { claimDurationMs?: number; semWaitMs?: number }): Promise<void> {
    // ADMISSION GATE: Catalog Discovery Global Guard
    if (job.task_type === 'DISCOVER_WORKS' || job.task_type === 'SYNC_WORK') {
      const isDiscoveryAllowed = await this.isDiscoveryAllowed();
      if (!isDiscoveryAllowed) {
        this.logger.warn(`Catalog discovery is globally DISABLED. Postponing ${job.task_type} job ${job.id}`, { jobId: job.id, source: job.source });
        await this.supabase.from('importer_queue').update({
          status: 'PAUSED_BY_STAFF',
          pause_reason: 'CATALOG_DISCOVERY_DISABLED',
          locked_by: null,
          locked_at: null,
          lease_expires_at: null,
          updated_at: new Date().toISOString(),
        }).eq('id', job.id);
        return;
      }
    }

    // ADMISSION GATE: Circuit Breaker / Health Check
    if (job.task_type === 'IMPORT_CHAPTER') {
      const isAvailable = await this.checkSourceAvailability(job.source, 'IMPORT_CHAPTER');
      if (!isAvailable) {
        // Source is down. Do we have fallbacks?
        const fallbacks = job.payload?.fallbackSources || [];
        if (!Array.isArray(fallbacks) || fallbacks.length === 0) {
          this.logger.warn(`Source ${job.source} is blocked/tarpitting and job ${job.id} has no fallbacks. Rejecting at admission gate.`, { source: job.source, jobId: job.id });
          await this.supabase.from('importer_queue').update({
            status: 'RETRY',
            next_run_at: new Date(Date.now() + 15 * 60 * 1000).toISOString(),
            locked_by: null,
            locked_at: null,
            last_error: 'Source circuit breaker OPEN or UPSTREAM_BLOCKED. No fallbacks available.'
          }).eq('id', job.id);
          return;
        }
      }
    }

    let cancelSignalTriggered = false;
    const heartbeat = this.queue.startHeartbeat(
      job.id,
      this.config.QUEUE_HEARTBEAT_INTERVAL_SECONDS,
      () => {
        cancelSignalTriggered = true;
      }
    );

    // Dynamic safety timeout: scales with page count if available, with a minimum of 2.5 minutes and maximum of 4 minutes
    const pageCountHint = typeof job.payload?.pageCount === 'number' ? job.payload.pageCount : (job.progress_total || 40);
    const maxJobDurationMs = job.task_type === 'IMPORT_CHAPTER'
      ? Math.min(4 * 60 * 1000, Math.max(150 * 1000, pageCountHint * 4 * 1000))
      : 3 * 60 * 1000;
    let jobTimeoutTimer: NodeJS.Timeout | null = null;
    const timeoutPromise = new Promise<never>((_, reject) => {
      jobTimeoutTimer = setTimeout(() => {
        cancelSignalTriggered = true;
        reject(
          new Error(
            `JobExecutionTimeout: Job ${job.id} (${job.task_type}) exceeded safety limit of ${Math.round(maxJobDurationMs / 60000)} minutes`
          )
        );
      }, maxJobDurationMs);
    });

    try {
      const executionPromise = this.processJob(job, () => cancelSignalTriggered, extraTiming);
      await Promise.race([executionPromise, timeoutPromise]);
    } finally {
      if (jobTimeoutTimer) clearTimeout(jobTimeoutTimer);
      heartbeat.stop();
    }
  }

  /**
   * Backward-compatible entrypoint used by step() and test suites.
   */
  private async executeJobWithLimits(job: QueueJob): Promise<void> {
    if (job.task_type === 'IMPORT_CHAPTER') {
      const globalSem = this.autotuner.getGlobalChapterSemaphore();
      const sourceSem = this.autotuner.getSourceSemaphore(job.source);

      await withSourceChapterPermits(sourceSem, globalSem, () => this.executeJobDirectly(job), this.abortController.signal);
    } else {
      await this.executeJobDirectly(job);
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

    await this.executeJobWithLimits(job);
    return true;
  }

  private async scheduleSources(): Promise<void> {
    const isDiscoveryAllowed = await this.isDiscoveryAllowed();
    if (!isDiscoveryAllowed) {
      return;
    }

    let { data: sources, error } = await this.supabase
      .from('importer_sources')
      .select('*');

    if (error || !sources) return;

    const now = Date.now();

    for (const src of sources) {
      if (
        src.enabled === false ||
        (src as any).catalog_discovery_enabled === false ||
        (src as any).catalog_discovery_enabled === 0 ||
        src.status === 'DISABLED' ||
        src.status === 'PAUSED' ||
        src.status === 'UPSTREAM_BLOCKED'
      ) {
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
        // Prevent duplicate DISCOVER_WORKS jobs from piling up if one is already active or in retry
        let hasActive = false;
        try {
          const q = this.supabase
            .from('importer_queue')
            .select('id, status, created_at')
            .eq('task_type', 'DISCOVER_WORKS')
            .eq('source', src.id);

          let { data: existingActive } = typeof (q as any).in === 'function'
            ? await (q as any).in('status', ['QUEUED', 'IMPORTING', 'RETRY']).limit(10)
            : await q.limit(10);

          if (existingActive && Array.isArray(existingActive)) {
            const DISCOVERY_TTL_MS = 60 * 60 * 1000; // 1 hour TTL
            for (const j of existingActive) {
              const age = now - new Date(j.created_at).getTime();
              if (['QUEUED', 'RETRY'].includes(j.status) && age > DISCOVERY_TTL_MS) {
                this.logger.warn(`Consolidating stale DISCOVER_WORKS job ${j.id} for ${src.id} (age: ${Math.round(age / 60000)}m)`, {
                  jobId: j.id,
                  source: src.id,
                  ageMinutes: Math.round(age / 60000),
                });
                await this.supabase
                  .from('importer_queue')
                  .update({
                    status: 'SUPERSEDED',
                    last_error: 'superseded_stale_discovery_ttl',
                    updated_at: new Date().toISOString(),
                  })
                  .eq('id', j.id);
              } else if (['QUEUED', 'IMPORTING', 'RETRY'].includes(j.status)) {
                hasActive = true;
              }
            }
          }
        } catch (err: any) {
          this.logger.warn('Error checking existing active discoveries', { error: err?.message });
        }

        if (hasActive) {
          continue;
        }

        const checkpoint = await this.checkpoints.getCheckpoint(src.id);
        const isCompleted = Boolean(checkpoint?.metadata?.catalog_completed);
        const discoveryMode = isCompleted ? 'maintenance' : 'bootstrap';

        const dedupeKey = `${src.id}:discover:${Math.floor(now / intervalMs)}`;
        await this.queue.enqueue(
          'DISCOVER_WORKS',
          src.id,
          dedupeKey,
          {
            workTitle: `Varredura de Catálogo (${src.name || src.id})`,
            mode: discoveryMode,
          },
          10
        );
      }
    }
  }

  private async processJob(
    job: QueueJob,
    isCancelled?: () => boolean,
    extraTiming?: { claimDurationMs?: number; semWaitMs?: number }
  ): Promise<void> {
    try {
      // Checkpoint 0: Staff cancellation pre-flight check
      if (job.cancel_requested || isCancelled?.() || (await this.queue.isCancelRequested(job.id))) {
        this.logger.info(`Job ${job.id} cancelled by staff prior to execution.`);
        await this.queue.releaseJob(job.id, 'CANCELLED_BY_STAFF');
        return;
      }

      // Checkpoint 0b: Publication Safety Barrier check for chapter ingestion
      if (job.task_type === 'IMPORT_CHAPTER') {
        const canAcquire = await this.safetyBarrier.canProcessChapter(job.payload?.workId, Number(job.chapter_sort_key ?? job.payload?.chapterNumber));
        if (!canAcquire) {
          this.logger.warn(`Skipping chapter job ${job.id}: PublicationSafetyBarrier is CLOSED/RECOVERING`);
          await this.queue.releaseJob(job.id, 'QUEUED', 'PublicationSafetyBarrier is CLOSED/RECOVERING', 15);
          return;
        }
      }

      let { data: sourceRec } = await this.supabase
        .from('importer_sources')
        .select('id, status, cooldown_until, enabled')
        .eq('id', job.source)
        .maybeSingle();

      if (sourceRec) {
        if (sourceRec.status === 'UPSTREAM_BLOCKED') {
          // If this is a chapter import job, check if other healthy sources exist for the work
          let hasHealthyFallback = false;
          if (job.task_type === 'IMPORT_CHAPTER' && job.payload?.workId && job.payload?.chapterNumber) {
            const candidateFallbacks = await this.resolveDynamicCandidateFallbacks(
              job.payload.workId,
              job.payload.chapterNumber,
              job.source,
              (job.payload?.fallbackSources as any) || []
            );
            if (candidateFallbacks.length > 0) {
              hasHealthyFallback = true;
            }
          }

          if (!hasHealthyFallback) {
            this.logger.warn(`Parking job ${job.id}: source ${job.source} is UPSTREAM_BLOCKED (retaining safely in BLOCKED_BY_UPSTREAM)`);
            await this.queue.releaseJob(
              job.id,
              'BLOCKED_BY_UPSTREAM',
              `Bloqueado a montante: upstream_blocked (${sourceRec.status})`
            );
            return;
          }
        }

        if (sourceRec.status === 'PAUSED' || sourceRec.status === 'DISABLED' || !sourceRec.enabled) {
          this.logger.info(`Source ${job.source} is ${sourceRec.status}. Parking job ${job.id} to avoid retry spinning.`);
          await this.queue.releaseJob(
            job.id,
            'PAUSED_BY_STAFF',
            `Source ${job.source} is ${sourceRec.status} (automatically parked by engine)`,
            0,
            'SOURCE_PAUSED'
          );
          return;
        }

        if (sourceRec.status === 'COOLDOWN') {
          const cooldownUntil = sourceRec.cooldown_until ? new Date(sourceRec.cooldown_until).getTime() : 0;
          if (Date.now() < cooldownUntil) {
            const waitSeconds = Math.max(10, Math.ceil((cooldownUntil - Date.now()) / 1000));
            await this.queue.releaseJob(
              job.id,
              'RETRY',
              `Source in COOLDOWN until ${sourceRec.cooldown_until}`,
              waitSeconds,
              'SOURCE_TIMEOUT'
            );
            return;
          } else {
            await this.supabase
              .from('importer_sources')
              .update({ status: 'ACTIVE', cooldown_until: null, updated_at: new Date().toISOString() })
              .eq('id', job.source);
            this.circuitBreaker.reset(job.source);
          }
        }
      }

      // Prioridade Absoluta Guard: if an active focus work exists, ONLY jobs for that work may run
      try {
        let reqQuery: any = this.supabase
          .from('importer_staff_requests')
          .select('id, work_id');
        if (typeof reqQuery?.in === 'function') {
          reqQuery = reqQuery.in('status', ['QUEUED', 'IMPORTING', 'RETRYING']);
        }
        if (typeof reqQuery?.maybeSingle === 'function') {
          let { data: activeFocus } = await reqQuery.maybeSingle();
          if (activeFocus?.work_id && job.payload?.workId && job.payload.workId !== activeFocus.work_id) {
            this.logger.info(`Focus Mode active for work ${activeFocus.work_id}. Deferring non-priority job for ${job.payload?.workId}`, {
              jobId: job.id,
              focusWorkId: activeFocus.work_id,
              jobWorkId: job.payload?.workId,
            });
            await this.queue.releaseJob(job.id, 'RETRY', `Focus mode active for work ${activeFocus.work_id}`, 15);
            return;
          }
        }
      } catch {
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
        } catch {
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
          await this.handleImportChapter(job, isCancelled, extraTiming);
          break;
        default:
          throw new Error(`Unknown task type: ${job.task_type}`);
      }

      await this.queue.releaseJob(job.id, 'COMPLETED');
    } catch (err: any) {
      // Check if job was cancelled by staff at safe checkpoint
      if (err instanceof JobCancelledByStaffError || isCancelled?.()) {
        this.logger.info(`Job ${job.id} safely cancelled by staff at checkpoint`);
        if (job.payload?.sourceChapterId && job.source) {
          try {
            await this.supabase
              .from('importer_chapter_mappings')
              .update({ status: 'QUEUED', updated_at: new Date().toISOString() })
              .eq('source', job.source)
              .eq('source_chapter_id', job.payload.sourceChapterId);
          } catch {}
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

      // Inspect error for Cloudflare / WAF block patterns
      const statusFromErr = (err as any)?.status || (errorMessage.includes('403') ? 403 : errorMessage.includes('429') ? 429 : 500);
      const cfInsp = CloudflareClassifier.inspect(
        statusFromErr,
        (err as any)?.headers || {},
        errorMessage,
        {
          expectedType: job.task_type === 'IMPORT_CHAPTER' ? 'image' : 'json',
          isIsolatedRequest: false,
        }
      );

      const isUpstreamBlocked =
        ((cfInsp.isBlocked && statusFromErr !== 429) ||
          cfInsp.isChallenge ||
          /403|turnstile|challenge|upstream_blocked|just a moment/i.test(errorMessage) ||
          /bloqueado por cloudflare/i.test(errorMessage)) &&
        statusFromErr !== 429;

      if (isUpstreamBlocked) {
        const classification: CloudflareClassification = cfInsp.classification || 'DATACENTER_ASN_BLOCK';

        // 1. Record failure in circuit breaker
        const { tripped, cooldownMs } = this.circuitBreaker.recordFailure(job.source, classification);

        // 2. Track in shared network detector to avoid probe storms across sources
        this.sharedNetworkDetector.recordBlockEvent({
          sourceId: job.source,
          classification,
          cfRay: cfInsp.cfRay,
        });

        // 3. Invalidate source availability cache
        this.sourceStatusCache.delete(job.source);

        const nowIso = new Date().toISOString();

        if (tripped) {
          // Tripped circuit breaker after reaching failure threshold: move to COOLDOWN (not permanent UPSTREAM_BLOCKED!)
          const cooldownUntilIso = new Date(Date.now() + cooldownMs).toISOString();
          this.logger.warn(
            `Source ${job.source} tripped circuit breaker (${classification}). Entering COOLDOWN until ${cooldownUntilIso}.`,
            { jobId: job.id, classification, cooldownMs, error: errorMessage }
          );

          try {
            await this.supabase
              .from('importer_sources')
              .update({
                status: 'COOLDOWN',
                cooldown_until: cooldownUntilIso,
                blocked_reason: classification,
                blocked_details: {
                  classification,
                  reason: cfInsp.reason,
                  last_checked_at: nowIso,
                  cooldown_until: cooldownUntilIso,
                },
                updated_at: nowIso,
              })
              .eq('id', job.source);
          } catch {}
        } else {
          // 1st or 2nd failure: mark DEGRADED (can still process other jobs or be retried)
          this.logger.warn(
            `Source ${job.source} recorded transient block (${classification}). Marking DEGRADED (failure threshold not reached).`,
            { jobId: job.id, classification, error: errorMessage }
          );

          try {
            await this.supabase
              .from('importer_sources')
              .update({
                status: 'DEGRADED',
                blocked_reason: classification,
                updated_at: nowIso,
              })
              .eq('id', job.source);
          } catch {}
        }

        // Release job to RETRY with backoff delay so it is not permanently stuck
        const backoffSeconds = Math.max(60, Math.round(cooldownMs / 1000) || 60);
        await this.queue.releaseJob(
          job.id,
          'RETRY',
          `Bloqueio a montante transitório (${classification}): ${cfInsp.reason || errorMessage}`,
          backoffSeconds
        );
        return;
      }

      const classification = RetryPolicy.classify(err);
      
      // If it's a network/timeout error, record it in the circuit breaker!
      if (classification.retryClass === 'QUEUE_RETRY_TIMEOUT' || errorMessage.toLowerCase().includes('timeout') || errorMessage.toLowerCase().includes('abort')) {
        this.logger.warn(`Recording timeout/network failure for ${job.source} in circuit breaker.`);
        const { tripped, cooldownMs } = this.circuitBreaker.recordFailure(job.source, 'TIMEOUT_TARPIT' as any);
        if (tripped) {
          // If tripped, set the DB source status as well
          this.supabase.from('importer_sources').update({
            status: 'COOLDOWN',
            cooldown_until: new Date(Date.now() + cooldownMs).toISOString(),
            blocked_reason: 'TIMEOUT_TARPIT'
          }).eq('id', job.source).then();
        }
      }

      const isStaffPriority = Boolean(job.payload?.staffRequested) || (job.priority >= 100);
      const decision = RetryPolicy.decide(classification, job.attempts, job.max_attempts, { isStaffPriority });

      if (classification.retryClass === 'QUEUE_RETRY_429') {
        if (classification.sourceStage === 'storage') {
          // Storage rate limit (Telegram / Storage Bridge):
          // Throttles ONLY the Storage rate limiter; DO NOT scale down general job concurrency!
          if (typeof (this.storage as any).getRateLimiter === 'function') {
            (this.storage as any).getRateLimiter().recordRateLimit(classification.retryAfterSeconds);
          }
        } else {
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
      } else if (classification.retryClass === 'QUEUE_RETRY_STORAGE_502' || classification.retryClass === 'QUEUE_RETRY_STORAGE_503') {
        if (typeof (this.storage as any).getRateLimiter === 'function') {
          (this.storage as any).getRateLimiter().recordTransientError();
        }
      } else if (classification.sourceStage === 'provider' && /rate\s*limit/i.test(errorMessage)) {
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
      } else if (classification.retryClass === 'QUEUE_RETRY_TIMEOUT' && classification.sourceStage !== 'provider') {
        this.autotuner.recordError('timeout');
      } else if (classification.sourceStage === 'system') {
        this.autotuner.recordError('error');
      }

      if (classification.sourceStage === 'provider') {
        this.autotuner.recordSourceFailure(job.source);
      }

      const structuredMsg = decision.status === 'FAILED'
        ? decision.reason
        : (classification.structuredMessage || errorMessage);

      this.logger.warn(
        `Job ${job.id} retry decision: ${decision.status} (delay: ${decision.delaySeconds}s, code: ${classification.taxonomyCode})`,
        {
          jobId: job.id,
          source: job.source,
          workId: job.payload?.workId,
          chapterSortKey: job.chapter_sort_key,
          taxonomyCode: classification.taxonomyCode,
          retryClass: classification.retryClass,
          attempt: job.attempts,
          delaySeconds: decision.delaySeconds,
          reason: decision.reason,
        }
      );

      await this.queue.releaseJob(
        job.id,
        decision.status,
        this.sanitizeErrorMessage(structuredMsg),
        decision.delaySeconds,
        classification.taxonomyCode
      );

      if (job.payload?.workId) {
        if (decision.status === 'RETRY') {
          const nextAttemptIso = new Date(Date.now() + decision.delaySeconds * 1000).toISOString();
          try {
            await this.supabase
              .from('importer_staff_requests')
              .update({
                status: 'RETRYING',
                last_error: this.sanitizeErrorMessage(structuredMsg),
                last_attempt_at: new Date().toISOString(),
                next_attempt_at: nextAttemptIso,
                attempt_count: job.attempts,
                updated_at: new Date().toISOString(),
              })
              .eq('work_id', job.payload.workId)
              .in('status', ['QUEUED', 'IMPORTING', 'RETRYING']);
          } catch {
            // Non-blocking telemetry
          }
        } else if (decision.status === 'FAILED') {
          try {
            await this.supabase
              .from('importer_staff_requests')
              .update({
                status: 'FAILED',
                last_error: this.sanitizeErrorMessage(decision.reason),
                updated_at: new Date().toISOString(),
              })
              .eq('work_id', job.payload.workId)
              .in('status', ['QUEUED', 'IMPORTING', 'RETRYING']);
          } catch {
            // Non-blocking telemetry
          }
        }
      }
    }
  }

  private async handleDiscoverWorks(job: QueueJob): Promise<void> {
    const adapter = this.registry.get(job.source);
    if (!adapter) throw new Error(`Source adapter not registered: ${job.source}`);

    const checkpoint = await this.checkpoints.getCheckpoint(job.source);
    const isCompleted = Boolean(checkpoint?.metadata?.catalog_completed);
    const mode: 'bootstrap' | 'maintenance' = job.payload?.mode || (isCompleted ? 'maintenance' : 'bootstrap');
    const currentCursor = job.payload?.cursor !== undefined ? job.payload.cursor : checkpoint?.cursor_value;

    this.logger.info(`Running ${mode} discovery for source ${job.source}`, {
      source: job.source,
      mode,
      cursor: currentCursor,
    });

    let { works, nextCursor } = await callProvider(() => adapter.fetchUpdatedWorks(currentCursor, { mode }));

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
        60
      );
    }

    if (mode === 'bootstrap') {
      if (!nextCursor || works.length === 0) {
        await this.checkpoints.markCatalogCompleted(job.source, null, {
          lastDiscoveredCount: works.length,
          lastBootstrapCursor: currentCursor,
        });
        this.logger.info(`[Catalog Backfill] Source ${job.source} reached end of catalog. Backfill pass completed.`);
      } else {
        await this.checkpoints.saveCheckpoint(job.source, nextCursor, {
          ...(checkpoint?.metadata || {}),
          catalog_completed: false,
          lastDiscoveredCount: works.length,
          lastBackfillAt: new Date().toISOString(),
        });
        this.logger.info(`[Catalog Backfill] Source ${job.source} advanced to cursor: ${nextCursor}`);
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

    // Update last_sync_at now that discovery actually executed and succeeded
    await this.supabase
      .from('importer_sources')
      .update({ last_sync_at: new Date().toISOString(), updated_at: new Date().toISOString() })
      .eq('id', job.source);
  }

  private async handleSyncWork(job: QueueJob): Promise<void> {
    let { sourceWorkId } = job.payload;
    if (!sourceWorkId && job.payload?.workId) {
      let { data: mapping } = await this.supabase
        .from('importer_work_mappings')
        .select('source_work_id, source')
        .eq('work_id', job.payload.workId)
        .eq('source', job.source)
        .maybeSingle();

      if (mapping?.source_work_id) {
        sourceWorkId = mapping.source_work_id;
      } else {
        let { data: anyMapping } = await this.supabase
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
    if (!sourceWorkId) throw new Error('Missing sourceWorkId in payload');

    const adapter = this.registry.get(job.source);
    if (!adapter) throw new Error(`Source adapter not registered: ${job.source}`);

    const details = await callProvider(() => adapter.fetchWorkDetails(sourceWorkId));
    const botUserId = await this.resolveBotUserId();

    let coverMediaId: string | null = null;
    if (details.coverUrl) {
      try {
        coverMediaId = await this.downloadAndRegisterImage(details.coverUrl, botUserId, 'editorial', job.source);
      } catch (coverErr: any) {
        this.logger.warn('Failed to import cover image from primary coverUrl, attempting fallback', {
          error: coverErr?.message,
          coverUrl: details.coverUrl,
          source: job.source,
        });
      }
    }

    // Fallback 1: Raw metadata cover alternatives
    if (!coverMediaId && details.raw) {
      coverMediaId = await this.tryRawMetadataCoverFallback(details.raw, botUserId, job.source, details.coverUrl);
    }

    // Fallback 2: Sibling mappings from other mapped sources
    if (!coverMediaId && (job.payload?.workId || details.sourceWorkId)) {
      coverMediaId = await this.trySiblingMappingCoverFallback(
        job.payload?.workId,
        job.source,
        details.slug || details.title,
        botUserId
      );
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
      genres: details.genres,
      contentRating: ADULT_SOURCES.has(job.source)
        ? 'ADULT_18'
        : (details.ageRating && details.ageRating >= 18 ? 'ADULT_18' : 'GENERAL'),
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
    let { data: existingMappings } = await this.supabase
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
    let { data: publishedChapters } = await this.supabase
      .from('chapters')
      .select('id, number, title')
      .eq('work_id', result.workId)
      .not('published_at', 'is', null);

    // Match each candidate chapter against published chapters
    const missingChapters: typeof chapters = [];

    for (const ch of chapters) {
      if (completedIds.has(ch.sourceChapterId)) {
        continue;
      }

      const chKey = this.computeCanonicalChapterKey(ch.number, ch.title);

      // Find if an existing published chapter matches canonical key
      const matchedPublished = (publishedChapters || []).find((pub) => {
        const pubKey = this.computeCanonicalChapterKey(pub.number, pub.title);
        if (pubKey.normalizedNumber !== chKey.normalizedNumber) return false;
        // Do not merge specials with regular chapters
        if (pubKey.isSpecial !== chKey.isSpecial) return false;
        if (chKey.specialCategory && pubKey.specialCategory && chKey.specialCategory !== pubKey.specialCategory) return false;
        return true;
      });

      if (matchedPublished) {
        // Chapter is ALREADY published: link mapping to canonical chapter, zero re-download!
        await this.supabase.from('importer_chapter_mappings').upsert(
          {
            source: job.source,
            source_chapter_id: ch.sourceChapterId,
            chapter_id: matchedPublished.id,
            work_mapping_id: result.mappingId,
            chapter_number: ch.number,
            page_count: ch.pageCount || 0,
            is_page_provider: false,
            status: 'COMPLETED',
            last_error: null,
          },
          { onConflict: 'source,source_chapter_id' }
        );
        continue;
      }

      missingChapters.push(ch);
    }

    // For missing chapters, check if another source already has an active job in queue
    let { data: activeJobs } = await this.supabase
      .from('importer_queue')
      .select('payload, source, status')
      .eq('task_type', 'IMPORT_CHAPTER')
      .in('status', ['QUEUED', 'IMPORTING', 'RETRY']);

    const activeJobsForWork = (activeJobs || []).filter(
      (j) => j.payload?.workId === result.workId
    );

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
      let staffPriorityBoost = 0;
      try {
        let { data: staffReq } = await this.supabase
          .from('importer_staff_requests')
          .select('id, priority_boost')
          .eq('work_id', result.workId)
          .eq('status', 'ACTIVE')
          .maybeSingle();
        if (staffReq) {
          isStaffPriority = true;
          staffPriorityBoost = staffReq.priority_boost || 0;
        }
      } catch {
        // Safe fallback
      }
      const chapterPriority = isStaffPriority ? 1000 + staffPriorityBoost : 30;

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

      // Check persistent watermark and catalog existence
      const watermark = await this.scheduler.getWatermark(result.workId, job.source);
      const isWorkAlreadyOnSite = (publishedChapters && publishedChapters.length > 0) || false;

      // Admission policy for new works cohort (Strict Section 5 Admission Gate)
      let isAdmitted = isWorkAlreadyOnSite || isStaffPriority;
      if (!isAdmitted) {
        const gate = await this.admissionController.canAdmitNewWork();
        if (gate.allowed) {
          const config = this.schedulerStateStore.getConfig();
          isAdmitted = true;
          this.schedulerStateStore.setActiveWork({
            workId: result.workId,
            workTitle: (result as any).work?.title || details.title || 'Unknown Title',
            lane: 'P2',
            state: 'FILLING',
            primarySource: job.source,
            admittedAt: new Date().toISOString(),
            lastActivityAt: new Date().toISOString(),
            totalChapters: chaptersToEnqueue.length,
            publishedChapters: 0,
            queuedChapters: Math.min(chaptersToEnqueue.length, config.slidingWindowSize || 8),
            inFlightChapters: 0,
            frontierSortKey: null,
            criticalGapSortKey: null,
            criticalGapUnblockCount: 0,
          });
          this.logger.info(`[ADMISSION_GATE_PASSED] Admitted new work "${details.title}" into P2 cohort`);
        } else {
          isAdmitted = false;
          this.logger.info(`[ADMISSION_GATE_HELD] New work "${details.title}" (${result.workId}) kept in WAITING_ADMISSION: ${gate.reason}`);
        }
      }

      // P3 discoveries must NOT flood the queue. If not admitted, set WAITING_ADMISSION.
      if (!isAdmitted) {
        await this.supabase
          .from('importer_work_mappings')
          .update({ sync_status: 'WAITING_ADMISSION' })
          .eq('id', result.mappingId);
      }

      // 2. Batch enqueue tasks to importer_queue with Work-Oriented Priority & Sliding Window
      const queueJobs = chaptersToEnqueue.map((ch, idx) => {
        const dedupeKey = `${job.source}:chapter:${ch.sourceChapterId}`;
        const chKey = this.computeCanonicalChapterKey(ch.number, ch.title);

        let priority = 50;
        let isFreshRelease = false;

        if (isStaffPriority) {
          priority = 1000 + staffPriorityBoost;
          isFreshRelease = true;
        } else if (isWorkAlreadyOnSite && watermark && chKey.sortKey > watermark.lastSeenSortKey) {
          // P0: Fresh New Release of tracked/existing work
          priority = 100;
          isFreshRelease = true;
        } else if (isWorkAlreadyOnSite) {
          // P1: Backfill of existing work
          priority = 75;
          isFreshRelease = false;
        } else {
          // P2: New work initial fill (dormant until admitted)
          priority = 50;
          isFreshRelease = false;
        }

        // Sliding window: only admitted works or staff/fresh releases can start in QUEUED (first 8 chapters).
        // Works waiting for cohort admission stay completely in PAUSED_BY_STAFF.
        let status: 'QUEUED' | 'PAUSED_BY_STAFF' = 'PAUSED_BY_STAFF';
        if (isStaffPriority || isFreshRelease) {
          status = 'QUEUED';
        } else if (isAdmitted && idx < 8) {
          status = 'QUEUED';
        }

        return {
          taskType: 'IMPORT_CHAPTER' as const,
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
            staffForced: isStaffPriority,
            originalPriority: isWorkAlreadyOnSite ? 75 : 50,
            isFreshRelease,
          },
          priority,
          chapterSortKey: chKey.sortKey,
          status,
        };
      });

      await this.queue.enqueueBatch(queueJobs);

      // Update persistent watermark
      if (chapters.length > 0) {
        let maxSort = 0;
        let maxChNum = 0;
        let maxChId = '';
        for (const c of chapters) {
          const k = this.computeCanonicalChapterKey(c.number, c.title).sortKey;
          if (k > maxSort) {
            maxSort = k;
            maxChNum = typeof c.number === 'number' ? c.number : parseFloat(String(c.number));
            maxChId = c.sourceChapterId;
          }
        }
        await this.scheduler.setWatermark({
          workId: result.workId,
          source: job.source,
          lastSeenChapter: maxChNum,
          lastSeenSortKey: maxSort,
          lastSeenChapterId: maxChId,
          lastDiscoveryAt: new Date().toISOString(),
        });
      }
    }

    // Discovery complete for this work sync cycle: sweep any STAGED chapters that were waiting on discovery
    await this.publicationBarrier.sweepStagedPublications();
  }

  public computeCanonicalChapterKey(chapterNumber: number | string, chapterTitle?: string) {
    return computeCanonicalChapterKey(chapterNumber, chapterTitle);
  }


  private computeChapterSortKey(chapterNumber: number | string, chapterTitle?: string): number {
    return this.computeCanonicalChapterKey(chapterNumber, chapterTitle).sortKey;
  }

  private async handleImportChapter(
    job: QueueJob,
    isCancelled?: () => boolean,
    extraTiming?: { claimDurationMs?: number; semWaitMs?: number }
  ): Promise<void> {
    const jobStart = performance.now();
    const metaStart = performance.now();
    let sourceFetchMs = 0;
    let pageResolutionMs = 0;
    let chDownloadMs = 0;
    let chTelegramUploadMs = 0;
    let chRateLimitWaitMs = 0;
    let chDownloadSemWaitMs = 0;
    let chTelegramSemWaitMs = 0;
    let metadataLoadMs = 0;

    let {
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

    // Gracefully handle missing workMappingId (e.g. from manual gap revivals)
    if (!workMappingId) {
      let { data: wm } = await this.supabase
        .from('importer_work_mappings')
        .select('id')
        .eq('work_id', workId)
        .eq('source', job.source)
        .maybeSingle();
      if (wm?.id) {
        workMappingId = wm.id;
      }
    }

    // Checkpoint 1: Pre-flight check for staff cancellation
    if (isCancelled?.() || (await this.queue.isCancelRequested(job.id))) {
      throw new JobCancelledByStaffError(job.id);
    }

    // Pre-flight check: if already published by concurrent worker, skip download
    let { data: alreadyPub } = await this.supabase
      .from('chapters')
      .select('id, number, title')
      .eq('work_id', workId)
      .eq('number', chapterNumber)
      .not('published_at', 'is', null)
      .maybeSingle();

    if (alreadyPub && job.payload.readerRepair !== true) {
      this.logger.info('Chapter already published by concurrent source, linking mapping and skipping duplicate download', {
        workId,
        chapterNumber,
        source: job.source,
        canonicalChapterId: alreadyPub.id,
      });
      const sortKey = this.computeChapterSortKey(chapterNumber, chapterTitle);
      await this.supabase.from('importer_chapter_mappings').upsert(
        {
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
        },
        { onConflict: 'source,source_chapter_id' }
      );

      // Auto-cancel any other sibling QUEUED/RETRY jobs for this work + chapter
      try {
        const pool = getYugabytePool();
        await pool.query(`
          UPDATE importer_queue
          SET status = 'COMPLETED',
              updated_at = NOW(),
              last_error = 'CANONICAL_ALREADY_SATISFIED'
          WHERE task_type = 'IMPORT_CHAPTER'
            AND status IN ('QUEUED', 'RETRY')
            AND chapter_sort_key = $1
            AND (payload->>'workId') = $2;
        `, [sortKey, workId]);
      } catch {}

      return;
    }

    let effectiveSource = job.source;
    let effectiveSourceChapterId = sourceChapterId;
    let effectiveWorkMappingId = workMappingId;
    const initialSource = job.source;



    // Detect Source / ID format mismatch
    // (e.g. MangaFlix job with numeric Manhastro/Kuro chapter ID)
    if (effectiveSource === 'mangaflix' && /^\d+$/.test(effectiveSourceChapterId)) {
      let { data: realMapping } = await this.supabase
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
    let { data: existingChapter } = await this.supabase
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
    // TELEMETRY PATCH
    const telemetry = {
      jobId: job.id,
      source: effectiveSource,
      workId: workId.substring(0,8),
      chapter: chapterNumber,
      tStart,
      tDownloadStart: 0,
      tDownloadEnd: 0,
      tUploadStart: 0,
      tUploadEnd: 0,
      tStaged: 0,
      tPublished: 0,
      totalBytesDown: 0,
      totalBytesUp: 0,
      pages: 0
    };
    let tDownload = 0;
    let tUpload = 0;
    let tDb = 0;
    let totalBytes = 0;

    let successfulExecution = false;
    let lastRescuedError: string | null = null;
    let validPages: Array<{ mediaId: string; width: number; height: number }> = [];
    let skipDownloadDueToExistingPages = false;
    const targetChapterId = existingChapter?.id || crypto.randomUUID();

    // Dynamic candidate fallbacks resolution across payload, mappings, manifest, and work sources
    const candidateFallbacks = await this.resolveDynamicCandidateFallbacks(
      workId,
      chapterNumber,
      effectiveSource,
      (job.payload?.fallbackSources as any) || []
    );

    let allSourceCandidates = [
      { source: effectiveSource, sourceChapterId: effectiveSourceChapterId, mappingId: effectiveWorkMappingId },
      ...candidateFallbacks,
    ];

    // If primary source is UPSTREAM_BLOCKED, skip directly to first healthy fallback
    let { data: primarySrc } = await this.supabase
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

        let pageUrls: string[] = [];
        let primaryError: Error | null = null;
        try {
          pageUrls = await callProvider(() => adapter.fetchChapterPages(effectiveSourceChapterId, chapterNumber));
        } catch (adapterErr: any) {
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
              throw Object.assign(new Error(`Source ${effectiveSource} failed to return pages for chapter ${chapterNumber} (${effectiveSourceChapterId})${detail}`), { sourceStage: 'provider' });
            } else {
              const detail = primaryError?.message ? ` (Primary error: ${primaryError.message})` : '';
              throw Object.assign(new Error(`Failed to obtain pages for chapter ${chapterNumber} (${effectiveSourceChapterId}) across primary and fallback sources [${allSourceCandidates.map(c => c.source).join(', ')}]${detail}`), { sourceStage: 'provider' });
            }
          }
          continue;
        }

        const expectedCount = pageUrls.length;
        this.supabase.from('importer_queue').update({
          progress_total: expectedCount,
          progress_stage: 'DOWNLOADING',
          progress_current: 0,
        }).eq('id', job.id).then(() => {}, () => {});

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
        if (existingChapter && job.payload.readerRepair !== true) {
          let { data: existingPages } = await this.supabase
            .from('pages')
            .select('position, media_id, width, height')
            .eq('chapter_id', existingChapter.id)
            .order('position', { ascending: true });

          if (
            existingPages &&
            existingPages.length === expectedCount &&
            existingPages.every((p) => Boolean(p.media_id))
          ) {
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

        const storedPages: Array<{ mediaId: string; width: number; height: number } | null> = new Array(
          expectedCount
        ).fill(null);
        const mediaRecordsToInsert: Record<string, any>[] = [];

        // Per-source page download concurrency & priority boost
        const baseSourcePageConcurrency = this.autotuner.getSourcePageConcurrency(effectiveSource);
        const downloadConcurrency = isPriority
          ? Math.min(12, Math.max(8, baseSourcePageConcurrency * 2))
          : Math.min(baseSourcePageConcurrency, this.config.BATCH_PAGE_DOWNLOAD_CONCURRENCY || 8);

        // Upload pool concurrency: up to 6, bounded by autotuner and globalMediaSemaphore
        const uploadConcurrency = Math.min(6, Math.max(2, Math.floor(this.autotuner.getCurrentConcurrency() / 2)));
        const globalMediaSemaphore = this.autotuner.getGlobalMediaSemaphore();
        const globalInflightRequestSemaphore = this.autotuner.getGlobalInflightRequestSemaphore();

        const bufferedPageSemaphore = this.autotuner.getBufferedPageSemaphore();
        const bufferedWaitAbort = new AbortController();
        interface DownloadedPage {
          index: number;
          releaseBuffer: () => void;
          pageBytes: Uint8Array;
        }

        const readyQueue: DownloadedPage[] = [];
        let nextDownloadIndex = 0;
        let allDownloadsFinished = false;
        let pipelineError: Error | null = null;
        let manifestRefreshed = false;
        let failed404Count = 0;
        const consumerResolvers: Array<() => void> = [];

        const notifyConsumer = () => {
          if (pipelineError || this.stopSignal || isCancelled?.()) {
            bufferedWaitAbort.abort();
            while (readyQueue.length) {
              const discarded = readyQueue.shift()!;
              this.autotuner.releaseActiveBufferedBytes(discarded.pageBytes.length);
              ImporterEngine.activeBufferedBytes = this.autotuner.getBufferedBytes();
              discarded.releaseBuffer();
            }
          }
          while (consumerResolvers.length > 0) {
            const resolve = consumerResolvers.shift();
            if (resolve) resolve();
          }
        };

        const waitForPage = (): Promise<void> => {
          if (readyQueue.length > 0 || allDownloadsFinished || pipelineError || this.stopSignal || isCancelled?.()) {
            return Promise.resolve();
          }
          return new Promise<void>((resolve) => {
            const timer = setTimeout(() => {
              const idx = consumerResolvers.indexOf(onResolve);
              if (idx !== -1) consumerResolvers.splice(idx, 1);
              resolve();
            }, 3000);
            const onResolve = () => {
              clearTimeout(timer);
              resolve();
            };
            consumerResolvers.push(onResolve);
          });
        };

        // Producer: downloads raw page bytes from source CDN into memory
        const producer = async () => {
          try {
            while (!this.stopSignal && !pipelineError && !isCancelled?.()) {
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
            const rl0 = performance.now();
            await this.rateLimiter.acquire(parsedUrl.host);
            chRateLimitWaitMs += (performance.now() - rl0);

            const buf0 = performance.now();
            let reservation = await this.autotuner.reserveBufferBudget(
              2.0 * 1024 * 1024,
              AbortSignal.any([this.abortController.signal, bufferedWaitAbort.signal])
            );
            await bufferedPageSemaphore.acquire(AbortSignal.any([this.abortController.signal, bufferedWaitAbort.signal]));
            chDownloadSemWaitMs += (performance.now() - buf0);
            let bufferTransferred = false;
            let reservationCommitted = false;
            let pageBytes: Uint8Array | null = null;
            try {
            if (this.stopSignal || pipelineError || isCancelled?.()) break;
            let attempts = 0;
            let lastErr: any = null;
            let currentUrl = pageUrls[idx] || pageUrl;

            const d0 = Date.now();
            while (attempts < 4 && !this.stopSignal && !pipelineError) {
              attempts++;
              const timeoutMs = attempts === 1 ? 15_000 : attempts === 2 ? 25_000 : 35_000;
              const freshConnection = attempts >= 2;

              // On attempt 3: refresh manifest before download to check for updated CDN tokens or corrected URLs
              if (attempts === 3 && !manifestRefreshed) {
                manifestRefreshed = true;
                try {
                  const refreshAdapter = this.registry.get(effectiveSource);
                  if (refreshAdapter && typeof refreshAdapter.fetchChapterPages === 'function') {
                    const refreshedUrls = await callProvider(() =>
                      refreshAdapter.fetchChapterPages(effectiveSourceChapterId, chapterNumber)
                    );
                    if (refreshedUrls && refreshedUrls.length === expectedCount) {
                      const isDifferent = refreshedUrls.some((u, i) => u !== pageUrls[i]);
                      if (isDifferent) {
                        this.logger.info(
                          `MANIFEST_REFRESH: Upstream manifest refreshed with updated URLs for chapter ${chapterNumber} on ${effectiveSource}`,
                          {
                            workId,
                            chapterNumber,
                            oldUrl: currentUrl,
                            newUrl: refreshedUrls[idx],
                          }
                        );
                        pageUrls = refreshedUrls;
                        currentUrl = pageUrls[idx] || currentUrl;
                      }
                    }
                  }
                } catch (refreshErr: any) {
                  this.logger.warn(
                    `MANIFEST_REFRESH failed during page retry for ${effectiveSource} ch ${chapterNumber}`,
                    { error: refreshErr?.message }
                  );
                }
              }

              try {
                const inf0 = performance.now();
                pageBytes = await globalInflightRequestSemaphore.runExclusive(async () => {
                  chDownloadSemWaitMs += (performance.now() - inf0);
                  telemetryCollector.trackActiveDownload(1);
                  try {
                    return await callProvider(() =>
                      this.fetchImageBytes(currentUrl, effectiveSource, {
                        timeoutMs,
                        freshConnection,
                        reservation,
                      })
                    );
                  } finally {
                    telemetryCollector.trackActiveDownload(-1);
                  }
                });

                const dlMs = Date.now() - d0;
                tDownload += dlMs;
                chDownloadMs += dlMs;
                telemetryCollector.recordImageDownload(effectiveSource, dlMs, pageBytes.length);
                totalBytes += pageBytes.length;
                reservation.commit(pageBytes.length);
                reservationCommitted = true;
                ImporterEngine.activeBufferedBytes = this.autotuner.getBufferedBytes();

                if (attempts > 1) {
                  const autoRecoverReason = attempts === 2
                    ? 'TRANSIENT_TIMEOUT_RECOVERED'
                    : 'IMAGE_URL_REFRESHED';
                  this.logger.info(`AUTO_RECOVERED: Page ${idx + 1}/${expectedCount} recovered on attempt ${attempts}`, {
                    reason: autoRecoverReason,
                    source: effectiveSource,
                    chapterNumber,
                    pageIndex: idx,
                  });
                }
                break;
              } catch (err: any) {
                lastErr = err;
                telemetryCollector.recordDownloadError(attempts < 4);
                if (err instanceof InvalidMediaError) {
                  break;
                }
                if (attempts < 4 && !this.stopSignal && !pipelineError) {
                  if (reservation.isReleased) {
                    reservation = await this.autotuner.reserveBufferBudget(
                      2.0 * 1024 * 1024,
                      AbortSignal.any([this.abortController.signal, bufferedWaitAbort.signal])
                    );
                  }
                  await this.sleep(400 * attempts);
                }
              }
            }

            if (!pageBytes) {
              const errMsg = (lastErr instanceof Error && lastErr.message) ? lastErr.message : (lastErr ? String(lastErr) : 'Erro desconhecido');
              const is404 = errMsg.includes('HTTP 404') || errMsg.includes('status: 404');
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
              pipelineError = (lastErr instanceof InvalidMediaError || errMsg.includes('Formato não permitido') || errMsg.includes('INVALID_MEDIA'))
                ? lastErr
                : new NarrativePageUnavailableError(
                    effectiveSource,
                    idx,
                    expectedCount,
                    errMsg
                  );
              notifyConsumer();
              break;
            }

            readyQueue.push({ index: idx, pageBytes, releaseBuffer: () => bufferedPageSemaphore.release() });
            bufferTransferred = true;
            notifyConsumer();
            } finally {
              if (!bufferTransferred) {
                if (reservationCommitted && pageBytes) {
                  this.autotuner.releaseActiveBufferedBytes(pageBytes.length);
                  ImporterEngine.activeBufferedBytes = this.autotuner.getBufferedBytes();
                } else if (!reservationCommitted) {
                  reservation.release();
                }
                bufferedPageSemaphore.release();
              }
            }
          }
        } catch (err: any) {
          if (!pipelineError) pipelineError = err;
        } finally {
          notifyConsumer();
        }
      };

      let completedUploadsCount = 0;

      // Consumer: uploads downloaded pages to Storage Bridge / Telegram concurrently
      const consumer = async () => {
        try {
          while (!this.stopSignal && !pipelineError && !isCancelled?.()) {
            if (isCancelled?.()) {
              pipelineError = new JobCancelledByStaffError(job.id);
              break;
            }

            while (readyQueue.length === 0) {
              if (allDownloadsFinished || pipelineError || this.stopSignal || isCancelled?.()) {
                return;
              }
              await waitForPage();
            }

            if (isCancelled?.()) {
              pipelineError = new JobCancelledByStaffError(job.id);
              break;
            }

            const item = readyQueue.shift();
            if (!item) continue;

            let pageBytes: Uint8Array | null = item.pageBytes;
            let uploadDuration = 0;
            try {
              const uWait0 = performance.now();
              const res = await globalMediaSemaphore.runExclusive(async () => {
                const uWaitMs = performance.now() - uWait0;
                chTelegramSemWaitMs += uWaitMs;
                telemetryCollector.recordTelegramSemaphoreWait(uWaitMs);

                const u0 = performance.now();
                telemetryCollector.trackActiveTelegramUpload(1);
                try {
                  return await processAndStoreMedia(
                    this.supabase,
                    this.storage,
                    pageBytes!,
                    botUserId,
                    'editorial',
                    targetChapterId,
                    { skipDbInsert: true, skipDedupLookup: !job.payload?.readerRepair }
                  );
                } finally {
                  telemetryCollector.trackActiveTelegramUpload(-1);
                  uploadDuration = performance.now() - u0;
                  tUpload += uploadDuration;
                  chTelegramUploadMs += uploadDuration;
                  telemetryCollector.recordTelegramUpload(uploadDuration, pageBytes!.length);
                }
              });

              if (typeof (this.storage as any).getRateLimiter === 'function') {
                const limiter = (this.storage as any).getRateLimiter();
                if (typeof limiter.recordSuccess === 'function') {
                  limiter.recordSuccess(pageBytes.length, uploadDuration);
                }
              }

              storedPages[item.index] = {
                mediaId: res.mediaId,
                width: res.width,
                height: res.height,
              };

              if (res.mediaRecord && !res.reused) {
                mediaRecordsToInsert.push(res.mediaRecord);
              }

              completedUploadsCount++;
              diagnostics.updateJobProgress(job.id, completedUploadsCount);
            } catch (err: any) {
              if (!pipelineError) pipelineError = err;
              this.logger.error(`Failed to upload page ${item.index + 1}/${expectedCount}`, { error: err?.message });
              notifyConsumer();
              break;
            } finally {
              if (pageBytes) {
                this.autotuner.releaseActiveBufferedBytes(pageBytes.length);
                ImporterEngine.activeBufferedBytes = this.autotuner.getBufferedBytes();
                pageBytes = null;
              }
              item.releaseBuffer();
            }
          }
        } catch (err: any) {
          if (!pipelineError) pipelineError = err;
        } finally {
          notifyConsumer();
        }
      };

        telemetry.tDownloadStart = Date.now();
        const producerPromises = Array.from({ length: downloadConcurrency }, () => producer());
        telemetry.tUploadStart = Date.now();
        const consumerPromises = Array.from({ length: uploadConcurrency }, () => consumer());

        try {
          await Promise.all(producerPromises);
          telemetry.tDownloadEnd = Date.now();
          allDownloadsFinished = true;
          notifyConsumer();

          await Promise.all(consumerPromises);
          telemetry.tUploadEnd = Date.now();
          telemetry.totalBytesDown = totalBytes;
          telemetry.pages = expectedCount;
        } finally {
          // RAII Cleanup: Drain any unconsumed items left in readyQueue
          // to prevent leaking bytes into ImporterEngine.activeBufferedBytes
          while (readyQueue.length > 0) {
            const leftover = readyQueue.shift();
            if (leftover?.pageBytes) {
              this.autotuner.releaseActiveBufferedBytes(leftover.pageBytes.length);
              ImporterEngine.activeBufferedBytes = this.autotuner.getBufferedBytes();
              leftover.releaseBuffer();
            }
          }
        }

        if (pipelineError) {
          // Type assertion: TS can't track mutations from async closures (producer/consumer)
          const resolvedError = pipelineError as Error;
          const isRescueCandidate =
            resolvedError instanceof NarrativePageUnavailableError ||
            resolvedError instanceof InvalidMediaError ||
            resolvedError.name === 'InvalidMediaError' ||
            resolvedError.message.includes('Formato não permitido') ||
            resolvedError.message.includes('INVALID_MEDIA') ||
            resolvedError.message.includes('IMAGE_TIMEOUT') ||
            resolvedError.message.includes('Cloudflare blocked') ||
            resolvedError.message.includes('ProviderDownloadError');

          // If image/media error occurred and we have remaining candidate sources, rescue entire chapter!
          if (isRescueCandidate && candidateIdx < allSourceCandidates.length - 1) {
            lastRescuedError = resolvedError.message;
            this.logger.warn(
              `CROSS_PROVIDER_RESCUE: Page failure on ${effectiveSource} for ch ${chapterNumber} (${resolvedError.message}). Rescuing entire chapter cleanly from ${allSourceCandidates[candidateIdx + 1].source}.`,
              {
                failedSource: effectiveSource,
                reason: resolvedError.message,
                nextCandidateSource: allSourceCandidates[candidateIdx + 1].source,
              }
            );
            continue;
          }

          // If narrative page was unavailable with no candidate left:
          if (resolvedError instanceof NarrativePageUnavailableError) {
            // If permanent 404 or multiple attempts, mark as permanent gap
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
                    is_gap: false,
                    last_error: gapReason,
                    updated_at: new Date().toISOString(),
                  })
                  .eq('work_id', workId)
                  .eq('chapter_number', chapterNumber);
              } catch {}

              // Update chapter manifest
              try {
                await this.supabase
                  .from('importer_chapter_manifest')
                  .update({
                    status: 'UNRESOLVED_GAP',
                    is_gap: false,
                    gap_reason: 'PERMANENT_404_UNRESOLVED',
                    last_error: gapReason,
                    last_checked_at: new Date().toISOString(),
                  })
                  .eq('work_id', workId)
                  .eq('chapter_number', chapterNumber);
              } catch {}

              const gapErr = new Error(`[PERMANENT_404_UNRESOLVED] ${gapReason}: ${resolvedError.message}`);
              (gapErr as any).sourceStage = 'provider';
              throw gapErr;
            }
          }
          throw resolvedError;
        }

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

        validPages = storedPages.filter(
          (p): p is { mediaId: string; width: number; height: number } =>
            Boolean(p && p.mediaId && p.mediaId !== '__SKIPPED_NON_CONTENT_PAGE__')
        );

        if (validPages.length === 0) {
          throw new Error(`Chapter ${chapterNumber} contains 0 valid content pages`);
        }

        // Batch insert media records for the entire chapter in a single round-trip
        if (mediaRecordsToInsert.length > 0) {
          const { error: mediaErr } = await this.supabase
            .from('media')
            .upsert(mediaRecordsToInsert, { onConflict: 'id', ignoreDuplicates: true });
          if (mediaErr) {
            this.logger.error('Failed to batch insert media records', { error: mediaErr.message, count: mediaRecordsToInsert.length });
            throw new Error(`Media batch registration failed: ${mediaErr.message}`);
          }
        }

        successfulExecution = true;
        this.autotuner.recordSourceSuccess(effectiveSource);
        this.circuitBreaker.recordSuccess(effectiveSource);
        if (this.sourceStatusCache.get(effectiveSource)?.status === 'DEGRADED') {
          this.sourceStatusCache.delete(effectiveSource);
          this.supabase
            .from('importer_sources')
            .update({ status: 'ACTIVE', blocked_reason: null, updated_at: new Date().toISOString() })
            .eq('id', effectiveSource)
            .then(undefined, () => {});
        }
        telemetry.tStaged = Date.now();
        this.logger.info('TELEMETRY_JOB_STAGED', telemetry);
        this.supabase.from('importer_queue').update({
          payload: { ...job.payload, telemetry }
        }).eq('id', job.id).then(undefined, () => {});
        break;
      }

      if (!successfulExecution) {
        throw new Error(`Failed to import chapter ${chapterNumber} across all candidates${lastRescuedError ? ': ' + lastRescuedError : ''}`);
      }

      // Ensure work has a valid cover with storage_ready = true before publishing chapter (measured separately)
      const cover0 = performance.now();
      let tCoverCheck = 0;
      if (!this.knownCoveredWorks.has(workId)) {
        try {
          const botUserId = await this.resolveBotUserId();
          const coverId = await this.ensureWorkHasCover(workId, botUserId);
          if (coverId) {
            this.knownCoveredWorks.add(workId);
          } else {
            // Cooldown so subsequent chapters of this work don't repeatedly crawl sibling sources
            this.knownCoveredWorks.add(workId);
            setTimeout(() => this.knownCoveredWorks.delete(workId), 10 * 60 * 1000);
          }
        } catch (coverCheckErr: any) {
          this.logger.warn('Non-fatal error verifying work cover before chapter publish', {
            workId,
            error: coverCheckErr?.message,
          });
        }
      }
      tCoverCheck = Math.round(performance.now() - cover0);

      const db0 = Date.now();

      // Step 1: Find or create chapter record in public.chapters
      const chUpsert0 = performance.now();
      let chapterId: string;
      if (existingChapter) {
        chapterId = existingChapter.id;
        if (chapterTitle) {
          await this.supabase
            .from('chapters')
            .update({ title: chapterTitle.slice(0, 200) })
            .eq('id', chapterId);
        }
      } else {
        chapterId = targetChapterId;
        let { error: chErr } = await this.supabase.from('chapters').insert({
          id: chapterId,
          work_id: workId,
          number: chapterNumber,
          title: (chapterTitle || '').slice(0, 200),
          origin: 'IMPORTER',
        });
        if (chErr) {
          if (chErr.code === '23505' || chErr.message?.includes('violates unique constraint')) {
            let { data: raceCh } = await this.supabase
              .from('chapters')
              .select('id')
              .eq('work_id', workId)
              .eq('number', chapterNumber)
              .maybeSingle();
            if (raceCh) {
              chapterId = raceCh.id;
            } else {
              throw chErr;
            }
          } else {
            throw chErr;
          }
        }
      }
      const tChapterUpsert = Math.round(performance.now() - chUpsert0);

      // SAFEGUARD 0: Re-check if published by another worker during download (only possible if existing chapter)
      if (existingChapter) {
        let { data: latePubCheck } = await this.supabase
          .from('chapters')
          .select('published_at')
          .eq('id', chapterId)
          .maybeSingle();

        if (latePubCheck?.published_at && job.payload.readerRepair !== true) {
          this.logger.info('Chapter published by concurrent worker during download, skipping upsert', { chapterId });
          await this.supabase.from('importer_chapter_mappings').upsert(
            {
              source: job.source,
              source_chapter_id: sourceChapterId,
              chapter_id: chapterId,
              work_id: workId,
              work_mapping_id: workMappingId,
              chapter_number: chapterNumber,
              chapter_sort_key: this.computeChapterSortKey(chapterNumber, chapterTitle),
              is_page_provider: false,
              status: 'COMPLETED',
              last_error: null,
            },
            { onConflict: 'source,source_chapter_id' }
          );
          return;
        }
      }

      // Step 2: SAFEGUARD 1: Batch upsert into public.pages ONLY after ALL pages are verified
      const pagesRpc0 = performance.now();
      if (!skipDownloadDueToExistingPages) {
        const pagesToUpsert = validPages.map((p, idx) => ({
          chapter_id: chapterId,
          position: idx + 1,
          media_id: p.mediaId,
          width: p.width,
          height: p.height,
        }));

        let { error: pageErr } = await this.supabase.rpc('importer_replace_pages', {
          p_chapter_id: chapterId,
          p_pages: pagesToUpsert,
        });

        if (pageErr) throw pageErr;
      }
      const tPagesRpc = Math.round(performance.now() - pagesRpc0);

      // Step 3: SAFEGUARD 2: Stage chapter with published_at = NULL and update queue concurrently
      const stage0 = performance.now();
      const chKey = this.computeCanonicalChapterKey(chapterNumber, chapterTitle);
      const queueProgressUpdate: Record<string, any> = {
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

      await Promise.all([
        this.publicationBarrier.stageChapter({
          workId,
          chapterId,
          chapterNumber,
          sortKey: chKey.sortKey,
          source: effectiveSource,
          sourceChapterId: effectiveSourceChapterId,
          workMappingId: effectiveWorkMappingId,
          pageCount: validPages.length,
          isPageProvider: true,
        }),
        this.supabase.from('importer_queue').update(queueProgressUpdate).eq('id', job.id),
      ]);

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
      const tStage = Math.round(performance.now() - stage0);

      // Step 4: SAFEGUARD 3: Try to publish immediately 1x via barrier. If blocked, release worker slot immediately!
      const isFreshRelease = Boolean(job.payload?.isFreshRelease);
      const pubResult = await this.publicationBarrier.tryPublish(workId, chKey.sortKey, chapterId, isFreshRelease);

      tDb = Date.now() - db0;
      const tBarrierCheck = Math.round(pubResult.timings?.barrierCheckMs || 0);
      const tPublishUpdate = Math.round(pubResult.timings?.publishUpdateMs || 0);
      const tCascade = Math.round(pubResult.timings?.cascadeMs || 0);

      this.logger.info(`[DB_DIAGNOSTIC] ${effectiveSource} ch ${chapterNumber}: db_total=${tDb}ms (cover=${tCoverCheck}ms, ch_upsert=${tChapterUpsert}ms, pages_rpc=${tPagesRpc}ms, stage=${tStage}ms, barrier=${tBarrierCheck}ms, pub_update=${tPublishUpdate}ms, cascade=${tCascade}ms)`);

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

      const chTotalDuration = performance.now() - jobStart;
      telemetryCollector.recordChapterMetric({
        jobId: job.id,
        source: effectiveSource,
        chapterNumber: Number(chapterNumber),
        pageCount: validPages.length,
        totalBytes,
        totalDurationMs: Math.round(chTotalDuration),
        claim_acquire_ms: Math.round(extraTiming?.claimDurationMs || 0),
        metadata_load_ms: Math.round(metadataLoadMs),
        source_fetch_ms: Math.round(sourceFetchMs),
        page_resolution_ms: Math.round(pageResolutionMs),
        download_ms: Math.round(chDownloadMs),
        telegram_upload_ms: Math.round(chTelegramUploadMs),
        db_wait_ms: 0,
        db_publish_ms: Math.round(tDb),
        rate_limit_wait_ms: Math.round(chRateLimitWaitMs),
        semaphore_wait_ms: Math.round(extraTiming?.semWaitMs || 0) + Math.round(chDownloadSemWaitMs) + Math.round(chTelegramSemWaitMs),
        other_wait_ms: Math.max(0, Math.round(chTotalDuration - (sourceFetchMs + chDownloadMs + chTelegramUploadMs + tDb))),
        timestamp: new Date().toISOString(),
      });

      this.lastProgressTimestamp = Date.now();
      if (pubResult.published) {
        this.logger.info('Successfully imported and published chapter in canonical order', {
          workId,
          chapterNumber,
          sortKey: chKey.sortKey,
          pageCount: validPages.length,
        });

        if (Boolean(job.payload?.staffRequested)) {
          await this.checkStaffRequestCompletion(workId);
        }
      } else {
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
        } catch {
          // Non-blocking
        }
      }
    } catch (err: any) {
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
    } finally {
      if (Boolean(job.payload?.staffRequested)) {
        this.rateLimiter.setTurboMode(false);
      }
      diagnostics.unregisterJob(job.id);
    }
  }

  private async recordJobMetric(metric: {
    workerId: string;
    source: string;
    workId: string;
    chapterId: string | null;
    chapterNumber: number;
    pageCount: number;
    totalBytes: number;
    durationMs: number;
    downloadMs: number;
    uploadMs: number;
    dbMs: number;
    status: string;
    errorMessage?: string;
  }): Promise<void> {
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
    } catch (err: any) {
      this.logger.warn('Failed to record job metric to Supabase (non-fatal)', { error: err?.message });
    }
  }

  private async recordTelemetrySnapshot(snapshot: {
    workerId: string;
    rssMb: number;
    heapUsedMb: number;
    heapTotalMb: number;
    externalMb: number;
    arrayBuffersMb: number;
    eventLoopLagMs: number;
    concurrency: number;
    activeJobs: number;
    cycleAction: string;
    cycleReason: string;
  }): Promise<void> {
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
    } catch (err: any) {
      this.logger.warn('Failed to record telemetry snapshot to Supabase (non-fatal)', { error: err?.message });
    }
  }

  private async pruneTelemetry(): Promise<void> {
    try {
      await this.supabase.rpc('importer_prune_telemetry', {
        p_telemetry_hours: 24,
        p_job_metrics_days: 7,
      });
      this.logger.info('Pruned old importer telemetry and job metrics');
    } catch (err: any) {
      this.logger.warn('Failed to prune telemetry (non-fatal)', { error: err?.message });
    }
  }

  private sanitizeErrorMessage(raw: string): string {
    if (!raw || raw.trim() === '' || raw === 'undefined') return '';
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

  public async downloadAndRegisterImage(
    url: string,
    userId: string,
    purpose: string = 'editorial',
    source: string = 'unknown'
  ): Promise<string> {
    const parsedUrl = new URL(url);
    if (purpose === 'editorial') {
      const isChapterPattern = /\/(chapter|capitulo|reader|leitor|page|pagina|paginas)\b|_page_\d+/i.test(parsedUrl.pathname);
      if (isChapterPattern) {
        throw new Error(`Rejected cover URL matching chapter pattern: ${url}`);
      }
    }
    await this.rateLimiter.acquire(parsedUrl.host);
    const bytes = await this.fetchImageBytes(url, source);
    if (bytes.length < 1500) {
      throw new Error(`Downloaded image is too small (${bytes.length} bytes), likely a placeholder or spacer: ${url}`);
    }
    if (purpose === 'editorial' && bytes.length > 10_000_000) {
      throw new Error(`Cover image too large (${bytes.length} bytes), max 10MB allowed: ${url}`);
    }
    const res = await processAndStoreMedia(this.supabase, this.storage, bytes, userId, purpose);
    if (res.width <= 50 || res.height <= 50) {
      throw new Error(`Downloaded image dimensions are too small (${res.width}x${res.height}), likely a placeholder: ${url}`);
    }
    if (purpose === 'editorial') {
      if (res.mime === 'image/gif') {
        throw new Error(`Animated GIF rejected as cover image: ${url}`);
      }
      if (res.width < 100 || res.height < 140) {
        throw new Error(`Cover dimensions too small (${res.width}x${res.height}): ${url}`);
      }
      const ratio = res.height / res.width;
      const invRatio = res.width / res.height;
      if (ratio > 2.5 || invRatio > 2.5) {
        throw new Error(`Cover aspect ratio rejected (${res.width}x${res.height}, ratio=${ratio.toFixed(2)}): ${url}`);
      }
    }
    return res.mediaId;
  }

  public async fetchImageBytes(
    url: string,
    source: string = 'unknown',
    options?: {
      timeoutMs?: number;
      freshConnection?: boolean;
      refererOverride?: string;
      reservation?: BufferReservation;
    }
  ): Promise<Uint8Array> {
    const parsedUrl = new URL(url);
    const isKuro = parsedUrl.host.includes('kuromangas.com');

    const referer = options?.refererOverride
      ? options.refererOverride
      : isKuro
      ? 'https://kuromangas.com/'
      : `${parsedUrl.origin}/`;

    let customHeaders: Record<string, string> = {};
    if (source && source !== 'unknown') {
      try {
        const adapter = this.registry.get(source);
        if (adapter && typeof adapter.getImageHeaders === 'function') {
          const resH = await adapter.getImageHeaders(url);
          if (resH) customHeaders = resH;
        }
      } catch {
        // Non-blocking
      }
    }

    const timeoutDuration = Math.min(40_000, Math.max(10_000, options?.timeoutMs || 15_000));
    const requestHeaders: Record<string, string> = {
      'User-Agent':
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
      Referer: referer,
      ...customHeaders,
    };

    if (options?.freshConnection) {
      requestHeaders['Connection'] = 'close';
    }

    let res: Response | null = null;
    let fetchError: any = null;

    let attempts = 0;
    const maxAttempts = 3;
    while (attempts < maxAttempts) {
      attempts++;
      fetchError = null;
      try {
        res = await fetch(url, {
          headers: requestHeaders,
          signal: AbortSignal.timeout(timeoutDuration),
        });
        if (res.ok) break;
        if (res.status === 429) {
          const retryAfter = res.headers.get('Retry-After');
          this.rateLimiter.handle429(parsedUrl.host, retryAfter);
          break;
        }
        if (res.status >= 500 && res.status <= 504 && attempts < maxAttempts) {
          await new Promise((r) => setTimeout(r, 400 * attempts));
          continue;
        }
        break;
      } catch (err: any) {
        fetchError = err;
        if (
          attempts < maxAttempts &&
          (err?.name === 'TimeoutError' ||
            err?.name === 'AbortError' ||
            err?.message?.includes('fetch failed') ||
            err?.message?.includes('network'))
        ) {
          await new Promise((r) => setTimeout(r, 400 * attempts));
          continue;
        }
        break;
      }
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
          signal: AbortSignal.timeout(timeoutDuration),
        });

        if (bridgeRes.ok) {
          this.rateLimiter.recordSuccess(parsedUrl.host);
          const bridgeLenStr = bridgeRes.headers.get('content-length');
          if (bridgeLenStr) {
            const bridgeBytes = parseInt(bridgeLenStr, 10);
            if (!Number.isNaN(bridgeBytes)) {
              if (bridgeBytes > 20 * 1024 * 1024) {
                await bridgeRes.body?.cancel().catch(() => {});
                throw new InvalidMediaError(url, `Bridge image declared content-length exceeds 20MB limit`);
              }
              if (options?.reservation && bridgeBytes > options.reservation.reservedBytes) {
                await options.reservation.upgrade(bridgeBytes);
              }
            }
          }
          return await readImageBody(bridgeRes, { reservation: options?.reservation });
        }
      } catch (bridgeErr: any) {
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

    // Inspect declared Content-Length and enforce 20MB upper ceiling + reservation upgrade
    const declaredLengthStr = res.headers.get('content-length');
    if (declaredLengthStr) {
      const declaredBytes = parseInt(declaredLengthStr, 10);
      if (!Number.isNaN(declaredBytes)) {
        if (declaredBytes > 20 * 1024 * 1024) {
          await res.body?.cancel().catch(() => {});
          throw new InvalidMediaError(url, `Image declared content-length (${Math.round(declaredBytes / 1024 / 1024)}MB) exceeds maximum safe limit of 20MB`);
        }
        if (options?.reservation && declaredBytes > options.reservation.reservedBytes) {
          await options.reservation.upgrade(declaredBytes);
        }
      }
    }

    const uint8 = await readImageBody(res, { reservation: options?.reservation });

    // Validate binary image integrity and check for fake HTML challenge pages returned with HTTP 200
    const bodySnippet = new TextDecoder('utf-8', { fatal: false }).decode(uint8.slice(0, 8192));
    const imgInsp = CloudflareClassifier.inspect(res.status, res.headers, bodySnippet, {
      url,
      expectedType: 'image',
      buffer: uint8,
      isIsolatedRequest: false,
    });

    if (!imgInsp.isValidImage || imgInsp.isBlocked || imgInsp.isChallenge || uint8.byteLength === 0) {
      const cls = imgInsp.classification || 'IMAGE_CDN_BLOCK';
      this.circuitBreaker.recordFailure(source, cls, parsedUrl.host);

      if (imgInsp.isChallenge || imgInsp.isFakeContent) {
        throw new InvalidMediaError(
          url,
          source,
          `Cloudflare challenge HTML received instead of image (${cls}): ${imgInsp.reason}`
        );
      }

      if (!imgInsp.isValidImage) {
        throw new InvalidMediaError(
          url,
          source,
          `Invalid image format: magic bytes do not match PNG, JPEG, WebP, GIF or AVIF`
        );
      }

      throw new ProviderDownloadError(
        res.status === 200 ? 403 : res.status,
        url,
        source,
        `Cloudflare blocked image download (${cls}): ${imgInsp.reason}`
      );
    }

    this.rateLimiter.recordSuccess(parsedUrl.host);
    this.circuitBreaker.recordSuccess(source, parsedUrl.host);
    return uint8;
  }

  public async tryRawMetadataCoverFallback(
    raw: Record<string, any>,
    botUserId: string,
    source: string,
    excludeUrl?: string | null
  ): Promise<string | null> {
    const candidateUrls = [
      raw?.poster?.default_url,
      raw?.poster?.large_url,
      raw?.poster?.url,
      raw?.cover_url,
      raw?.coverUrl,
      raw?.thumbnail,
      raw?.thumbnail_url,
      raw?.imagem,
      raw?.image,
      raw?.banner_imagem,
    ].filter(
      (u): u is string =>
        typeof u === 'string' &&
        u.length > 5 &&
        u.startsWith('http') &&
        u !== excludeUrl
    );

    for (const url of candidateUrls) {
      try {
        const mediaId = await this.downloadAndRegisterImage(url, botUserId, 'editorial', source);
        this.logger.info('Recovered cover from raw metadata fallback', { source, url, mediaId });
        return mediaId;
      } catch {
        // Continue trying next candidate
      }
    }
    return null;
  }

  public async trySiblingMappingCoverFallback(
    workId: string | undefined,
    excludeSource: string,
    slugOrTitle: string,
    botUserId: string
  ): Promise<string | null> {
    try {
      let targetWorkId = workId;
      if (!targetWorkId && slugOrTitle) {
        const cleanSlug = slugOrTitle.toLowerCase().replace(/[^a-z0-9]+/g, '-');
        const { data: matched } = await this.supabase
          .from('works')
          .select('id')
          .or(`slug.eq.${cleanSlug},title.eq.${slugOrTitle}`)
          .maybeSingle();
        targetWorkId = matched?.id;
      }

      if (!targetWorkId) return null;

      const { data: mappings } = await this.supabase
        .from('importer_work_mappings')
        .select('source, source_work_id, metadata')
        .eq('work_id', targetWorkId)
        .neq('source', excludeSource);

      if (!mappings || mappings.length === 0) return null;

      for (const m of mappings) {
        if (m.metadata) {
          const recovered = await this.tryRawMetadataCoverFallback(m.metadata, botUserId, m.source);
          if (recovered) return recovered;
        }

        try {
          const siblingAdapter = this.registry.get(m.source);
          if (siblingAdapter && typeof siblingAdapter.fetchWorkDetails === 'function') {
            const sibDetails = await callProvider(() => siblingAdapter.fetchWorkDetails(m.source_work_id));
            if (sibDetails?.coverUrl) {
              const mediaId = await this.downloadAndRegisterImage(
                sibDetails.coverUrl,
                botUserId,
                'editorial',
                m.source
              );
              this.logger.info('Recovered cover from sibling source adapter', {
                source: m.source,
                workId: targetWorkId,
                mediaId,
              });
              return mediaId;
            }
          }
        } catch {
          // Continue to next sibling
        }
      }
    } catch (err: any) {
      this.logger.warn('Failed in sibling cover fallback resolution', { error: err?.message });
    }
    return null;
  }

  public async ensureWorkHasCover(workId: string, botUserId: string): Promise<string | null> {
    const { data: work } = await this.supabase
      .from('works')
      .select('id, cover_id, title, slug, metadata_provenance')
      .eq('id', workId)
      .maybeSingle();

    if (!work) return null;

    if (work.cover_id) {
      const { data: m } = await this.supabase
        .from('media')
        .select('id, storage_ready, bytes, width, height')
        .eq('id', work.cover_id)
        .maybeSingle();

      if (m && m.storage_ready && (m.bytes || 0) >= 1500 && (m.width || 0) >= 100 && (m.height || 0) >= 140) {
        return work.cover_id;
      }
      this.logger.warn('Work has broken/unready cover, attempting recovery', {
        workId,
        badCoverId: work.cover_id,
      });
    }

    const recoveredMediaId = await this.trySiblingMappingCoverFallback(workId, '', work.slug || work.title, botUserId);
    if (recoveredMediaId) {
      await this.supabase
        .from('works')
        .update({
          cover_id: recoveredMediaId,
          updated_at: new Date().toISOString(),
        })
        .eq('id', workId);

      this.logger.info('Repaired work cover before chapter publish', { workId, coverId: recoveredMediaId });
      return recoveredMediaId;
    }

    return null;
  }

  private async resolveDynamicCandidateFallbacks(
    workId: string,
    chapterNumber: number,
    excludeSource: string,
    payloadFallbacks: Array<{ source: string; sourceChapterId: string; mappingId?: string }> = []
  ): Promise<Array<{ source: string; sourceChapterId: string; mappingId?: string }>> {
    const candidates: Array<{ source: string; sourceChapterId: string; mappingId?: string }> = [];
    const seen = new Set<string>();

    const addCandidate = (s: string, id: string, mapId?: string) => {
      if (!s || !id || s === excludeSource) return;
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
      let { data: chMappings } = await this.supabase
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
    } catch {}

    // 3. Alternative available sources in importer_chapter_manifest
    try {
      let { data: manifestCh } = await this.supabase
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
    } catch {}

    // 4. Discover active sources in importer_work_mappings not yet in candidates
    try {
      let { data: workMappings } = await this.supabase
        .from('importer_work_mappings')
        .select('id, source, source_work_id')
        .eq('work_id', workId)
        .neq('source', excludeSource)
        .neq('sync_status', 'UNMATCHED');

      if (workMappings) {
        for (const wm of workMappings) {
          if (candidates.some((c) => c.source === wm.source)) continue;
          let { data: altSrcCheck } = await this.supabase
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
            } catch {}
          }
        }
      }
    } catch {}

    // 4b. Discover candidate fallbacks across sibling works with matching title
    try {
      const { data: currentWork } = await this.supabase
        .from('works')
        .select('title')
        .eq('id', workId)
        .maybeSingle();

      if (currentWork?.title) {
        const cleanTitle = currentWork.title.trim();
        const { data: siblingWorks } = await this.supabase
          .from('works')
          .select('id')
          .neq('id', workId)
          .ilike('title', cleanTitle);

        if (siblingWorks && siblingWorks.length > 0) {
          const siblingIds = siblingWorks.map((w: any) => w.id);
          const { data: siblingMappings } = await this.supabase
            .from('importer_work_mappings')
            .select('id, source, source_work_id')
            .in('work_id', siblingIds)
            .neq('source', excludeSource)
            .neq('sync_status', 'UNMATCHED');

          if (siblingMappings) {
            for (const wm of siblingMappings) {
              if (candidates.some((c) => c.source === wm.source)) continue;
              const altAdapter = this.registry.get(wm.source);
              if (altAdapter && typeof altAdapter.fetchChapters === 'function') {
                try {
                  const altChapters = await altAdapter.fetchChapters(wm.source_work_id);
                  const matched = altChapters.find((c) => c.number === chapterNumber);
                  if (matched && matched.sourceChapterId) {
                    addCandidate(wm.source, matched.sourceChapterId, wm.id);
                  }
                } catch {}
              }
            }
          }
        }
      }
    } catch {}

    // 5. Filter candidates against operational sources (exclude disabled, paused, or cooling down)
    const healthyCandidates: Array<{ source: string; sourceChapterId: string; mappingId?: string }> = [];
    for (const c of candidates) {
      try {
        let { data: srcCheck } = await this.supabase
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
            if (Date.now() < cd) continue;
          }
        }
        healthyCandidates.push(c);
      } catch {
        healthyCandidates.push(c);
      }
    }

    return healthyCandidates;
  }

  private cachedBotUserId: string | null = null;

  private async resolveBotUserId(): Promise<string> {
    if (this.config.IMPORTER_USER_ID) {
      return this.config.IMPORTER_USER_ID;
    }
    if (this.cachedBotUserId) {
      return this.cachedBotUserId;
    }

    let { data: adminRole } = await this.supabase
      .from('access_roles')
      .select('user_id')
      .eq('role', 'ADMIN')
      .limit(1)
      .maybeSingle();

    if (adminRole?.user_id) {
      this.cachedBotUserId = adminRole.user_id;
      return adminRole.user_id;
    }

    let { data: anyMember } = await this.supabase
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
  async checkStaffRequestCompletion(workId: string): Promise<void> {
    try {
      const staffQuery = this.supabase.from('importer_staff_requests');
      if (!staffQuery || typeof staffQuery.select !== 'function') return;

      let { data: activeRequests } = await staffQuery
        .select('id, status')
        .eq('work_id', workId)
        .in('status', ['QUEUED', 'IMPORTING', 'RETRYING']);

      if (!activeRequests || activeRequests.length === 0) return;

      // 1. Check if active jobs remain in importer_queue
      const qQuery = this.supabase.from('importer_queue');
      if (qQuery && typeof qQuery.select === 'function') {
        let { count } = await qQuery
          .select('id', { count: 'exact', head: true })
          .eq('payload->>workId', workId)
          .in('status', ['QUEUED', 'IMPORTING', 'RETRY']);

        if (count && count > 0) return;
      }

      // 2. Check if chapters remain uncompleted in importer_chapter_manifest
      const manQuery = this.supabase.from('importer_chapter_manifest');
      if (manQuery && typeof manQuery.select === 'function') {
        let { data: pendingChapters } = await manQuery
          .select('chapter_sort_key, status')
          .eq('work_id', workId)
          .in('status', ['QUEUED', 'STAGED']);

        if (pendingChapters && pendingChapters.length > 0) return;
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
    } catch (err: any) {
      this.logger.warn(`Failed to check staff request completion for work ${workId}`, { error: err?.message });
    }
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}
