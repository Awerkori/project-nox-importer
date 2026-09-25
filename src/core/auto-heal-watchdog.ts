import type { Pool } from 'pg';
import { Logger } from './logger.js';
import type { WorkAffinityScheduler } from './scheduler/work-affinity-scheduler.js';
import type { AdmissionController } from './scheduler/admission-controller.js';
import type { ProtectiveSentinel } from './protective-sentinel.js';
import type { PublicationBarrier } from './publication.js';
import { diagnostics } from './diagnostics.js';

export type ImporterHealthStatus =
  | 'HEALTHY'
  | 'DEGRADED'
  | 'STALLED'
  | 'CRITICAL_STALL'
  | 'IDLE'
  | 'PAUSED_BY_PROTECTION';

export type ProcessingHealth = 'HEALTHY' | 'DEGRADED' | 'STALLED' | 'CRITICAL_STALL';
export type PublicationHealth = 'HEALTHY' | 'DEGRADED' | 'STALLED' | 'CRITICAL_STALL' | 'NO_FRESH_EXPECTED';

export type AutoHealState =
  | 'IDLE'
  | 'MONITORING'
  | 'LEVEL_1_LIGHT_RECONCILIATION'
  | 'LEVEL_2_STUCK_STATE_AUDIT'
  | 'LEVEL_3_RESTART_PENDING'
  | 'CIRCUIT_OPEN'
  | 'RECOVERED';

export interface HealthPanelMetrics {
  status: ImporterHealthStatus;
  autoHealState: AutoHealState;
  processingHealth: ProcessingHealth;
  publicationHealth: PublicationHealth;
  lastStartedAgeSec: number;
  lastCompletedAgeSec: number;
  lastFreshVisibleAgeSec: number;
  startedLast15m: number;
  completedLast15m: number;
  freshLast15m: number;
  eligibleJobs: number;
  claimableWorks: number;
  activeWorksCount: number;
  zombieWorksCount: number;
  importingCount: number;
  retryCount: number;
  stagedUnique: number;
  publishableStaged: number;
  waitingPredecessorStaged: number;
  stuckStaged: number;
  recentCorrelatedBreakdown?: {
    alreadyCanonical: number;
    dedupeSource: number;
    freshPublished: number;
    freshExpected: number;
  };
  lastAutoHealAt: string | null;
  autoRestartCount1h: number;
  circuitBreakerOpen: boolean;
  protectiveStopActive: boolean;
  protectiveStopReason?: string | null;
  rssMb: number;
  pid: number;
  timestamp: string;
}

export interface AutoRestartRecord {
  timestamp: string;
  reason: string;
  progressAgeSec: number;
  eligibleJobs: number;
}

export interface AutoHealWatchdogOptions {
  pool: Pool;
  scheduler?: WorkAffinityScheduler;
  admissionController?: AdmissionController;
  protectiveSentinel?: ProtectiveSentinel;
  publicationBarrier?: PublicationBarrier;
  onControlledRestart?: (reason: string, metrics: HealthPanelMetrics) => Promise<void>;
  intervalMs?: number;
  workerId?: string;
}

/**
 * AutoHealWatchdog
 *
 * Implements permanent autonomous recovery for Project Nox Importer:
 * - Real-progress based health classification (HEALTHY, DEGRADED, STALLED, CRITICAL_STALL, IDLE, PAUSED_BY_PROTECTION)
 * - Silent stall detection (workers alive + eligible > 0 but 0 completions => STALL)
 * - Escalated recovery ladder:
 *     Level 1: Light reconciliation (scheduler state, active works, cooldowns, caches, in-flight, admission, publication sweep)
 *     Level 2: Stuck state reconciliation (expired leases >15m, zombie active works eviction)
 *     Level 3: Controlled graceful self-restart (circuit breaker protected: max 1/15m, max 3/1h)
 * - Circuit breaker protection to prevent restart loops
 * - Complete data safety preservation (canonical ordering, barriers, gap safety)
 */
export class AutoHealWatchdog {
  private logger = new Logger('AutoHealWatchdog');
  private pool: Pool;
  private scheduler?: WorkAffinityScheduler;
  private admissionController?: AdmissionController;
  private protectiveSentinel?: ProtectiveSentinel;
  private publicationBarrier?: PublicationBarrier;
  private onControlledRestart?: (reason: string, metrics: HealthPanelMetrics) => Promise<void>;
  private intervalMs: number;
  private workerId: string;

  private isRunning = false;
  private stopSignal = false;
  private timer: NodeJS.Timeout | null = null;

  private autoHealState: AutoHealState = 'MONITORING';
  private lastAutoHealAt: string | null = null;
  private lastLevel1At = 0;
  private lastLevel2At = 0;
  private lastRestartAt = 0;
  private circuitBreakerOpen = false;

  constructor(options: AutoHealWatchdogOptions) {
    this.pool = options.pool;
    this.scheduler = options.scheduler;
    this.admissionController = options.admissionController;
    this.protectiveSentinel = options.protectiveSentinel;
    this.publicationBarrier = options.publicationBarrier;
    this.onControlledRestart = options.onControlledRestart;
    this.intervalMs = options.intervalMs ?? 60_000;
    this.workerId = options.workerId ?? 'discloud-importer-1';
  }

  /**
   * Starts the background evaluation loop.
   */
  start(): void {
    if (this.isRunning) return;
    this.isRunning = true;
    this.stopSignal = false;
    this.logger.info('AutoHealWatchdog started with interval ' + this.intervalMs + 'ms');

    const tick = async () => {
      if (this.stopSignal) return;
      try {
        await this.evaluateCycle();
      } catch (err: any) {
        this.logger.warn('Error during AutoHealWatchdog cycle', { error: err?.message });
      } finally {
        if (!this.stopSignal) {
          this.timer = setTimeout(tick, this.intervalMs);
          this.timer.unref?.();
        }
      }
    };

    // First evaluation after a 15s startup grace period
    this.timer = setTimeout(tick, 15_000);
    this.timer.unref?.();
  }

  /**
   * Stops the background loop cleanly.
   */
  stop(): void {
    this.stopSignal = true;
    this.isRunning = false;
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = null;
    }
    this.logger.info('AutoHealWatchdog stopped');
  }

  /**
   * Collects real-time telemetry from database and memory.
   */
  async collectTelemetry(): Promise<HealthPanelMetrics> {
    const mem = diagnostics.getMemorySnapshot();
    const now = new Date();

    // 1. Publication, Completion, and Started Timestamps
    const timeRes = await this.pool.query(`
      SELECT 
        (SELECT EXTRACT(EPOCH FROM (NOW() - MAX(locked_at))) FROM importer_queue WHERE locked_at IS NOT NULL) as started_age,
        (SELECT EXTRACT(EPOCH FROM (NOW() - MAX(updated_at))) FROM importer_queue WHERE status = 'COMPLETED' AND task_type = 'IMPORT_CHAPTER') as completed_age,
        (SELECT EXTRACT(EPOCH FROM (NOW() - MAX(published_at))) FROM chapters WHERE published_at IS NOT NULL) as fresh_age,
        (SELECT count(*) FROM importer_queue WHERE locked_at >= NOW() - INTERVAL '15 minutes') as started_15m,
        (SELECT count(*) FROM importer_queue WHERE status = 'COMPLETED' AND task_type = 'IMPORT_CHAPTER' AND updated_at >= NOW() - INTERVAL '15 minutes') as completed_15m,
        (SELECT count(*) FROM chapters WHERE published_at >= NOW() - INTERVAL '15 minutes') as fresh_15m
    `);
    const times = timeRes.rows[0] || {};
    const lastStartedAgeSec = Math.round(parseFloat(times.started_age || '99999'));
    const lastCompletedAgeSec = Math.round(parseFloat(times.completed_age || '99999'));
    const lastFreshVisibleAgeSec = Math.round(parseFloat(times.fresh_age || '99999'));
    const startedLast15m = parseInt(times.started_15m || '0', 10);
    const completedLast15m = parseInt(times.completed_15m || '0', 10);
    const freshLast15m = parseInt(times.fresh_15m || '0', 10);

    // 2. Queue Status Counts
    const qRes = await this.pool.query(`
      SELECT 
        COUNT(CASE WHEN status IN ('QUEUED', 'RETRY') AND (next_run_at IS NULL OR next_run_at <= NOW()) AND task_type = 'IMPORT_CHAPTER' THEN 1 END) as eligible_cnt,
        COUNT(CASE WHEN status = 'IMPORTING' THEN 1 END) as importing_cnt,
        COUNT(CASE WHEN status = 'RETRY' THEN 1 END) as retry_cnt
      FROM importer_queue
    `);
    const qRow = qRes.rows[0] || {};
    const eligibleJobs = parseInt(qRow.eligible_cnt || '0', 10);
    const importingCount = parseInt(qRow.importing_cnt || '0', 10);
    const retryCount = parseInt(qRow.retry_cnt || '0', 10);

    // 3. Staged Unique & Publishable Staged Chapters
    let stagedUnique = 0;
    let publishableStaged = 0;
    let waitingPredecessorStaged = 0;
    let stuckStaged = 0;

    try {
      const mapRes = await this.pool.query(`
        SELECT count(DISTINCT (work_id || ':' || chapter_sort_key::text)) as staged_unique
        FROM importer_chapter_mappings
        WHERE status IN ('STAGED', 'WAITING_FOR_GAP')
      `);
      stagedUnique = parseInt(mapRes.rows[0]?.staged_unique || '0', 10);

      if (stagedUnique > 0) {
        const pubStagedRes = await this.pool.query(`
          WITH staged_works AS (
            SELECT work_id, MIN(chapter_sort_key) as min_staged
            FROM importer_chapter_mappings
            WHERE status IN ('STAGED', 'WAITING_FOR_GAP') AND work_id IS NOT NULL
            GROUP BY work_id
          ),
          publishable_works AS (
            SELECT sw.work_id, COALESCE((
              SELECT MAX(number) FROM chapters c WHERE c.work_id = sw.work_id AND c.published_at IS NOT NULL
            ), -1) as max_published
            FROM staged_works sw
            WHERE sw.min_staged <= COALESCE((
              SELECT MAX(number) FROM chapters c WHERE c.work_id = sw.work_id AND c.published_at IS NOT NULL
            ), -1) + 1.05
            OR NOT EXISTS (
              SELECT 1 FROM chapters c WHERE c.work_id = sw.work_id AND c.published_at IS NOT NULL
            )
            LIMIT 40
          )
          SELECT COUNT(*) as publishable_staged
          FROM importer_chapter_mappings m
          JOIN publishable_works pw ON m.work_id = pw.work_id
          WHERE m.status IN ('STAGED', 'WAITING_FOR_GAP')
            AND (m.chapter_sort_key <= pw.max_published + 1.05 OR pw.max_published = -1);
        `);
        publishableStaged = parseInt(pubStagedRes.rows[0]?.publishable_staged || '0', 10);

        const remainingNonPublishable = Math.max(0, stagedUnique - publishableStaged);
        if (remainingNonPublishable > 0) {
          if (eligibleJobs > 0 || importingCount > 0 || retryCount > 0) {
            waitingPredecessorStaged = remainingNonPublishable;
          } else {
            stuckStaged = remainingNonPublishable;
          }
        }
      }
    } catch (err: any) {
      this.logger.warn('Failed querying staged classification', { error: err?.message });
    }

    // 4. Scheduler State (active_works, claimable_works)
    let activeWorks: any[] = [];
    let claimableWorks = 0;
    let zombieWorksCount = 0;
    try {
      const schedRes = await this.pool.query("SELECT key, value FROM importer_scheduler_state WHERE key = 'active_works'");
      if (schedRes.rows[0]?.value) {
        const raw = schedRes.rows[0].value;
        activeWorks = typeof raw === 'string' ? JSON.parse(raw) : raw;
        if (!Array.isArray(activeWorks)) activeWorks = [];
      }
      for (const w of activeWorks) {
        if ((w.queuedChapters || 0) > 0) claimableWorks++;
        if (w.state === 'FILLING' && (w.queuedChapters || 0) === 0 && (w.inFlightChapters || 0) === 0) {
          zombieWorksCount++;
        }
      }
    } catch {}

    // 5. Protective Stop State
    let protectiveStopActive = false;
    let protectiveStopReason: string | null = null;
    let protectiveStopTriggeredAt: string | null = null;
    try {
      const psRes = await this.pool.query("SELECT value FROM settings WHERE key = 'importer_protective_stop'");
      if (psRes.rows[0]?.value) {
        const raw = psRes.rows[0].value;
        const parsed = typeof raw === 'string' ? JSON.parse(raw) : raw;
        protectiveStopActive = Boolean(parsed.active);
        protectiveStopReason = parsed.reason || null;
        protectiveStopTriggeredAt = parsed.triggered_at || null;
      }
    } catch {}

    // 6. Recent Auto-Restarts and Circuit Breaker
    const recentRestarts = await this.getRecentAutoRestarts();
    const nowMs = Date.now();
    const restartsLast1h = recentRestarts.filter((r) => nowMs - new Date(r.timestamp).getTime() <= 60 * 60 * 1000);
    const autoRestartCount1h = restartsLast1h.length;
    this.circuitBreakerOpen = autoRestartCount1h >= 3;

    // 7. Correlated dedupe classification of recent completed jobs
    let recentCorrelatedBreakdown = {
      alreadyCanonical: 0,
      dedupeSource: 0,
      freshPublished: 0,
      freshExpected: 0,
    };
    let recentCompletionsAreDedupeOnly = false;

    try {
      const correlatedRes = await this.pool.query(`
        WITH recent_completed_jobs AS (
          SELECT 
            q.id,
            q.source,
            q.chapter_sort_key,
            (q.payload->>'workId')::uuid as work_id,
            (q.payload->>'chapterId')::uuid as chapter_id,
            q.last_error,
            q.updated_at
          FROM importer_queue q
          WHERE q.status = 'COMPLETED'
            AND q.task_type = 'IMPORT_CHAPTER'
            AND q.updated_at >= NOW() - INTERVAL '30 minutes'
          ORDER BY q.updated_at DESC
          LIMIT 50
        ),
        correlated AS (
          SELECT DISTINCT ON (j.id)
            j.id as job_id,
            j.work_id,
            j.chapter_sort_key,
            j.source,
            CASE
              WHEN j.last_error IN ('CANONICAL_ALREADY_SATISFIED', 'ALREADY_CANONICAL') THEN 'ALREADY_CANONICAL'
              WHEN m.is_page_provider IS FALSE THEN 'DEDUPE_SOURCE'
              WHEN c.published_at IS NOT NULL AND c.is_fresh_release IS TRUE THEN 'FRESH_PUBLISHED'
              WHEN c.published_at IS NOT NULL THEN 'ALREADY_CANONICAL'
              ELSE 'FRESH_EXPECTED'
            END as classification
          FROM recent_completed_jobs j
          LEFT JOIN importer_chapter_mappings m 
            ON m.work_id = j.work_id 
           AND m.source = j.source 
           AND m.chapter_sort_key = j.chapter_sort_key
          LEFT JOIN chapters c 
            ON c.id = COALESCE(j.chapter_id, m.chapter_id)
        )
        SELECT 
          classification,
          count(*) as cnt
        FROM correlated
        GROUP BY classification;
      `);

      for (const row of correlatedRes.rows) {
        const cnt = parseInt(row.cnt, 10);
        if (row.classification === 'ALREADY_CANONICAL') recentCorrelatedBreakdown.alreadyCanonical = cnt;
        else if (row.classification === 'DEDUPE_SOURCE') recentCorrelatedBreakdown.dedupeSource = cnt;
        else if (row.classification === 'FRESH_PUBLISHED') recentCorrelatedBreakdown.freshPublished = cnt;
        else if (row.classification === 'FRESH_EXPECTED') recentCorrelatedBreakdown.freshExpected = cnt;
      }

      const totalRecentJobs =
        recentCorrelatedBreakdown.alreadyCanonical +
        recentCorrelatedBreakdown.dedupeSource +
        recentCorrelatedBreakdown.freshPublished +
        recentCorrelatedBreakdown.freshExpected;

      if (
        totalRecentJobs > 0 &&
        recentCorrelatedBreakdown.freshExpected === 0 &&
        publishableStaged === 0
      ) {
        recentCompletionsAreDedupeOnly = true;
      }
    } catch (err: any) {
      this.logger.warn('Failed querying correlated dedupe breakdown', { error: err?.message });
    }

    // 8. Compute Multidimensional Health Status
    const health = this.evaluateMultidimensionalHealth({
      eligibleJobs,
      importingCount,
      lastCompletedAgeSec,
      lastFreshVisibleAgeSec,
      protectiveStopActive,
      protectiveStopReason,
      protectiveStopTriggeredAt,
      recentCompletionsAreDedupeOnly,
      hasStagedPublications: stagedUnique > 0,
      publishableStaged,
      waitingPredecessorStaged,
      stuckStaged,
    });

    return {
      status: health.status,
      autoHealState: this.circuitBreakerOpen ? 'CIRCUIT_OPEN' : this.autoHealState,
      processingHealth: health.processingHealth,
      publicationHealth: health.publicationHealth,
      lastStartedAgeSec,
      lastCompletedAgeSec,
      lastFreshVisibleAgeSec,
      startedLast15m,
      completedLast15m,
      freshLast15m,
      eligibleJobs,
      claimableWorks,
      activeWorksCount: activeWorks.length,
      zombieWorksCount,
      importingCount,
      retryCount,
      stagedUnique,
      publishableStaged,
      waitingPredecessorStaged,
      stuckStaged,
      recentCorrelatedBreakdown,
      lastAutoHealAt: this.lastAutoHealAt,
      autoRestartCount1h,
      circuitBreakerOpen: this.circuitBreakerOpen,
      protectiveStopActive,
      protectiveStopReason,
      rssMb: mem.rssMb,
      pid: process.pid,
      timestamp: now.toISOString(),
    };
  }

  /**
   * Deterministic Multidimensional Health evaluation separating processing and publication health.
   */
  evaluateMultidimensionalHealth(params: {
    eligibleJobs: number;
    importingCount: number;
    lastCompletedAgeSec: number;
    lastFreshVisibleAgeSec: number;
    protectiveStopActive: boolean;
    protectiveStopReason?: string | null;
    protectiveStopTriggeredAt?: string | null;
    recentCompletionsAreDedupeOnly?: boolean;
    hasStagedPublications?: boolean;
    publishableStaged?: number;
    waitingPredecessorStaged?: number;
    stuckStaged?: number;
  }): {
    status: ImporterHealthStatus;
    processingHealth: ProcessingHealth;
    publicationHealth: PublicationHealth;
  } {
    const publishableStaged = params.publishableStaged ?? (params.hasStagedPublications ? 1 : 0);
    const waitingPredecessorStaged = params.waitingPredecessorStaged ?? 0;

    // 1. Processing Health Dimension (based on chapter completions)
    let processingHealth: ProcessingHealth;
    if (params.lastCompletedAgeSec <= 10 * 60) {
      processingHealth = 'HEALTHY';
    } else if (params.lastCompletedAgeSec <= 15 * 60) {
      processingHealth = 'DEGRADED';
    } else if (params.lastCompletedAgeSec < 30 * 60) {
      processingHealth = 'STALLED';
    } else {
      processingHealth = 'CRITICAL_STALL';
    }

    // 2. Publication Health Dimension (based on fresh visible chapters and staged backlog)
    let publicationHealth: PublicationHealth;
    if (params.lastFreshVisibleAgeSec <= 10 * 60) {
      publicationHealth = 'HEALTHY';
    } else if (params.lastFreshVisibleAgeSec <= 15 * 60) {
      publicationHealth = 'DEGRADED';
    } else if (params.recentCompletionsAreDedupeOnly && publishableStaged === 0) {
      // Completed chapters were deduplicated/canonical-only and no publishable staged backlog exists
      publicationHealth = 'NO_FRESH_EXPECTED';
    } else if (
      params.eligibleJobs === 0 &&
      params.importingCount === 0 &&
      publishableStaged === 0
    ) {
      // No active work and zero publishable chapters waiting -> no fresh chapters expected
      publicationHealth = 'NO_FRESH_EXPECTED';
    } else if (params.lastFreshVisibleAgeSec < 30 * 60) {
      publicationHealth = 'STALLED';
    } else {
      publicationHealth = 'CRITICAL_STALL';
    }

    // 3. Resolve Overall Status
    let status: ImporterHealthStatus;

    // IDLE is ONLY allowed when:
    // eligibleJobs === 0 AND importingCount === 0 AND publishableStaged === 0
    // (Meaning: no processable work AND no actionable publication)
    const isTrulyIdle =
      params.eligibleJobs === 0 &&
      params.importingCount === 0 &&
      publishableStaged === 0;

    if (isTrulyIdle) {
      status = 'IDLE';
    } else if (params.protectiveStopActive) {
      const isManual =
        params.protectiveStopReason?.toLowerCase().includes('manual') ||
        params.protectiveStopReason?.toLowerCase().includes('staff');
      if (isManual) {
        status = 'PAUSED_BY_PROTECTION';
      } else {
        const stoppedAgeSec = params.protectiveStopTriggeredAt
          ? Math.floor((Date.now() - new Date(params.protectiveStopTriggeredAt).getTime()) / 1000)
          : 0;
        if (stoppedAgeSec < 15 * 60) {
          status = 'PAUSED_BY_PROTECTION';
        } else {
          status = 'STALLED';
        }
      }
    } else if (params.eligibleJobs === 0 && params.importingCount === 0 && publishableStaged > 0) {
      // Eligible = 0, Importing = 0, BUT publishableStaged > 0!
      // This is NEVER IDLE! It is driven strictly by publicationHealth!
      if (publicationHealth === 'CRITICAL_STALL') {
        status = 'CRITICAL_STALL';
      } else if (publicationHealth === 'STALLED') {
        status = 'STALLED';
      } else if (publicationHealth === 'DEGRADED') {
        status = 'DEGRADED';
      } else {
        status = 'HEALTHY';
      }
    } else if (publicationHealth === 'NO_FRESH_EXPECTED') {
      status = processingHealth;
    } else if (processingHealth === 'CRITICAL_STALL' || publicationHealth === 'CRITICAL_STALL') {
      status = 'CRITICAL_STALL';
    } else if (processingHealth === 'STALLED' || publicationHealth === 'STALLED') {
      status = 'STALLED';
    } else if (processingHealth === 'DEGRADED' || publicationHealth === 'DEGRADED') {
      status = 'DEGRADED';
    } else {
      status = 'HEALTHY';
    }

    return { status, processingHealth, publicationHealth };
  }

  /**
   * Deterministic Health Status evaluation based on REAL PROGRESS (backward-compatible).
   */
  determineHealthStatus(params: {
    eligibleJobs: number;
    importingCount: number;
    lastCompletedAgeSec: number;
    lastFreshVisibleAgeSec: number;
    protectiveStopActive: boolean;
    protectiveStopReason?: string | null;
    protectiveStopTriggeredAt?: string | null;
    recentCompletionsAreDedupeOnly?: boolean;
    hasStagedPublications?: boolean;
    publishableStaged?: number;
    waitingPredecessorStaged?: number;
    stuckStaged?: number;
  }): ImporterHealthStatus {
    return this.evaluateMultidimensionalHealth(params).status;
  }

  /**
   * Executes a single evaluation cycle:
   * 1. Collect telemetry & determine status
   * 2. Persist heartbeat / health metrics
   * 3. Trigger Escalated Recovery Ladder if STALLED / CRITICAL_STALL
   */
  async evaluateCycle(): Promise<HealthPanelMetrics> {
    const metrics = await this.collectTelemetry();

    // If pipeline is IDLE or PAUSED_BY_PROTECTION
    if (metrics.status === 'IDLE' || metrics.status === 'PAUSED_BY_PROTECTION') {
      this.autoHealState = 'IDLE';
      metrics.autoHealState = this.autoHealState;
      await this.persistHealthMetrics(metrics);
      return metrics;
    }

    // If progress is healthy
    if (metrics.status === 'HEALTHY') {
      if (this.autoHealState !== 'MONITORING' && this.autoHealState !== 'RECOVERED') {
        this.logger.info(`✨ [AUTO-HEAL SUCCESS] Real progress verified (${metrics.completedLast15m} completed, ${metrics.freshLast15m} fresh in last 15m, processing: ${metrics.processingHealth}, publication: ${metrics.publicationHealth}). Pipeline RECOVERED!`);
        this.autoHealState = 'RECOVERED';
        this.lastAutoHealAt = new Date().toISOString();
      }
      metrics.autoHealState = this.autoHealState;
      await this.persistHealthMetrics(metrics);
      return metrics;
    }

    if (metrics.status === 'DEGRADED') {
      this.logger.warn(`⚠️ [AUTO-HEAL WARNING] Importer DEGRADED: Processing: ${metrics.processingHealth} (${Math.round(metrics.lastCompletedAgeSec / 60)}m), Publication: ${metrics.publicationHealth} (${Math.round(metrics.lastFreshVisibleAgeSec / 60)}m) while ${metrics.eligibleJobs} jobs eligible. Monitoring closely.`);
      await this.persistHealthMetrics(metrics);
      return metrics;
    }

    // Execute Recovery Ladder
    await this.executeRecoveryLadder(metrics);
    metrics.autoHealState = this.circuitBreakerOpen ? 'CIRCUIT_OPEN' : this.autoHealState;
    metrics.lastAutoHealAt = this.lastAutoHealAt;

    // Persist health panel to settings table for supervisor, site, and external monitors
    await this.persistHealthMetrics(metrics);

    return metrics;
  }

  /**
   * Escalated Recovery Ladder:
   * Level 1 (STALLED >= 15m): Light reconciliation
   * Level 2 (STALLED >= 20m): Stuck state audit (expired leases, zombie active works)
   * Level 3 (CRITICAL_STALL >= 30m): Controlled graceful self-restart
   */
  async executeRecoveryLadder(metrics: HealthPanelMetrics): Promise<void> {
    const nowMs = Date.now();

    // Determine the relevant stall duration driving the recovery ladder
    let effectiveStallAgeSec = metrics.lastCompletedAgeSec;
    if (metrics.publicationHealth !== 'NO_FRESH_EXPECTED') {
      if (metrics.publicationHealth === 'CRITICAL_STALL' || metrics.publicationHealth === 'STALLED') {
        effectiveStallAgeSec = Math.max(effectiveStallAgeSec, metrics.lastFreshVisibleAgeSec);
      }
    }

    // NÍVEL 1 — RECONCILIAÇÃO LEVE (>= 15m stall)
    if (effectiveStallAgeSec >= 15 * 60 && nowMs - this.lastLevel1At >= 3 * 60 * 1000) {
      this.lastLevel1At = nowMs;
      this.autoHealState = 'LEVEL_1_LIGHT_RECONCILIATION';
      this.lastAutoHealAt = new Date().toISOString();
      this.logger.warn(`🔧 [AUTO-HEAL NÍVEL 1] Initiating Light Reconciliation (Stall age: ${Math.round(effectiveStallAgeSec / 60)}m, Eligible: ${metrics.eligibleJobs})...`);

      try {
        await this.runLevel1LightReconciliation(metrics);
        this.logger.info('✅ [AUTO-HEAL NÍVEL 1] Light reconciliation executed. Awaiting progress...');
      } catch (err: any) {
        this.logger.error('Failed executing Level 1 reconciliation', { error: err?.message });
      }
      return;
    }

    // NÍVEL 2 — ESTADO PRESO (>= 20m stall, after Level 1 attempted)
    if (effectiveStallAgeSec >= 20 * 60 && nowMs - this.lastLevel2At >= 5 * 60 * 1000) {
      this.lastLevel2At = nowMs;
      this.autoHealState = 'LEVEL_2_STUCK_STATE_AUDIT';
      this.lastAutoHealAt = new Date().toISOString();
      this.logger.warn(`🔧 [AUTO-HEAL NÍVEL 2] Initiating Stuck State Audit (Stall age: ${Math.round(effectiveStallAgeSec / 60)}m, Eligible: ${metrics.eligibleJobs})...`);

      try {
        await this.runLevel2StuckStateAudit(metrics);
        this.logger.info('✅ [AUTO-HEAL NÍVEL 2] Stuck state audit executed. Awaiting progress...');
      } catch (err: any) {
        this.logger.error('Failed executing Level 2 stuck state audit', { error: err?.message });
      }
      return;
    }

    // NÍVEL 3 — RESTART CONTROLADO (>= 30m stall / CRITICAL_STALL)
    if (effectiveStallAgeSec >= 30 * 60 && metrics.status === 'CRITICAL_STALL') {
      if (metrics.protectiveStopActive && metrics.protectiveStopReason?.toLowerCase().includes('manual')) {
        this.logger.info('[AUTO-HEAL NÍVEL 3] Manual staff stop active; skipping self-restart.');
        return;
      }

      // Check Circuit Breaker & Persistent Cooldown from DB
      const recentRestarts = await this.getRecentAutoRestarts();
      const restartsLast1h = recentRestarts.filter((r) => nowMs - new Date(r.timestamp).getTime() <= 60 * 60 * 1000);
      if (this.circuitBreakerOpen || restartsLast1h.length >= 3) {
        this.circuitBreakerOpen = true;
        this.autoHealState = 'CIRCUIT_OPEN';
        this.logger.error(
          `🚨 [AUTO-RECOVERY CIRCUIT OPEN] Reached max 3 auto-restarts in 1h (Current count: ${restartsLast1h.length}). Halting automatic restarts to prevent loop. ROOT CAUSE REQUIRED.`
        );
        return;
      }

      // Persistent 15-minute cooldown check across processes
      let latestPersistedRestartMs = 0;
      for (const r of recentRestarts) {
        const t = new Date(r.timestamp).getTime();
        if (!isNaN(t) && t > latestPersistedRestartMs) {
          latestPersistedRestartMs = t;
        }
      }
      const effectiveLastRestartMs = Math.max(this.lastRestartAt, latestPersistedRestartMs);
      const timeSinceLastRestartMs = nowMs - effectiveLastRestartMs;

      if (effectiveLastRestartMs > 0 && timeSinceLastRestartMs < 15 * 60 * 1000) {
        this.logger.warn(
          `⏳ [AUTO-HEAL NÍVEL 3] Throttle active: Last restart was ${Math.round(timeSinceLastRestartMs / 60000)}m ago (min 15m cooldown). Waiting...`
        );
        return;
      }

      // Circuit Breaker allows restart
      this.autoHealState = 'LEVEL_3_RESTART_PENDING';
      this.lastRestartAt = nowMs;
      this.lastAutoHealAt = new Date().toISOString();

      const reason = `CRITICAL_STALL: 0 completions/fresh for ${Math.round(effectiveStallAgeSec / 60)}m while ${metrics.eligibleJobs} jobs eligible (processing: ${metrics.processingHealth}, publication: ${metrics.publicationHealth})`;
      this.logger.error(`🚨 [AUTO-HEAL NÍVEL 3] ${reason}. Initiating controlled graceful self-restart...`);

      // Record restart event in DB before process exits
      await this.recordAutoRestart({
        timestamp: new Date().toISOString(),
        reason,
        progressAgeSec: effectiveStallAgeSec,
        eligibleJobs: metrics.eligibleJobs,
      });

      if (this.onControlledRestart) {
        await this.onControlledRestart(reason, metrics);
      }
    }
  }

  /**
   * NÍVEL 1 — RECONCILIAÇÃO LEVE
   */
  async runLevel1LightReconciliation(metrics: HealthPanelMetrics): Promise<void> {
    // 1. Reconcile in-flight counts and chapter keys in scheduler from DB
    if (this.scheduler) {
      await this.scheduler.syncInFlightCountsFromDb();
      await (this.scheduler as any).reloadActiveWorks?.();
    }

    // 2. Clean expired source cooldowns
    try {
      const cdRes = await this.pool.query("SELECT value FROM settings WHERE key = 'importer_cooldowns'");
      if (cdRes.rows[0]?.value) {
        const raw = cdRes.rows[0].value;
        const cooldowns = typeof raw === 'string' ? JSON.parse(raw) : raw;
        let modified = false;
        const nowIso = new Date().toISOString();
        for (const [src, expiry] of Object.entries(cooldowns)) {
          if (typeof expiry === 'string' && expiry <= nowIso) {
            delete cooldowns[src];
            modified = true;
          }
        }
        if (modified) {
          await this.pool.query("UPDATE settings SET value = $1 WHERE key = 'importer_cooldowns'", [JSON.stringify(cooldowns)]);
          this.logger.info('[Level 1] Cleared expired source cooldowns from settings');
        }
      }
    } catch {}

    // 3. Clear stale protective stop if reason was transient (e.g. past lag or RAM)
    if (metrics.protectiveStopActive && this.protectiveSentinel) {
      try {
        await this.protectiveSentinel.evaluateAutoResume();
      } catch (e: any) {
        this.logger.warn('[Level 1] Sentinel auto-resume evaluation failed', { error: e?.message });
      }
    }

    // 4. Force a safe admission cycle
    if (this.admissionController) {
      try {
        await this.admissionController.runAdmissionCycle();
      } catch (e: any) {
        this.logger.warn('[Level 1] Admission cycle failed', { error: e?.message });
      }
    }

    // 5. If publication is stalled or publishable staged chapters exist, trigger safe bounded sweep
    if (
      this.publicationBarrier &&
      (metrics.publicationHealth === 'STALLED' ||
        metrics.publicationHealth === 'CRITICAL_STALL' ||
        (metrics.publishableStaged || 0) > 0)
    ) {
      try {
        const swept = await this.publicationBarrier.sweepStagedPublications(40, 6);
        if (swept > 0) {
          this.logger.info(`[Level 1] Publication recovery sweep: published ${swept} staged chapter(s) safely through barrier.`);
        }
      } catch (e: any) {
        this.logger.warn('[Level 1] Publication recovery sweep failed', { error: e?.message });
      }
    }
  }

  /**
   * NÍVEL 2 — ESTADO PRESO
   */
  async runLevel2StuckStateAudit(metrics: HealthPanelMetrics): Promise<void> {
    // 1. Reclaim IMPORTING jobs with expired leases (>15 minutes / 900s)
    // Preserves attempt counters, unlocks worker, reverts status to QUEUED
    const reclaimRes = await this.pool.query(`
      UPDATE importer_queue
      SET status = 'QUEUED',
          locked_by = NULL,
          locked_at = NULL,
          lease_expires_at = NULL,
          next_run_at = NOW(),
          updated_at = NOW()
      WHERE status = 'IMPORTING'
        AND (
          lease_expires_at < NOW()
          OR (lease_expires_at IS NULL AND locked_at < NOW() - INTERVAL '15 minutes')
          OR locked_at < NOW() - INTERVAL '20 minutes'
        )
      RETURNING id, task_type, source, chapter_sort_key;
    `);

    if (reclaimRes.rows.length > 0) {
      this.logger.info(`[Level 2] Safely reclaimed ${reclaimRes.rows.length} expired lease job(s) back to QUEUED:`, {
        jobs: reclaimRes.rows.map((r) => `${r.source}:${r.chapter_sort_key || r.task_type}`),
      });
    }

    // 2. Audit active works: Evict empty works (0 claimable, 0 in-flight) from active set
    try {
      const actRes = await this.pool.query("SELECT value FROM importer_scheduler_state WHERE key = 'active_works'");
      if (actRes.rows[0]?.value) {
        let activeWorks = actRes.rows[0].value;
        if (typeof activeWorks === 'string') activeWorks = JSON.parse(activeWorks);
        if (Array.isArray(activeWorks) && activeWorks.length > 0) {
          const survivingWorks: any[] = [];
          let evictedCount = 0;

          for (const w of activeWorks) {
            // Check if this work has any claimable jobs in queue
            const countRes = await this.pool.query(
              `SELECT count(*) as count 
               FROM importer_queue 
               WHERE (payload->>'workId') = $1 
                 AND status IN ('QUEUED', 'RETRY') 
                 AND (next_run_at IS NULL OR next_run_at <= NOW())`,
              [w.workId]
            );
            const claimableCount = parseInt(countRes.rows[0]?.count || '0', 10);
            const inFlight = w.inFlightChapters || 0;

            if (claimableCount === 0 && inFlight === 0) {
              this.logger.info(`[Level 2] Evicting empty work "${w.workTitle}" (${w.workId}) from active set (0 claimable, 0 inflight)`);
              evictedCount++;
            } else {
              survivingWorks.push({
                ...w,
                queuedChapters: claimableCount,
              });
            }
          }

          if (evictedCount > 0) {
            await this.pool.query(
              "UPDATE importer_scheduler_state SET value = $1, updated_at = NOW() WHERE key = 'active_works'",
              [JSON.stringify(survivingWorks)]
            );
            this.logger.info(`[Level 2] Evicted ${evictedCount} empty active works. Retained ${survivingWorks.length} healthy works.`);
          }
        }
      }
    } catch (e: any) {
      this.logger.warn('[Level 2] Active works audit failed', { error: e?.message });
    }

    // 3. Force admission cycle to bring in fresh claimable works
    if (this.admissionController) {
      try {
        await this.admissionController.runAdmissionCycle();
      } catch (e: any) {
        this.logger.warn('[Level 2] Admission cycle failed', { error: e?.message });
      }
    }
  }

  /**
   * Persists health panel to settings table for supervisor, site, and external monitors.
   */
  async persistHealthMetrics(metrics: HealthPanelMetrics): Promise<void> {
    const payload = JSON.stringify({
      status: metrics.status,
      autoHealState: metrics.autoHealState,
      processingHealth: metrics.processingHealth,
      publicationHealth: metrics.publicationHealth,
      lastStartedAge: metrics.lastStartedAgeSec,
      lastCompletedAge: metrics.lastCompletedAgeSec,
      lastFreshVisibleAge: metrics.lastFreshVisibleAgeSec,
      startedLast15m: metrics.startedLast15m,
      completedLast15m: metrics.completedLast15m,
      freshLast15m: metrics.freshLast15m,
      eligibleJobs: metrics.eligibleJobs,
      claimableWorks: metrics.claimableWorks,
      activeWorks: metrics.activeWorksCount,
      zombieWorks: metrics.zombieWorksCount,
      importing: metrics.importingCount,
      retry: metrics.retryCount,
      stagedUnique: metrics.stagedUnique,
      publishableStaged: metrics.publishableStaged,
      waitingPredecessorStaged: metrics.waitingPredecessorStaged,
      stuckStaged: metrics.stuckStaged,
      lastAutoHeal: metrics.lastAutoHealAt,
      autoRestartCount1h: metrics.autoRestartCount1h,
      circuitBreakerOpen: metrics.circuitBreakerOpen,
      protectiveStop: metrics.protectiveStopActive,
      protectiveStopReason: metrics.protectiveStopReason,
      rssMb: metrics.rssMb,
      pid: metrics.pid,
      workerId: this.workerId,
      timestamp: metrics.timestamp,
    });

    try {
      await this.pool.query(
        `INSERT INTO settings (key, value)
         VALUES ('importer_heartbeat', $1)
         ON CONFLICT (key) DO UPDATE SET value = $1`,
        [payload]
      );
    } catch (err: any) {
      this.logger.warn('Failed persisting importer_heartbeat to settings', { error: err?.message });
    }
  }

  /**
   * Records an auto-restart event to settings.importer_auto_restarts.
   */
  async recordAutoRestart(record: AutoRestartRecord): Promise<void> {
    try {
      const existing = await this.getRecentAutoRestarts();
      existing.push(record);
      // Keep only last 20 records
      const pruned = existing.slice(-20);
      await this.pool.query(
        `INSERT INTO settings (key, value)
         VALUES ('importer_auto_restarts', $1)
         ON CONFLICT (key) DO UPDATE SET value = $1`,
        [JSON.stringify(pruned)]
      );
    } catch (err: any) {
      this.logger.warn('Failed recording auto-restart to settings', { error: err?.message });
    }
  }

  /**
   * Reads recent auto-restarters from settings.importer_auto_restarts.
   */
  async getRecentAutoRestarts(): Promise<AutoRestartRecord[]> {
    try {
      const res = await this.pool.query("SELECT value FROM settings WHERE key = 'importer_auto_restarts'");
      if (res.rows[0]?.value) {
        const raw = res.rows[0].value;
        const parsed = typeof raw === 'string' ? JSON.parse(raw) : raw;
        if (Array.isArray(parsed)) return parsed;
      }
    } catch {}
    return [];
  }
}
