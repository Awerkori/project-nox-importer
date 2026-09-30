/**
 * Admission Controller for Project Nox Work-Oriented Scheduler.
 * 
 * Responsibilities:
 * 1. Maintains bounded active sets (ACTIVE_BACKFILL_WORKS <= 10, ACTIVE_NEW_WORKS <= 8).
 * 2. Sliding window admission: promotes 5-8 chapters per active work to QUEUED, keeping
 *    the executable queue small (~150 jobs), fast, and contention-free.
 * 3. Work lifecycle transitions (NEW -> FILLING -> CAUGHT_UP / COMPLETE / BLOCKED).
 * 4. Critical Gap & Barrier Frontier detection (BARRIER_UNBLOCK_SCORE).
 * 5. Source diversity (prevents active set concentration on a single source).
 * 6. Adheres strictly to PROTECTIVE_STOP and auto-healing circuit breakers.
 */

import { getYugabytePool } from '../../db/yugabyte-direct.js';
import { Logger } from '../logger.js';
import { ProtectiveSentinel } from '../protective-sentinel.js';
import { SchedulerStateStore } from './state-store.js';
import { confirmUpstreamGapInterval } from '../gap-validator.js';
import {
  ActiveWork,
  SchedulerLane,
  WorkSchedulerState,
} from './types.js';

export class AdmissionController {
  private logger = new Logger('AdmissionController');
  private pool: any;
  private isRunning = false;
  private loopTimer: NodeJS.Timeout | null = null;
  private sourcePermitProvider?: (source: string) => number;
  private chapterCapacityProvider: () => number = () => 1;

  public setChapterCapacityProvider(provider: () => number): void {
    this.chapterCapacityProvider = provider;
  }
  private admissionInFlight: Promise<void> | null = null;
  private demandFlights = new Map<string, Promise<ActiveWork | null>>();
  // P2 admission can be evaluated by discovery bursts. Keep the P1 pressure
  // probe short-lived and single-flight: it is an admission signal, never a
  // cache of editorial state.
  private p1BacklogProbeAt = 0;
  private p1BacklogProbeFlight: Promise<{ claimable: number; available: number; works: number }> | null = null;
  private p1BacklogSnapshot = { claimable: 0, available: 0, works: 0 };
  // A previous scheduler generation could vacate a visible P2 work after its
  // first window, leaving the rest of its queue at priority 50. Repair that
  // legacy state in small work-scoped batches; never scan or rewrite the P2
  // catalog as part of normal admission.
  private visibleP2LifecycleRepairComplete = false;

  private getP1AdmissionCursors(): Record<string, string> {
    const getter = (this.stateStore as any).getP1AdmissionCursors;
    return typeof getter === 'function' ? getter.call(this.stateStore) : {};
  }

  private advanceP1AdmissionCursor(source: string, workId: string): void {
    const setter = (this.stateStore as any).setP1AdmissionCursor;
    if (typeof setter === 'function') setter.call(this.stateStore, source, workId);
  }

  public setSourcePermitProvider(provider: (source: string) => number): void {
    this.sourcePermitProvider = provider;
  }

  constructor(
    private stateStore: SchedulerStateStore,
    private protectiveSentinel: ProtectiveSentinel,
    pool?: any
  ) {
    // Keep in-memory scheduler tests independent of runtime Yugabyte secrets.
    // Engine passes the real bounded pool in production.
    const rawPool = pool || (process.env.NODE_ENV === 'test'
      ? { query: async () => ({ rows: [] }) }
      : getYugabytePool());
    if (typeof rawPool.connect === 'function') {
      this.pool = rawPool;
    } else {
      this.pool = {
        connect: async () => ({
          query: (text: string, params?: any[]) => rawPool.query(text, params),
          release: () => {},
        }),
        query: (text: string, params?: any[]) => rawPool.query(text, params),
      };
    }
  }

  private async runQuery(text: string, params?: any[]): Promise<any> {
    if (typeof this.pool?.query === 'function') {
      return this.pool.query(text, params);
    }
    if (typeof this.pool?.connect === 'function') {
      const client = await this.pool.connect();
      try {
        return await client.query(text, params);
      } finally {
        if (typeof client?.release === 'function') client.release();
      }
    }
    throw new Error('Pool has neither query nor connect');
  }

  /**
   * Periodic reconciliation is the fallback; real vacancies trigger an
   * immediate coalesced cycle. Keep this cadence low enough that aggregate
   * queue scans do not compete with claims and publication.
   */
  start(): void {
    if (this.isRunning) return;
    this.isRunning = true;
    this.logger.info('Starting AdmissionController background loop');
    this.scheduleNextCycle(1000);
  }

  stop(): void {
    this.isRunning = false;
    if (this.immediateReplenishTimer) clearTimeout(this.immediateReplenishTimer);
    this.immediateReplenishTimer = null;
    if (this.loopTimer) {
      clearTimeout(this.loopTimer);
      this.loopTimer = null;
    }
  }

  private immediateReplenishTimer: NodeJS.Timeout | null = null;
  private isReplenishingCycle = false;

  private scheduleNextCycle(delayMs: number): void {
    if (!this.isRunning) return;
    this.loopTimer = setTimeout(async () => {
      try {
        await this.runAdmissionCycle();
      } catch (err: any) {
        this.logger.error('Error during admission cycle', { error: err?.message });
      } finally {
        this.scheduleNextCycle(10_000);
      }
    }, delayMs);
  }

  /**
   * Triggers immediate admission reconciliation and replenishment.
   * Debounced with 50ms trailing window to collapse concurrent vacate events.
   */
  private lastVacancyReplenishAt = 0;

  public triggerImmediateReplenishment(reason: string): void {
    if (!this.isRunning || this.isReplenishingCycle) return;
    if (this.immediateReplenishTimer) return;
    // Polling hints must not rerun expensive admission after every claim.
    // Real work-vacated events remain immediate and bypass this coalescing window.
    if (reason === 'PRODUCTIVE_SLOT_VACANCY') {
      if (Date.now() - this.lastVacancyReplenishAt < 5000) return;
      this.lastVacancyReplenishAt = Date.now();
    }
    this.immediateReplenishTimer = setTimeout(async () => {
      this.immediateReplenishTimer = null;
      if (!this.isRunning || this.isReplenishingCycle) return;
      this.isReplenishingCycle = true;
      try {
        await this.runAdmissionCycle();
      } catch (err: any) {
        this.logger.warn(`Error during immediate replenishment (${reason}): ${err?.message}`);
      } finally {
        this.isReplenishingCycle = false;
      }
    }, 50);
  }

  /**
   * Section 5: Admission Gate Obrigatório
   * 
   * CAN_ADMIT_NEW_WORK =
   *   NO_P0_WAITING
   *   AND NO_HEALTHY_P1_CLAIMABLE
   *   AND P2_ACTIVE_COHORT_BELOW_LIMIT
   *   AND SYSTEM_HEALTHY
   * 
   * Se false: obra permanece WAITING_ADMISSION.
   */
  async canAdmitNewWork(): Promise<{
    allowed: boolean;
    reason: string;
    metrics: {
      p0Waiting: number;
      p1Claimable: number;
      p1AvailableChapters: number;
      p1WorksWaiting: number;
      p2ActiveCohortSize: number;
      p2UnfinishedCount: number;
      systemHealthy: boolean;
    };
  }> {
    const config = this.stateStore.getConfig();

    // 1. SYSTEM_HEALTHY
    const isStopActive = await this.protectiveSentinel.isProtectiveStopActive();
    if (isStopActive) {
      return {
        allowed: false,
        reason: 'SYSTEM_UNHEALTHY: PROTECTIVE_STOP is active',
        metrics: {
          p0Waiting: 0,
          p1Claimable: 0,
          p1AvailableChapters: 0,
          p1WorksWaiting: 0,
          p2ActiveCohortSize: 0,
          p2UnfinishedCount: 0,
          systemHealthy: false,
        },
      };
    }

    // 2. NO_P0_WAITING (Real P0 releases: 100 <= priority < 1000; staff-forced >= 1000 belongs to chapter data plane)
    const p0Res = await this.runQuery(`
      SELECT COUNT(*) as p0_cnt
      FROM importer_queue
      WHERE task_type = 'IMPORT_CHAPTER'
        AND status IN ('QUEUED', 'RETRY')
        AND priority >= 100 AND priority < 1000
    `);
    const p0Waiting = parseInt(p0Res.rows[0]?.p0_cnt || '0', 10);
    if (p0Waiting > 0) {
      return {
        allowed: false,
        reason: `P0_WAITING: ${p0Waiting} P0 releases/jobs waiting`,
        metrics: {
          p0Waiting,
          p1Claimable: 0,
          p1AvailableChapters: 0,
          p1WorksWaiting: 0,
          p2ActiveCohortSize: 0,
          p2UnfinishedCount: 0,
          systemHealthy: true,
        },
      };
    }

    // 3. Existing visible works are P1 regardless of the priority that was
    // assigned when they were first discovered.  This is deliberately based
    // on the canonical work visibility, not latest_chapter_published_at: that
    // denormalized timestamp may legitimately be null for older works.
    const p1 = await this.getP1BacklogSnapshot();
    if (p1.available > 0) {
      return {
        allowed: false,
        reason: 'P1_BACKLOG_WAITING: existing catalog work must advance before P2 admission',
        metrics: {
          p0Waiting: 0,
          p1Claimable: p1.claimable,
          p1AvailableChapters: p1.available,
          p1WorksWaiting: p1.works,
          p2ActiveCohortSize: 0,
          p2UnfinishedCount: 0,
          systemHealthy: true,
        },
      };
    }

    // 4. P2_ACTIVE_COHORT_BELOW_LIMIT:
    // Strictly restrict active P2 cohort to <= config.maxActiveNewWorks (default 4).
    // Filter out stale P2 works that have been in FILLING for >= 30m without progress.
    const activeWorks = this.stateStore.getActiveWorks();
    const nowMs = Date.now();
    const activeP2Works = activeWorks.filter((w) => {
      if (w.lane !== 'P2' || w.state !== 'FILLING') return false;
      // If work has no queued or in-flight chapters, it has completed its initial batch and does not block new admissions
      if (w.queuedChapters === 0 && (w.inFlightChapters || 0) === 0) return false;
      const admittedTime = new Date(w.admittedAt).getTime();
      if (w.publishedChapters === 0 && (nowMs - admittedTime >= 30 * 60 * 1000)) {
        return false; // Stale cohort work does not block new admissions
      }
      return true;
    });
    const maxP2Cohort = config.maxActiveNewWorks || 8;

    if (activeP2Works.length >= maxP2Cohort) {
      return {
        allowed: false,
        reason: `P2_ACTIVE_COHORT_FULL: ${activeP2Works.length}/${maxP2Cohort} active works`,
        metrics: {
          p0Waiting: 0,
          p1Claimable: 0,
          p1AvailableChapters: 0,
          p1WorksWaiting: 0,
          p2ActiveCohortSize: activeP2Works.length,
          p2UnfinishedCount: activeP2Works.length,
          systemHealthy: true,
        },
      };
    }

    // 5. WORKER_CAPACITY_CHECK:
    // P0 >>> P1 > P2 > P3. P2 must use the effective chapter capacity, not
    // a historical runner count: otherwise a 3-slot runtime keeps admitting
    // new work and repeatedly scans the hot queue while all slots are busy.
    // If there is spare capacity and activeP2Works < maxP2Cohort:
    // allow P2/P3 new works to admit so P3 never dies of starvation from catalog backlog.
    const inFlightRes = await this.runQuery(
      `SELECT COUNT(*) as cnt FROM importer_queue WHERE status = 'IMPORTING' AND task_type = 'IMPORT_CHAPTER'`
    );
    const importingCnt = parseInt(inFlightRes.rows[0]?.cnt || '0', 10);
    const maxTotalWorkers = Math.max(1, this.chapterCapacityProvider());

    if (importingCnt >= maxTotalWorkers) {
      return {
        allowed: false,
        reason: `WORKERS_FULLY_UTILIZED: ${importingCnt}/${maxTotalWorkers} chapters in-flight`,
        metrics: {
          p0Waiting: 0,
          p1Claimable: 0,
          p1AvailableChapters: 0,
          p1WorksWaiting: 0,
          p2ActiveCohortSize: activeP2Works.length,
          p2UnfinishedCount: activeP2Works.length,
          systemHealthy: true,
        },
      };
    }

    return {
      allowed: true,
      reason: 'CAN_ADMIT_NEW_WORK_ALLOWED',
      metrics: {
        p0Waiting: 0,
        p1Claimable: 0,
        p1AvailableChapters: 0,
        p1WorksWaiting: 0,
        p2ActiveCohortSize: activeP2Works.length,
        p2UnfinishedCount: 0,
        systemHealthy: true,
      },
    };
  }

  /**
   * A bounded health-aware P1 admission signal. This is an existence check,
   * not a catalog aggregate: admission only needs to know whether P1 must go
   * first, and a full COUNT(DISTINCT ...) scan would compete with imports.
   */
  private async getP1BacklogSnapshot(): Promise<{ claimable: number; available: number; works: number }> {
    const now = Date.now();
    if (now - this.p1BacklogProbeAt < 2_000) return this.p1BacklogSnapshot;
    if (this.p1BacklogProbeFlight) return this.p1BacklogProbeFlight;

    const flight = (async () => {
      // Ready P1 uses the existing partial claim index. Only if no ready P1
      // exists do we check the paused window backlog; that slower path is
      // exceptional and avoids turning a normal admission probe into a scan.
      const ready = await this.runQuery(`
        SELECT q.status
        FROM importer_queue q
        JOIN importer_sources s ON s.id = q.source
        WHERE q.task_type = 'IMPORT_CHAPTER'
          AND (q.status = 'QUEUED' OR (q.status = 'RETRY' AND q.next_run_at <= NOW()))
          AND q.attempts < COALESCE(q.max_attempts, 7)
          AND q.priority >= 75 AND q.priority < 100
          AND COALESCE(q.payload->>'staffForced', 'false') <> 'true'
          AND s.enabled = true
          AND (s.status = 'ACTIVE' OR (s.status IN ('COOLDOWN', 'PROBING', 'DEGRADED') AND (s.cooldown_until IS NULL OR s.cooldown_until <= NOW())))
        LIMIT 1
      `);
      const candidate = ready.rows[0];
      if (candidate) {
        this.p1BacklogSnapshot = { claimable: 1, available: 1, works: 1 };
        this.p1BacklogProbeAt = Date.now();
        return this.p1BacklogSnapshot;
      }
      const paused = await this.runQuery(`
        SELECT 1
        FROM importer_queue q
        JOIN importer_sources s ON s.id = q.source
        WHERE q.task_type = 'IMPORT_CHAPTER'
          AND q.status = 'PAUSED_BY_STAFF'
          AND q.attempts < COALESCE(q.max_attempts, 7)
          AND q.priority >= 75 AND q.priority < 100
          AND COALESCE(q.payload->>'staffForced', 'false') <> 'true'
          AND s.enabled = true
          AND (s.status = 'ACTIVE' OR (s.status IN ('COOLDOWN', 'PROBING', 'DEGRADED') AND (s.cooldown_until IS NULL OR s.cooldown_until <= NOW())))
        LIMIT 1
      `);
      this.p1BacklogSnapshot = {
        claimable: 0,
        available: paused.rows.length > 0 ? 1 : 0,
        works: paused.rows.length > 0 ? 1 : 0,
      };
      this.p1BacklogProbeAt = Date.now();
      return this.p1BacklogSnapshot;
    })();
    this.p1BacklogProbeFlight = flight;
    try {
      return await flight;
    } catch (error: any) {
      // Fail closed for P2 admission: if the P1 pressure probe is unavailable,
      // do not make the catalog less fair by admitting more new works.
      this.logger.warn('Unable to read P1 admission pressure; holding P2 admission', { error: error?.message });
      return { claimable: 1, available: 1, works: 0 };
    } finally {
      if (this.p1BacklogProbeFlight === flight) this.p1BacklogProbeFlight = null;
    }
  }

  /** Promote the remaining non-terminal initial batch of one visible work. */
  private async promoteP2WorkToP1(workId: string): Promise<void> {
    await this.runQuery(`
      UPDATE importer_queue
      SET priority = 75,
          payload = jsonb_set(COALESCE(payload, '{}'::jsonb), '{originalPriority}', '75'::jsonb, true),
          updated_at = NOW()
      WHERE task_type = 'IMPORT_CHAPTER'
        AND (payload->>'workId') = $1
        AND status IN ('QUEUED', 'RETRY', 'PAUSED_BY_STAFF')
        AND priority >= 50 AND priority < 75
        AND COALESCE(payload->>'staffForced', 'false') <> 'true'
    `, [workId]);
  }

  /**
   * Reclassify only visible mappings that an older scheduler stranded in P2.
   *
   * The candidate set starts from the indexed ACTIVE mapping state and probes
   * each work through the queue work-id index. Each cycle touches at most 12
   * works, stops permanently when there is nothing left, and is naturally
   * idempotent because promoted rows no longer match priority 50..74. A
   * matching initial window is reopened in the same statement so the normal
   * indexed P1 claim path can immediately see the repaired work.
   */
  private async repairVisibleP2LifecycleBacklog(): Promise<void> {
    if (this.visibleP2LifecycleRepairComplete) return;

    const windowSize = Math.max(1, Math.min(12, this.stateStore.getConfig().slidingWindowSize || 8));
    const result = await this.runQuery(`
      WITH candidate_works AS MATERIALIZED (
        SELECT wm.work_id::text AS work_id
        FROM importer_work_mappings wm
        JOIN works w ON w.id = wm.work_id AND w.published IS TRUE
        WHERE wm.sync_status = 'ACTIVE'
          AND EXISTS (
            SELECT 1
            FROM importer_queue q
            WHERE q.task_type = 'IMPORT_CHAPTER'
              AND (q.payload->>'workId') = wm.work_id::text
              AND q.status IN ('QUEUED', 'RETRY', 'PAUSED_BY_STAFF')
              AND q.priority >= 50 AND q.priority < 75
              AND COALESCE(q.payload->>'staffForced', 'false') <> 'true'
            LIMIT 1
          )
        GROUP BY wm.work_id
        ORDER BY MAX(wm.updated_at) DESC
        LIMIT 12
      ), window_state AS MATERIALIZED (
        SELECT c.work_id,
          COUNT(*) FILTER (
            WHERE q.status = 'QUEUED'
              OR q.status = 'IMPORTING'
              OR (q.status = 'RETRY' AND q.next_run_at <= NOW())
          ) AS ready_count
        FROM candidate_works c
        JOIN importer_queue q ON (q.payload->>'workId') = c.work_id
        WHERE q.task_type = 'IMPORT_CHAPTER'
          AND q.status IN ('QUEUED', 'RETRY', 'PAUSED_BY_STAFF')
          AND q.priority >= 50 AND q.priority < 75
          AND COALESCE(q.payload->>'staffForced', 'false') <> 'true'
        GROUP BY c.work_id
      ), paused_window AS MATERIALIZED (
        SELECT ranked.id
        FROM (
          SELECT q.id, ws.work_id,
            ROW_NUMBER() OVER (PARTITION BY ws.work_id ORDER BY q.chapter_sort_key ASC NULLS LAST) AS position
          FROM window_state ws
          JOIN importer_queue q ON (q.payload->>'workId') = ws.work_id
          WHERE q.task_type = 'IMPORT_CHAPTER'
            AND q.status = 'PAUSED_BY_STAFF'
            AND q.priority >= 50 AND q.priority < 75
            AND COALESCE(q.payload->>'staffForced', 'false') <> 'true'
        ) ranked
        JOIN window_state ws ON ws.work_id = ranked.work_id
        WHERE ranked.position <= GREATEST(0, $1::int - ws.ready_count)
      ), target_jobs AS MATERIALIZED (
        SELECT q.id, ws.work_id
        FROM window_state ws
        JOIN importer_queue q ON (q.payload->>'workId') = ws.work_id
        WHERE q.task_type = 'IMPORT_CHAPTER'
          AND q.status IN ('QUEUED', 'RETRY', 'PAUSED_BY_STAFF')
          AND q.priority >= 50 AND q.priority < 75
          AND COALESCE(q.payload->>'staffForced', 'false') <> 'true'
      ), promoted AS (
        UPDATE importer_queue q
        SET priority = 75,
            payload = jsonb_set(COALESCE(q.payload, '{}'::jsonb), '{originalPriority}', '75'::jsonb, true),
            status = CASE WHEN pw.id IS NOT NULL THEN 'QUEUED' ELSE q.status END,
            next_run_at = CASE WHEN pw.id IS NOT NULL THEN NOW() ELSE q.next_run_at END,
            updated_at = NOW()
        FROM target_jobs t
        LEFT JOIN paused_window pw ON pw.id = t.id
        WHERE q.id = t.id
        RETURNING t.work_id
      )
      SELECT COUNT(DISTINCT work_id)::int AS works, COUNT(*)::int AS jobs
      FROM promoted
    `, [windowSize]);

    const repairedWorks = parseInt(result.rows[0]?.works || '0', 10);
    const repairedJobs = parseInt(result.rows[0]?.jobs || '0', 10);
    if (repairedWorks === 0) {
      this.visibleP2LifecycleRepairComplete = true;
      return;
    }
    this.logger.info(`[P2_TO_P1_LEGACY_REPAIR] Promoted ${repairedJobs} queued chapter(s) across ${repairedWorks} visible work(s).`);
  }

  /**
   * Executes a single admission reconciliation cycle.
   */
  runAdmissionCycle(): Promise<void> {
    if (this.admissionInFlight) return this.admissionInFlight;
    this.admissionInFlight = this.executeAdmissionCycle().finally(() => { this.admissionInFlight = null; });
    return this.admissionInFlight;
  }

  private async executeAdmissionCycle(): Promise<void> {
    const config = this.stateStore.getConfig();
    if (!config.enabled && !config.shadowMode) {
      return;
    }

    // Step 0: Check PROTECTIVE_STOP
    if (await this.protectiveSentinel.isProtectiveStopActive()) {
      this.logger.warn('PROTECTIVE_STOP active, skipping admission cycle');
      return;
    }

    // Run before P1/P2 admission so legacy visible works cannot be bypassed
    // by discovery during the first post-deploy cycle.
    await this.repairVisibleP2LifecycleBacklog();

    // Step 1: Reconcile current active works (check caught-up, in-flight, queued)
    await this.reconcileActiveWorks();

    // Step 2: Replenish active sets if below capacity
    await this.replenishActiveSets();

    // Step 3: Maintain sliding admission windows for all active works
    await this.maintainSlidingWindows();
  }

  /**
   * Step 1: Reconciles all currently tracked active works.
   * Updates their progress, checks if they reached CAUGHT_UP, detects barrier gaps.
   */
  private async reconcileActiveWorks(): Promise<void> {
    const activeWorks = this.stateStore.getActiveWorks();
    if (activeWorks.length === 0) return;

    try {
      // One bounded snapshot for the active cohort; no cache of editorial state.
      // Keep indexed work predicates inside each aggregate to avoid scanning the hot queue.
      const snapshot = await this.runQuery(`
        SELECT w.work_id, q.*, p.*, m.*, s.status AS source_status, s.cooldown_until
        FROM unnest($1::text[], $2::text[]) AS w(work_id, source)
        CROSS JOIN LATERAL (
          SELECT COUNT(*) FILTER (WHERE status='QUEUED' AND attempts < COALESCE(max_attempts,7)) AS queued_cnt,
            COUNT(*) FILTER (WHERE status='IMPORTING') AS importing_cnt,
            COUNT(*) FILTER (WHERE status='PAUSED_BY_STAFF') AS paused_cnt,
            COUNT(*) FILTER (WHERE status='RETRY' AND attempts < COALESCE(max_attempts,7)) AS retry_cnt,
            MIN(chapter_sort_key) FILTER (WHERE status='QUEUED' AND attempts < COALESCE(max_attempts,7)) AS min_queued,
            MIN(chapter_sort_key) FILTER (WHERE status IN ('QUEUED','RETRY','PAUSED_BY_STAFF') AND attempts < COALESCE(max_attempts,7)) AS min_sort_key
          FROM importer_queue WHERE task_type='IMPORT_CHAPTER' AND payload->>'workId'=w.work_id
            AND status IN ('QUEUED','IMPORTING','RETRY','PAUSED_BY_STAFF')
        ) q
        CROSS JOIN LATERAL (
          SELECT COUNT(*) AS pub_cnt, COALESCE(MAX(number),-1) AS max_pub
          FROM chapters WHERE work_id=w.work_id::uuid AND published_at IS NOT NULL
        ) p
        CROSS JOIN LATERAL (
          SELECT COUNT(*) FILTER (WHERE status='STAGED') AS staged_cnt,
            MIN(chapter_sort_key) FILTER (WHERE status='STAGED') AS min_staged,
            COUNT(*) FILTER (WHERE status NOT IN ('COMPLETED','FAILED')) AS unimported_cnt
          FROM importer_chapter_mappings WHERE work_id=w.work_id::uuid
        ) m
        LEFT JOIN importer_sources s ON s.id=w.source
      `, [activeWorks.map(w => w.workId), activeWorks.map(w => w.primarySource)]);
      const byWork = new Map<string, any>(snapshot.rows.map((r: any) => [r.work_id, r]));
      for (const work of activeWorks) {
        try {
          const qRow = byWork.get(work.workId);
          if (!qRow) continue; // Never vacate a work because a snapshot is missing.
          const pubRes = { rows: [qRow] };
          const stagedRes = { rows: [qRow] };
          const queuedCnt = parseInt(qRow?.queued_cnt || '0', 10);
          const importingCnt = parseInt(qRow?.importing_cnt || '0', 10);
          const pausedCnt = parseInt(qRow?.paused_cnt || '0', 10);
          const retryCnt = parseInt(qRow?.retry_cnt || '0', 10);
          const minSortKey = qRow?.min_sort_key ? parseFloat(qRow.min_sort_key) : null;
          const minQueued = qRow?.min_queued ? parseFloat(qRow.min_queued) : minSortKey;
          const pubCnt = parseInt(pubRes.rows[0]?.pub_cnt || '0', 10);
          const rawMaxPub = pubRes.rows[0]?.max_pub;
          const maxPub = rawMaxPub !== undefined && rawMaxPub !== null ? parseFloat(rawMaxPub) : (pubCnt > 0 ? pubCnt : -1);
          const stagedCnt = parseInt(stagedRes.rows[0]?.staged_cnt || '0', 10);
          const minStaged = stagedRes.rows[0]?.min_staged ? parseFloat(stagedRes.rows[0].min_staged) : null;
          const unimportedCnt = parseInt(stagedRes.rows[0]?.unimported_cnt || '0', 10);

          work.queuedChapters = queuedCnt;
          work.inFlightChapters = importingCnt;
          work.publishedChapters = pubCnt;
          work.totalChapters = pubCnt + queuedCnt + importingCnt + pausedCnt + retryCnt;
          work.frontierSortKey = queuedCnt > 0 && minQueued !== null ? minQueued : minSortKey;

          // Promote P2 work to P1 if it has published chapters
          if (pubCnt > 0 && work.lane === 'P2') {
            await this.promoteP2WorkToP1(work.workId);
            this.logger.info(`Work ${work.workTitle} (${work.workId}) promoted from P2 to P1 (${pubCnt} published chapters).`);
            work.lane = 'P1';
          }

          // Critical gap detection: only an actually queued job can unblock the barrier
          if (stagedCnt > 0 && minQueued !== null && minStaged !== null && minQueued < minStaged) {
            work.criticalGapSortKey = minQueued;
            work.criticalGapUnblockCount = stagedCnt;
          } else {
            work.criticalGapSortKey = null;
            work.criticalGapUnblockCount = 0;
          }

          // Gap blocking check: for works with queued chapters, check if the queued frontier is ahead of expected frontier
          const effectiveFrontier = queuedCnt > 0 && minQueued !== null ? minQueued : minSortKey;
          const expectedFrontier = maxPub >= 0 ? maxPub + 1.5 : 1.5;
          let isGapBlocked = effectiveFrontier !== null && effectiveFrontier > expectedFrontier;

          if (isGapBlocked && effectiveFrontier !== null) {
            const gapStart = maxPub >= 0 ? maxPub + 1 : 1;
            const gapEnd = effectiveFrontier - 1;
            // 1. Check if covered by existing confirmed gaps
            const confCheck = await this.runQuery(`
              SELECT COUNT(*) as gap_cnt
              FROM importer_confirmed_gaps
              WHERE work_id = $1::uuid
                AND start_sort_key <= $2::numeric
                AND end_sort_key >= $3::numeric;
            `, [work.workId, gapStart, gapEnd]);
            const confCnt = parseInt(confCheck.rows[0]?.gap_cnt || '0', 10);
            if (confCnt > 0) {
              isGapBlocked = false;
            } else {
              // 2. Try to confirm the upstream gap interval
              const confRes = await confirmUpstreamGapInterval(this.pool, {
                workId: work.workId,
                startSortKey: gapStart,
                endSortKey: gapEnd,
                primarySource: work.primarySource,
                reason: `ADMISSION_RECONCILE_GAP_${gapStart}_TO_${gapEnd}`,
              });
              if (confRes.confirmed) {
                isGapBlocked = false;
                this.logger.info(`Confirmed upstream gap [${gapStart}..${gapEnd}] during admission reconcile for work ${work.workTitle} (${work.workId}).`);
              }
            }
          }

          if (isGapBlocked) {
            this.logger.warn(`Work ${work.workTitle} (${work.workId}) marked BLOCKED due to unresolvable gap. Vacating active slot.`);
            work.state = 'BLOCKED';
            this.stateStore.removeActiveWork(work.workId);
            this.triggerImmediateReplenishment('WORK_GAP_BLOCKED_VACATED');
            continue;
          }

          // Check primary source health
          const srcRow = { status: qRow.source_status, cooldown_until: qRow.cooldown_until };
          const isSourceBlocked = srcRow && (
            srcRow.status === 'DISABLED' ||
            srcRow.status === 'PAUSED' ||
            srcRow.status === 'DEGRADED' ||
            (srcRow.status === 'COOLDOWN' && srcRow.cooldown_until && new Date(srcRow.cooldown_until) > new Date())
          );

          if (isSourceBlocked) {
            this.logger.info(`Work ${work.workTitle} (${work.workId}) marked BLOCKED (source ${work.primarySource} in cooldown/blocked). Vacating active slot.`);
            work.state = 'BLOCKED';
            this.stateStore.removeActiveWork(work.workId);
            this.triggerImmediateReplenishment('WORK_SOURCE_BLOCKED_VACATED');
            continue;
          } else if (work.state === 'BLOCKED') {
            this.logger.info(`Work ${work.workTitle} (${work.workId}) unblocked as source ${work.primarySource} recovered.`);
            work.state = 'FILLING';
            this.stateStore.setActiveWork(work);
          }

          // A P1 work has consumed its admitted window.  Return it to the
          // rotating cohort before opening another window so a large source
          // cannot keep the same handful of works active indefinitely. Its
          // remaining PAUSED_BY_STAFF jobs stay intact and are admitted again
          // by the durable per-source cursor; no chapter is discarded.
          if (work.lane === 'P1' && queuedCnt === 0 && importingCnt === 0 && retryCnt === 0 && pausedCnt > 0) {
            this.logger.info(`[P1_COHORT_ROTATED] Work ${work.workTitle} (${work.workId}) completed its admission window; rotating to another eligible P1 work.`);
            this.stateStore.removeActiveWork(work.workId);
            this.triggerImmediateReplenishment('P1_WINDOW_ROTATED');
            continue;
          }

          // A cohort is only drained when it has no non-terminal queue state.
          // PAUSED_BY_STAFF is the sliding-window backlog, not completion;
          // RETRY is pending work, not completion. Treating either as empty
          // abandoned newly-visible works after their first small P2 window.
          if (queuedCnt === 0 && importingCnt === 0 && pausedCnt === 0 && retryCnt === 0) {
            const isCaughtUp = unimportedCnt === 0 && pubCnt > 0;
            const stateLabel = isCaughtUp ? 'CAUGHT_UP' : 'DRAINED';
            // Capture the cohort before mutating this work's state. Counting
            // only FILLING after changing it to COMPLETE produced misleading
            // "0 -> -1" vacancy telemetry for a real one-work cohort.
            const beforeCount = this.stateStore.getActiveWorks().length;
            work.state = isCaughtUp ? 'CAUGHT_UP' : 'COMPLETE';
            this.logger.info(`[ACTIVE_SET_VACATED] Work ${work.workTitle} (${work.workId}) reached ${stateLabel} state (${queuedCnt} queued, ${importingCnt} in-flight, ${pausedCnt} paused, ${retryCnt} retry, ${unimportedCnt} unimported mappings). Vacating active slot. ACTIVE SET BEFORE: ${beforeCount} -> AFTER: ${beforeCount - 1}`);
            this.stateStore.removeActiveWork(work.workId);
            this.triggerImmediateReplenishment('WORK_DRAINED_VACATED');
            continue;
          }

          // Check if P2 new work is stale in cohort (admitted >= 30m ago with 0 in-flight and no progress)
          if (work.lane === 'P2') {
            const admittedMs = new Date(work.admittedAt).getTime();
            const nowMs = Date.now();
            if (work.publishedChapters === 0 && importingCnt === 0 && (nowMs - admittedMs >= 30 * 60 * 1000)) {
              this.logger.warn(`[ACTIVE_SET_VACATED] Stale P2 work ${work.workTitle} (${work.workId}) vacated from active cohort (>30m with 0 in-flight) to allow new admissions.`);
              this.stateStore.removeActiveWork(work.workId);
              this.triggerImmediateReplenishment('STALE_P2_VACATED');
              continue;
            }
          }

          work.state = 'FILLING';
          if (importingCnt > 0) {
            work.lastActivityAt = new Date().toISOString();
          }
          this.stateStore.setActiveWork(work);
        } catch (workErr: any) {
          this.logger.warn(`Failed to reconcile active work ${work.workId}`, { error: workErr?.message });
        }
      }
    } catch (err: any) {
      this.logger.warn('Failed to reconcile active works', { error: err?.message });
    }
  }

  /**
   * Step 2: Replenishes active sets (P1 Backfill and P2 New Works) if slots are free.
   * Work-conserving: considers actual worker utilization and elastic capacity.
   */
  private async replenishActiveSets(): Promise<void> {
    const config = this.stateStore.getConfig();
    const activeWorks = this.stateStore.getActiveWorks();

    // Only FILLING works consume active logical capacity (BLOCKED works do not)
    const nowMs = Date.now();
    const activeBackfills = activeWorks.filter((w) => w.lane === 'P1' && w.state === 'FILLING');
    const activeNewWorks = activeWorks.filter((w) => {
      if (w.lane !== 'P2' || w.state !== 'FILLING') return false;
      const admittedMs = new Date(w.admittedAt).getTime();
      if (w.publishedChapters === 0 && (nowMs - admittedMs >= 30 * 60 * 1000)) {
        return false;
      }
      return true;
    });

    // Measure current worker utilization for elastic scheduling (Section 7 & 8)
    let idleWorkers = 0;
    try {
      const qAct = await this.runQuery(
        `SELECT COUNT(*) as cnt FROM importer_queue WHERE status = 'IMPORTING' AND task_type = 'IMPORT_CHAPTER'`
      );
      const importingCnt = parseInt(qAct.rows[0]?.cnt || '0', 10);
      idleWorkers = Math.max(0, this.chapterCapacityProvider() - importingCnt);
    } catch {}

    // Elastic backfill: if workers are idle, allow expanding active P1 up to 36 works
    // to guarantee full worker utilization without violating maxInflightPerWork = 2.
    const targetBackfillLimit = idleWorkers >= 1 ? 36 : config.maxActiveBackfillWorks;
    const backfillSlotsAvailable = Math.max(0, targetBackfillLimit - activeBackfills.length);

    // P2 uses spare capacity when P1 cannot occupy available workers
    // Strictly restrict active P2 cohort to <= config.maxActiveNewWorks (default 8).
    const maxP2Cohort = config.maxActiveNewWorks || 8;
    const targetNewWorksLimit = idleWorkers >= 1 ? maxP2Cohort : 4;
    const newWorkSlotsAvailable = Math.max(0, targetNewWorksLimit - activeNewWorks.length);

    // Track only work that is actually consuming a chapter slot. A logical
    // cohort entry without in-flight work must not reserve a source forever.
    const sourceCounts = new Map<string, number>();
    for (const w of activeWorks.filter((w) => w.state === 'FILLING' && (w.inFlightChapters || 0) > 0)) {
      sourceCounts.set(w.primarySource, (sourceCounts.get(w.primarySource) || 0) + 1);
    }

    // 1. Replenish P1 Backfill Works
    if (backfillSlotsAvailable > 0) {
      const activeIds = activeWorks.map((w) => w.workId);
      const candidatesRes = await this.runQuery(
        `WITH queue_candidate_groups AS MATERIALIZED (
           SELECT payload->>'workId' AS work_id, source, COUNT(*) AS pending_jobs,
             COUNT(*) FILTER (WHERE status = 'QUEUED' OR (status = 'RETRY' AND next_run_at <= NOW())) AS queued_count,
             COUNT(*) FILTER (WHERE status = 'PAUSED_BY_STAFF') AS paused_count,
             MIN(chapter_sort_key) AS min_sort_key
           FROM importer_queue
           WHERE task_type='IMPORT_CHAPTER'
             AND (status = 'QUEUED' OR status = 'PAUSED_BY_STAFF' OR (status = 'RETRY' AND next_run_at <= NOW()))
             AND attempts < COALESCE(max_attempts,7)
             AND priority >= 75 AND priority < 100
             AND COALESCE(payload->>'staffForced', 'false') <> 'true'
             AND NOT ((payload->>'workId') = ANY($1::text[]))
           GROUP BY payload->>'workId', source
         ),
         queue_candidates AS MATERIALIZED (
           -- Rotate work selection independently from chapter number. Keep the
           -- earliest frontier as one bounded fallback per source: a cursor
           -- candidate can legitimately be parked behind an unresolved gap.
           SELECT ranked.*
           FROM (
             SELECT g.*,
             ROW_NUMBER() OVER (
               PARTITION BY source
               ORDER BY CASE WHEN work_id > COALESCE($4::jsonb ->> source, '') THEN 0 ELSE 1 END,
                        work_id
             ) AS rotation_rank,
             ROW_NUMBER() OVER (
               PARTITION BY source
               ORDER BY min_sort_key ASC NULLS LAST, pending_jobs DESC
             ) AS frontier_rank
             FROM queue_candidate_groups g
           ) ranked
           WHERE rotation_rank <= $3 OR frontier_rank = 1
         )
         SELECT q.work_id,
                w.title,
                q.source,
                q.pending_jobs,
                q.queued_count,
                q.paused_count,
                q.min_sort_key,
                CASE WHEN q.rotation_rank <= $3 THEN q.rotation_rank ELSE 100 + q.frontier_rank END AS admission_rank
         FROM queue_candidates q
         JOIN works w ON w.id = q.work_id::uuid
         JOIN importer_sources s ON s.id = q.source
         WHERE w.published = true
           AND s.enabled = true
           AND (s.status = 'ACTIVE' OR (s.status IN ('COOLDOWN', 'PROBING', 'DEGRADED') AND (s.cooldown_until IS NULL OR s.cooldown_until <= NOW())))
         -- One candidate from every source before a second candidate from any
         -- source. The bounded result remains diverse even with many sources.
         ORDER BY admission_rank, source
         LIMIT $2`,
        [
          activeIds.length > 0 ? activeIds : ['00000000-0000-0000-0000-000000000000'],
          Math.max(50, backfillSlotsAvailable * 5),
          4,
          JSON.stringify(this.getP1AdmissionCursors()),
        ]
      );

      // Fast frontier check: query max_published for candidate works only
      const candidateWorkIds = candidatesRes.rows.map((r: any) => r.work_id);
      const pubMap = new Map<string, number>();
      if (candidateWorkIds.length > 0) {
        const pubRes = await this.runQuery(
          `SELECT work_id::text, COALESCE(MAX(number), -1) as max_pub
           FROM chapters
           WHERE work_id = ANY($1::uuid[]) AND published_at IS NOT NULL
           GROUP BY work_id`,
          [candidateWorkIds]
        );
        for (const pr of pubRes.rows) {
          pubMap.set(pr.work_id, parseFloat(pr.max_pub));
        }
      }

      // Check confirmed gaps for candidate works
      const gapsMap = new Map<string, Array<{ start: number; end: number }>>();
      if (candidateWorkIds.length > 0) {
        try {
          const gapsRes = await this.runQuery(
            `SELECT work_id::text, start_sort_key, end_sort_key
             FROM importer_confirmed_gaps
             WHERE work_id = ANY($1::uuid[])`,
            [candidateWorkIds]
          );
          for (const gr of gapsRes.rows) {
            const arr = gapsMap.get(gr.work_id) || [];
            arr.push({ start: parseFloat(gr.start_sort_key), end: parseFloat(gr.end_sort_key) });
            gapsMap.set(gr.work_id, arr);
          }
        } catch {}
      }

      const isContiguousOrConfirmed = (workId: string, minSort: number, maxPub: number): boolean => {
        if (maxPub === -1) {
          if (minSort <= 1.5) return true;
        } else {
          if (minSort <= maxPub + 1.5) return true;
        }
        const gapStart = maxPub >= 0 ? maxPub + 1 : 1;
        const gapEnd = minSort - 1;
        const intervals = gapsMap.get(workId) || [];
        return intervals.some((g) => g.start <= gapStart && g.end >= gapEnd);
      };

      // Keep contiguous or confirmed candidates
      const contiguousCandidates = candidatesRes.rows.filter((cand: any) => {
        const maxPub = pubMap.get(cand.work_id) ?? -1;
        const minSort = cand.min_sort_key ? parseFloat(cand.min_sort_key) : 0;
        return isContiguousOrConfirmed(cand.work_id, minSort, maxPub);
      });

      // Try confirming upstream gaps for non-contiguous candidates if slots need replenishment
      if (contiguousCandidates.length < backfillSlotsAvailable) {
        for (const cand of candidatesRes.rows) {
          if (contiguousCandidates.some((c: any) => c.work_id === cand.work_id)) continue;
          const maxPub = pubMap.get(cand.work_id) ?? -1;
          const minSort = cand.min_sort_key ? parseFloat(cand.min_sort_key) : 0;
          const gapStart = maxPub >= 0 ? maxPub + 1 : 1;
          const gapEnd = minSort - 1;
          if (gapStart <= gapEnd) {
            try {
              const conf = await confirmUpstreamGapInterval(this.pool, {
                workId: cand.work_id,
                startSortKey: gapStart,
                endSortKey: gapEnd,
                primarySource: cand.source,
                reason: 'ADMISSION_CANDIDATE_GAP_CONFIRM',
              });
              if (conf.confirmed) {
                contiguousCandidates.push(cand);
              }
            } catch {}
          }
        }
      }

      // Sort candidates by source permit headroom and diversity (Section 8 & 11)
      contiguousCandidates.sort((a: any, b: any) => {
        const permitsA = this.sourcePermitProvider ? this.sourcePermitProvider(a.source) : 1;
        const permitsB = this.sourcePermitProvider ? this.sourcePermitProvider(b.source) : 1;
        const activeA = sourceCounts.get(a.source) || 0;
        const activeB = sourceCounts.get(b.source) || 0;

        if ((permitsA > 0) !== (permitsB > 0)) {
          return permitsA > 0 ? -1 : 1;
        }
        if (activeA !== activeB) {
          return activeA - activeB;
        }
        const rankA = parseInt(a.admission_rank || '1000', 10);
        const rankB = parseInt(b.admission_rank || '1000', 10);
        if (rankA !== rankB) return rankA - rankB;
        return parseInt(b.queued_count || '0', 10) - parseInt(a.queued_count || '0', 10);
      });

      let admitted = 0;
      for (const cand of contiguousCandidates) {
        if (admitted >= backfillSlotsAvailable) break;

        const srcCount = sourceCounts.get(cand.source) || 0;
        const maxWorksPerSource = idleWorkers >= 2 ? 4 : 3;
        const otherSourceCandidates = candidatesRes.rows.filter((r: any) => (sourceCounts.get(r.source) || 0) < maxWorksPerSource);
        if (srcCount >= maxWorksPerSource && otherSourceCandidates.length > 0) {
          continue;
        }

        const newWork: ActiveWork = {
          workId: cand.work_id,
          workTitle: cand.title || 'Unknown Title',
          lane: 'P1',
          state: 'FILLING',
          primarySource: cand.source,
          admittedAt: new Date().toISOString(),
          lastActivityAt: new Date().toISOString(),
          totalChapters: parseInt(cand.pending_jobs, 10),
          publishedChapters: 0,
          queuedChapters: parseInt(cand.queued_count, 10),
          inFlightChapters: 0,
          frontierSortKey: cand.min_sort_key ? parseFloat(cand.min_sort_key) : null,
          criticalGapSortKey: null,
          criticalGapUnblockCount: 0,
        };

        this.stateStore.setActiveWork(newWork);
        this.advanceP1AdmissionCursor(cand.source, cand.work_id);
        sourceCounts.set(cand.source, srcCount + 1);
        admitted++;
        const activeAfter = this.stateStore.getActiveWorks().filter((w) => w.state === 'FILLING').length;
        this.logger.info(`[ADMISSION_TRIGGERED] Admitted work into ACTIVE_BACKFILL_WORKS (P1). Active Set Now: ${activeAfter}`, {
          workId: newWork.workId,
          title: newWork.workTitle,
          source: newWork.primarySource,
          pendingJobs: cand.pending_jobs,
          admissionsTriggered: admitted,
        });
      }
    }

    // 2. Replenish P2 New Works (Strictly guarded by Admission Gate)
    const p2Gate = await this.canAdmitNewWork();
    if (!p2Gate.allowed) {
      this.logger.debug(`[ADMISSION_GATE_HOLD] P2 replenishment blocked: ${p2Gate.reason}`);
    } else if (newWorkSlotsAvailable > 0) {
      const activeIds = this.stateStore.getActiveWorks().map((w) => w.workId);
      const candidatesRes = await this.runQuery(
        `WITH queue_candidates AS MATERIALIZED (
           SELECT (q.payload->>'workId') as work_id,
                w.title,
                q.source,
                COUNT(*) as pending_jobs,
                COUNT(CASE WHEN q.status = 'QUEUED' THEN 1 END) as queued_count,
                COUNT(CASE WHEN q.status = 'PAUSED_BY_STAFF' THEN 1 END) as paused_count,
                MIN(q.chapter_sort_key) as min_sort_key,
                w.created_at
         FROM importer_queue q
         JOIN works w ON w.id = (q.payload->>'workId')::uuid
         JOIN importer_sources s ON s.id = q.source
         WHERE q.task_type = 'IMPORT_CHAPTER'
           AND q.status IN ('QUEUED', 'RETRY', 'PAUSED_BY_STAFF')
           AND w.published IS FALSE
           AND s.enabled = true
           AND (s.status = 'ACTIVE' OR (s.status IN ('COOLDOWN', 'PROBING', 'DEGRADED') AND (s.cooldown_until IS NULL OR s.cooldown_until <= NOW())))
           AND NOT ((q.payload->>'workId') = ANY($1::text[]))
         GROUP BY (q.payload->>'workId'), w.title, q.source, w.created_at
         )
         SELECT work_id, title, source, pending_jobs, queued_count, paused_count, min_sort_key, created_at
         FROM (
           SELECT q.*,
             ROW_NUMBER() OVER (
               PARTITION BY source
               ORDER BY created_at DESC
             ) AS source_rank
           FROM queue_candidates q
         ) ranked
         WHERE source_rank <= $3
         ORDER BY created_at DESC
         LIMIT $2`,
        [
          activeIds.length > 0 ? activeIds : ['00000000-0000-0000-0000-000000000000'],
          newWorkSlotsAvailable * 3,
          4,
        ]
      );

      // Sort P2 candidates by permit headroom and source diversity
      candidatesRes.rows.sort((a: any, b: any) => {
        const permitsA = this.sourcePermitProvider ? this.sourcePermitProvider(a.source) : 1;
        const permitsB = this.sourcePermitProvider ? this.sourcePermitProvider(b.source) : 1;
        const activeA = sourceCounts.get(a.source) || 0;
        const activeB = sourceCounts.get(b.source) || 0;

        if ((permitsA > 0) !== (permitsB > 0)) {
          return permitsA > 0 ? -1 : 1;
        }
        if (activeA !== activeB) {
          return activeA - activeB;
        }
        return new Date(b.created_at || 0).getTime() - new Date(a.created_at || 0).getTime();
      });

      let admitted = 0;
      for (const cand of candidatesRes.rows) {
        if (admitted >= newWorkSlotsAvailable) break;

        const srcCount = sourceCounts.get(cand.source) || 0;
        const maxWorksPerSource = idleWorkers >= 2 ? 4 : 3;
        const otherSourceCandidates = candidatesRes.rows.filter((r: any) => (sourceCounts.get(r.source) || 0) < maxWorksPerSource);
        if (srcCount >= maxWorksPerSource && otherSourceCandidates.length > 0) {
          continue;
        }

        // Validate initial upstream gap if catalog begins beyond chapter 1 (Section 6 & Test E)
        const minSort = cand.min_sort_key ? parseFloat(cand.min_sort_key) : 1;
        if (minSort > 1.5) {
          let gapCovered = false;
          try {
            const confCheck = await this.runQuery(`
              SELECT COUNT(*) as gap_cnt
              FROM importer_confirmed_gaps
              WHERE work_id = $1::uuid
                AND start_sort_key <= 1.5
                AND end_sort_key >= $2::numeric - 1
            `, [cand.work_id, minSort]);
            if (parseInt(confCheck.rows[0]?.gap_cnt || '0', 10) > 0) {
              gapCovered = true;
            } else {
              const conf = await confirmUpstreamGapInterval(this.pool, {
                workId: cand.work_id,
                startSortKey: 1,
                endSortKey: minSort - 1,
                primarySource: cand.source,
                reason: 'P2_INITIAL_UPSTREAM_GAP',
              });
              gapCovered = conf.confirmed;
            }
          } catch {}
          if (!gapCovered) {
            continue;
          }
        }

        const newWork: ActiveWork = {
          workId: cand.work_id,
          workTitle: cand.title || 'Unknown Title',
          lane: 'P2',
          state: 'FILLING',
          primarySource: cand.source,
          admittedAt: new Date().toISOString(),
          lastActivityAt: new Date().toISOString(),
          totalChapters: parseInt(cand.pending_jobs, 10),
          publishedChapters: 0,
          queuedChapters: parseInt(cand.queued_count, 10),
          inFlightChapters: 0,
          frontierSortKey: cand.min_sort_key ? parseFloat(cand.min_sort_key) : null,
          criticalGapSortKey: null,
          criticalGapUnblockCount: 0,
        };

        this.stateStore.setActiveWork(newWork);
        sourceCounts.set(cand.source, srcCount + 1);
        admitted++;

        // 1. Transition work mapping from WAITING_ADMISSION to ACTIVE
        try {
          await this.runQuery(
            `UPDATE importer_work_mappings 
             SET sync_status = 'ACTIVE', updated_at = NOW() 
             WHERE work_id = $1::uuid AND sync_status = 'WAITING_ADMISSION'`,
            [newWork.workId]
          );
        } catch {}

        // 2. Promote initial sliding window of chapters from PAUSED_BY_STAFF to QUEUED (priority 50)
        try {
          const promRes = await this.runQuery(
            `WITH to_promote AS (
               SELECT id FROM importer_queue
               WHERE (payload->>'workId') = $1
                 AND task_type = 'IMPORT_CHAPTER'
                 AND status = 'PAUSED_BY_STAFF'
               ORDER BY chapter_sort_key ASC NULLS LAST
               LIMIT 8
             )
             UPDATE importer_queue q
             SET status = 'QUEUED', priority = 50, next_run_at = NOW(), updated_at = NOW()
             FROM to_promote
             WHERE q.id = to_promote.id
             RETURNING q.id`,
            [newWork.workId]
          );
          if (promRes.rows.length > 0) {
            newWork.queuedChapters = promRes.rows.length;
            this.stateStore.setActiveWork(newWork);
          }
        } catch {}

        this.logger.info(`[ADMISSION_TRIGGERED] Admitted work into ACTIVE_NEW_WORKS (P2) via spare capacity`, {
          workId: newWork.workId,
          title: newWork.workTitle,
          source: newWork.primarySource,
          pendingJobs: cand.pending_jobs,
          idleWorkers,
        });
      }
    }
  }

  /**
   * Step 3: Maintains sliding windows for active works.
   * When an active work has fewer than `slidingWindowMin` queued chapters,
   * promotes the next batch (up to `slidingWindowSize`) from PAUSED_BY_STAFF to QUEUED.
   */
  private async maintainSlidingWindows(): Promise<void> {
    const config = this.stateStore.getConfig();
    const activeWorks = this.stateStore.getActiveWorks();

    for (const work of activeWorks) {
      if (work.state !== 'FILLING') continue;
      if (work.queuedChapters < config.slidingWindowMin) {
        const needed = config.slidingWindowSize - work.queuedChapters;
        if (needed <= 0) continue;

        // Check if this work has critical unblocking gap
        let targetPriority = work.lane === 'P1' ? 75 : 50;
        if (work.criticalGapSortKey !== null) {
          targetPriority = 95; // P1_CRITICAL_GAP boost
        }

        // Select the next slice of chapters from PAUSED_BY_STAFF ordered by chapter_sort_key ASC
        const promoteRes = await this.runQuery(
          `WITH to_promote AS (
             SELECT id
             FROM importer_queue
             WHERE status = 'PAUSED_BY_STAFF'
               AND task_type = 'IMPORT_CHAPTER'
               AND (payload->>'workId') = $1
             ORDER BY chapter_sort_key ASC NULLS LAST
             LIMIT $2
           )
           UPDATE importer_queue q
           SET status = 'QUEUED',
               priority = $3,
               next_run_at = NOW(),
               updated_at = NOW()
           FROM to_promote
           WHERE q.id = to_promote.id
           RETURNING q.id, q.chapter_sort_key;`,
          [work.workId, needed, targetPriority]
        );

        if (promoteRes.rows.length > 0) {
          work.queuedChapters += promoteRes.rows.length;
          this.stateStore.setActiveWork(work);
          this.logger.info(`Promoted ${promoteRes.rows.length} chapters into QUEUED for work ${work.workTitle}`, {
            workId: work.workId,
            lane: work.lane,
            priority: targetPriority,
            chapters: promoteRes.rows.map((r: any) => r.chapter_sort_key),
          });
        }
      }
    }
  }

  /**
   * On-demand admission: admits the highest priority waiting work into the active set
   * when workers are idle and currently active works cannot supply jobs.
   * Work-conserving and strictly controlled: preserves work-affinity, fairness, and sliding window.
   */
  admitNextWorkOnDemand(
    preferredLane?: 'P1' | 'P2',
    allowedSources?: string[]
  ): Promise<ActiveWork | null> {
    const key = `${preferredLane || 'all'}:${[...(allowedSources || [])].sort().join(',')}`;
    const existing = this.demandFlights.get(key);
    if (existing) return existing;
    const flight = this.executeOnDemandAdmission(preferredLane, allowedSources)
      .finally(() => { this.demandFlights.delete(key); });
    this.demandFlights.set(key, flight);
    return flight;
  }

  private async executeOnDemandAdmission(
    preferredLane?: 'P1' | 'P2',
    allowedSources?: string[]
  ): Promise<ActiveWork | null> {
    const config = this.stateStore.getConfig();
    if (!config.enabled && !config.shadowMode) return null;
    if (await this.protectiveSentinel.isProtectiveStopActive()) return null;

    const activeWorks = this.stateStore.getActiveWorks();
    const activeIds = activeWorks.map((w) => w.workId);

    const sourceCounts = new Map<string, number>();
    for (const w of activeWorks.filter((w) => w.state === 'FILLING' && (w.inFlightChapters || 0) > 0)) {
      sourceCounts.set(w.primarySource, (sourceCounts.get(w.primarySource) || 0) + 1);
    }
    const saturatedSources = Array.from(sourceCounts.entries())
      .filter(([src, cnt]) => cnt >= 4)
      .map(([src]) => src);

    const lanesToTry = preferredLane === 'P1'
      ? (['P1', 'P2'] as const)
      : (preferredLane ? [preferredLane] : (['P1', 'P2'] as const));

    for (const lane of lanesToTry) {
      if (lane === 'P2') {
        const gate = await this.canAdmitNewWork();
        if (!gate.allowed) {
          this.logger.debug(`[ADMISSION_GATE_HOLD] admitNextWorkOnDemand blocked for P2: ${gate.reason}`);
          continue;
        }
      }
      const isP1 = lane === 'P1';
      const maxPriority = isP1 ? 100 : 75;
      const query = `
        WITH queue_candidates AS MATERIALIZED (
          SELECT payload->>'workId' AS work_id, source, COUNT(*) AS pending_jobs,
            COUNT(*) FILTER (WHERE status = 'QUEUED' OR (status = 'RETRY' AND next_run_at <= NOW())) AS queued_count,
            COUNT(*) FILTER (WHERE status = 'PAUSED_BY_STAFF') AS paused_count,
            MIN(chapter_sort_key) AS min_sort_key
          FROM importer_queue
          WHERE task_type='IMPORT_CHAPTER'
            AND (status = 'QUEUED' OR status = 'PAUSED_BY_STAFF' OR (status = 'RETRY' AND next_run_at <= NOW()))
            AND attempts < COALESCE(max_attempts,7)
            AND priority >= ${isP1 ? 75 : 50} AND priority < ${maxPriority}
            AND COALESCE(payload->>'staffForced', 'false') <> 'true'
            AND ($1::text[] IS NULL OR source = ANY($1::text[]))
            AND NOT ((payload->>'workId') = ANY($2::text[]))
            AND ($3::text[] IS NULL OR NOT (source = ANY($3::text[])))
          GROUP BY payload->>'workId', source
        )
        SELECT q.work_id,
               w.title,
               q.source,
               q.pending_jobs,
               q.queued_count,
               q.min_sort_key
        FROM queue_candidates q
        JOIN works w ON w.id = q.work_id::uuid
        JOIN importer_sources s ON s.id = q.source
        WHERE ${isP1 ? 'w.published = true' : 'w.published IS FALSE'}
          AND s.enabled = true
          AND (s.status = 'ACTIVE' OR (s.status IN ('COOLDOWN', 'PROBING', 'DEGRADED') AND (s.cooldown_until IS NULL OR s.cooldown_until <= NOW())))
        -- On-demand admission is only a spare-capacity fallback. Preserve its
        -- proven publication-frontier order; periodic cohort rotation provides
        -- the cross-work fairness.
        ORDER BY min_sort_key ASC NULLS LAST, queued_count DESC
        LIMIT 10;
      `;

      const res = await this.runQuery(query, [
        allowedSources && allowedSources.length > 0 ? allowedSources : null,
        activeIds.length > 0 ? activeIds : ['00000000-0000-0000-0000-000000000000'],
        saturatedSources.length > 0 ? saturatedSources : null,
      ]);

      if (res.rows.length === 0) continue;

      const candWorkIds = res.rows.map((r: any) => r.work_id);
      const pubMap = new Map<string, number>();
      if (candWorkIds.length > 0) {
        const pubRes = await this.runQuery(
          `SELECT work_id::text, COALESCE(MAX(number), -1) as max_pub
           FROM chapters
           WHERE work_id = ANY($1::uuid[]) AND published_at IS NOT NULL
           GROUP BY work_id`,
          [candWorkIds]
        );
        for (const pr of pubRes.rows) {
          pubMap.set(pr.work_id, parseFloat(pr.max_pub));
        }
      }

      // Check confirmed gaps
      const gapsMap = new Map<string, Array<{ start: number; end: number }>>();
      if (candWorkIds.length > 0) {
        try {
          const gapsRes = await this.runQuery(
            `SELECT work_id::text, start_sort_key, end_sort_key
             FROM importer_confirmed_gaps
             WHERE work_id = ANY($1::uuid[])`,
            [candWorkIds]
          );
          for (const gr of gapsRes.rows) {
            const arr = gapsMap.get(gr.work_id) || [];
            arr.push({ start: parseFloat(gr.start_sort_key), end: parseFloat(gr.end_sort_key) });
            gapsMap.set(gr.work_id, arr);
          }
        } catch {}
      }

      // Sort candidate rows by permit headroom and diversity
      res.rows.sort((a: any, b: any) => {
        const permitsA = this.sourcePermitProvider ? this.sourcePermitProvider(a.source) : 1;
        const permitsB = this.sourcePermitProvider ? this.sourcePermitProvider(b.source) : 1;
        const activeA = sourceCounts.get(a.source) || 0;
        const activeB = sourceCounts.get(b.source) || 0;

        if ((permitsA > 0) !== (permitsB > 0)) {
          return permitsA > 0 ? -1 : 1;
        }
        if (activeA !== activeB) {
          return activeA - activeB;
        }
        return parseInt(b.queued_count || '0', 10) - parseInt(a.queued_count || '0', 10);
      });

      const isCandidateFrontierValid = (workId: string, minSort: number, maxPub: number): boolean => {
        if (isP1) {
          if (maxPub === -1 || minSort <= maxPub + 1.5) return true;
        } else {
          if (minSort <= 1.5) return true;
        }
        const gapStart = maxPub >= 0 ? maxPub + 1 : 1;
        const gapEnd = minSort - 1;
        const intervals = gapsMap.get(workId) || [];
        return intervals.some((g) => g.start <= gapStart && g.end >= gapEnd);
      };

      let match = res.rows.find((cand: any) => {
        const maxPub = pubMap.get(cand.work_id) ?? -1;
        const minSort = cand.min_sort_key ? parseFloat(cand.min_sort_key) : 0;
        return isCandidateFrontierValid(cand.work_id, minSort, maxPub);
      });

      // If no match found directly, attempt confirmUpstreamGapInterval on top candidates
      if (!match && res.rows.length > 0) {
        for (const cand of res.rows) {
          const maxPub = pubMap.get(cand.work_id) ?? -1;
          const minSort = cand.min_sort_key ? parseFloat(cand.min_sort_key) : 0;
          const gapStart = maxPub >= 0 ? maxPub + 1 : 1;
          const gapEnd = minSort - 1;
          if (gapStart <= gapEnd) {
            try {
              const conf = await confirmUpstreamGapInterval(this.pool, {
                workId: cand.work_id,
                startSortKey: gapStart,
                endSortKey: gapEnd,
                primarySource: cand.source,
                reason: 'ON_DEMAND_ADMISSION_GAP_CONFIRM',
              });
              if (conf.confirmed) {
                match = cand;
                break;
              }
            } catch {}
          }
        }
      }

      if (match) {
        const cand = match;
        const newWork: ActiveWork = {
          workId: cand.work_id,
          workTitle: cand.title || 'Unknown Title',
          lane,
          state: 'FILLING',
          primarySource: cand.source,
          admittedAt: new Date().toISOString(),
          lastActivityAt: new Date().toISOString(),
          totalChapters: parseInt(cand.pending_jobs || '0', 10),
          publishedChapters: 0,
          queuedChapters: parseInt(cand.queued_count || '0', 10),
          inFlightChapters: 0,
          frontierSortKey: cand.min_sort_key ? parseFloat(cand.min_sort_key) : null,
          criticalGapSortKey: null,
          criticalGapUnblockCount: 0,
        };

        this.stateStore.setActiveWork(newWork);
        if (isP1) this.advanceP1AdmissionCursor(cand.source, cand.work_id);

        // If this work has fewer than slidingWindowMin queued chapters, promote next batch
        if (newWork.queuedChapters < config.slidingWindowMin) {
          const needed = config.slidingWindowSize - newWork.queuedChapters;
          const targetPriority = isP1 ? 75 : 50;
          const promoteRes = await this.runQuery(
            `WITH to_promote AS (
               SELECT id
               FROM importer_queue
               WHERE status = 'PAUSED_BY_STAFF'
                 AND task_type = 'IMPORT_CHAPTER'
                 AND (payload->>'workId') = $1
               ORDER BY chapter_sort_key ASC NULLS LAST
               LIMIT $2
             )
             UPDATE importer_queue q
             SET status = 'QUEUED',
                 priority = $3,
                 next_run_at = NOW(),
                 updated_at = NOW()
             FROM to_promote
             WHERE q.id = to_promote.id
             RETURNING q.id;`,
            [newWork.workId, needed, targetPriority]
          );
          newWork.queuedChapters += promoteRes.rows.length;
          this.stateStore.setActiveWork(newWork);
        }

        this.logger.info(`[ON_DEMAND_ADMISSION] Admitted work ${newWork.workTitle} into ${lane}`, {
          workId: newWork.workId,
          lane,
          source: newWork.primarySource,
        });

        return newWork;
      }
    }
    return null;
  }
}
