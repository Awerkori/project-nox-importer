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
import { isSourceExecutionEligible, SOURCE_EXECUTION_ELIGIBILITY_SQL } from '../source-eligibility.js';

// P1 is the shared existing-catalog lane.  A one-chapter admission window is
// deliberately a *scheduling* fairness quantum, not a media/DB resource
// limit: it prevents a large work from receiving another window before other
// healthy P1 works get their first opportunity on the same source.
const P1_FAIR_WINDOW_CHAPTERS = 1;

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
  private deadWorksCache = new Map<string, number>();
  // Periodic reconciliation and on-demand admission both execute bounded
  // GROUP BY/frontier queries against the same small YSQL pool. Keep them on
  // one FIFO lane so a vacancy cannot start a second scan while the periodic
  // cycle is still holding a pool client (and vice versa).
  private admissionOperationTail: Promise<void> = Promise.resolve();
  // P2 admission can be evaluated by discovery bursts. Keep the P1 pressure
  // probe short-lived and single-flight: it is an admission signal, never a
  // cache of editorial state.
  private p1BacklogProbeAt = 0;
  private p1BacklogProbeFlight: Promise<{ claimable: number; available: number; works: number }> | null = null;
  private p1BacklogSnapshot = { claimable: 0, available: 0, works: 0 };
  // On-demand P1 admission is intentionally exceptional, but it still has
  // to share opportunities across sources when a cohort vacancy occurs.
  // This cursor is process-local because the durable per-source P1 cursor is
  // the authority for work order; it merely avoids choosing the first source
  // alphabetically on every spare-capacity pass.
  private lastOnDemandP1Source: string | null = null;
  // Admission queries use a per-source indexed window.  Sampling every
  // healthy source in one cycle turns a five-slot controller into dozens of
  // distributed reads, which can starve the claims the controller exists to
  // feed.  Rotate a small source subset instead; this is scheduling fairness,
  // not a resource limit or health classification.
  private p1SourceWindowCursor: string | null = null;
  private readonly p1SourceWindowSize = 10;
  // A previous scheduler generation could vacate a visible P2 work after its
  // first window, leaving the rest of its queue at priority 50. Repair that
  // legacy state in small work-scoped batches; never scan or rewrite the P2
  // catalog as part of normal admission.
  private visibleP2LifecycleRepairComplete = false;
  // Older workers could leave a mapping QUEUED while its dedupe row was
  // released as CANCELLED_BY_STAFF without any cancellation metadata.  This
  // is distinct from an explicit staff pause (which always carries the
  // cancellation fields) and needs a small, idempotent recovery pass so a
  // canonical frontier does not remain permanently invisible to admission.
  private orphanCancelledRecoveryAt = 0;
  private legacyTransientFailureRecoveryAt = 0;
  private sourceRecoveredFailureRecoveryAt = 0;
  // A continuously busy bounded pool must not permanently starve the one
  // reconciliation pass that can retire stale active_works entries.  Allow a
  // single bounded maintenance pass after sustained pressure, then keep the
  // normal claim-first backpressure until the next interval.
  private claimPressureSince = 0;
  private lastPressureMaintenanceAt = 0;
  private static readonly PRESSURE_MAINTENANCE_AFTER_MS = 30_000;
  private static readonly PRESSURE_MAINTENANCE_INTERVAL_MS = 60_000;

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

  private async getP1SourceWindow(allowedSources?: string[]): Promise<string[]> {
    const result = await this.runQuery(
      `SELECT s.id
       FROM importer_sources s
       WHERE s.enabled = true
         AND ${SOURCE_EXECUTION_ELIGIBILITY_SQL}
         AND ($1::text[] IS NULL OR s.id = ANY($1::text[]))
       ORDER BY CASE WHEN $2::text IS NULL OR s.id > $2::text THEN 0 ELSE 1 END, s.id ASC
       LIMIT $3`,
      [allowedSources && allowedSources.length > 0 ? allowedSources : null, this.p1SourceWindowCursor, this.p1SourceWindowSize],
    );
    const sources = result.rows.map((row: any) => String(row.id)).filter(Boolean);
    if (sources.length > 0) this.p1SourceWindowCursor = sources[sources.length - 1];
    return sources;
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
    if (typeof rawPool.connect === 'function' || typeof rawPool.query === 'function') {
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
   * Admission is control-plane work.  Never start a broad reconciliation or
   * recovery query while the bounded pool is already servicing/waiting for
   * chapter claims.  Claims are the work-conserving data plane; letting a
   * maintenance scan take the last idle connection can leave every chapter
   * slot parked in WAITING_CLAIM_DB.  The next cycle retries automatically
   * once the pool drains, so this is backpressure, not a disabled recovery.
   */
  private isPoolUnderClaimPressure(): boolean {
    const pool = this.pool as any;
    const waiting = Number(pool?.waitingCount || 0);
    const total = Number(pool?.totalCount || 0);
    const idle = Number(pool?.idleCount || 0);
    // An occupied connection is not, by itself, claim pressure.  The old
    // `idle < total` check classified the normal state (one claim using one
    // connection while another was idle) as pressure and skipped every
    // reconciliation cycle.  That allowed durable active_works entries to
    // remain stale indefinitely.  Only a real pool queue, or a pool with no
    // idle connection at all, must defer control-plane work.
    return waiting > 0 || (total > 0 && idle === 0);
  }

  private shouldRunPressureMaintenance(): boolean {
    const now = Date.now();
    if (!this.isPoolUnderClaimPressure()) {
      this.claimPressureSince = 0;
      return false;
    }
    if (this.claimPressureSince === 0) this.claimPressureSince = now;
    if (now - this.claimPressureSince < AdmissionController.PRESSURE_MAINTENANCE_AFTER_MS) return false;
    if (now - this.lastPressureMaintenanceAt < AdmissionController.PRESSURE_MAINTENANCE_INTERVAL_MS) return false;
    this.lastPressureMaintenanceAt = now;
    return true;
  }

  /**
   * Keep one executable P1 chapter per active work. Older scheduler versions
   * could leave an entire backfill window (or more) QUEUED, then replenish it
   * before it drained. That made the durable source cursor fair only on
   * paper: a large work could retain its cohort position indefinitely.
   *
   * This is work-scoped, idempotent and touches no P0/Staff row. Remaining
   * chapters remain PAUSED_BY_STAFF and are re-admitted through the ordinary
   * per-source cursor. A retry remains ahead of a new promotion so retry and
   * frontier safety retain their existing semantics.
   */
  private async enforceP1FairWindow(workId: string, admittedSource?: string): Promise<number> {
    // The fair window is a *canonical frontier* window.  Looking only at
    // already-QUEUED rows can leave a later chapter QUEUED while its lower
    // predecessor remains PAUSED_BY_STAFF.  The claim fence then rejects the
    // later row (correctly), but admission believes the work already owns its
    // one chapter and repeatedly forms an unclaimable cohort.  Rank both
    // non-terminal window states together, preserving only the lowest
    // candidate from the healthy source that admitted this work as QUEUED.
    // A lower row from another source may be blocked, stale, or merely an
    // alternate mapping; reopening it instead would strand the valid source
    // frontier while falsely keeping this work active. RETRY is deliberately not rewritten:
    // it retains its retry/backoff semantics and the claim fence still keeps
    // a later retry behind the promoted predecessor.
    const normalized = await this.runQuery(
      `WITH ranked AS MATERIALIZED (
         SELECT id,
                status AS prior_status,
                CASE WHEN ($3::text IS NULL OR source = $3::text)
                           AND ROW_NUMBER() OVER (
                             PARTITION BY CASE WHEN $3::text IS NULL THEN '' ELSE source END
                             ORDER BY chapter_sort_key ASC NULLS LAST, id ASC
                           ) <= $2
                     THEN 'QUEUED' ELSE 'PAUSED_BY_STAFF' END AS target_status
         FROM importer_queue
         WHERE task_type = 'IMPORT_CHAPTER'
           AND (payload->>'workId') = $1
           AND status IN ('QUEUED', 'PAUSED_BY_STAFF')
           AND priority >= 75 AND priority < 100
           AND COALESCE(payload->>'staffForced', 'false') <> 'true'
       ), normalized AS (
         UPDATE importer_queue q
         SET status = r.target_status,
             priority = CASE WHEN r.target_status = 'QUEUED' THEN 75 ELSE q.priority END,
             next_run_at = CASE WHEN r.target_status = 'QUEUED' THEN NOW() ELSE q.next_run_at END,
             updated_at = NOW()
         FROM ranked r
         WHERE q.id = r.id
           AND q.status IS DISTINCT FROM r.target_status
         RETURNING q.id, r.prior_status, r.target_status
       )
       SELECT COUNT(*) FILTER (WHERE target_status = 'QUEUED')::int AS queued_count,
              COUNT(*) FILTER (WHERE prior_status = 'QUEUED' AND target_status = 'PAUSED_BY_STAFF')::int AS capped_count,
              COUNT(*) FILTER (WHERE prior_status = 'PAUSED_BY_STAFF' AND target_status = 'QUEUED')::int AS promoted_count
       FROM ranked;`,
      [workId, P1_FAIR_WINDOW_CHAPTERS, admittedSource ?? null],
    );

    const capped = parseInt(normalized.rows[0]?.capped_count || '0', 10);
    const promoted = parseInt(normalized.rows[0]?.promoted_count || '0', 10);
    if (capped > 0) {
      this.logger.info('[P1_FAIR_WINDOW_CAPPED]', {
        workId,
        kept: P1_FAIR_WINDOW_CHAPTERS,
        paused: capped,
      });
    }
    if (promoted > 0) {
      this.logger.info('[P1_CANONICAL_FRONTIER_PROMOTED]', { workId, promoted });
    }

    // A retained retry is intentionally not counted as a new QUEUED window;
    // the normal retry claim path remains responsible for it.
    return parseInt(normalized.rows[0]?.queued_count || '0', 10);
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
    this.deadWorksCache.clear();
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
  async canAdmitNewWork(options: { allowDuringClaimPressure?: boolean } = {}): Promise<{
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

    // Admission pressure is advisory; claims are the work-conserving path.
    // When a bounded pool already has claim waiters, running the P1 frontier
    // probe would consume the other connection and make those claims wait
    // behind a query that cannot itself publish work. Defer the probe until
    // the pool drains; this preserves P1-before-P2 ordering without allowing
    // maintenance/admission to starve the chapter data plane.
    const poolWaiters = Number((this.pool as any)?.waitingCount || 0);
    if (poolWaiters > 0 && !options.allowDuringClaimPressure) {
      return {
        allowed: false,
        reason: `YSQL_POOL_BUSY: ${poolWaiters} claim/query waiter(s)`,
        metrics: {
          p0Waiting: 0,
          p1Claimable: 0,
          p1AvailableChapters: 0,
          p1WorksWaiting: 0,
          p2ActiveCohortSize: 0,
          p2UnfinishedCount: 0,
          systemHealthy: true,
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
    const p1 = await this.getP1BacklogSnapshot(options);
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
  private async getP1BacklogSnapshot(options: { allowDuringClaimPressure?: boolean } = {}): Promise<{ claimable: number; available: number; works: number }> {
    const now = Date.now();
    if (now - this.p1BacklogProbeAt < 2_000) return this.p1BacklogSnapshot;
    if (this.p1BacklogProbeFlight) return this.p1BacklogProbeFlight;

    const flight = (async () => {
      // A P1 row only represents admission pressure when it is the current
      // executable frontier.  Later rows behind a failed/waiting predecessor
      // must not block healthy P2 work indefinitely.
      const frontierEligibility = `
          AND NOT EXISTS (
            SELECT 1
            FROM importer_queue predecessor
            WHERE predecessor.task_type = 'IMPORT_CHAPTER'
              AND predecessor.payload->>'workId' = q.payload->>'workId'
              AND predecessor.chapter_sort_key < q.chapter_sort_key
              AND predecessor.status IN ('QUEUED', 'RETRY', 'IMPORTING')
              AND NOT EXISTS (
                SELECT 1
                FROM chapters predecessor_canonical
                WHERE predecessor_canonical.work_id = (q.payload->>'workId')::uuid
                  AND predecessor_canonical.published_at IS NOT NULL
                  AND (
                    predecessor_canonical.number = NULLIF(predecessor.payload->>'chapterNumber', '')::numeric
                    OR predecessor_canonical.number = predecessor.chapter_sort_key
                  )
              )
          )
          AND (
            (pub.max_published IS NOT NULL AND q.chapter_sort_key <= pub.max_published + 1.5)
            OR (
              pub.max_published IS NOT NULL
              AND EXISTS (
                SELECT 1
                FROM importer_confirmed_gaps gap
                WHERE gap.work_id = (q.payload->>'workId')::uuid
                  AND gap.start_sort_key <= pub.max_published + 1
                  AND gap.end_sort_key >= q.chapter_sort_key - 1
              )
            )
            OR (
              pub.max_published IS NULL
              AND q.chapter_sort_key <= 1.5
              AND NOT EXISTS (
                SELECT 1
                FROM importer_chapter_mappings predecessor_mapping
                WHERE predecessor_mapping.work_id = (q.payload->>'workId')::uuid
                  AND predecessor_mapping.chapter_sort_key < q.chapter_sort_key
                  AND predecessor_mapping.is_gap = false
                  AND predecessor_mapping.status NOT IN ('STAGED', 'WAITING_FOR_GAP')
              )
            )
          )
          AND NOT EXISTS (
            SELECT 1
            FROM chapters canonical_chapter
            WHERE canonical_chapter.work_id = (q.payload->>'workId')::uuid
              AND canonical_chapter.published_at IS NOT NULL
              AND (
                canonical_chapter.number = COALESCE(NULLIF(q.payload->>'chapterNumber', '')::numeric, q.chapter_sort_key)
              )
          )`;

      // Ready P1 uses the existing partial claim index. Only if no ready P1
      // exists do we check the paused window backlog; that slower path is
      // exceptional and avoids turning a normal admission probe into a scan.
      const ready = await this.runQuery(`
        SELECT q.status
        FROM importer_queue q
        JOIN importer_sources s ON s.id = q.source
        JOIN works w ON w.id = (q.payload->>'workId')::uuid
        CROSS JOIN LATERAL (
          SELECT MAX(c.number) AS max_published
          FROM chapters c
          WHERE c.work_id = (q.payload->>'workId')::uuid
            AND c.published_at IS NOT NULL
        ) pub
        WHERE q.task_type = 'IMPORT_CHAPTER'
          AND (q.status = 'QUEUED' OR (q.status = 'RETRY' AND q.next_run_at <= NOW()))
          AND q.attempts < COALESCE(q.max_attempts, 7)
          AND q.priority >= 75 AND q.priority < 100
          AND COALESCE(q.payload->>'staffForced', 'false') <> 'true'
          AND w.published IS TRUE
          AND s.enabled = true
          AND ${SOURCE_EXECUTION_ELIGIBILITY_SQL}
          ${frontierEligibility}
        LIMIT 1
      `);
      const candidate = ready.rows[0];
      if (candidate) {
        this.p1BacklogSnapshot = { claimable: 1, available: 1, works: 1 };
        this.p1BacklogProbeAt = Date.now();
        return this.p1BacklogSnapshot;
      }
      // PAUSED_BY_STAFF rows are not executable by the claim path. They are
      // useful for the normal maintenance cycle, but probing that historical
      // backlog requires a broad scan and can consume the last bounded pool
      // connection while claims are waiting. Under claim pressure the
      // work-conserving P2 probe must not turn this advisory fallback into a
      // permanent admission deadlock; the next non-pressure cycle will still
      // inspect paused P1 backlog before admitting new catalog work.
      if (options.allowDuringClaimPressure) {
        this.p1BacklogSnapshot = { claimable: 0, available: 0, works: 0 };
        this.p1BacklogProbeAt = Date.now();
        return this.p1BacklogSnapshot;
      }
      const paused = await this.runQuery(`
        SELECT 1
        FROM importer_queue q
        JOIN importer_sources s ON s.id = q.source
        JOIN works w ON w.id = (q.payload->>'workId')::uuid
        CROSS JOIN LATERAL (
          SELECT MAX(c.number) AS max_published
          FROM chapters c
          WHERE c.work_id = (q.payload->>'workId')::uuid
            AND c.published_at IS NOT NULL
        ) pub
        WHERE q.task_type = 'IMPORT_CHAPTER'
          AND q.status = 'PAUSED_BY_STAFF'
          AND q.attempts < COALESCE(q.max_attempts, 7)
          AND q.priority >= 75 AND q.priority < 100
          AND COALESCE(q.payload->>'staffForced', 'false') <> 'true'
          AND w.published IS TRUE
          AND s.enabled = true
          AND ${SOURCE_EXECUTION_ELIGIBILITY_SQL}
          ${frontierEligibility}
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
        AND priority < 75
        AND COALESCE(payload->>'staffForced', 'false') <> 'true'
    `, [workId]);
  }

  /**
   * Reclassify visible works that an older scheduler stranded below P1.
   *
   * The candidate set starts from bounded ready queue rows, so it also covers
   * mappings whose sync state is SYNCED rather than ACTIVE. Each cycle touches
   * at most 12 works and is naturally idempotent because promoted rows no
   * longer match priority <75. The matching initial window is reopened in
   * the same statement so the normal
   * indexed P1 claim path can immediately see the repaired work.
   */
  private async repairVisibleP2LifecycleBacklog(): Promise<void> {
    if (this.visibleP2LifecycleRepairComplete) return;

    const windowSize = Math.max(1, Math.min(12, this.stateStore.getConfig().slidingWindowSize || 8));
    const result = await this.runQuery(`
      WITH candidate_works AS MATERIALIZED (
        SELECT q.payload->>'workId' AS work_id
        FROM importer_queue q
        JOIN works w ON w.id::text = q.payload->>'workId' AND w.published IS TRUE
        WHERE q.task_type = 'IMPORT_CHAPTER'
          -- Start from ready/retry rows covered by the hot claim index. The
          -- selected work is then repaired atomically including its paused
          -- siblings below, avoiding a recurring full paused-queue scan.
          AND q.status IN ('QUEUED', 'RETRY')
          AND q.priority < 75
          AND COALESCE(q.payload->>'staffForced', 'false') <> 'true'
        GROUP BY q.payload->>'workId'
        ORDER BY MIN(q.next_run_at) ASC NULLS FIRST, MIN(q.chapter_sort_key) ASC NULLS LAST
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
          AND q.priority < 75
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
            AND q.priority < 75
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
          AND q.priority < 75
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
    this.logger.info(`[VISIBLE_WORK_TO_P1_LEGACY_REPAIR] Promoted ${repairedJobs} queued chapter(s) across ${repairedWorks} visible work(s).`);
  }

  /**
   * Requeue legacy cancellation rows only when the canonical mapping is still
   * executable and there is no active staff request for the work.  The strict
   * null metadata predicates are intentional: explicit staff cancellations
   * remain untouched.  Keep the batch bounded and rate-limited so recovery
   * cannot turn into a queue-wide scan or compete with claims.
   */
  private async recoverOrphanedCancelledChapterJobs(): Promise<number> {
    const now = Date.now();
    if (now - this.orphanCancelledRecoveryAt < 60_000) return 0;
    this.orphanCancelledRecoveryAt = now;

    try {
      const result = await this.runQuery(`
        WITH candidates AS MATERIALIZED (
          SELECT q.id
          FROM importer_queue q
          WHERE q.task_type = 'IMPORT_CHAPTER'
            AND q.status = 'CANCELLED_BY_STAFF'
            AND q.cancel_reason IS NULL
            AND q.cancelled_by IS NULL
            AND q.cancelled_at IS NULL
            AND q.updated_at < NOW() - INTERVAL '5 minutes'
            AND EXISTS (
              SELECT 1
              FROM importer_chapter_mappings m
              WHERE m.work_id::text = q.payload->>'workId'
                AND m.source = q.source
                AND m.source_chapter_id = q.payload->>'sourceChapterId'
                AND m.status IN ('QUEUED', 'PENDING')
                AND m.is_gap IS FALSE
            )
            AND NOT EXISTS (
              SELECT 1
              FROM importer_staff_requests sr
              WHERE sr.work_id::text = q.payload->>'workId'
                AND sr.status IN ('QUEUED', 'IMPORTING', 'RETRYING', 'ACTIVE')
            )
          ORDER BY q.updated_at ASC, q.id ASC
          LIMIT 100
          FOR UPDATE SKIP LOCKED
        ), revived AS (
          UPDATE importer_queue q
          SET status = 'QUEUED',
              locked_by = NULL,
              locked_at = NULL,
              lease_expires_at = NULL,
              cancel_requested = FALSE,
              next_run_at = NOW(),
              last_error = NULL,
              last_error_at = NULL,
              retry_reason = 'ORPHANED_CANCELLED_MAPPING_RECOVERY',
              last_recovered_error = 'Recovered legacy CANCELLED_BY_STAFF row with queued canonical mapping',
              recovered_at = NOW(),
              updated_at = NOW()
          FROM candidates c
          WHERE q.id = c.id
            AND q.status = 'CANCELLED_BY_STAFF'
          RETURNING q.id
        )
        SELECT COUNT(*)::int AS recovered FROM revived;
      `);
      const recovered = Number(result.rows[0]?.recovered || 0);
      if (recovered > 0) {
        this.logger.warn('[ORPHANED_CANCELLED_MAPPING_RECOVERY] Requeued legacy chapter jobs', { recovered });
      }
      return recovered;
    } catch (err: any) {
      // Recovery is best-effort; admission must never fail closed because an
      // optional legacy cleanup query is unavailable during a deploy.
      this.logger.warn('[ORPHANED_CANCELLED_MAPPING_RECOVERY] Query failed; no rows changed', { error: err?.message });
      return 0;
    }
  }

  /**
   * Reopen failures produced by the old controlled-recovery path.  That path
   * cancelled in-flight work during a process recovery and then exhausted the
   * normal retry budget, even when the source is healthy again.  Only its
   * exact diagnostic marker is eligible here; permanent media/identity/source
   * failures remain terminal.  Queue and mapping are repaired in one bounded
   * statement so the canonical frontier cannot observe a half-recovered pair.
   */
  private async recoverLegacyTransientFailures(): Promise<number> {
    const now = Date.now();
    if (now - this.legacyTransientFailureRecoveryAt < 60_000) return 0;
    this.legacyTransientFailureRecoveryAt = now;

    try {
      const result = await this.runQuery(`
        WITH candidates AS MATERIALIZED (
          SELECT q.id, m.id AS mapping_id
          FROM importer_queue q
          JOIN importer_chapter_mappings m
            ON m.work_id::text = q.payload->>'workId'
           AND m.source = q.source
           AND m.source_chapter_id = q.payload->>'sourceChapterId'
          JOIN importer_sources s ON s.id = q.source
          WHERE q.task_type = 'IMPORT_CHAPTER'
            AND q.status = 'FAILED'
            AND q.retry_reason = 'TRANSIENT_NETWORK'
            AND q.last_error ILIKE '%Controlled recovery cancelled stalled in-flight work%'
            AND m.status = 'FAILED'
            AND m.chapter_id IS NULL
            AND m.is_gap IS FALSE
            AND s.status = 'ACTIVE'
            AND NOT EXISTS (
              SELECT 1
              FROM importer_staff_requests sr
              WHERE sr.work_id::text = q.payload->>'workId'
                AND sr.status IN ('QUEUED', 'IMPORTING', 'RETRYING', 'ACTIVE')
            )
          ORDER BY q.updated_at ASC, q.id ASC
          LIMIT 100
          FOR UPDATE OF q, m SKIP LOCKED
        ), revived_mappings AS (
          UPDATE importer_chapter_mappings m
          SET status = 'QUEUED',
              last_error = NULL,
              updated_at = NOW()
          FROM candidates c
          WHERE m.id = c.mapping_id
            AND m.status = 'FAILED'
          RETURNING m.id
        ), revived_jobs AS (
          UPDATE importer_queue q
          SET status = 'QUEUED',
              attempts = 0,
              locked_by = NULL,
              locked_at = NULL,
              lease_expires_at = NULL,
              cancel_requested = FALSE,
              next_run_at = NOW(),
              last_error = NULL,
              last_error_at = NULL,
              retry_reason = 'LEGACY_TRANSIENT_FAILURE_RECOVERY',
              last_recovered_error = 'Reopened legacy controlled-recovery cancellation on active source',
              recovered_at = NOW(),
              updated_at = NOW()
          FROM candidates c
          WHERE q.id = c.id
            AND q.status = 'FAILED'
            AND EXISTS (SELECT 1 FROM revived_mappings m WHERE m.id = c.mapping_id)
          RETURNING q.id
        )
        SELECT COUNT(*)::int AS recovered FROM revived_jobs;
      `);
      const recovered = Number(result.rows[0]?.recovered || 0);
      if (recovered > 0) {
        this.logger.warn('[LEGACY_TRANSIENT_FAILURE_RECOVERY] Reopened failed chapter frontiers', { recovered });
      }
      return recovered;
    } catch (err: any) {
      this.logger.warn('[LEGACY_TRANSIENT_FAILURE_RECOVERY] Query failed; no rows changed', { error: err?.message });
      return 0;
    }
  }

  /**
   * Reopen a transient frontier only after its source has demonstrably
   * recovered.  A failed predecessor can otherwise leave every later
   * canonical chapter behind a permanent barrier even though the source is
   * healthy again.  The source-row update is the recovery edge: a retry is
   * eligible once, and a second failure is not reopened until a newer source
   * probe records another recovery.  Permanent failures and failures without
   * a source recovery marker remain terminal.
   */
  private async recoverSourceRecoveredTransientFailures(): Promise<number> {
    const now = Date.now();
    if (now - this.sourceRecoveredFailureRecoveryAt < 60_000) return 0;
    this.sourceRecoveredFailureRecoveryAt = now;

    try {
      const result = await this.runQuery(`
        WITH candidates AS MATERIALIZED (
          SELECT q.id, m.id AS mapping_id
          FROM importer_queue q
          JOIN importer_chapter_mappings m
            ON m.work_id::text = q.payload->>'workId'
           AND m.source = q.source
           AND m.source_chapter_id = q.payload->>'sourceChapterId'
          JOIN importer_sources s ON s.id = q.source
          WHERE q.task_type = 'IMPORT_CHAPTER'
            AND q.status = 'FAILED'
            AND q.retry_reason = 'TRANSIENT_NETWORK'
            AND q.last_error ILIKE '%RETRY_BUDGET_EXHAUSTED%'
            AND q.updated_at < NOW() - INTERVAL '1 minute'
            AND m.status IN ('PENDING', 'FAILED')
            AND m.is_gap IS FALSE
            AND s.enabled = TRUE
            AND s.status = 'ACTIVE'
            AND s.blocked_details->>'recovered_at' IS NOT NULL
            AND s.updated_at > COALESCE(q.last_error_at, q.updated_at)
            AND NOT EXISTS (
              SELECT 1
              FROM chapters c
              WHERE c.work_id = (q.payload->>'workId')::uuid
                AND c.published_at IS NOT NULL
                AND (
                  c.number = NULLIF(q.payload->>'chapterNumber', '')::numeric
                  OR c.number = q.chapter_sort_key
                )
            )
            AND NOT EXISTS (
              SELECT 1
              FROM importer_staff_requests sr
              WHERE sr.work_id::text = q.payload->>'workId'
                AND sr.status IN ('QUEUED', 'IMPORTING', 'RETRYING', 'ACTIVE')
            )
          ORDER BY q.updated_at ASC, q.id ASC
          LIMIT 50
          FOR UPDATE OF q, m SKIP LOCKED
        ), revived_mappings AS (
          UPDATE importer_chapter_mappings m
          SET status = CASE WHEN m.status = 'FAILED' THEN 'QUEUED' ELSE m.status END,
              last_error = NULL,
              updated_at = NOW()
          FROM candidates c
          WHERE m.id = c.mapping_id
          RETURNING m.id
        ), revived_jobs AS (
          UPDATE importer_queue q
          SET status = 'QUEUED',
              attempts = 0,
              locked_by = NULL,
              locked_at = NULL,
              lease_expires_at = NULL,
              cancel_requested = FALSE,
              next_run_at = NOW(),
              last_error = NULL,
              last_error_at = NULL,
              retry_reason = 'SOURCE_RECOVERY_RETRY',
              last_recovered_error = 'Reopened transient frontier after source recovery probe',
              recovered_at = NOW(),
              updated_at = NOW()
          FROM candidates c
          WHERE q.id = c.id
            AND EXISTS (SELECT 1 FROM revived_mappings m WHERE m.id = c.mapping_id)
          RETURNING q.id
        )
        SELECT COUNT(*)::int AS recovered FROM revived_jobs;
      `);
      const recovered = Number(result.rows[0]?.recovered || 0);
      if (recovered > 0) {
        this.logger.warn('[SOURCE_RECOVERY_TRANSIENT_RETRY] Reopened failed frontiers after source recovery', { recovered });
      }
      return recovered;
    } catch (err: any) {
      this.logger.warn('[SOURCE_RECOVERY_TRANSIENT_RETRY] Query failed; no rows changed', { error: err?.message });
      return 0;
    }
  }

  /**
   * Executes a single admission reconciliation cycle.
   */
  runAdmissionCycle(): Promise<void> {
    if (this.admissionInFlight) return this.admissionInFlight;
    this.admissionInFlight = this.enqueueAdmissionOperation(() => this.executeAdmissionCycle())
      .finally(() => { this.admissionInFlight = null; });
    return this.admissionInFlight;
  }

  private enqueueAdmissionOperation<T>(operation: () => Promise<T>): Promise<T> {
    const previous = this.admissionOperationTail;
    let release!: () => void;
    this.admissionOperationTail = new Promise<void>((resolve) => { release = resolve; });
    return previous
      .then(operation)
      .finally(release);
  }

  private async executeAdmissionCycle(): Promise<void> {
    const now = Date.now();
    for (const [wid, ts] of this.deadWorksCache.entries()) { if (now - ts > 10 * 60 * 1000) this.deadWorksCache.delete(wid); }
    const config = this.stateStore.getConfig();
    if (!config.enabled && !config.shadowMode) {
      return;
    }

    // Step 0: Check PROTECTIVE_STOP
    if (await this.protectiveSentinel.isProtectiveStopActive()) {
      this.logger.warn('PROTECTIVE_STOP active, skipping admission cycle');
      return;
    }

    // Do not let control-plane scans consume the bounded pool while claims
    // are active or queued.  A later scheduled/vacancy cycle will retry.
    if (this.isPoolUnderClaimPressure() && !this.shouldRunPressureMaintenance()) {
      // Claims are the work-conserving data plane, but a pool waiter must not
      // permanently starve P2 discovery when the bounded P1 probe has no
      // executable catalog frontier. This path performs exactly one bounded
      // P1 probe followed by the bounded P2 source-window admission; it does
      // not run reconciliation, recovery, GROUP BY maintenance, or paused
      // backlog scans. A real P1 frontier still wins in canAdmitNewWork().
      await this.executeOnDemandAdmission('P2', undefined, true);
      this.logger.debug('[ADMISSION_P2_PRESSURE_PROBE] bounded P2 admission attempted while claims are waiting');
      return;
    }

    // If claim pressure remains continuous, one bounded reconciliation pass is
    // necessary to retire stale active_works and replenish a legitimate
    // frontier.  Do not run the broad recovery/scanning chain in this escape
    // hatch; it is intentionally limited to the active-work snapshot and its
    // bounded replenishment.
    if (this.isPoolUnderClaimPressure()) {
      this.logger.warn('[ADMISSION_PRESSURE_MAINTENANCE] sustained claim pressure; running bounded active-work reconciliation');
      await this.runPressureMaintenance();
      return;
    }

    // Run before P1/P2 admission so legacy visible works cannot be bypassed
    // by discovery during the first post-deploy cycle.
    await this.recoverOrphanedCancelledChapterJobs();
    if (this.isPoolUnderClaimPressure()) return;
    await this.recoverLegacyTransientFailures();
    if (this.isPoolUnderClaimPressure()) return;
    await this.recoverSourceRecoveredTransientFailures();
    if (this.isPoolUnderClaimPressure()) return;
    await this.repairVisibleP2LifecycleBacklog();
    if (this.isPoolUnderClaimPressure()) return;

    // Step 1: Reconcile current active works (check caught-up, in-flight, queued)
    await this.reconcileActiveWorks();
    if (this.isPoolUnderClaimPressure()) return;

    // Step 2: Replenish active sets if below capacity
    await this.replenishActiveSets();
    if (this.isPoolUnderClaimPressure()) return;

    // Step 3: Maintain sliding admission windows for all active works
    await this.maintainSlidingWindows();
  }

  private async runPressureMaintenance(): Promise<void> {
    await this.reconcileActiveWorks();
    await this.replenishActiveSets();
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
        SELECT w.work_id, q.*, p.*, m.*, s.status AS source_status, s.cooldown_until,
               s.blocked_reason AS source_blocked_reason, s.blocked_details AS source_blocked_details
        FROM unnest($1::text[], $2::text[]) AS w(work_id, source)
        CROSS JOIN LATERAL (
          SELECT COUNT(*) FILTER (WHERE q.status='QUEUED' AND q.attempts < COALESCE(q.max_attempts,7)) AS queued_cnt,
            COUNT(*) FILTER (WHERE q.status='IMPORTING') AS importing_cnt,
            COUNT(*) FILTER (WHERE q.status='PAUSED_BY_STAFF') AS paused_cnt,
            COUNT(*) FILTER (WHERE q.status='RETRY' AND q.attempts < COALESCE(q.max_attempts,7)) AS retry_cnt,
            MIN(q.chapter_sort_key) FILTER (WHERE q.status='QUEUED' AND q.attempts < COALESCE(q.max_attempts,7)) AS min_queued,
            MIN(q.chapter_sort_key) FILTER (WHERE q.status IN ('QUEUED','RETRY','PAUSED_BY_STAFF') AND q.attempts < COALESCE(q.max_attempts,7)) AS min_sort_key,
            CASE WHEN EXISTS (
              -- This is only a zero/non-zero signal used to rotate an active
              -- work.  The former COUNT(*) FILTER evaluated every queue row
              -- and repeatedly held a pool connection for seconds.  EXISTS
              -- preserves the exact frontier predicates but stops at the
              -- first executable chapter.
              SELECT 1
              FROM importer_queue candidate
              JOIN importer_sources s_candidate ON s_candidate.id = candidate.source
              LEFT JOIN LATERAL (
                SELECT MAX(c.number) AS max_published
                FROM chapters c
                WHERE c.work_id = w.work_id::uuid
                  AND c.published_at IS NOT NULL
              ) candidate_pub ON TRUE
              WHERE candidate.task_type = 'IMPORT_CHAPTER'
                AND candidate.payload->>'workId' = w.work_id
                AND (candidate.status='QUEUED' OR (candidate.status='RETRY' AND candidate.next_run_at <= NOW()))
                AND candidate.attempts < COALESCE(candidate.max_attempts,7)
                AND s_candidate.enabled = true
                AND (
                  (s_candidate.status = 'ACTIVE' AND (s_candidate.blocked_reason IS NULL OR s_candidate.blocked_details->>'probe_success' = 'true' OR s_candidate.blocked_details->>'recovered_at' IS NOT NULL))
                  OR (s_candidate.status IN ('COOLDOWN','PROBING','DEGRADED') AND (s_candidate.blocked_reason IS NULL OR s_candidate.blocked_details->>'probe_success' = 'true' OR s_candidate.blocked_details->>'recovered_at' IS NOT NULL) AND (s_candidate.cooldown_until IS NULL OR s_candidate.cooldown_until <= NOW()))
                )
                AND NOT EXISTS (
                  SELECT 1 FROM chapters canonical_chapter
                  WHERE canonical_chapter.work_id = w.work_id::uuid
                    AND canonical_chapter.published_at IS NOT NULL
                    AND (canonical_chapter.number = COALESCE(NULLIF(candidate.payload->>'chapterNumber', '')::numeric, candidate.chapter_sort_key))
                )
                AND NOT EXISTS (
                  SELECT 1 FROM importer_queue predecessor
                  WHERE predecessor.task_type = 'IMPORT_CHAPTER'
                    AND predecessor.payload->>'workId' = candidate.payload->>'workId'
                    AND predecessor.chapter_sort_key < candidate.chapter_sort_key
                    AND predecessor.status IN ('QUEUED', 'RETRY', 'IMPORTING')
                    AND NOT EXISTS (
                      SELECT 1 FROM chapters predecessor_canonical
                      WHERE predecessor_canonical.work_id = w.work_id::uuid
                        AND predecessor_canonical.published_at IS NOT NULL
                        AND (predecessor_canonical.number = NULLIF(predecessor.payload->>'chapterNumber', '')::numeric OR predecessor_canonical.number = predecessor.chapter_sort_key)
                    )
                )
                AND NOT EXISTS (
                  SELECT 1 FROM importer_chapter_mappings staged_frontier
                  WHERE staged_frontier.work_id = w.work_id::uuid
                    AND staged_frontier.chapter_sort_key = candidate.chapter_sort_key
                    AND staged_frontier.status IN ('STAGED', 'WAITING_FOR_GAP')
                )
                AND (
                  (candidate_pub.max_published IS NOT NULL AND candidate.chapter_sort_key <= candidate_pub.max_published + 1.5)
                  OR EXISTS (
                    SELECT 1 FROM importer_confirmed_gaps gap
                    WHERE gap.work_id = w.work_id::uuid
                      AND gap.start_sort_key <= COALESCE(candidate_pub.max_published + 1, 1)
                      AND gap.end_sort_key >= candidate.chapter_sort_key - 1
                  )
                  OR (
                    candidate_pub.max_published IS NULL
                    AND candidate.chapter_sort_key <= 1.5
                    AND NOT EXISTS (
                      SELECT 1 FROM importer_chapter_mappings predecessor_mapping
                      WHERE predecessor_mapping.work_id = w.work_id::uuid
                        AND predecessor_mapping.chapter_sort_key < candidate.chapter_sort_key
                        AND predecessor_mapping.is_gap = false
                        AND predecessor_mapping.status NOT IN ('STAGED', 'WAITING_FOR_GAP')
                    )
                  )
                )
            ) THEN 1 ELSE 0 END AS claimable_cnt
          FROM importer_queue q
          LEFT JOIN importer_sources s ON s.id = q.source
          WHERE q.task_type='IMPORT_CHAPTER' AND q.payload->>'workId'=w.work_id
            AND q.status IN ('QUEUED','IMPORTING','RETRY','PAUSED_BY_STAFF')
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
          // Older test doubles and persisted snapshots do not expose the
          // source/canonical-aware count.  Fall back to the legacy queued
          // count there, while production uses the bounded claimable count.
          const hasClaimableSnapshot = qRow?.claimable_cnt !== undefined;
          const claimableCnt = hasClaimableSnapshot
            ? parseInt(qRow.claimable_cnt || '0', 10)
            : queuedCnt;
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
          const srcRow = {
            status: qRow.source_status,
            cooldown_until: qRow.cooldown_until,
            blocked_reason: qRow.source_blocked_reason,
            blocked_details: qRow.source_blocked_details,
          };
          const isSourceBlocked = srcRow && !isSourceExecutionEligible({
            status: srcRow.status,
            enabled: true,
            chapterIngestionEnabled: true,
            cooldownUntil: srcRow.cooldown_until ? new Date(srcRow.cooldown_until).getTime() : null,
            blockedReason: srcRow.blocked_reason,
            blockedDetails: srcRow.blocked_details,
          });

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

          // A queued row is not enough to hold an active slot: it may belong
          // to a blocked source, be already satisfied canonically, or be a
          // retry whose cooldown has not elapsed.  If no chapter is actually
          // claimable and nothing is in flight, rotate the work so admission
          // can use the slot for another executable frontier.  The queue rows
          // remain intact and will be eligible for a later admission cycle
          // when their source/predecessor recovers.
          if (hasClaimableSnapshot && claimableCnt === 0 && importingCnt === 0 && (queuedCnt > 0 || pausedCnt > 0 || retryCnt > 0)) {
            this.logger.info(`[ACTIVE_SET_VACATED] Work ${work.workTitle} (${work.workId}) has no source/canonical-eligible chapter; rotating empty executable window.`);
            this.stateStore.removeActiveWork(work.workId);
            this.triggerImmediateReplenishment('WORK_NO_EXECUTABLE_FRONTIER');
            continue;
          }

          // Normalize only legacy/previously-admitted P1 cohorts that still
          // have more than the fairness quantum open. New P1 admissions are
          // normalized below before they enter the active set.
          if (work.lane === 'P1' && work.criticalGapSortKey === null && queuedCnt > P1_FAIR_WINDOW_CHAPTERS) {
            work.queuedChapters = await this.enforceP1FairWindow(work.workId, work.primarySource);
            this.stateStore.setActiveWork(work);
            continue;
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

    // Keep the admission query proportional to the actual chapter capacity.
    //
    // `importingCnt` only includes jobs after the durable claim.  A slot that
    // is waiting on the bounded claim/validation path is therefore invisible
    // to that count.  The previous `idleWorkers >= 1 ? 36 : ...` expansion
    // treated those transiently unclaimed slots as real spare capacity and
    // repeatedly ran the full queue GROUP BY (20k+ paused/queued rows) every
    // admission cycle.  On the production pool (max 2) those scans competed
    // with claims and could leave only two chapter slots productive.
    //
    // A one-chapter P1 fairness window only needs one active work per
    // effective chapter slot.  Admit up to that bounded target when the
    // active set is genuinely below capacity; otherwise skip the expensive
    // candidate scan entirely.  This preserves P0/P1/P2 ordering, source
    // rotation and maxInflightPerWork while making admission work-conserving.
    const effectiveChapterCapacity = Math.max(1, this.chapterCapacityProvider());
    const targetBackfillLimit = activeBackfills.length < effectiveChapterCapacity
      ? Math.min(config.maxActiveBackfillWorks, effectiveChapterCapacity)
      : effectiveChapterCapacity;
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
      const p1SourceWindow = await this.getP1SourceWindow();
      // The normal path must only inspect executable work.  Including the
      // entire PAUSED_BY_STAFF backlog here turns every 4s admission cycle
      // into a full GROUP BY over hundreds of thousands of rows, competing
      // with claims on the bounded YSQL pool.  A paused-only fallback is kept
      // for the rare case where there are not enough executable candidates.
      const loadP1Candidates = (includePaused: boolean) => this.runQuery(
        `WITH eligible_sources AS MATERIALIZED (
           SELECT s.*
           FROM importer_sources s
           WHERE s.enabled = true
             AND ${SOURCE_EXECUTION_ELIGIBILITY_SQL}
             AND s.id = ANY($6::text[])
         ),
         source_window AS MATERIALIZED (
           -- Keep the hot path bounded by source and use the existing
           -- (source,status,task_type,created_at) access path.  Ordering by
           -- priority/chapter here looked semantically attractive but forced
           -- YSQL to sort a full per-source backlog before applying LIMIT;
           -- with a large source that held a bounded pool client for the
           -- statement timeout and starved claims.  The later bounded
           -- frontier/contiguity pass still decides canonical executability.
           SELECT q.*
           FROM eligible_sources s
           CROSS JOIN LATERAL (
             SELECT q.*
             FROM importer_queue q
             WHERE q.source = s.id
               AND q.task_type = 'IMPORT_CHAPTER'
               AND (q.status = 'QUEUED' OR (q.status = 'RETRY' AND q.next_run_at <= NOW())${includePaused ? " OR q.status = 'PAUSED_BY_STAFF'" : ''})
               AND q.attempts < COALESCE(q.max_attempts, 7)
               AND q.priority >= 75 AND q.priority < 100
               AND COALESCE(q.payload->>'staffForced', 'false') <> 'true'
               AND NOT ((q.payload->>'workId') = ANY($1::text[]))
               AND q.payload->>'workId' IS NOT NULL
               AND NOT ((q.payload->>'workId') = ANY($7::text[]))
             ORDER BY q.priority DESC, q.chapter_sort_key ASC
             LIMIT $5
           ) q
         ),
         queue_candidate_groups AS MATERIALIZED (
           SELECT q.payload->>'workId' AS work_id, q.source, COUNT(*) AS pending_jobs,
             COUNT(*) FILTER (WHERE q.status = 'QUEUED' OR (q.status = 'RETRY' AND q.next_run_at <= NOW())) AS queued_count,
             COUNT(*) FILTER (WHERE q.status = 'PAUSED_BY_STAFF') AS paused_count,
             MIN(q.chapter_sort_key) AS min_sort_key
           FROM source_window q
           WHERE NOT EXISTS (
               SELECT 1
               FROM chapters canonical_chapter
               WHERE canonical_chapter.work_id = (q.payload->>'workId')::uuid
                 AND canonical_chapter.published_at IS NOT NULL
                 AND (
                   canonical_chapter.number = COALESCE(NULLIF(q.payload->>'chapterNumber', '')::numeric, q.chapter_sort_key)
                 )
             )
           GROUP BY q.payload->>'workId', q.source
         ),
         queue_candidates AS MATERIALIZED (
           -- Rotate work selection independently from chapter number. Keep the
           -- earliest frontiers as a bounded fallback per source: the cursor
           -- candidate can be parked behind an unresolved gap, so keeping a
           -- small frontier window lets contiguity filtering find the next
           -- executable work without scanning or admitting the whole source.
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
           WHERE rotation_rank <= $3 OR frontier_rank <= $3
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
         WHERE w.published = true
         -- One candidate from every source before a second candidate from any
         -- source. The bounded result remains diverse even with many sources.
         ORDER BY admission_rank, source
         LIMIT $2`,
        [
          activeIds.length > 0 ? activeIds : ['00000000-0000-0000-0000-000000000000'],
          Math.max(50, backfillSlotsAvailable * 5),
          4,
          JSON.stringify(this.getP1AdmissionCursors()),
          Math.max(64, backfillSlotsAvailable * 32),
          p1SourceWindow,
          Array.from(this.deadWorksCache.keys()).length > 0 ? Array.from(this.deadWorksCache.keys()) : ['00000000-0000-0000-0000-000000000000']
        ]
      );
      const resolveP1Frontiers = async (candidateRows: any[]) => {
        // A non-empty QUEUED window is not necessarily executable: it can be
        // wholly behind a canonical gap. Evaluate the actual frontier before
        // deciding whether the bounded PAUSED_BY_STAFF fallback is needed.
        const candidateWorkIds = candidateRows.map((r: any) => r.work_id);
        const pubMap = new Map<string, number>();
        if (candidateWorkIds.length > 0) {
          const pubRes = await this.runQuery(
            `SELECT work_id::text, COALESCE(MAX(number), -1) as max_pub
             FROM chapters
             WHERE work_id = ANY($1::uuid[]) AND published_at IS NOT NULL
             GROUP BY work_id`,
            [candidateWorkIds]
          );
          for (const pr of pubRes.rows) pubMap.set(pr.work_id, parseFloat(pr.max_pub));
        }

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
          if (maxPub === -1 ? minSort <= 1.5 : minSort <= maxPub + 1.5) return true;
          const gapStart = maxPub >= 0 ? maxPub + 1 : 1;
          const gapEnd = minSort - 1;
          return (gapsMap.get(workId) || []).some((g) => g.start <= gapStart && g.end >= gapEnd);
        };

        const contiguous = candidateRows.filter((cand: any) => {
          if (parseInt(cand.queued_count || '0', 10) === 0) return false;
          const maxPub = pubMap.get(cand.work_id) ?? -1;
          const minSort = cand.min_sort_key ? parseFloat(cand.min_sort_key) : 0;
          return isContiguousOrConfirmed(cand.work_id, minSort, maxPub);
        });

        // Gap confirmation remains bounded to the small source window. It is
        // only attempted after all immediately-contiguous candidates.
        if (contiguous.length < backfillSlotsAvailable) {
          for (const cand of candidateRows) {
            if (parseInt(cand.queued_count || '0', 10) === 0) continue;
            if (contiguous.some((c: any) => c.work_id === cand.work_id)) continue;
            const maxPub = pubMap.get(cand.work_id) ?? -1;
            const minSort = cand.min_sort_key ? parseFloat(cand.min_sort_key) : 0;
            const gapStart = maxPub >= 0 ? maxPub + 1 : 1;
            const gapEnd = minSort - 1;
            if (gapStart > gapEnd) continue;
            try {
              const conf = await confirmUpstreamGapInterval(this.pool, {
                workId: cand.work_id,
                startSortKey: gapStart,
                endSortKey: gapEnd,
                primarySource: cand.source,
                reason: 'ADMISSION_CANDIDATE_GAP_CONFIRM',
              });
              if (conf.confirmed) contiguous.push(cand);
              else this.deadWorksCache.set(cand.work_id, Date.now());
            } catch {}
          }
        }
        return contiguous;
      };

      let candidatesRes = await loadP1Candidates(false);
      let contiguousCandidates = await resolveP1Frontiers(candidatesRes.rows);
      // A queued candidate list can be full while every row is behind a gap.
      // In that case the old `rows.length` guard skipped the paused frontier
      // that would actually make progress, leaving the P2 gate held forever.
      if (contiguousCandidates.length < backfillSlotsAvailable) {
        const pausedCandidates = await loadP1Candidates(true);
        const pausedContiguous = await resolveP1Frontiers(pausedCandidates.rows);
        if (pausedContiguous.length >= contiguousCandidates.length) {
          candidatesRes = pausedCandidates;
          contiguousCandidates = pausedContiguous;
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

        newWork.queuedChapters = await this.enforceP1FairWindow(newWork.workId, cand.source);
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
      // P2 has the same two populations as P1: executable QUEUED/RETRY rows
      // and a potentially very large paused backlog.  Keep the common path
      // on the queue index and only inspect paused rows when the executable
      // frontier cannot fill the available cohort slots.
      const loadP2Candidates = (includePaused: boolean) => this.runQuery(
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
           AND (q.status IN ('QUEUED', 'RETRY')${includePaused ? " OR q.status = 'PAUSED_BY_STAFF'" : ''})
           AND w.published IS FALSE
           AND s.enabled = true
           AND ${SOURCE_EXECUTION_ELIGIBILITY_SQL}
           AND NOT EXISTS (
             SELECT 1
             FROM chapters canonical_chapter
             WHERE canonical_chapter.work_id = (q.payload->>'workId')::uuid
               AND canonical_chapter.published_at IS NOT NULL
               AND (
                 canonical_chapter.number = COALESCE(NULLIF(q.payload->>'chapterNumber', '')::numeric, q.chapter_sort_key)
               )
           )
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
      let candidatesRes = await loadP2Candidates(false);
      if (candidatesRes.rows.length < newWorkSlotsAvailable) {
        candidatesRes = await loadP2Candidates(true);
      }

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
      // Ordinary P1 is intentionally not a sliding window. Its one chapter
      // window is opened only at admission, then the work rotates out.
      // A verified critical-gap P1 remains exempt: it already has the
      // higher, bounded barrier-unblock semantics and must not be delayed by
      // unrelated catalog work.
      if (work.lane === 'P1' && work.criticalGapSortKey === null) continue;
      if (work.queuedChapters < config.slidingWindowMin) {
        const needed = config.slidingWindowSize - work.queuedChapters;
        if (needed <= 0) continue;

        // Check if this work has critical unblocking gap
        let targetPriority = 50;
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
    const flight = this.enqueueAdmissionOperation(() => this.executeOnDemandAdmission(preferredLane, allowedSources))
      .finally(() => { this.demandFlights.delete(key); });
    this.demandFlights.set(key, flight);
    return flight;
  }

  private async executeOnDemandAdmission(
    preferredLane?: 'P1' | 'P2',
    allowedSources?: string[],
    allowDuringClaimPressure = false,
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
        const gate = await this.canAdmitNewWork({ allowDuringClaimPressure });
        if (!gate.allowed) {
          this.logger.debug(`[ADMISSION_GATE_HOLD] admitNextWorkOnDemand blocked for P2: ${gate.reason}`);
          continue;
        }
      }
      const isP1 = lane === 'P1';
      const p1SourceWindow = isP1 ? await this.getP1SourceWindow(allowedSources) : null;
      if (isP1 && p1SourceWindow?.length === 0) continue;
      const maxPriority = isP1 ? 100 : 75;
      const loadOnDemandCandidates = (includePaused: boolean) => {
        // This path is called from failed claim attempts. Keep it bounded by
        // source and ordered on the existing source/status/task/created_at
        // index; sorting the entire source backlog by chapter number before
        // LIMIT can otherwise consume a bounded YSQL client for 25 seconds.
        // The JS frontier check below remains the canonical authority.
        const query = `
        WITH eligible_sources AS MATERIALIZED (
          SELECT s.id
          FROM importer_sources s
          WHERE s.enabled = true
            AND ${SOURCE_EXECUTION_ELIGIBILITY_SQL}
            AND ($1::text[] IS NULL OR s.id = ANY($1::text[]))
            AND ($3::text[] IS NULL OR NOT (s.id = ANY($3::text[])))
            AND ($6::text[] IS NULL OR s.id = ANY($6::text[]))
        ), source_window AS MATERIALIZED (
          SELECT q.*
          FROM eligible_sources s
          CROSS JOIN LATERAL (
            SELECT q.*
            FROM importer_queue q
            WHERE q.source = s.id
              AND q.task_type = 'IMPORT_CHAPTER'
              AND (q.status = 'QUEUED' OR (q.status = 'RETRY' AND q.next_run_at <= NOW())${includePaused ? " OR q.status = 'PAUSED_BY_STAFF'" : ''})
              AND q.attempts < COALESCE(q.max_attempts,7)
              AND q.priority >= ${isP1 ? 75 : 50} AND q.priority < ${maxPriority}
              AND COALESCE(q.payload->>'staffForced', 'false') <> 'true'
              AND NOT ((q.payload->>'workId') = ANY($2::text[]))
              AND q.payload->>'workId' IS NOT NULL
              AND NOT ((q.payload->>'workId') = ANY($7::text[]))
            ORDER BY q.priority DESC, q.chapter_sort_key ASC
            LIMIT $5
          ) q
        ), queue_candidates AS MATERIALIZED (
          SELECT q.payload->>'workId' AS work_id, q.source, COUNT(*) AS pending_jobs,
            COUNT(*) FILTER (WHERE q.status = 'QUEUED' OR (q.status = 'RETRY' AND q.next_run_at <= NOW())) AS queued_count,
            COUNT(*) FILTER (WHERE q.status = 'PAUSED_BY_STAFF') AS paused_count,
            MIN(q.chapter_sort_key) AS min_sort_key
          FROM source_window q
          WHERE NOT EXISTS (
            SELECT 1
            FROM chapters canonical_chapter
            WHERE canonical_chapter.work_id = (q.payload->>'workId')::uuid
              AND canonical_chapter.published_at IS NOT NULL
              AND (
                canonical_chapter.number = COALESCE(NULLIF(q.payload->>'chapterNumber', '')::numeric, q.chapter_sort_key)
              )
          )
          GROUP BY q.payload->>'workId', q.source
        ), p1_rotation AS MATERIALIZED (
          SELECT ranked.*
          FROM (
            SELECT q.*,
              ROW_NUMBER() OVER (
                PARTITION BY source
                ORDER BY CASE
                  WHEN work_id > COALESCE($4::jsonb ->> source, '') THEN 0
                  ELSE 1
                END,
                work_id
              ) AS rotation_rank,
              ROW_NUMBER() OVER (
                PARTITION BY source
                ORDER BY min_sort_key ASC NULLS LAST, pending_jobs DESC, work_id
              ) AS frontier_rank
            FROM queue_candidates q
          ) ranked
          -- A vacancy must preserve the circular source cursor, but a single
          -- cursor work may be parked behind an unresolved canonical gap.
          -- Keep a small bounded frontier window so the JS contiguity check
          -- can select the next executable work without scanning/admitting an
          -- entire source backlog. The bound is intentionally the same four-
          -- candidate window used by the periodic P1 admission path.
          WHERE rotation_rank <= 4 OR frontier_rank <= 4
        )
        SELECT q.work_id,
               w.title,
               q.source,
               q.pending_jobs,
               q.queued_count,
               q.min_sort_key,
               ${isP1 ? 'q.rotation_rank, q.frontier_rank' : 'NULL::int AS rotation_rank, NULL::int AS frontier_rank'}
        FROM ${isP1 ? 'p1_rotation' : 'queue_candidates'} q
        JOIN works w ON w.id = q.work_id::uuid
        WHERE ${isP1 ? 'w.published = true' : 'w.published IS FALSE'}
        ORDER BY q.source, q.min_sort_key ASC NULLS LAST
        LIMIT ${isP1 ? 64 : 10};
      `;
        return this.runQuery(query, [
          allowedSources && allowedSources.length > 0 ? allowedSources : null,
          activeIds.length > 0 ? activeIds : ['00000000-0000-0000-0000-000000000000'],
          saturatedSources.length > 0 ? saturatedSources : null,
          JSON.stringify(this.getP1AdmissionCursors()),
          16,
          p1SourceWindow,
          Array.from(this.deadWorksCache.keys()).length > 0 ? Array.from(this.deadWorksCache.keys()) : ['00000000-0000-0000-0000-000000000000']
        ]);
      };

      const findOnDemandFrontier = async (candidateRows: any[]): Promise<any | null> => {
        if (candidateRows.length === 0) return null;
        this.logger.info(`[DEBUG_ADM_CANDIDATES] ${JSON.stringify(candidateRows.map(c => ({w:c.work_id, q:c.queued_count})))}`);
        const candWorkIds = candidateRows.map((r: any) => r.work_id);
        const pubMap = new Map<string, number>();
        const pubRes = await this.runQuery(
          `SELECT work_id::text, COALESCE(MAX(number), -1) as max_pub
           FROM chapters
           WHERE work_id = ANY($1::uuid[]) AND published_at IS NOT NULL
           GROUP BY work_id`,
          [candWorkIds]
        );
        for (const pr of pubRes.rows) pubMap.set(pr.work_id, parseFloat(pr.max_pub));

        const gapsMap = new Map<string, Array<{ start: number; end: number }>>();
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

        // Sort candidates by source permit headroom and diversity. P1 has one
        // circular candidate per source, so this preserves its cursor fairness.
        const p1Sources = isP1
          ? Array.from(new Set<string>(candidateRows.map((row: any) => String(row.source)))).sort()
          : [];
        const nextP1SourceRank = (source: string): number => {
          if (!isP1 || p1Sources.length === 0) return 0;
          if (!this.lastOnDemandP1Source) return p1Sources.indexOf(source);
          const firstAfterCursor = p1Sources.findIndex((candidate) => candidate > this.lastOnDemandP1Source!);
          const start = firstAfterCursor >= 0 ? firstAfterCursor : 0;
          return (p1Sources.indexOf(source) - start + p1Sources.length) % p1Sources.length;
        };
        candidateRows.sort((a: any, b: any) => {
          const permitsA = this.sourcePermitProvider ? this.sourcePermitProvider(a.source) : 1;
          const permitsB = this.sourcePermitProvider ? this.sourcePermitProvider(b.source) : 1;
          const activeA = sourceCounts.get(a.source) || 0;
          const activeB = sourceCounts.get(b.source) || 0;
          if ((permitsA > 0) !== (permitsB > 0)) return permitsA > 0 ? -1 : 1;
          if (activeA !== activeB) return activeA - activeB;
          const sourceRankA = nextP1SourceRank(a.source);
          const sourceRankB = nextP1SourceRank(b.source);
          if (sourceRankA !== sourceRankB) return sourceRankA - sourceRankB;
          return parseInt(b.queued_count || '0', 10) - parseInt(a.queued_count || '0', 10);
        });

        const isCandidateFrontierValid = (workId: string, minSort: number, maxPub: number): boolean => {
          if (minSort <= (maxPub === -1 ? 1.5 : maxPub + 1.5)) return true;
          const gapStart = maxPub >= 0 ? maxPub + 1 : 1;
          const gapEnd = minSort - 1;
          return (gapsMap.get(workId) || []).some((g) => g.start <= gapStart && g.end >= gapEnd);
        };

        let match = candidateRows.find((cand: any) => {
          if (parseInt(cand.queued_count || '0', 10) === 0) return false;
          const maxPub = pubMap.get(cand.work_id) ?? -1;
          const minSort = cand.min_sort_key ? parseFloat(cand.min_sort_key) : 0;
          return isCandidateFrontierValid(cand.work_id, minSort, maxPub);
        });
        if (match) return match;

        // Only the bounded source window is considered for gap confirmation.
        for (const cand of candidateRows) {
          if (parseInt(cand.queued_count || '0', 10) === 0) continue;
          const maxPub = pubMap.get(cand.work_id) ?? -1;
          const minSort = cand.min_sort_key ? parseFloat(cand.min_sort_key) : 0;
          const gapStart = maxPub >= 0 ? maxPub + 1 : 1;
          const gapEnd = minSort - 1;
          if (gapStart > gapEnd) continue;
          try {
            const conf = await confirmUpstreamGapInterval(this.pool, {
              workId: cand.work_id,
              startSortKey: gapStart,
              endSortKey: gapEnd,
              primarySource: cand.source,
              reason: 'ON_DEMAND_ADMISSION_GAP_CONFIRM',
            });
            if (conf.confirmed) return cand;
            else this.deadWorksCache.set(cand.work_id, Date.now());
          } catch {}
        }
        return null;
      };

      let res = await loadOnDemandCandidates(false);
      let match = await findOnDemandFrontier(res.rows);
      // A non-empty QUEUED source window may contain only rows behind a gap.
      // Retry the existing bounded paused-window path when no candidate is
      // actually executable, rather than treating row presence as progress.
      if (!match) {
        const pausedRes = await loadOnDemandCandidates(true);
        const pausedMatch = await findOnDemandFrontier(pausedRes.rows);
        if (pausedMatch) {
          res = pausedRes;
          match = pausedMatch;
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

        if (isP1) {
          newWork.queuedChapters = await this.enforceP1FairWindow(newWork.workId, cand.source);
          // The P1 query is restricted to rotation_rank=1. This admission is
          // therefore real circular progress, not a frontier leap: persist
          // it so the same work cannot be immediately re-admitted after it
          // consumes its one chapter window.
          this.advanceP1AdmissionCursor(cand.source, cand.work_id);
          this.lastOnDemandP1Source = cand.source;
        }
        this.stateStore.setActiveWork(newWork);

        // P1 receives its one fair chapter through enforceP1FairWindow above.
        // P2 keeps its existing sliding window semantics.
        if (!isP1 && newWork.queuedChapters < config.slidingWindowMin) {
          const needed = config.slidingWindowSize - newWork.queuedChapters;
          const targetPriority = 50;
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
