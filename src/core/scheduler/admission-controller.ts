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

  constructor(
    private stateStore: SchedulerStateStore,
    private protectiveSentinel: ProtectiveSentinel,
    pool?: any
  ) {
    const rawPool = pool || getYugabytePool();
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

  /**
   * Starts the periodic admission background loop (every 5 seconds).
   */
  start(): void {
    if (this.isRunning) return;
    this.isRunning = true;
    this.logger.info('Starting AdmissionController background loop');
    this.scheduleNextCycle(1000);
  }

  stop(): void {
    this.isRunning = false;
    if (this.loopTimer) {
      clearTimeout(this.loopTimer);
      this.loopTimer = null;
    }
  }

  private scheduleNextCycle(delayMs: number): void {
    if (!this.isRunning) return;
    this.loopTimer = setTimeout(async () => {
      try {
        await this.runAdmissionCycle();
      } catch (err: any) {
        this.logger.error('Error during admission cycle', { error: err?.message });
      } finally {
        this.scheduleNextCycle(5000);
      }
    }, delayMs);
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

    const client = await this.pool.connect();
    try {
      // 2. NO_P0_WAITING (Real P0 releases: 100 <= priority < 1000; staff-forced >= 1000 belongs to chapter data plane)
      const p0Res = await client.query(`
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

      // 3. P2_ACTIVE_COHORT_BELOW_LIMIT:
      // Strictly restrict active P2 cohort to <= config.maxActiveNewWorks (default 4).
      // Filter out stale P2 works that have been in FILLING for >= 30m without progress.
      const activeWorks = this.stateStore.getActiveWorks();
      const nowMs = Date.now();
      const activeP2Works = activeWorks.filter((w) => {
        if (w.lane !== 'P2' || w.state !== 'FILLING') return false;
        const admittedTime = new Date(w.admittedAt).getTime();
        const lastActTime = new Date(w.lastActivityAt || w.admittedAt).getTime();
        if (w.publishedChapters === 0 && (nowMs - admittedTime >= 30 * 60 * 1000) && (nowMs - lastActTime >= 15 * 60 * 1000)) {
          return false; // Stale cohort work does not block new admissions
        }
        return true;
      });
      const maxP2Cohort = config.maxActiveNewWorks || 4;

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

      // 4. WORKER_CAPACITY_CHECK:
      // P0 >>> P1 > P2 > P3. If all 18 workers are fully utilized, wait.
      // But if there is spare worker capacity (< 18 in-flight) AND activeP2Works < maxP2Cohort:
      // allow P2/P3 new works to admit so P3 never dies of starvation from catalog backlog.
      const inFlightRes = await client.query(
        `SELECT COUNT(*) as cnt FROM importer_queue WHERE status = 'IMPORTING' AND task_type = 'IMPORT_CHAPTER'`
      );
      const importingCnt = parseInt(inFlightRes.rows[0]?.cnt || '0', 10);
      const maxTotalWorkers = 18;

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
    } finally {
      client.release();
    }
  }

  /**
   * Executes a single admission reconciliation cycle.
   */
  async runAdmissionCycle(): Promise<void> {
    const config = this.stateStore.getConfig();
    if (!config.enabled && !config.shadowMode) {
      return;
    }

    // Step 0: Check PROTECTIVE_STOP
    if (await this.protectiveSentinel.isProtectiveStopActive()) {
      this.logger.warn('PROTECTIVE_STOP active, skipping admission cycle');
      return;
    }

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
      const client = await this.pool.connect();
      try {
        for (const work of activeWorks) {
          try {
            // A. Count queued & importing jobs for this work
            const queueRes = await client.query(
              `SELECT 
                 COUNT(CASE WHEN status = 'QUEUED' THEN 1 END) as queued_cnt,
                 COUNT(CASE WHEN status = 'IMPORTING' THEN 1 END) as importing_cnt,
                 COUNT(CASE WHEN status = 'PAUSED_BY_STAFF' THEN 1 END) as paused_cnt,
                 MIN(CASE WHEN status IN ('QUEUED', 'PAUSED_BY_STAFF') THEN chapter_sort_key END) as min_sort_key
               FROM importer_queue
               WHERE task_type = 'IMPORT_CHAPTER' AND (payload->>'workId') = $1`,
              [work.workId]
            );

            // B. Count published chapters
            const pubRes = await client.query(
              `SELECT COUNT(*) as pub_cnt, COALESCE(MAX(number), -1) as max_pub FROM chapters WHERE work_id = $1::uuid AND published_at IS NOT NULL`,
              [work.workId]
            );

            // C. Detect STAGED barrier gaps and unimported mappings for this work
            const stagedRes = await client.query(
              `SELECT 
                 COUNT(CASE WHEN status = 'STAGED' THEN 1 END) as staged_cnt,
                 MIN(CASE WHEN status = 'STAGED' THEN chapter_sort_key END) as min_staged,
                 COUNT(CASE WHEN status NOT IN ('COMPLETED', 'FAILED') THEN 1 END) as unimported_cnt
               FROM importer_chapter_mappings
               WHERE work_id = $1::uuid`,
              [work.workId]
            );

            const qRow = queueRes.rows[0];
            const queuedCnt = parseInt(qRow?.queued_cnt || '0', 10);
            const importingCnt = parseInt(qRow?.importing_cnt || '0', 10);
            const pausedCnt = parseInt(qRow?.paused_cnt || '0', 10);
            const minSortKey = qRow?.min_sort_key ? parseFloat(qRow.min_sort_key) : null;
            const pubCnt = parseInt(pubRes.rows[0]?.pub_cnt || '0', 10);
            const maxPub = parseFloat(pubRes.rows[0]?.max_pub ?? '-1');
            const stagedCnt = parseInt(stagedRes.rows[0]?.staged_cnt || '0', 10);
            const minStaged = stagedRes.rows[0]?.min_staged ? parseFloat(stagedRes.rows[0].min_staged) : null;
            const unimportedCnt = parseInt(stagedRes.rows[0]?.unimported_cnt || '0', 10);

            work.queuedChapters = queuedCnt;
            work.inFlightChapters = importingCnt;
            work.publishedChapters = pubCnt;
            work.totalChapters = pubCnt + queuedCnt + importingCnt + pausedCnt;
            work.frontierSortKey = minSortKey;

            // Promote P2 work to P1 if it has published chapters
            if (pubCnt > 0 && work.lane === 'P2') {
              this.logger.info(`Work ${work.workTitle} (${work.workId}) promoted from P2 to P1 (${pubCnt} published chapters).`);
              work.lane = 'P1';
            }

            // Critical gap detection
            if (stagedCnt > 0 && minSortKey !== null && minStaged !== null && minSortKey < minStaged) {
              work.criticalGapSortKey = minSortKey;
              work.criticalGapUnblockCount = stagedCnt;
            } else {
              work.criticalGapSortKey = null;
              work.criticalGapUnblockCount = 0;
            }

            // Gap blocking check
            const expectedFrontier = maxPub >= 0 ? maxPub + 1.5 : 1.5;
            const isGapBlocked = minSortKey !== null && minSortKey > expectedFrontier;
            if (isGapBlocked) {
              this.logger.warn(`Work ${work.workTitle} (${work.workId}) marked BLOCKED due to unresolvable gap. Vacating active slot.`);
              work.state = 'BLOCKED';
              this.stateStore.removeActiveWork(work.workId);
              continue;
            }

            // Check primary source health
            const srcCheck = await client.query(
              `SELECT status, cooldown_until FROM importer_sources WHERE id = $1`,
              [work.primarySource]
            );
            const srcRow = srcCheck.rows[0];
            const isSourceBlocked = srcRow && (
              srcRow.status === 'DISABLED' ||
              srcRow.status === 'PAUSED' ||
              (srcRow.status === 'COOLDOWN' && srcRow.cooldown_until && new Date(srcRow.cooldown_until) > new Date())
            );

            if (isSourceBlocked) {
              this.logger.info(`Work ${work.workTitle} (${work.workId}) marked BLOCKED (source ${work.primarySource} in cooldown/blocked). Vacating active slot.`);
              work.state = 'BLOCKED';
              this.stateStore.removeActiveWork(work.workId);
              continue;
            } else if (work.state === 'BLOCKED') {
              this.logger.info(`Work ${work.workTitle} (${work.workId}) unblocked as source ${work.primarySource} recovered.`);
              work.state = 'FILLING';
              this.stateStore.setActiveWork(work);
            }

            // Check if caught up or drained (zero queued, zero importing, zero paused)
            if (queuedCnt === 0 && importingCnt === 0 && pausedCnt === 0) {
              const isCaughtUp = pubCnt > 0;
              work.state = isCaughtUp ? 'CAUGHT_UP' : 'COMPLETE';
              this.logger.info(`[ACTIVE_SET_VACATED] Work ${work.workTitle} (${work.workId}) reached ${work.state} state (${queuedCnt} queued, ${importingCnt} in-flight, ${pausedCnt} paused, ${pubCnt} published). Vacating active slot.`);
              this.stateStore.removeActiveWork(work.workId);
              continue;
            }

            // Check if P2 new work is stale in cohort (admitted >= 30m ago with 0 in-flight and no progress)
            if (work.lane === 'P2') {
              const admittedMs = new Date(work.admittedAt).getTime();
              const lastActMs = new Date(work.lastActivityAt || work.admittedAt).getTime();
              const nowMs = Date.now();
              if (work.publishedChapters === 0 && importingCnt === 0 && (nowMs - admittedMs >= 30 * 60 * 1000) && (nowMs - lastActMs >= 15 * 60 * 1000)) {
                this.logger.warn(`[ACTIVE_SET_VACATED] Stale P2 work ${work.workTitle} (${work.workId}) vacated from active cohort (>30m with 0 in-flight) to allow new admissions.`);
                this.stateStore.removeActiveWork(work.workId);
                continue;
              }
            }

            work.state = 'FILLING';
            work.lastActivityAt = new Date().toISOString();
            this.stateStore.setActiveWork(work);
          } catch (workErr: any) {
            this.logger.warn(`Failed to reconcile active work ${work.workId}`, { error: workErr?.message });
          }
        }
      } finally {
        client.release();
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
      const lastActMs = new Date(w.lastActivityAt || w.admittedAt).getTime();
      if (w.publishedChapters === 0 && (nowMs - admittedMs >= 30 * 60 * 1000) && (nowMs - lastActMs >= 15 * 60 * 1000)) {
        return false;
      }
      return true;
    });

    const client = await this.pool.connect();
    try {
      // Measure current worker utilization for elastic scheduling (Section 7 & 8)
      let idleWorkers = 0;
      try {
        const qAct = await client.query(
          `SELECT COUNT(*) as cnt FROM importer_queue WHERE status = 'IMPORTING' AND task_type = 'IMPORT_CHAPTER'`
        );
        const importingCnt = parseInt(qAct.rows[0]?.cnt || '0', 10);
        idleWorkers = Math.max(0, 18 - importingCnt);
      } catch {}

      // Elastic backfill: if workers are idle, allow expanding active P1 up to 36 works
      // to guarantee full worker utilization without violating maxInflightPerWork = 2.
      const targetBackfillLimit = idleWorkers >= 2
        ? Math.min(36, Math.max(config.maxActiveBackfillWorks, 18 + idleWorkers))
        : config.maxActiveBackfillWorks;
      const backfillSlotsAvailable = Math.max(0, targetBackfillLimit - activeBackfills.length);

      // P2 uses spare capacity when P1 cannot occupy available workers
      // Strictly restrict active P2 cohort to <= config.maxActiveNewWorks (default 4).
      const maxP2Cohort = config.maxActiveNewWorks || 4;
      const targetNewWorksLimit = idleWorkers >= 1 ? maxP2Cohort : 0;
      const newWorkSlotsAvailable = Math.max(0, targetNewWorksLimit - activeNewWorks.length);

      // Track active sources for source diversity (Section 81)
      const sourceCounts = new Map<string, number>();
      for (const w of activeWorks.filter((w) => w.state === 'FILLING')) {
        sourceCounts.set(w.primarySource, (sourceCounts.get(w.primarySource) || 0) + 1);
      }

      // 1. Replenish P1 Backfill Works
      if (backfillSlotsAvailable > 0) {
        const activeIds = activeWorks.map((w) => w.workId);
        const candidatesRes = await client.query(
          `SELECT (q.payload->>'workId') as work_id,
                  w.title,
                  q.source,
                  COUNT(*) as pending_jobs,
                  COUNT(CASE WHEN q.status = 'QUEUED' THEN 1 END) as queued_count,
                  COUNT(CASE WHEN q.status = 'PAUSED_BY_STAFF' THEN 1 END) as paused_count,
                  MIN(q.chapter_sort_key) as min_sort_key
           FROM importer_queue q
           JOIN works w ON w.id = (q.payload->>'workId')::uuid
           JOIN importer_sources s ON s.id = q.source
           LEFT JOIN (
             SELECT work_id, COALESCE(MAX(number), -1) as max_published
             FROM chapters
             WHERE published_at IS NOT NULL
             GROUP BY work_id
           ) p ON p.work_id = w.id
           WHERE q.task_type = 'IMPORT_CHAPTER'
             AND q.status IN ('QUEUED', 'RETRY', 'PAUSED_BY_STAFF')
             AND w.published = true
             AND w.latest_chapter_published_at IS NOT NULL
             AND s.enabled = true
             AND (s.status = 'ACTIVE' OR (s.status IN ('COOLDOWN', 'PROBING', 'DEGRADED') AND (s.cooldown_until IS NULL OR s.cooldown_until <= NOW())))
             AND NOT ((q.payload->>'workId') = ANY($1::text[]))
           GROUP BY (q.payload->>'workId'), w.title, q.source, p.max_published
           HAVING (MIN(q.chapter_sort_key) <= COALESCE(p.max_published, -1) + 1.5 OR p.max_published IS NULL)
           ORDER BY queued_count DESC, pending_jobs DESC
           LIMIT $2`,
          [activeIds.length > 0 ? activeIds : ['00000000-0000-0000-0000-000000000000'], Math.max(50, backfillSlotsAvailable * 5)]
        );

        let admitted = 0;
        for (const cand of candidatesRes.rows) {
          if (admitted >= backfillSlotsAvailable) break;

          const srcCount = sourceCounts.get(cand.source) || 0;
          const maxWorksPerSource = idleWorkers >= 4 ? 4 : (cand.source === 'mangaflix' ? 2 : 3);
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
        const candidatesRes = await client.query(
          `SELECT (q.payload->>'workId') as work_id,
                  w.title,
                  q.source,
                  COUNT(*) as pending_jobs,
                  COUNT(CASE WHEN q.status = 'QUEUED' THEN 1 END) as queued_count,
                  COUNT(CASE WHEN q.status = 'PAUSED_BY_STAFF' THEN 1 END) as paused_count,
                  MIN(q.chapter_sort_key) as min_sort_key
           FROM importer_queue q
           JOIN works w ON w.id = (q.payload->>'workId')::uuid
           JOIN importer_sources s ON s.id = q.source
           WHERE q.task_type = 'IMPORT_CHAPTER'
             AND q.status IN ('QUEUED', 'RETRY', 'PAUSED_BY_STAFF')
             AND (w.published IS FALSE OR w.latest_chapter_published_at IS NULL)
             AND s.enabled = true
             AND (s.status = 'ACTIVE' OR (s.status IN ('COOLDOWN', 'PROBING', 'DEGRADED') AND (s.cooldown_until IS NULL OR s.cooldown_until <= NOW())))
             AND NOT ((q.payload->>'workId') = ANY($1::text[]))
           GROUP BY (q.payload->>'workId'), w.title, q.source, w.created_at
           ORDER BY w.created_at DESC
           LIMIT $2`,
          [activeIds.length > 0 ? activeIds : ['00000000-0000-0000-0000-000000000000'], newWorkSlotsAvailable * 3]
        );

        let admitted = 0;
        for (const cand of candidatesRes.rows) {
          if (admitted >= newWorkSlotsAvailable) break;

          const srcCount = sourceCounts.get(cand.source) || 0;
          const maxWorksPerSource = cand.source === 'mangaflix' ? 2 : 3;
          const otherSourceCandidates = candidatesRes.rows.filter((r: any) => (sourceCounts.get(r.source) || 0) < (r.source === 'mangaflix' ? 2 : 3));
          if (srcCount >= maxWorksPerSource && otherSourceCandidates.length > 0) {
            continue;
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
            await client.query(
              `UPDATE importer_work_mappings 
               SET sync_status = 'ACTIVE', updated_at = NOW() 
               WHERE work_id = $1::uuid AND sync_status = 'WAITING_ADMISSION'`,
              [newWork.workId]
            );
          } catch {}

          // 2. Promote initial sliding window of chapters from PAUSED_BY_STAFF to QUEUED (priority 50)
          try {
            const promRes = await client.query(
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
    } finally {
      client.release();
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

    const client = await this.pool.connect();
    try {
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
          const promoteRes = await client.query(
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
    } finally {
      client.release();
    }
  }

  /**
   * On-demand admission: admits the highest priority waiting work into the active set
   * when workers are idle and currently active works cannot supply jobs.
   * Work-conserving and strictly controlled: preserves work-affinity, fairness, and sliding window.
   */
  async admitNextWorkOnDemand(
    preferredLane?: 'P1' | 'P2',
    allowedSources?: string[]
  ): Promise<ActiveWork | null> {
    const config = this.stateStore.getConfig();
    if (!config.enabled && !config.shadowMode) return null;
    if (await this.protectiveSentinel.isProtectiveStopActive()) return null;

    const activeWorks = this.stateStore.getActiveWorks();
    const activeIds = activeWorks.map((w) => w.workId);

    const sourceCounts = new Map<string, number>();
    for (const w of activeWorks.filter((w) => w.state === 'FILLING')) {
      sourceCounts.set(w.primarySource, (sourceCounts.get(w.primarySource) || 0) + 1);
    }
    const saturatedSources = Array.from(sourceCounts.entries())
      .filter(([src, cnt]) => cnt >= (src === 'mangaflix' ? 2 : 3))
      .map(([src]) => src);

    const client = await this.pool.connect();
    try {
      const lanesToTry = preferredLane ? [preferredLane] : (['P1', 'P2'] as const);

      for (const lane of lanesToTry) {
        if (lane === 'P2') {
          const gate = await this.canAdmitNewWork();
          if (!gate.allowed) {
            this.logger.debug(`[ADMISSION_GATE_HOLD] admitNextWorkOnDemand blocked for P2: ${gate.reason}`);
            continue;
          }
        }
        const isP1 = lane === 'P1';
        const query = `
          SELECT (q.payload->>'workId') as work_id,
                 w.title,
                 q.source,
                 COUNT(*) as pending_jobs,
                 COUNT(CASE WHEN q.status = 'QUEUED' THEN 1 END) as queued_count,
                 MIN(q.chapter_sort_key) as min_sort_key
          FROM importer_queue q
          JOIN works w ON w.id = (q.payload->>'workId')::uuid
          JOIN importer_sources s ON s.id = q.source
          LEFT JOIN (
            SELECT work_id, COALESCE(MAX(number), -1) as max_published
            FROM chapters
            WHERE published_at IS NOT NULL
            GROUP BY work_id
          ) p ON p.work_id = w.id
          WHERE q.task_type = 'IMPORT_CHAPTER'
            AND q.status IN ('QUEUED', 'RETRY', 'PAUSED_BY_STAFF')
            AND ${isP1 ? 'w.published = true AND w.latest_chapter_published_at IS NOT NULL' : '(w.published IS FALSE OR w.latest_chapter_published_at IS NULL)'}
            AND s.enabled = true
            AND (s.status = 'ACTIVE' OR (s.status IN ('COOLDOWN', 'PROBING', 'DEGRADED') AND (s.cooldown_until IS NULL OR s.cooldown_until <= NOW())))
            AND ($1::text[] IS NULL OR q.source = ANY($1::text[]))
            AND NOT ((q.payload->>'workId') = ANY($2::text[]))
            AND ($3::text[] IS NULL OR NOT (q.source = ANY($3::text[])))
          GROUP BY (q.payload->>'workId'), w.title, q.source, p.max_published
          HAVING ${isP1 ? '(MIN(q.chapter_sort_key) <= COALESCE(p.max_published, -1) + 1.5 OR p.max_published IS NULL)' : '(MIN(q.chapter_sort_key) <= 1.5)'}
          ORDER BY queued_count DESC, pending_jobs DESC
          LIMIT 1;
        `;

        const res = await client.query(query, [
          allowedSources && allowedSources.length > 0 ? allowedSources : null,
          activeIds.length > 0 ? activeIds : ['00000000-0000-0000-0000-000000000000'],
          saturatedSources.length > 0 ? saturatedSources : null,
        ]);

        if (res.rows.length > 0) {
          const cand = res.rows[0];
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

          // If this work has fewer than slidingWindowMin queued chapters, promote next batch
          if (newWork.queuedChapters < config.slidingWindowMin) {
            const needed = config.slidingWindowSize - newWork.queuedChapters;
            const targetPriority = isP1 ? 75 : 50;
            const promoteRes = await client.query(
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
    } finally {
      client.release();
    }
  }
}
