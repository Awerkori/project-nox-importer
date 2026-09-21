/**
 * Work-Affinity Scheduler for Project Nox Importer.
 * 
 * Implements:
 * - P0: Absolute priority preemption for fresh new releases (priority >= 100).
 * - P1 Critical Gap: Prioritizes missing chapters unblocking STAGED barrier cascade (priority 90-95).
 * - P1 Backfill: Fair scheduling across ACTIVE_BACKFILL_WORKS (<= 10 works).
 * - P2 Active New Works: Fair scheduling with work affinity across ACTIVE_NEW_WORKS (<= 8 works).
 * - Max in-flight per work: MAX_INFLIGHT_PER_WORK = 2 (ensures >= 9 concurrent works across 18 workers).
 * - Anti-starvation: P1 and P2 make steady progress even under sustained P0 traffic.
 * - Work-conserving fallback: No worker sits idle if any eligible job exists.
 * - Explainable scheduler: Detailed telemetry on why each job was selected.
 * - Shadow mode & live cutover toggle.
 */

import { getYugabytePool, acquireJobsDirect } from '../../db/yugabyte-direct.js';
import { Logger } from '../logger.js';
import { ProtectiveSentinel } from '../protective-sentinel.js';
import { AdmissionController } from './admission-controller.js';
import { SchedulerStateStore } from './state-store.js';
import {
  ActiveWork,
  SchedulerDecision,
  SchedulerLane,
  SchedulerMetrics,
  WorkSchedulerState,
} from './types.js';

export interface AcquiredSchedulerJob {
  job: any;
  lane: SchedulerLane;
  decision: SchedulerDecision;
}

export class WorkAffinityScheduler {
  private logger = new Logger('WorkAffinityScheduler');
  private pool = getYugabytePool();
  private inFlightByWork: Map<string, number> = new Map();
  private p0ConsecutiveClaims = 0;
  private rrIndexP1 = 0;
  private rrIndexP2 = 0;

  // Performance telemetry
  private p0WaitTimes: number[] = [];
  private p0Count1h = 0;
  private p1Count1h = 0;
  private p2Count1h = 0;

  constructor(
    private stateStore: SchedulerStateStore,
    private admissionController: AdmissionController,
    private protectiveSentinel: ProtectiveSentinel
  ) {}

  /**
   * Initializes state and synchronizes in-flight counts from DB.
   */
  async initialize(): Promise<void> {
    await this.stateStore.initialize();
    await this.syncInFlightCountsFromDb();
    this.admissionController.start();
    this.startMetricsReporter();
    this.logger.info('WorkAffinityScheduler initialized and running');
  }

  /**
   * Synchronizes in-flight job counts per work from DB at startup.
   */
  private async syncInFlightCountsFromDb(): Promise<void> {
    try {
      const client = await this.pool.connect();
      try {
        const res = await client.query(`
          SELECT (payload->>'workId') as work_id, COUNT(*) as cnt
          FROM importer_queue
          WHERE status = 'IMPORTING' AND task_type = 'IMPORT_CHAPTER' AND (payload->>'workId') IS NOT NULL
          GROUP BY (payload->>'workId');
        `);
        this.inFlightByWork.clear();
        for (const r of res.rows) {
          if (r.work_id) {
            this.inFlightByWork.set(r.work_id, parseInt(r.cnt, 10));
          }
        }
        this.logger.info('Synchronized in-flight counts from DB', {
          activeWorksWithInFlight: this.inFlightByWork.size,
        });
      } finally {
        client.release();
      }
    } catch (err: any) {
      this.logger.warn('Failed to sync in-flight counts from DB', { error: err?.message });
    }
  }

  /**
   * Main entry point for worker slots claiming IMPORT_CHAPTER jobs.
   */
  async acquireNextChapterJob(options: {
    workerId: string;
    leaseDurationMinutes?: number;
    allowedSources?: string[];
  }): Promise<any | null> {
    const config = this.stateStore.getConfig();
    const t0 = performance.now();

    // 0. Check PROTECTIVE_STOP
    if (await this.protectiveSentinel.isProtectiveStopActive()) {
      return null;
    }

    // If disabled and not in shadow mode, directly use legacy claim
    if (!config.enabled && !config.shadowMode) {
      const legacyJobs = await acquireJobsDirect({
        workerId: options.workerId,
        leaseDurationMinutes: options.leaseDurationMinutes,
        allowedSources: options.allowedSources,
        taskType: 'IMPORT_CHAPTER',
        batchSize: 1,
      });
      return legacyJobs.length > 0 ? legacyJobs[0] : null;
    }

    // Shadow Mode simulation branch
    if (config.shadowMode && !config.enabled) {
      return this.executeShadowModeSimulation(options, t0);
    }

    // LIVE WORK-ORIENTED SCHEDULING
    return this.executeIntelligentClaim(options, t0);
  }

  /**
   * Core intelligent claim logic implementing P0 -> P1 -> P2 -> Fallback.
   */
  private async executeIntelligentClaim(
    options: { workerId: string; leaseDurationMinutes?: number; allowedSources?: string[] },
    t0: number
  ): Promise<any | null> {
    const config = this.stateStore.getConfig();
    const leaseMin = Math.max(1, Math.min(60, options.leaseDurationMinutes || 5));
    const allowedSources = options.allowedSources && options.allowedSources.length > 0 ? options.allowedSources : null;

    const client = await this.pool.connect();
    try {
      // -------------------------------------------------------------
      // LANE P0: Fresh New Releases (Priority >= 100)
      // -------------------------------------------------------------
      const shouldCheckP0 = this.p0ConsecutiveClaims < config.antiStarvationRatio;
      if (shouldCheckP0) {
        const p0Job = await this.claimSingleJob(client, {
          workerId: options.workerId,
          leaseMin,
          allowedSources,
          minPriority: 100,
        });

        if (p0Job) {
          const waitTimeMs = performance.now() - t0;
          this.p0ConsecutiveClaims++;
          this.p0Count1h++;
          this.p0WaitTimes.push(waitTimeMs);
          if (this.p0WaitTimes.length > 100) this.p0WaitTimes.shift();

          const workId = p0Job.payload?.workId || '';
          this.onJobStarted(workId);

          const decision: SchedulerDecision = {
            jobId: p0Job.id,
            workId,
            workTitle: p0Job.payload?.chapterTitle || 'P0 Release',
            chapterNumber: p0Job.payload?.chapterNumber ?? 0,
            chapterSortKey: p0Job.chapter_sort_key ?? 0,
            lane: SchedulerLane.P0_FRESH_RELEASE,
            reason: 'FRESH_RELEASE',
            workState: 'UPDATING',
            source: p0Job.source,
            waitTimeMs: Math.round(waitTimeMs * 10) / 10,
            decisionTime: new Date().toISOString(),
          };
          this.logDecision(decision);
          return p0Job;
        }
      }

      // Reset anti-starvation counter if P0 is drained or threshold reached
      if (this.p0ConsecutiveClaims >= config.antiStarvationRatio) {
        this.p0ConsecutiveClaims = 0;
      }

      // -------------------------------------------------------------
      // LANE P1: Critical Gap (Priority >= 90, unblocks STAGED barrier)
      // -------------------------------------------------------------
      const activeWorks = this.stateStore.getActiveWorks();
      const p1Works = activeWorks.filter((w) => w.lane === 'P1');
      const p2Works = activeWorks.filter((w) => w.lane === 'P2');

      // Check critical gaps first
      const criticalWorks = p1Works.filter(
        (w) => w.criticalGapSortKey !== null && (this.inFlightByWork.get(w.workId) || 0) < config.maxInflightPerWork
      );

      for (const cw of criticalWorks) {
        const gapJob = await this.claimSingleJob(client, {
          workerId: options.workerId,
          leaseMin,
          allowedSources,
          workId: cw.workId,
          sortKey: cw.criticalGapSortKey!,
        });

        if (gapJob) {
          const waitTimeMs = performance.now() - t0;
          this.onJobStarted(cw.workId);
          this.p1Count1h++;

          const decision: SchedulerDecision = {
            jobId: gapJob.id,
            workId: cw.workId,
            workTitle: cw.workTitle,
            chapterNumber: gapJob.payload?.chapterNumber ?? 0,
            chapterSortKey: gapJob.chapter_sort_key ?? 0,
            lane: SchedulerLane.P1_CRITICAL_GAP,
            reason: `BARRIER_UNBLOCK (unblocks ${cw.criticalGapUnblockCount} staged chapters)`,
            workState: 'FILLING',
            source: gapJob.source,
            waitTimeMs: Math.round(waitTimeMs * 10) / 10,
            decisionTime: new Date().toISOString(),
          };
          this.logDecision(decision);
          return gapJob;
        }
      }

      // -------------------------------------------------------------
      // LANE P1: Active Backfill Works (Fair Round-Robin)
      // -------------------------------------------------------------
      const eligibleP1Works = p1Works.filter(
        (w) => (this.inFlightByWork.get(w.workId) || 0) < config.maxInflightPerWork
      );

      if (eligibleP1Works.length > 0) {
        const startIdx = this.rrIndexP1 % eligibleP1Works.length;
        for (let i = 0; i < eligibleP1Works.length; i++) {
          const idx = (startIdx + i) % eligibleP1Works.length;
          const targetWork = eligibleP1Works[idx];

          const p1Job = await this.claimSingleJob(client, {
            workerId: options.workerId,
            leaseMin,
            allowedSources,
            workId: targetWork.workId,
          });

          if (p1Job) {
            this.rrIndexP1 = idx + 1;
            const waitTimeMs = performance.now() - t0;
            this.onJobStarted(targetWork.workId);
            this.p1Count1h++;

            const decision: SchedulerDecision = {
              jobId: p1Job.id,
              workId: targetWork.workId,
              workTitle: targetWork.workTitle,
              chapterNumber: p1Job.payload?.chapterNumber ?? 0,
              chapterSortKey: p1Job.chapter_sort_key ?? 0,
              lane: SchedulerLane.P1_BACKFILL,
              reason: 'WORK_AFFINITY_BACKFILL',
              workState: 'FILLING',
              source: p1Job.source,
              waitTimeMs: Math.round(waitTimeMs * 10) / 10,
              decisionTime: new Date().toISOString(),
            };
            this.logDecision(decision);
            return p1Job;
          }
        }
      }

      // -------------------------------------------------------------
      // LANE P2: Active New Works (Fair Round-Robin with Affinity)
      // -------------------------------------------------------------
      const eligibleP2Works = p2Works.filter(
        (w) => (this.inFlightByWork.get(w.workId) || 0) < config.maxInflightPerWork
      );

      if (eligibleP2Works.length > 0) {
        const startIdx = this.rrIndexP2 % eligibleP2Works.length;
        for (let i = 0; i < eligibleP2Works.length; i++) {
          const idx = (startIdx + i) % eligibleP2Works.length;
          const targetWork = eligibleP2Works[idx];

          const p2Job = await this.claimSingleJob(client, {
            workerId: options.workerId,
            leaseMin,
            allowedSources,
            workId: targetWork.workId,
          });

          if (p2Job) {
            this.rrIndexP2 = idx + 1;
            const waitTimeMs = performance.now() - t0;
            this.onJobStarted(targetWork.workId);
            this.p2Count1h++;

            const decision: SchedulerDecision = {
              jobId: p2Job.id,
              workId: targetWork.workId,
              workTitle: targetWork.workTitle,
              chapterNumber: p2Job.payload?.chapterNumber ?? 0,
              chapterSortKey: p2Job.chapter_sort_key ?? 0,
              lane: SchedulerLane.P2_ACTIVE_NEW_WORK,
              reason: 'WORK_AFFINITY_NEW_WORK',
              workState: 'FILLING',
              source: p2Job.source,
              waitTimeMs: Math.round(waitTimeMs * 10) / 10,
              decisionTime: new Date().toISOString(),
            };
            this.logDecision(decision);
            return p2Job;
          }
        }
      }

      // -------------------------------------------------------------
      // WORK-CONSERVING FALLBACK: Claim any available job belonging to ACTIVE works
      // If ACTIVE_NEW_WORKS = 0, unadmitted P2 works MUST NOT be claimed.
      // -------------------------------------------------------------
      const activeWorkIds = activeWorks.map((w) => w.workId);
      let fallbackJob = null;
      if (activeWorkIds.length > 0) {
        fallbackJob = await this.claimSingleJob(client, {
          workerId: options.workerId,
          leaseMin,
          allowedSources,
          allowedWorkIds: activeWorkIds,
        });
      }

      if (fallbackJob) {
        const waitTimeMs = performance.now() - t0;
        const workId = fallbackJob.payload?.workId || '';
        this.onJobStarted(workId);

        const decision: SchedulerDecision = {
          jobId: fallbackJob.id,
          workId,
          workTitle: fallbackJob.payload?.chapterTitle || 'Fallback Job',
          chapterNumber: fallbackJob.payload?.chapterNumber ?? 0,
          chapterSortKey: fallbackJob.chapter_sort_key ?? 0,
          lane: SchedulerLane.FALLBACK,
          reason: 'WORK_CONSERVING_DRAIN',
          workState: 'FILLING',
          source: fallbackJob.source,
          waitTimeMs: Math.round(waitTimeMs * 10) / 10,
          decisionTime: new Date().toISOString(),
        };
        this.logDecision(decision);
        return fallbackJob;
      }

      return null;
    } finally {
      client.release();
    }
  }

  /**
   * Helper to atomically claim 1 job with SKIP LOCKED.
   */
  private async claimSingleJob(
    client: any,
    opts: {
      workerId: string;
      leaseMin: number;
      allowedSources: string[] | null;
      minPriority?: number;
      workId?: string;
      sortKey?: number;
      allowedWorkIds?: string[] | null;
    }
  ): Promise<any | null> {
    const query = `
      WITH to_lock AS (
        SELECT q.id
        FROM importer_queue q
        WHERE (
          q.status = 'QUEUED'
          OR (q.status = 'RETRY' AND q.next_run_at <= NOW())
        )
          AND q.task_type = 'IMPORT_CHAPTER'
          AND q.attempts < COALESCE(q.max_attempts, 7)
          AND ($1::text[] IS NULL OR q.source = ANY($1::text[]))
          AND ($2::int IS NULL OR q.priority >= $2::int)
          AND ($3::text IS NULL OR (q.payload->>'workId') = $3::text)
          AND ($4::numeric IS NULL OR q.chapter_sort_key = $4::numeric)
          AND ($7::text[] IS NULL OR (q.payload->>'workId') = ANY($7::text[]))
        ORDER BY q.priority DESC, q.chapter_sort_key ASC NULLS LAST, q.next_run_at ASC
        FOR UPDATE SKIP LOCKED
        LIMIT 1
      )
      UPDATE importer_queue q
      SET status = 'IMPORTING',
          locked_by = $5,
          locked_at = NOW(),
          lease_expires_at = NOW() + ($6::text || ' minutes')::interval,
          attempts = q.attempts + 1,
          updated_at = NOW()
      FROM to_lock
      WHERE q.id = to_lock.id
      RETURNING q.id, q.task_type, q.source, q.priority, q.payload, q.dedupe_key,
                q.status, q.attempts, q.max_attempts, q.locked_by, q.locked_at,
                q.lease_expires_at, q.next_run_at, q.last_error, q.chapter_sort_key;
    `;

    const res = await client.query(query, [
      opts.allowedSources,
      opts.minPriority || null,
      opts.workId || null,
      opts.sortKey || null,
      opts.workerId,
      opts.leaseMin,
      opts.allowedWorkIds || null,
    ]);

    if (res.rows.length === 0) return null;
    const r = res.rows[0];
    return {
      ...r,
      payload: typeof r.payload === 'string' ? JSON.parse(r.payload) : (r.payload || {}),
      chapter_sort_key: r.chapter_sort_key ? parseFloat(r.chapter_sort_key) : null,
    };
  }

  /**
   * Shadow Mode simulation: calculates what the intelligent scheduler would choose,
   * compares with the legacy choice, and returns the legacy job.
   */
  private async executeShadowModeSimulation(
    options: { workerId: string; leaseDurationMinutes?: number; allowedSources?: string[] },
    t0: number
  ): Promise<any | null> {
    // 1. Run legacy claim
    const legacyJobs = await acquireJobsDirect({
      workerId: options.workerId,
      leaseDurationMinutes: options.leaseDurationMinutes,
      allowedSources: options.allowedSources,
      taskType: 'IMPORT_CHAPTER',
      batchSize: 1,
    });
    const chosenJob = legacyJobs.length > 0 ? legacyJobs[0] : null;

    // 2. Simulate intelligent choice
    try {
      const activeWorks = this.stateStore.getActiveWorks();
      const client = await this.pool.connect();
      try {
        const cand = await client.query(`
          SELECT q.id, q.source, q.priority, q.chapter_sort_key, (q.payload->>'workId') as work_id
          FROM importer_queue q
          WHERE q.status = 'QUEUED' AND q.task_type = 'IMPORT_CHAPTER'
          ORDER BY q.priority DESC, q.chapter_sort_key ASC
          LIMIT 1;
        `);
        if (cand.rows.length > 0) {
          const c = cand.rows[0];
          this.logger.info('[SHADOW_MODE] Decision comparison', {
            intelligentCandidate: {
              workId: c.work_id,
              priority: c.priority,
              sortKey: c.chapter_sort_key,
              source: c.source,
            },
            legacyChosen: chosenJob ? {
              workId: chosenJob.payload?.workId,
              priority: chosenJob.priority,
              sortKey: chosenJob.chapter_sort_key,
              source: chosenJob.source,
            } : null,
            activeWorksCount: activeWorks.length,
          });
        }
      } finally {
        client.release();
      }
    } catch {}

    if (chosenJob?.payload?.workId) {
      this.onJobStarted(chosenJob.payload.workId);
    }
    return chosenJob;
  }

  // --- In-Flight Accounting ---

  onJobStarted(workId: string): void {
    if (!workId) return;
    const current = this.inFlightByWork.get(workId) || 0;
    this.inFlightByWork.set(workId, current + 1);
  }

  onJobFinished(workId: string): void {
    if (!workId) return;
    const current = this.inFlightByWork.get(workId) || 1;
    if (current <= 1) {
      this.inFlightByWork.delete(workId);
    } else {
      this.inFlightByWork.set(workId, current - 1);
    }
  }

  getInFlightCount(workId: string): number {
    return this.inFlightByWork.get(workId) || 0;
  }

  // --- Watermarks ---

  async getWatermark(workId: string, source: string) {
    return this.stateStore.getWatermark(workId, source);
  }

  async setWatermark(watermark: any) {
    return this.stateStore.setWatermark(watermark);
  }

  // --- Explainability & Logging ---

  private logDecision(d: SchedulerDecision): void {
    this.logger.info(
      `[SCHEDULER_DECISION] SELECTED: ${d.workTitle} #${d.chapterNumber} | LANE: ${d.lane} | REASON: ${d.reason} | WORK STATE: ${d.workState} | SOURCE: ${d.source} | WAIT TIME: ${d.waitTimeMs}ms`
    );
  }

  // --- Metrics & Telemetry ---

  private startMetricsReporter(): void {
    setInterval(async () => {
      try {
        const metrics = await this.collectMetrics();
        await this.stateStore.saveMetrics(metrics);
      } catch {}
    }, 15000);
  }

  async collectMetrics(): Promise<SchedulerMetrics> {
    const activeWorks = this.stateStore.getActiveWorks();
    const p1Works = activeWorks.filter((w) => w.lane === 'P1');
    const p2Works = activeWorks.filter((w) => w.lane === 'P2');

    let p0Queued = 0;
    let p1Queued = 0;
    let p2Queued = 0;
    let p3Waiting = 0;
    let stagedWaitingForGap = 0;

    const client = await this.pool.connect();
    try {
      const qRes = await client.query(`
        SELECT 
          COUNT(CASE WHEN priority >= 100 THEN 1 END) as p0_cnt,
          COUNT(CASE WHEN priority >= 70 AND priority < 100 THEN 1 END) as p1_cnt,
          COUNT(CASE WHEN priority >= 30 AND priority < 70 THEN 1 END) as p2_cnt,
          COUNT(CASE WHEN priority < 30 OR task_type IN ('DISCOVER_WORKS', 'SYNC_WORK') THEN 1 END) as p3_cnt
        FROM importer_queue
        WHERE status = 'QUEUED' AND task_type = 'IMPORT_CHAPTER';
      `);

      const stagedRes = await client.query(`
        SELECT COUNT(*) as staged_cnt FROM importer_chapter_mappings WHERE status = 'STAGED';
      `);

      const qRow = qRes.rows[0];
      p0Queued = parseInt(qRow?.p0_cnt || '0', 10);
      p1Queued = parseInt(qRow?.p1_cnt || '0', 10);
      p2Queued = parseInt(qRow?.p2_cnt || '0', 10);
      p3Waiting = parseInt(qRow?.p3_cnt || '0', 10);
      stagedWaitingForGap = parseInt(stagedRes.rows[0]?.staged_cnt || '0', 10);
    } finally {
      client.release();
    }

    const avgWait = this.p0WaitTimes.length > 0
      ? this.p0WaitTimes.reduce((a, b) => a + b, 0) / this.p0WaitTimes.length
      : 0;

    const sortedWait = [...this.p0WaitTimes].sort((a, b) => a - b);
    const p95Wait = sortedWait.length > 0
      ? sortedWait[Math.floor(sortedWait.length * 0.95)]
      : 0;

    const criticalGapsCount = activeWorks.filter((w) => w.criticalGapSortKey !== null).length;

    return {
      p0Queued,
      p1Queued,
      p2Queued,
      p3Waiting,
      activeNewWorksCount: p2Works.length,
      activeBackfillWorksCount: p1Works.length,
      fillingWorksCount: activeWorks.filter((w) => w.state === 'FILLING').length,
      caughtUpWorksCount: activeWorks.filter((w) => w.state === 'CAUGHT_UP').length,
      blockedWorksCount: activeWorks.filter((w) => w.state === 'BLOCKED').length,
      criticalGapsCount,
      stagedWaitingForGapCount: stagedWaitingForGap,
      p0Completed1h: this.p0Count1h,
      p1Completed1h: this.p1Count1h,
      p2Completed1h: this.p2Count1h,
      p0AvgWaitMs: Math.round(avgWait * 10) / 10,
      p0P95WaitMs: Math.round(p95Wait * 10) / 10,
      lastUpdated: new Date().toISOString(),
    };
  }
}
