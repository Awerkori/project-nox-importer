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
import { SchedulerLane, } from './types.js';
export class WorkAffinityScheduler {
    stateStore;
    admissionController;
    protectiveSentinel;
    logger = new Logger('WorkAffinityScheduler');
    pool = getYugabytePool();
    inFlightByWork = new Map();
    inFlightChapterKeys = new Set();
    p0ConsecutiveClaims = 0;
    rrIndexP1 = 0;
    rrIndexP2 = 0;
    // Performance telemetry
    p0WaitTimes = [];
    p0Count1h = 0;
    p1Count1h = 0;
    p2Count1h = 0;
    // Production Heartbeat & Publication Watchdog (Sections 16, 17, 18, 24, 25, 29)
    lastClaimTime = Date.now();
    lastCompletionTime = Date.now();
    lastAnyPublicationTime = Date.now();
    lastFreshReleaseTime = Date.now();
    lastBackfillPublicationTime = Date.now();
    watchdogRunning = false;
    constructor(stateStore, admissionController, protectiveSentinel) {
        this.stateStore = stateStore;
        this.admissionController = admissionController;
        this.protectiveSentinel = protectiveSentinel;
    }
    /**
     * Initializes state and synchronizes in-flight counts from DB.
     */
    async initialize() {
        await this.stateStore.initialize();
        await this.syncInFlightCountsFromDb();
        await this.hydrateHeartbeatFromDb();
        this.admissionController.start();
        this.startMetricsReporter();
        this.startPublicationWatchdog();
        this.logger.info('WorkAffinityScheduler initialized and running');
    }
    recordPublication(isFreshRelease) {
        const now = Date.now();
        this.lastAnyPublicationTime = now;
        if (isFreshRelease) {
            this.lastFreshReleaseTime = now;
        }
        else {
            this.lastBackfillPublicationTime = now;
        }
    }
    recordJobCompletion() {
        this.lastCompletionTime = Date.now();
    }
    /**
     * Hydrates publication and activity heartbeats from database on boot.
     * Prevents watchdog blindness across process restarts.
     */
    async hydrateHeartbeatFromDb() {
        try {
            const client = await this.pool.connect();
            try {
                const res = await client.query(`
          SELECT 
            (SELECT MAX(published_at) FROM chapters WHERE published_at IS NOT NULL) as last_publication,
            (SELECT MAX(updated_at) FROM importer_queue WHERE status = 'COMPLETED') as last_completion,
            (SELECT MAX(locked_at) FROM importer_queue WHERE status = 'IMPORTING') as last_claim;
        `);
                const r = res.rows[0];
                if (r?.last_publication) {
                    this.lastAnyPublicationTime = new Date(r.last_publication).getTime();
                    this.logger.info(`Hydrated last publication heartbeat: ${new Date(this.lastAnyPublicationTime).toISOString()}`);
                }
                if (r?.last_completion) {
                    this.lastCompletionTime = new Date(r.last_completion).getTime();
                    this.logger.info(`Hydrated last completion heartbeat: ${new Date(this.lastCompletionTime).toISOString()}`);
                }
                if (r?.last_claim) {
                    this.lastClaimTime = new Date(r.last_claim).getTime();
                    this.logger.info(`Hydrated last claim heartbeat: ${new Date(this.lastClaimTime).toISOString()}`);
                }
            }
            finally {
                client.release();
            }
        }
        catch (err) {
            this.logger.warn('Failed to hydrate heartbeats from DB', { error: err?.message });
        }
    }
    /**
     * Authoritative calculation of total workers currently executing chapter jobs.
     * Counts SUM of all in-flight jobs across all works, NOT merely Map keys count.
     */
    getTotalInFlight() {
        let total = 0;
        for (const cnt of this.inFlightByWork.values()) {
            total += cnt;
        }
        return total;
    }
    /**
     * Returns list of work IDs that have reached or exceeded MAX_INFLIGHT_PER_WORK.
     * Used to strictly prevent exceeding 2 concurrent jobs per work across all paths.
     */
    getFullInFlightWorkIds(maxInFlight = 2) {
        const full = [];
        for (const [wId, cnt] of this.inFlightByWork.entries()) {
            if (cnt >= maxInFlight) {
                full.push(wId);
            }
        }
        return full;
    }
    /**
     * Synchronizes in-flight job counts per work from DB at startup.
     */
    async syncInFlightCountsFromDb() {
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
                const resCh = await client.query(`
          SELECT (payload->>'workId') as work_id, chapter_sort_key
          FROM importer_queue
          WHERE status = 'IMPORTING' AND task_type = 'IMPORT_CHAPTER' AND (payload->>'workId') IS NOT NULL AND chapter_sort_key IS NOT NULL;
        `);
                this.inFlightChapterKeys.clear();
                for (const r of resCh.rows) {
                    if (r.work_id && r.chapter_sort_key) {
                        this.inFlightChapterKeys.add(`${r.work_id}:${r.chapter_sort_key}`);
                    }
                }
                this.logger.info('Synchronized in-flight counts and chapter keys from DB', {
                    activeWorksWithInFlight: this.inFlightByWork.size,
                    inFlightChapters: this.inFlightChapterKeys.size,
                    totalInFlightWorkers: this.getTotalInFlight(),
                });
            }
            finally {
                client.release();
            }
        }
        catch (err) {
            this.logger.warn('Failed to sync in-flight counts from DB', { error: err?.message });
        }
    }
    /**
     * Main entry point for worker slots claiming IMPORT_CHAPTER jobs.
     */
    async acquireNextChapterJob(options) {
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
    async executeIntelligentClaim(options, t0) {
        const config = this.stateStore.getConfig();
        const leaseMin = Math.max(1, Math.min(60, options.leaseDurationMinutes || 5));
        const allowedSources = options.allowedSources && options.allowedSources.length > 0 ? options.allowedSources : null;
        const client = await this.pool.connect();
        try {
            // -------------------------------------------------------------
            // LANE P0: Fresh New Releases (Priority >= 100) - ABSOLUTE PRIORITY
            // Next free slot ALWAYS goes to P0 if claimable. Never skipped.
            // -------------------------------------------------------------
            const fullWorkIds = this.getFullInFlightWorkIds(config.maxInflightPerWork);
            const p0Job = await this.claimSingleJob(client, {
                workerId: options.workerId,
                leaseMin,
                allowedSources,
                minPriority: 100,
                disallowedWorkIds: fullWorkIds,
            });
            if (p0Job) {
                const waitTimeMs = performance.now() - t0;
                this.p0Count1h++;
                this.p0WaitTimes.push(waitTimeMs);
                if (this.p0WaitTimes.length > 100)
                    this.p0WaitTimes.shift();
                const workId = p0Job.payload?.workId || '';
                this.onJobStarted(workId, p0Job.chapter_sort_key);
                this.lastClaimTime = Date.now();
                const decision = {
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
            // -------------------------------------------------------------
            // LANE P1: Critical Gap (Priority >= 90, unblocks STAGED barrier)
            // -------------------------------------------------------------
            const activeWorks = this.stateStore.getActiveWorks();
            const p1Works = activeWorks.filter((w) => w.lane === 'P1');
            const p2Works = activeWorks.filter((w) => w.lane === 'P2');
            // Check critical gaps first
            const criticalWorks = p1Works.filter((w) => w.criticalGapSortKey !== null && (this.inFlightByWork.get(w.workId) || 0) < config.maxInflightPerWork);
            for (const cw of criticalWorks) {
                const gapJob = await this.claimSingleJob(client, {
                    workerId: options.workerId,
                    leaseMin,
                    allowedSources,
                    workId: cw.workId,
                    sortKey: cw.criticalGapSortKey,
                    disallowedWorkIds: fullWorkIds,
                });
                if (gapJob) {
                    const waitTimeMs = performance.now() - t0;
                    this.onJobStarted(cw.workId, gapJob.chapter_sort_key);
                    this.p1Count1h++;
                    const decision = {
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
            const eligibleP1Works = p1Works.filter((w) => (this.inFlightByWork.get(w.workId) || 0) < config.maxInflightPerWork);
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
                        disallowedWorkIds: fullWorkIds,
                    });
                    if (p1Job) {
                        this.rrIndexP1 = idx + 1;
                        const waitTimeMs = performance.now() - t0;
                        this.onJobStarted(targetWork.workId, p1Job.chapter_sort_key);
                        this.p1Count1h++;
                        const decision = {
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
            const eligibleP2Works = p2Works.filter((w) => (this.inFlightByWork.get(w.workId) || 0) < config.maxInflightPerWork);
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
                        disallowedWorkIds: fullWorkIds,
                    });
                    if (p2Job) {
                        this.rrIndexP2 = idx + 1;
                        const waitTimeMs = performance.now() - t0;
                        this.onJobStarted(targetWork.workId, p2Job.chapter_sort_key);
                        this.p2Count1h++;
                        const decision = {
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
            // WORK-CONSERVING SPARE CAPACITY & ON-DEMAND ADMISSION:
            // If active works have no available jobs and workers are idle (totalInFlight < 18),
            // we do NOT claim random unadmitted works!
            // Instead, we admit a healthy waiting work via AdmissionController,
            // which sets up sliding window and work affinity, and then claim its job.
            // UNADMITTED WORK CLAIMS = 0!
            // -------------------------------------------------------------
            const activeWorkIds = activeWorks
                .filter((w) => w.state === 'FILLING' && (this.inFlightByWork.get(w.workId) || 0) < config.maxInflightPerWork)
                .map((w) => w.workId);
            let fallbackJob = null;
            if (activeWorkIds.length > 0) {
                fallbackJob = await this.claimSingleJob(client, {
                    workerId: options.workerId,
                    leaseMin,
                    allowedSources,
                    allowedWorkIds: activeWorkIds,
                    disallowedWorkIds: fullWorkIds,
                });
            }
            // If active works have no jobs, check real total worker occupancy
            const totalInFlight = this.getTotalInFlight();
            if (!fallbackJob && totalInFlight < 18) {
                const newlyAdmitted = await this.admissionController.admitNextWorkOnDemand(undefined, allowedSources || undefined);
                if (newlyAdmitted) {
                    fallbackJob = await this.claimSingleJob(client, {
                        workerId: options.workerId,
                        leaseMin,
                        allowedSources,
                        workId: newlyAdmitted.workId,
                        disallowedWorkIds: this.getFullInFlightWorkIds(config.maxInflightPerWork),
                    });
                }
            }
            if (fallbackJob) {
                const waitTimeMs = performance.now() - t0;
                const workId = fallbackJob.payload?.workId || '';
                this.onJobStarted(workId, fallbackJob.chapter_sort_key);
                this.lastClaimTime = Date.now();
                const decision = {
                    jobId: fallbackJob.id,
                    workId,
                    workTitle: fallbackJob.payload?.chapterTitle || 'Admitted Work Job',
                    chapterNumber: fallbackJob.payload?.chapterNumber ?? 0,
                    chapterSortKey: fallbackJob.chapter_sort_key ?? 0,
                    lane: SchedulerLane.FALLBACK,
                    reason: 'WORK_CONSERVING_ADMITTED_DRAIN',
                    workState: 'FILLING',
                    source: fallbackJob.source,
                    waitTimeMs: Math.round(waitTimeMs * 10) / 10,
                    decisionTime: new Date().toISOString(),
                };
                this.logDecision(decision);
                return fallbackJob;
            }
            return null;
        }
        finally {
            client.release();
        }
    }
    /**
     * Helper to atomically claim 1 job with SKIP LOCKED.
     * Ensures the source is enabled, active, and not in cooldown.
     */
    async claimSingleJob(client, opts) {
        const disallowedChapterKeys = Array.from(this.inFlightChapterKeys);
        const query = `
      WITH to_lock AS (
        SELECT q.id
        FROM importer_queue q
        JOIN importer_sources s ON s.id = q.source
        WHERE (
          q.status = 'QUEUED'
          OR (q.status = 'RETRY' AND q.next_run_at <= NOW())
        )
          AND q.task_type = 'IMPORT_CHAPTER'
          AND q.attempts < COALESCE(q.max_attempts, 7)
          AND s.enabled = true
          AND s.status = 'ACTIVE'
          AND (s.cooldown_until IS NULL OR s.cooldown_until <= NOW())
          AND ($1::text[] IS NULL OR q.source = ANY($1::text[]))
          AND ($2::int IS NULL OR q.priority >= $2::int)
          AND ($3::text IS NULL OR (q.payload->>'workId') = $3::text)
          AND ($4::numeric IS NULL OR q.chapter_sort_key = $4::numeric)
          AND ($7::text[] IS NULL OR (q.payload->>'workId') = ANY($7::text[]))
          AND ($8::text[] IS NULL OR NOT ((q.payload->>'workId') = ANY($8::text[])))
          AND ($9::text[] IS NULL OR NOT (((q.payload->>'workId') || ':' || q.chapter_sort_key::text) = ANY($9::text[])))
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
        for (let drainAttempt = 0; drainAttempt < 10; drainAttempt++) {
            const res = await client.query(query, [
                opts.allowedSources,
                opts.minPriority || null,
                opts.workId || null,
                opts.sortKey || null,
                opts.workerId,
                opts.leaseMin,
                opts.allowedWorkIds || null,
                opts.disallowedWorkIds || null,
                disallowedChapterKeys.length > 0 ? disallowedChapterKeys : null,
            ]);
            if (res.rows.length === 0)
                return null;
            const r = res.rows[0];
            const payload = typeof r.payload === 'string' ? JSON.parse(r.payload) : (r.payload || {});
            const sortKey = r.chapter_sort_key ? parseFloat(r.chapter_sort_key) : null;
            const workId = payload?.workId;
            const chapterNumber = payload?.chapterNumber;
            // Pre-flight check: is this chapter already published canonically in chapters table?
            if (workId && (chapterNumber !== undefined || sortKey !== null)) {
                const pubCheck = await client.query(`
          SELECT id FROM chapters 
          WHERE work_id = $1::uuid 
            AND (number = $2::numeric OR ($3::numeric IS NOT NULL AND number = $3::numeric))
            AND published_at IS NOT NULL
          LIMIT 1;
        `, [workId, chapterNumber !== undefined ? chapterNumber : sortKey, sortKey]);
                if (pubCheck.rows.length > 0) {
                    const publishedChapterId = pubCheck.rows[0].id;
                    this.logger.info(`Claimed job ${r.id} for work ${workId} ch ${chapterNumber} is already canonically published. Auto-completing immediately without worker execution.`);
                    await client.query(`
            UPDATE importer_queue 
            SET status = 'COMPLETED', updated_at = NOW(), last_error = 'CANONICAL_ALREADY_SATISFIED'
            WHERE id = $1;
          `, [r.id]);
                    if (sortKey !== null) {
                        await client.query(`
              UPDATE importer_queue
              SET status = 'COMPLETED', updated_at = NOW(), last_error = 'CANONICAL_ALREADY_SATISFIED'
              WHERE (payload->>'workId') = $1
                AND chapter_sort_key = $2
                AND status IN ('QUEUED', 'RETRY')
                AND task_type = 'IMPORT_CHAPTER';
            `, [workId, sortKey]);
                        await client.query(`
              UPDATE importer_chapter_mappings
              SET status = 'COMPLETED', is_page_provider = false, chapter_id = $3, updated_at = NOW()
              WHERE work_id = $1::uuid AND chapter_sort_key = $2 AND status IN ('PENDING', 'QUEUED');
            `, [workId, sortKey, publishedChapterId]);
                    }
                    // Continue loop to claim next genuine job
                    continue;
                }
            }
            this.lastClaimTime = Date.now();
            return {
                ...r,
                payload,
                chapter_sort_key: sortKey,
            };
        }
        return null;
    }
    /**
     * Publication Watchdog & Auto-Recovery Tree (Sections 6, 7, 8, 16).
     * Monitors elapsed time since last publication and real backlog.
     * If any safe backlog exists and no publication occurs for 5m -> WARNING.
     * If no publication occurs for 10m -> Triggers AUTO-RECOVERY routine!
     */
    startPublicationWatchdog() {
        if (this.watchdogRunning)
            return;
        this.watchdogRunning = true;
        setInterval(async () => {
            try {
                const client = await this.pool.connect();
                try {
                    const activeWorkIds = this.stateStore.getActiveWorks().map((w) => w.workId);
                    const statsRes = await client.query(`
            SELECT 
              COUNT(CASE WHEN q.status = 'QUEUED' AND s.enabled = true AND s.status = 'ACTIVE' AND (s.cooldown_until IS NULL OR s.cooldown_until <= NOW()) THEN 1 END) as claimable_now,
              COUNT(CASE WHEN q.status = 'RETRY' AND q.next_run_at <= NOW() AND s.enabled = true AND s.status = 'ACTIVE' AND (s.cooldown_until IS NULL OR s.cooldown_until <= NOW()) THEN 1 END) as retry_due,
              COUNT(CASE WHEN q.status = 'IMPORTING' THEN 1 END) as importing_cnt,
              COUNT(CASE WHEN q.status = 'PAUSED_BY_STAFF' THEN 1 END) as paused_by_staff_cnt,
              COUNT(DISTINCT CASE WHEN q.status IN ('QUEUED', 'RETRY', 'PAUSED_BY_STAFF') AND s.enabled = true AND s.status = 'ACTIVE' AND (s.cooldown_until IS NULL OR s.cooldown_until <= NOW()) THEN (q.payload->>'workId') END) as valid_waiting_works,
              COUNT(CASE WHEN q.status IN ('QUEUED', 'RETRY') AND (q.payload->>'workId') = ANY($1::text[]) THEN 1 END) as active_work_pending,
              COUNT(DISTINCT CASE WHEN q.status IN ('QUEUED', 'RETRY', 'PAUSED_BY_STAFF') AND s.enabled = true AND s.status = 'ACTIVE' AND (s.cooldown_until IS NULL OR s.cooldown_until <= NOW()) AND NOT ((q.payload->>'workId') = ANY($1::text[])) THEN (q.payload->>'workId') END) as admission_candidates
            FROM importer_queue q
            LEFT JOIN importer_sources s ON s.id = q.source
            WHERE q.task_type = 'IMPORT_CHAPTER';
          `, [activeWorkIds.length > 0 ? activeWorkIds : ['00000000-0000-0000-0000-000000000000']]);
                    const stagedRes = await client.query(`SELECT COUNT(*) as staged_cnt FROM importer_chapter_mappings WHERE status = 'STAGED'`);
                    const row = statsRes.rows[0];
                    const claimableNow = parseInt(row?.claimable_now || '0', 10);
                    const retryDue = parseInt(row?.retry_due || '0', 10);
                    const importingCnt = parseInt(row?.importing_cnt || '0', 10);
                    const validWaitingWorks = parseInt(row?.valid_waiting_works || '0', 10);
                    const activeWorkPending = parseInt(row?.active_work_pending || '0', 10);
                    const admissionCandidates = parseInt(row?.admission_candidates || '0', 10);
                    const stagedCnt = parseInt(stagedRes.rows[0]?.staged_cnt || '0', 10);
                    const hasSafeWork = claimableNow > 0 || retryDue > 0 || validWaitingWorks > 0 || activeWorkPending > 0 || admissionCandidates > 0 || stagedCnt > 0;
                    if (hasSafeWork) {
                        const elapsedPubMs = Date.now() - this.lastAnyPublicationTime;
                        if (elapsedPubMs >= 10 * 60 * 1000) {
                            this.logger.error(`🚨 [PUBLICATION_WATCHDOG] Pipeline stalled: 0 publications for ${(elapsedPubMs / 60000).toFixed(1)}m while safe backlog exists. ` +
                                `[claimable=${claimableNow}, retry_due=${retryDue}, importing=${importingCnt}, waiting_works=${validWaitingWorks}, active_pending=${activeWorkPending}, candidates=${admissionCandidates}, staged=${stagedCnt}]. Triggering AUTO-RECOVERY TREE!`);
                            // Auto-Recovery Action 1: Force admission cycle to unblock works and promote sliding window
                            await this.admissionController.runAdmissionCycle();
                            // Auto-Recovery Action 2: Recover stale leases
                            await client.query(`
                UPDATE importer_queue
                SET status = 'RETRY',
                    attempts = attempts + 1,
                    locked_by = NULL,
                    locked_at = NULL,
                    lease_expires_at = NULL,
                    next_run_at = NOW(),
                    updated_at = NOW()
                WHERE status = 'IMPORTING' AND lease_expires_at < NOW();
              `);
                            // Auto-Recovery Action 3: Sentinel auto-resume evaluation
                            if (await this.protectiveSentinel.isProtectiveStopActive()) {
                                await this.protectiveSentinel.evaluateAutoResume();
                            }
                            // Auto-Recovery Action 4: Sync in-flight map
                            await this.syncInFlightCountsFromDb();
                        }
                        else if (elapsedPubMs >= 5 * 60 * 1000) {
                            this.logger.warn(`⚠️ [PUBLICATION_WATCHDOG] Warning: 0 publications for ${(elapsedPubMs / 60000).toFixed(1)}m. ` +
                                `[claimable=${claimableNow}, retry_due=${retryDue}, importing=${importingCnt}, waiting_works=${validWaitingWorks}, active_pending=${activeWorkPending}, candidates=${admissionCandidates}, staged=${stagedCnt}]. Observing...`);
                        }
                    }
                }
                finally {
                    client.release();
                }
            }
            catch (err) {
                this.logger.warn('Error in Publication Watchdog loop', { error: err?.message });
            }
        }, 30000);
    }
    /**
     * Shadow Mode simulation: calculates what the intelligent scheduler would choose,
     * compares with the legacy choice, and returns the legacy job.
     */
    async executeShadowModeSimulation(options, t0) {
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
            }
            finally {
                client.release();
            }
        }
        catch { }
        if (chosenJob?.payload?.workId) {
            this.onJobStarted(chosenJob.payload.workId, chosenJob.chapter_sort_key);
        }
        return chosenJob;
    }
    // --- In-Flight Accounting ---
    onJobStarted(workId, chapterSortKey) {
        if (!workId)
            return;
        const current = this.inFlightByWork.get(workId) || 0;
        this.inFlightByWork.set(workId, current + 1);
        if (chapterSortKey !== undefined && chapterSortKey !== null) {
            this.inFlightChapterKeys.add(`${workId}:${chapterSortKey}`);
        }
    }
    onJobFinished(workId, chapterSortKey) {
        if (!workId)
            return;
        const current = this.inFlightByWork.get(workId) || 1;
        if (current <= 1) {
            this.inFlightByWork.delete(workId);
        }
        else {
            this.inFlightByWork.set(workId, current - 1);
        }
        if (chapterSortKey !== undefined && chapterSortKey !== null) {
            this.inFlightChapterKeys.delete(`${workId}:${chapterSortKey}`);
        }
    }
    getInFlightCount(workId) {
        return this.inFlightByWork.get(workId) || 0;
    }
    // --- Watermarks ---
    async getWatermark(workId, source) {
        return this.stateStore.getWatermark(workId, source);
    }
    async setWatermark(watermark) {
        return this.stateStore.setWatermark(watermark);
    }
    // --- Explainability & Logging ---
    logDecision(d) {
        this.logger.info(`[SCHEDULER_DECISION] SELECTED: ${d.workTitle} #${d.chapterNumber} | LANE: ${d.lane} | REASON: ${d.reason} | WORK STATE: ${d.workState} | SOURCE: ${d.source} | WAIT TIME: ${d.waitTimeMs}ms`);
    }
    // --- Metrics & Telemetry ---
    startMetricsReporter() {
        setInterval(async () => {
            try {
                const metrics = await this.collectMetrics();
                await this.stateStore.saveMetrics(metrics);
            }
            catch { }
        }, 15000);
    }
    async collectMetrics() {
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
        }
        finally {
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
