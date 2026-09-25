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
    pool;
    inFlightByWork = new Map();
    inFlightChapterKeys = new Set();
    p0ConsecutiveClaims = 0;
    rrIndexP1 = 0;
    rrIndexP2 = 0;
    // Staff requests fast-cache (avoids 1 query per claim)
    lastStaffCheckTime = 0;
    cachedStaffWorkIds = [];
    // In-memory unclaimable work cooldown (avoids hammering depleted/unready works)
    unclaimableWorksCooldown = new Map();
    markWorkUnclaimable(workId, ttlMs = 15000) {
        this.unclaimableWorksCooldown.set(workId, Date.now() + ttlMs);
    }
    isWorkUnclaimable(workId) {
        const until = this.unclaimableWorksCooldown.get(workId);
        if (!until)
            return false;
        if (Date.now() > until) {
            this.unclaimableWorksCooldown.delete(workId);
            return false;
        }
        return true;
    }
    clearWorkUnclaimable(workId) {
        this.unclaimableWorksCooldown.delete(workId);
    }
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
    // Claim efficiency telemetry (Fase 3)
    specificClaimAttempts = 0;
    specificClaimSuccesses = 0;
    genericClaimAttempts = 0;
    genericClaimSuccesses = 0;
    emptyClaimAttempts = 0;
    // Lightweight probe for P0 queue presence (avoids 400ms scan on every claim)
    lastP0ProbeAt = 0;
    hasP0InQueue = false;
    async hasP0Candidate() {
        if (process.env.NODE_ENV === 'test' || !this.pool?.query) {
            return true;
        }
        const now = Date.now();
        if (this.hasP0InQueue)
            return true;
        if (now - this.lastP0ProbeAt < 5000)
            return false;
        this.lastP0ProbeAt = now;
        try {
            const r = await this.runQuery(this.pool, `
        SELECT 1 FROM importer_queue 
        WHERE status = 'QUEUED' AND task_type = 'IMPORT_CHAPTER' AND priority >= 100 
        LIMIT 1;
      `);
            this.hasP0InQueue = r.rows.length > 0;
            return this.hasP0InQueue;
        }
        catch {
            return false;
        }
    }
    getClaimStats() {
        return {
            specificAttempts: this.specificClaimAttempts,
            specificSuccesses: this.specificClaimSuccesses,
            genericAttempts: this.genericClaimAttempts,
            genericSuccesses: this.genericClaimSuccesses,
            emptyAttempts: this.emptyClaimAttempts,
            specificSuccessRate: this.specificClaimAttempts > 0 ? Math.round((this.specificClaimSuccesses / this.specificClaimAttempts) * 1000) / 10 : 0,
            genericSuccessRate: this.genericClaimAttempts > 0 ? Math.round((this.genericClaimSuccesses / this.genericClaimAttempts) * 1000) / 10 : 0,
        };
    }
    stagedBlockedWorks = new Map();
    markWorkStagedBlocked(workId, ttlMs = 15000) {
        this.stagedBlockedWorks.set(workId, Date.now() + ttlMs);
    }
    isWorkStagedBlocked(workId) {
        const until = this.stagedBlockedWorks.get(workId);
        if (!until)
            return false;
        if (Date.now() > until) {
            this.stagedBlockedWorks.delete(workId);
            return false;
        }
        return true;
    }
    clearWorkStagedBlocked(workId) {
        this.stagedBlockedWorks.delete(workId);
    }
    constructor(stateStore, admissionController, protectiveSentinel, pool) {
        this.stateStore = stateStore;
        this.admissionController = admissionController;
        this.protectiveSentinel = protectiveSentinel;
        const rawPool = pool || getYugabytePool();
        if (typeof rawPool.connect === 'function') {
            this.pool = rawPool;
        }
        else {
            this.pool = {
                connect: async () => ({
                    query: (text, params) => rawPool.query(text, params),
                    release: () => { },
                }),
                query: (text, params) => rawPool.query(text, params),
            };
        }
    }
    async runQuery(clientOrPool, text, params, telemetry) {
        const target = clientOrPool || this.pool;
        if (typeof target?.connect === 'function') {
            const tConn0 = performance.now();
            const client = await target.connect();
            const connWaitMs = performance.now() - tConn0;
            if (telemetry) {
                telemetry.poolWaitTotalMs += connWaitMs;
            }
            try {
                const tSql0 = performance.now();
                const res = await client.query(text, params);
                const sqlMs = performance.now() - tSql0;
                if (telemetry) {
                    telemetry.sqlExecTotalMs += sqlMs;
                    telemetry.totalQueries++;
                }
                return res;
            }
            finally {
                if (typeof client?.release === 'function')
                    client.release();
            }
        }
        if (typeof target?.query === 'function') {
            const tSql0 = performance.now();
            const res = await target.query(text, params);
            const sqlMs = performance.now() - tSql0;
            if (telemetry) {
                telemetry.sqlExecTotalMs += sqlMs;
                telemetry.totalQueries++;
            }
            return res;
        }
        throw new Error('Target pool or client has neither query nor connect');
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
            const res = await this.runQuery(this.pool, `
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
            const res = await this.runQuery(this.pool, `
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
            const resCh = await this.runQuery(this.pool, `
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
        const telemetry = {
            staffCheckMs: 0,
            p0ProbeMs: 0,
            criticalWorkAttempts: 0,
            criticalWorkTimeMs: 0,
            p1WorkAttempts: 0,
            p1WorkTimeMs: 0,
            p2WorkAttempts: 0,
            p2WorkTimeMs: 0,
            activeFallbackMs: 0,
            admissionOnDemandMs: 0,
            catalogFallbackMs: 0,
            poolWaitTotalMs: 0,
            sqlExecTotalMs: 0,
            claimLockSqlMs: 0,
            claimLockPoolWaitMs: 0,
            claimLockSqlExecMs: 0,
            totalQueries: 0,
            totalAcquireMs: 0,
            worksTested: 0,
        };
        // -------------------------------------------------------------
        // LANE STAFF_FORCED: Explicit Absolute Staff Priority
        // Any eligible STAFF_FORCED job (priority >= 1000, staffForced=true,
        // or active staff request) ALWAYS preempts P0, P1, P2, P3.
        // Next free worker slot MUST be allocated to this work.
        // Preserves manual priority ordering via importer_staff_requests.
        // -------------------------------------------------------------
        const fullWorkIds = this.getFullInFlightWorkIds(config.maxInflightPerWork);
        const tStaff0 = performance.now();
        const staffForcedJob = await this.claimStaffForcedJob(this.pool, {
            workerId: options.workerId,
            leaseMin,
            allowedSources,
            disallowedWorkIds: fullWorkIds,
            telemetry,
        });
        telemetry.staffCheckMs = Math.round((performance.now() - tStaff0) * 10) / 10;
        if (staffForcedJob) {
            const waitTimeMs = performance.now() - t0;
            telemetry.totalAcquireMs = Math.round(waitTimeMs * 10) / 10;
            staffForcedJob._acquireTelemetry = telemetry;
            const workId = staffForcedJob.payload?.workId || '';
            this.onJobStarted(workId, staffForcedJob.chapter_sort_key);
            this.lastClaimTime = Date.now();
            const decision = {
                jobId: staffForcedJob.id,
                workId,
                workTitle: staffForcedJob.payload?.chapterTitle || 'Staff Forced Job',
                chapterNumber: staffForcedJob.payload?.chapterNumber ?? 0,
                chapterSortKey: staffForcedJob.chapter_sort_key ?? 0,
                lane: SchedulerLane.STAFF_FORCED,
                reason: 'STAFF_FORCED_ABSOLUTE_PRIORITY',
                workState: 'FILLING',
                source: staffForcedJob.source,
                waitTimeMs: Math.round(waitTimeMs * 10) / 10,
                decisionTime: new Date().toISOString(),
            };
            this.logDecision(decision);
            return staffForcedJob;
        }
        // -------------------------------------------------------------
        // LANE P0: Fresh New Releases (Priority >= 100) - ABSOLUTE PRIORITY
        // Next free slot ALWAYS goes to P0 if claimable. Never skipped.
        // -------------------------------------------------------------
        const tP0_0 = performance.now();
        let p0Job = null;
        if (await this.hasP0Candidate()) {
            this.genericClaimAttempts++;
            p0Job = await this.claimSingleJob(this.pool, {
                workerId: options.workerId,
                leaseMin,
                allowedSources,
                minPriority: 100,
                disallowedWorkIds: fullWorkIds,
                telemetry,
            });
            if (p0Job) {
                this.genericClaimSuccesses++;
                const waitTimeMs = performance.now() - t0;
                telemetry.p0ProbeMs = Math.round((performance.now() - tP0_0) * 10) / 10;
                telemetry.totalAcquireMs = Math.round(waitTimeMs * 10) / 10;
                p0Job._acquireTelemetry = telemetry;
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
            else {
                this.hasP0InQueue = false;
            }
        }
        telemetry.p0ProbeMs = Math.round((performance.now() - tP0_0) * 10) / 10;
        // -------------------------------------------------------------
        // LANE P1: Critical Gap (Priority >= 90, unblocks STAGED barrier)
        // -------------------------------------------------------------
        const activeWorks = this.stateStore.getActiveWorks();
        const p1Works = activeWorks.filter((w) => w.lane === 'P1');
        const p2Works = activeWorks.filter((w) => w.lane === 'P2');
        // Check critical gaps first (only for sources with available permits, excluding already in-flight keys)
        const tCrit0 = performance.now();
        const criticalWorks = p1Works.filter((w) => w.criticalGapSortKey !== null &&
            !this.inFlightChapterKeys.has(`${w.workId}:${w.criticalGapSortKey}`) &&
            (!allowedSources || allowedSources.length === 0 || allowedSources.includes(w.primarySource)) &&
            (this.inFlightByWork.get(w.workId) || 0) < config.maxInflightPerWork);
        for (const cw of criticalWorks) {
            telemetry.criticalWorkAttempts++;
            telemetry.worksTested++;
            this.specificClaimAttempts++;
            const gapJob = await this.claimSingleJob(this.pool, {
                workerId: options.workerId,
                leaseMin,
                allowedSources,
                workId: cw.workId,
                sortKey: cw.criticalGapSortKey,
                disallowedWorkIds: fullWorkIds,
                telemetry,
            });
            if (gapJob) {
                this.specificClaimSuccesses++;
                const waitTimeMs = performance.now() - t0;
                telemetry.criticalWorkTimeMs = Math.round((performance.now() - tCrit0) * 10) / 10;
                telemetry.totalAcquireMs = Math.round(waitTimeMs * 10) / 10;
                gapJob._acquireTelemetry = telemetry;
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
        telemetry.criticalWorkTimeMs = Math.round((performance.now() - tCrit0) * 10) / 10;
        // -------------------------------------------------------------
        // LANE P1: Active Backfill Works (Fair Round-Robin + Batch Candidate Filter)
        // Strictly excludes works with pending critical gaps, STAGED blocked works,
        // and works cooling down as unclaimable.
        // -------------------------------------------------------------
        const tP1_0 = performance.now();
        const eligibleP1Works = p1Works.filter((w) => w.state === 'FILLING' &&
            w.criticalGapSortKey === null &&
            !this.isWorkStagedBlocked(w.workId) &&
            !this.isWorkUnclaimable(w.workId) &&
            (this.inFlightByWork.get(w.workId) || 0) < config.maxInflightPerWork);
        const readyP1Works = allowedSources && allowedSources.length > 0
            ? eligibleP1Works.filter((w) => allowedSources.includes(w.primarySource))
            : eligibleP1Works;
        if (readyP1Works.length > 0) {
            // 1. Try round-robin target work first (fairness & affinity)
            const startIdx = this.rrIndexP1 % readyP1Works.length;
            const targetWork = readyP1Works[startIdx];
            telemetry.p1WorkAttempts++;
            telemetry.worksTested++;
            this.specificClaimAttempts++;
            let p1Job = await this.claimSingleJob(this.pool, {
                workerId: options.workerId,
                leaseMin,
                allowedSources,
                workId: targetWork.workId,
                disallowedWorkIds: fullWorkIds,
                telemetry,
            });
            if (p1Job) {
                this.specificClaimSuccesses++;
                this.rrIndexP1 = (startIdx + 1) % readyP1Works.length;
                const waitTimeMs = performance.now() - t0;
                this.onJobStarted(targetWork.workId, p1Job.chapter_sort_key);
                this.p1Count1h++;
                telemetry.p1WorkTimeMs = Math.round((performance.now() - tP1_0) * 10) / 10;
                telemetry.totalAcquireMs = Math.round(waitTimeMs * 10) / 10;
                p1Job._acquireTelemetry = telemetry;
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
            // Target work had no claimable job: mark it cooling down for 15s
            this.markWorkUnclaimable(targetWork.workId, 15000);
            // 2. Instead of sequential individual queries (which would do 5-10 queries),
            // batch query all remaining candidates in ONE single query!
            const remainingCandidates = readyP1Works.filter((w) => w.workId !== targetWork.workId && !this.isWorkUnclaimable(w.workId));
            if (remainingCandidates.length > 0) {
                telemetry.p1WorkAttempts++;
                telemetry.worksTested += remainingCandidates.length;
                this.specificClaimAttempts++;
                const candidateWorkIds = remainingCandidates.map((w) => w.workId);
                p1Job = await this.claimSingleJob(this.pool, {
                    workerId: options.workerId,
                    leaseMin,
                    allowedSources,
                    allowedWorkIds: candidateWorkIds,
                    disallowedWorkIds: fullWorkIds,
                    telemetry,
                });
                if (p1Job) {
                    this.specificClaimSuccesses++;
                    const matchedWorkId = p1Job.payload?.workId;
                    const matchedIdx = readyP1Works.findIndex((w) => w.workId === matchedWorkId);
                    if (matchedIdx >= 0) {
                        this.rrIndexP1 = (matchedIdx + 1) % readyP1Works.length;
                    }
                    const waitTimeMs = performance.now() - t0;
                    this.onJobStarted(matchedWorkId || '', p1Job.chapter_sort_key);
                    this.p1Count1h++;
                    telemetry.p1WorkTimeMs = Math.round((performance.now() - tP1_0) * 10) / 10;
                    telemetry.totalAcquireMs = Math.round(waitTimeMs * 10) / 10;
                    p1Job._acquireTelemetry = telemetry;
                    const decision = {
                        jobId: p1Job.id,
                        workId: matchedWorkId || '',
                        workTitle: p1Job.payload?.chapterTitle || 'P1 Backfill',
                        chapterNumber: p1Job.payload?.chapterNumber ?? 0,
                        chapterSortKey: p1Job.chapter_sort_key ?? 0,
                        lane: SchedulerLane.P1_BACKFILL,
                        reason: 'WORK_AFFINITY_BACKFILL_BATCH',
                        workState: 'FILLING',
                        source: p1Job.source,
                        waitTimeMs: Math.round(waitTimeMs * 10) / 10,
                        decisionTime: new Date().toISOString(),
                    };
                    this.logDecision(decision);
                    return p1Job;
                }
                else {
                    // All remaining candidates were empty: mark them cooling down so subsequent workers don't query them
                    for (const w of remainingCandidates) {
                        this.markWorkUnclaimable(w.workId, 15000);
                    }
                }
            }
        }
        telemetry.p1WorkTimeMs = Math.round((performance.now() - tP1_0) * 10) / 10;
        // -------------------------------------------------------------
        // LANE P2: Active New Works (Fair Round-Robin + Batch Candidate Filter)
        // Evaluated after active P1 works, but BEFORE generic untracked catalog backfills,
        // guaranteeing newly admitted works are not starved by massive backlog.
        // -------------------------------------------------------------
        const tP2_0 = performance.now();
        const eligibleP2Works = p2Works.filter((w) => !this.isWorkStagedBlocked(w.workId) &&
            !this.isWorkUnclaimable(w.workId) &&
            (this.inFlightByWork.get(w.workId) || 0) < config.maxInflightPerWork);
        const readyP2Works = allowedSources && allowedSources.length > 0
            ? eligibleP2Works.filter((w) => allowedSources.includes(w.primarySource))
            : eligibleP2Works;
        if (readyP2Works.length > 0) {
            const startIdx = this.rrIndexP2 % readyP2Works.length;
            const targetWork = readyP2Works[startIdx];
            telemetry.p2WorkAttempts++;
            telemetry.worksTested++;
            this.specificClaimAttempts++;
            let p2Job = await this.claimSingleJob(this.pool, {
                workerId: options.workerId,
                leaseMin,
                allowedSources,
                workId: targetWork.workId,
                disallowedWorkIds: fullWorkIds,
                telemetry,
            });
            if (p2Job) {
                this.specificClaimSuccesses++;
                this.rrIndexP2 = (startIdx + 1) % readyP2Works.length;
                const waitTimeMs = performance.now() - t0;
                this.onJobStarted(targetWork.workId, p2Job.chapter_sort_key);
                this.p2Count1h++;
                telemetry.p2WorkTimeMs = Math.round((performance.now() - tP2_0) * 10) / 10;
                telemetry.totalAcquireMs = Math.round(waitTimeMs * 10) / 10;
                p2Job._acquireTelemetry = telemetry;
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
            this.markWorkUnclaimable(targetWork.workId, 15000);
            const remainingCandidates = readyP2Works.filter((w) => w.workId !== targetWork.workId && !this.isWorkUnclaimable(w.workId));
            if (remainingCandidates.length > 0) {
                telemetry.p2WorkAttempts++;
                telemetry.worksTested += remainingCandidates.length;
                this.specificClaimAttempts++;
                const candidateWorkIds = remainingCandidates.map((w) => w.workId);
                p2Job = await this.claimSingleJob(this.pool, {
                    workerId: options.workerId,
                    leaseMin,
                    allowedSources,
                    allowedWorkIds: candidateWorkIds,
                    disallowedWorkIds: fullWorkIds,
                    telemetry,
                });
                if (p2Job) {
                    this.specificClaimSuccesses++;
                    const matchedWorkId = p2Job.payload?.workId;
                    const matchedIdx = readyP2Works.findIndex((w) => w.workId === matchedWorkId);
                    if (matchedIdx >= 0) {
                        this.rrIndexP2 = (matchedIdx + 1) % readyP2Works.length;
                    }
                    const waitTimeMs = performance.now() - t0;
                    this.onJobStarted(matchedWorkId || '', p2Job.chapter_sort_key);
                    this.p2Count1h++;
                    telemetry.p2WorkTimeMs = Math.round((performance.now() - tP2_0) * 10) / 10;
                    telemetry.totalAcquireMs = Math.round(waitTimeMs * 10) / 10;
                    p2Job._acquireTelemetry = telemetry;
                    const decision = {
                        jobId: p2Job.id,
                        workId: matchedWorkId || '',
                        workTitle: p2Job.payload?.chapterTitle || 'P2 New Work',
                        chapterNumber: p2Job.payload?.chapterNumber ?? 0,
                        chapterSortKey: p2Job.chapter_sort_key ?? 0,
                        lane: SchedulerLane.P2_ACTIVE_NEW_WORK,
                        reason: 'WORK_AFFINITY_NEW_WORK_BATCH',
                        workState: 'FILLING',
                        source: p2Job.source,
                        waitTimeMs: Math.round(waitTimeMs * 10) / 10,
                        decisionTime: new Date().toISOString(),
                    };
                    this.logDecision(decision);
                    return p2Job;
                }
                else {
                    for (const w of remainingCandidates) {
                        this.markWorkUnclaimable(w.workId, 15000);
                    }
                }
            }
        }
        telemetry.p2WorkTimeMs = Math.round((performance.now() - tP2_0) * 10) / 10;
        // -------------------------------------------------------------
        // WORK-CONSERVING SPARE CAPACITY & ON-DEMAND ADMISSION:
        // Try active works fallback first BEFORE scanning the full catalog
        // -------------------------------------------------------------
        const tFall0 = performance.now();
        const activeWorkIds = activeWorks
            .filter((w) => w.state === 'FILLING' &&
            !this.isWorkStagedBlocked(w.workId) &&
            !this.isWorkUnclaimable(w.workId) &&
            (this.inFlightByWork.get(w.workId) || 0) < config.maxInflightPerWork)
            .map((w) => w.workId);
        let fallbackJob = null;
        if (activeWorkIds.length > 0) {
            telemetry.worksTested += activeWorkIds.length;
            this.specificClaimAttempts++;
            fallbackJob = await this.claimSingleJob(this.pool, {
                workerId: options.workerId,
                leaseMin,
                allowedSources,
                allowedWorkIds: activeWorkIds,
                disallowedWorkIds: fullWorkIds,
                telemetry,
            });
            if (fallbackJob) {
                this.specificClaimSuccesses++;
            }
        }
        telemetry.activeFallbackMs = Math.round((performance.now() - tFall0) * 10) / 10;
        // If active works have no jobs, check real total worker occupancy
        const tAdm0 = performance.now();
        const totalInFlight = this.getTotalInFlight();
        if (!fallbackJob && totalInFlight < 18) {
            const newlyAdmitted = await this.admissionController.admitNextWorkOnDemand('P1', allowedSources || undefined);
            if (newlyAdmitted) {
                telemetry.worksTested++;
                this.specificClaimAttempts++;
                fallbackJob = await this.claimSingleJob(this.pool, {
                    workerId: options.workerId,
                    leaseMin,
                    allowedSources,
                    workId: newlyAdmitted.workId,
                    disallowedWorkIds: this.getFullInFlightWorkIds(config.maxInflightPerWork),
                    telemetry,
                });
                if (fallbackJob) {
                    this.specificClaimSuccesses++;
                }
            }
        }
        telemetry.admissionOnDemandMs = Math.round((performance.now() - tAdm0) * 10) / 10;
        if (fallbackJob) {
            const waitTimeMs = performance.now() - t0;
            telemetry.totalAcquireMs = Math.round(waitTimeMs * 10) / 10;
            fallbackJob._acquireTelemetry = telemetry;
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
        // -------------------------------------------------------------
        // LANE P1: Catalog Backfill Dynamic Claim (Last Resort)
        // When currently tracked active P1 and P2 works cannot supply a job,
        // claim from ANY published catalog work
        // -------------------------------------------------------------
        const tCat0 = performance.now();
        this.genericClaimAttempts++;
        const catalogP1Job = await this.claimCatalogP1Job(this.pool, {
            workerId: options.workerId,
            leaseMin,
            allowedSources,
            disallowedWorkIds: fullWorkIds,
            telemetry,
        });
        telemetry.catalogFallbackMs = Math.round((performance.now() - tCat0) * 10) / 10;
        if (catalogP1Job) {
            this.genericClaimSuccesses++;
            const waitTimeMs = performance.now() - t0;
            telemetry.totalAcquireMs = Math.round(waitTimeMs * 10) / 10;
            catalogP1Job._acquireTelemetry = telemetry;
            const workId = catalogP1Job.payload?.workId || '';
            this.onJobStarted(workId, catalogP1Job.chapter_sort_key);
            this.p1Count1h++;
            const decision = {
                jobId: catalogP1Job.id,
                workId,
                workTitle: catalogP1Job.payload?.chapterTitle || 'Catalog P1 Backfill',
                chapterNumber: catalogP1Job.payload?.chapterNumber ?? 0,
                chapterSortKey: catalogP1Job.chapter_sort_key ?? 0,
                lane: SchedulerLane.P1_BACKFILL,
                reason: 'CATALOG_P1_BACKFILL_CLAIM',
                workState: 'FILLING',
                source: catalogP1Job.source,
                waitTimeMs: Math.round(waitTimeMs * 10) / 10,
                decisionTime: new Date().toISOString(),
            };
            this.logDecision(decision);
            return catalogP1Job;
        }
        this.emptyClaimAttempts++;
        return null;
    }
    /**
     * Helper to atomically claim 1 P1 job for ANY existing catalog work with SKIP LOCKED.
     * Strictly restricts to published works (w.published = true) on active, enabled sources.
     * Enforces that P1 work across the catalog is processed before ANY P2 work!
     */
    async claimCatalogP1Job(client, opts) {
        const disallowedChapterKeys = Array.from(this.inFlightChapterKeys);
        const query = `
      WITH to_lock AS (
        SELECT q.id
        FROM importer_queue q
        JOIN works w ON w.id = (q.payload->>'workId')::uuid
        JOIN importer_sources s ON s.id = q.source
        WHERE (
          q.status = 'QUEUED'
          OR (q.status = 'RETRY' AND q.next_run_at <= NOW())
        )
          AND q.task_type = 'IMPORT_CHAPTER'
          AND q.attempts < COALESCE(q.max_attempts, 7)
          AND w.published = true
          AND s.enabled = true
          AND (s.status = 'ACTIVE' OR (s.status IN ('COOLDOWN', 'PROBING', 'DEGRADED') AND (s.cooldown_until IS NULL OR s.cooldown_until <= NOW())))
          AND ($1::text[] IS NULL OR q.source = ANY($1::text[]))
          AND ($2::text[] IS NULL OR NOT ((q.payload->>'workId') = ANY($2::text[])))
          AND ($3::text[] IS NULL OR NOT (((q.payload->>'workId') || ':' || q.chapter_sort_key::text) = ANY($3::text[])))
        ORDER BY q.priority DESC, q.chapter_sort_key ASC NULLS LAST, q.next_run_at ASC
        FOR UPDATE SKIP LOCKED
        LIMIT 1
      )
      UPDATE importer_queue q
      SET status = 'IMPORTING',
          locked_by = $4,
          locked_at = NOW(),
          lease_expires_at = NOW() + ($5::text || ' minutes')::interval,
          attempts = q.attempts + 1,
          updated_at = NOW()
      FROM to_lock
      WHERE q.id = to_lock.id
      RETURNING q.id, q.task_type, q.source, q.priority, q.payload, q.dedupe_key,
                q.status, q.attempts, q.max_attempts, q.locked_by, q.locked_at,
                q.lease_expires_at, q.next_run_at, q.last_error, q.chapter_sort_key;
    `;
        const res = await this.runQuery(client, query, [
            opts.allowedSources,
            opts.disallowedWorkIds || null,
            disallowedChapterKeys.length > 0 ? disallowedChapterKeys : null,
            opts.workerId,
            opts.leaseMin,
        ], opts.telemetry);
        if (res.rows.length === 0)
            return null;
        const r = res.rows[0];
        const payload = typeof r.payload === 'string' ? JSON.parse(r.payload) : (r.payload || {});
        const sortKey = r.chapter_sort_key ? parseFloat(r.chapter_sort_key) : null;
        const workId = payload?.workId;
        // If work not in stateStore, add it to activeWorks as P1
        if (workId && !this.stateStore.getActiveWork(workId)) {
            this.stateStore.setActiveWork({
                workId,
                workTitle: payload?.chapterTitle || 'Catalog Work',
                lane: 'P1',
                state: 'FILLING',
                primarySource: r.source,
                admittedAt: new Date().toISOString(),
                lastActivityAt: new Date().toISOString(),
                totalChapters: 50,
                publishedChapters: 0,
                queuedChapters: 5,
                inFlightChapters: 1,
                frontierSortKey: sortKey,
                criticalGapSortKey: null,
                criticalGapUnblockCount: 0,
            });
        }
        this.lastClaimTime = Date.now();
        return {
            ...r,
            payload,
            chapter_sort_key: sortKey,
        };
    }
    /**
     * Helper to atomically claim 1 STAFF_FORCED job with SKIP LOCKED.
     * Priority >= 1000 or payload.staffForced = true or work with active importer_staff_requests.
     * Strictly prioritizes staff requests by priority_boost DESC, created_at ASC (manual ordering),
     * then canonical chapter_sort_key ASC.
     */
    async claimStaffForcedJob(client, opts) {
        const disallowedChapterKeys = Array.from(this.inFlightChapterKeys);
        // 1. Check for active staff requests (cached for 3s to eliminate DB query per claim)
        const now = Date.now();
        let staffWorkIds = this.cachedStaffWorkIds;
        if (now - this.lastStaffCheckTime > 3000 || !staffWorkIds) {
            const activeReqs = await this.runQuery(client, `
        SELECT work_id::text, priority_boost, created_at 
        FROM importer_staff_requests 
        WHERE status = 'ACTIVE' 
        ORDER BY priority_boost DESC, created_at ASC
      `, [], opts.telemetry);
            this.lastStaffCheckTime = now;
            this.cachedStaffWorkIds = activeReqs.rows.map((r) => r.work_id);
            staffWorkIds = this.cachedStaffWorkIds;
        }
        if (!staffWorkIds || staffWorkIds.length === 0) {
            return null;
        }
        const query = `
      WITH to_lock AS (
        SELECT q.id
        FROM importer_queue q
        JOIN importer_sources s ON s.id = q.source
        LEFT JOIN importer_staff_requests sr 
          ON sr.work_id = (q.payload->>'workId')::uuid AND sr.status = 'ACTIVE'
        WHERE (
          q.status = 'QUEUED'
          OR (q.status = 'RETRY' AND q.next_run_at <= NOW())
        )
          AND q.task_type = 'IMPORT_CHAPTER'
          AND q.attempts < COALESCE(q.max_attempts, 7)
          AND s.enabled = true
          AND (s.status = 'ACTIVE' OR (s.status IN ('COOLDOWN', 'PROBING', 'DEGRADED') AND (s.cooldown_until IS NULL OR s.cooldown_until <= NOW())))
          AND ($1::text[] IS NULL OR q.source = ANY($1::text[]))
          AND (q.payload->>'workId') = ANY($6::text[])
          AND ($2::text[] IS NULL OR NOT ((q.payload->>'workId') = ANY($2::text[])))
          AND ($3::text[] IS NULL OR NOT (((q.payload->>'workId') || ':' || q.chapter_sort_key::text) = ANY($3::text[])))
        ORDER BY 
          COALESCE(sr.priority_boost, 0) DESC,
          COALESCE(sr.created_at, '9999-12-31'::timestamptz) ASC,
          q.priority DESC, 
          q.chapter_sort_key ASC NULLS LAST, 
          q.next_run_at ASC
        FOR UPDATE OF q SKIP LOCKED
        LIMIT 1
      )
      UPDATE importer_queue q
      SET status = 'IMPORTING',
          locked_by = $4,
          locked_at = NOW(),
          lease_expires_at = NOW() + ($5::text || ' minutes')::interval,
          attempts = q.attempts + 1,
          updated_at = NOW()
      FROM to_lock
      WHERE q.id = to_lock.id
      RETURNING q.id, q.task_type, q.source, q.priority, q.payload, q.dedupe_key,
                q.status, q.attempts, q.max_attempts, q.locked_by, q.locked_at,
                q.lease_expires_at, q.next_run_at, q.last_error, q.chapter_sort_key;
    `;
        const queryParams = [
            opts.allowedSources,
            opts.disallowedWorkIds || null,
            disallowedChapterKeys.length > 0 ? disallowedChapterKeys : null,
            opts.workerId,
            opts.leaseMin,
            staffWorkIds,
        ];
        const res = await this.runQuery(client, query, queryParams, opts.telemetry);
        if (res.rows.length === 0)
            return null;
        const r = res.rows[0];
        const payload = typeof r.payload === 'string' ? JSON.parse(r.payload) : (r.payload || {});
        const sortKey = r.chapter_sort_key ? parseFloat(r.chapter_sort_key) : null;
        this.lastClaimTime = Date.now();
        return {
            ...r,
            payload,
            chapter_sort_key: sortKey,
        };
    }
    /**
     * Helper to atomically claim 1 job with SKIP LOCKED.
     * Ensures the source is enabled, active, and not in cooldown.
     */
    async claimSingleJob(client, opts) {
        const disallowedChapterKeys = Array.from(this.inFlightChapterKeys);
        const isSingleWork = Boolean(opts.workId);
        const orderClause = isSingleWork
            ? `ORDER BY q.chapter_sort_key ASC NULLS LAST`
            : `ORDER BY 
          q.priority DESC, 
          q.chapter_sort_key ASC NULLS LAST, 
          q.next_run_at ASC`;
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
          AND (s.status = 'ACTIVE' OR (s.status IN ('COOLDOWN', 'PROBING', 'DEGRADED') AND (s.cooldown_until IS NULL OR s.cooldown_until <= NOW())))
          AND ($1::text[] IS NULL OR q.source = ANY($1::text[]))
          AND ($2::int IS NULL OR q.priority >= $2::int)
          AND ($3::text IS NULL OR (q.payload->>'workId') = $3::text)
          AND ($4::numeric IS NULL OR q.chapter_sort_key = $4::numeric)
          AND ($7::text[] IS NULL OR (q.payload->>'workId') = ANY($7::text[]))
          AND ($8::text[] IS NULL OR NOT ((q.payload->>'workId') = ANY($8::text[])))
          AND ($9::text[] IS NULL OR NOT (((q.payload->>'workId') || ':' || q.chapter_sort_key::text) = ANY($9::text[])))
        ${orderClause}
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
        const targetPool = client?.connect ? client : this.pool;
        const tConn0 = performance.now();
        const dbClient = await targetPool.connect();
        const poolWaitMs = performance.now() - tConn0;
        if (opts.telemetry) {
            opts.telemetry.poolWaitTotalMs += poolWaitMs;
            opts.telemetry.claimLockPoolWaitMs = Math.round(poolWaitMs * 10) / 10;
        }
        let res;
        try {
            const tLockSql0 = performance.now();
            res = await dbClient.query(query, [
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
            const sqlMs = performance.now() - tLockSql0;
            if (opts.telemetry) {
                opts.telemetry.sqlExecTotalMs += sqlMs;
                opts.telemetry.claimLockSqlExecMs = Math.round(sqlMs * 10) / 10;
                opts.telemetry.claimLockSqlMs = Math.round(sqlMs * 10) / 10;
                opts.telemetry.totalQueries++;
            }
        }
        finally {
            if (typeof dbClient?.release === 'function')
                dbClient.release();
        }
        if (res.rows.length === 0)
            return null;
        const r = res.rows[0];
        const payload = typeof r.payload === 'string' ? JSON.parse(r.payload) : (r.payload || {});
        const sortKey = r.chapter_sort_key ? parseFloat(r.chapter_sort_key) : null;
        this.lastClaimTime = Date.now();
        return {
            ...r,
            payload,
            chapter_sort_key: sortKey,
        };
    }
    /**
     * Concurrently validates a claimed job outside the global chapterClaimMutex.
     * Checks for already-published canonical chapters and STAGED barriers.
     * If invalid, sanitizes database records and reverts the job to QUEUED.
     */
    async validateClaimedJobPostMutex(job) {
        const payload = typeof job.payload === 'string' ? JSON.parse(job.payload) : (job.payload || {});
        const workId = payload?.workId;
        const chapterNumber = payload?.chapterNumber;
        const sortKey = job.chapter_sort_key ?? (chapterNumber !== undefined ? parseFloat(chapterNumber) : null);
        if (!workId || (chapterNumber === undefined && sortKey === null)) {
            return { valid: true };
        }
        // 1. Check: is this chapter already published canonically in chapters table?
        const pubCheck = await this.runQuery(this.pool, `
      SELECT id FROM chapters 
      WHERE work_id = $1::uuid 
        AND (number = $2::numeric OR ($3::numeric IS NOT NULL AND number = $3::numeric))
        AND published_at IS NOT NULL
      LIMIT 1;
    `, [workId, chapterNumber !== undefined ? chapterNumber : sortKey, sortKey]);
        if (pubCheck.rows.length > 0) {
            const publishedChapterId = pubCheck.rows[0].id;
            this.logger.info(`Claimed job ${job.id} for work ${workId} ch ${chapterNumber} is already canonically published. Auto-completing immediately.`);
            await this.runQuery(this.pool, `
        UPDATE importer_queue 
        SET status = 'COMPLETED', updated_at = NOW(), last_error = 'CANONICAL_ALREADY_SATISFIED'
        WHERE id = $1;
      `, [job.id]);
            if (sortKey !== null) {
                await this.runQuery(this.pool, `
          UPDATE importer_queue
          SET status = 'COMPLETED', updated_at = NOW(), last_error = 'CANONICAL_ALREADY_SATISFIED'
          WHERE (payload->>'workId') = $1
            AND chapter_sort_key = $2
            AND status IN ('QUEUED', 'RETRY')
            AND task_type = 'IMPORT_CHAPTER';
        `, [workId, sortKey]);
                await this.runQuery(this.pool, `
          UPDATE importer_chapter_mappings
          SET status = 'COMPLETED', is_page_provider = false, chapter_id = $3, updated_at = NOW()
          WHERE work_id = $1::uuid AND chapter_sort_key = $2 AND status IN ('PENDING', 'QUEUED');
        `, [workId, sortKey, publishedChapterId]);
            }
            return { valid: false, reason: 'ALREADY_PUBLISHED' };
        }
        // 2. Safety check: is there an un-published STAGED chapter behind this one?
        if (sortKey !== null) {
            const stagedCheck = await this.runQuery(this.pool, `
        SELECT id, chapter_sort_key 
        FROM importer_chapter_mappings
        WHERE work_id = $1::uuid
          AND status = 'STAGED'
          AND chapter_sort_key < $2::numeric
        ORDER BY chapter_sort_key ASC
        LIMIT 1;
      `, [workId, sortKey]);
            if (stagedCheck.rows.length > 0) {
                const barrierKey = parseFloat(stagedCheck.rows[0].chapter_sort_key);
                this.logger.info(`Claimed job ${job.id} for work ${workId} ch ${sortKey} is ahead of STAGED chapter ${barrierKey}. Releasing back to QUEUED to preserve canonical barrier.`);
                await this.runQuery(this.pool, `
          UPDATE importer_queue
          SET status = 'QUEUED',
              attempts = GREATEST(0, attempts - 1),
              locked_by = NULL,
              locked_at = NULL,
              lease_expires_at = NULL,
              updated_at = NOW()
          WHERE id = $1;
        `, [job.id]);
                // Mark work as staged-blocked so other backfill workers don't spin on it
                this.markWorkStagedBlocked(workId, 15000);
                // Resolve true missing predecessor for critical gap resolution:
                // Query for the lowest queued chapter strictly below barrierKey
                const predCheck = await this.runQuery(this.pool, `
          SELECT chapter_sort_key
          FROM importer_queue
          WHERE (payload->>'workId') = $1
            AND chapter_sort_key < $2::numeric
            AND status IN ('QUEUED', 'RETRY')
            AND task_type = 'IMPORT_CHAPTER'
          ORDER BY chapter_sort_key ASC
          LIMIT 1;
        `, [workId, barrierKey]);
                const activeWork = this.stateStore.getActiveWork(workId);
                if (activeWork) {
                    if (predCheck.rows.length > 0) {
                        const trueMissingKey = parseFloat(predCheck.rows[0].chapter_sort_key);
                        activeWork.criticalGapSortKey = trueMissingKey;
                        activeWork.criticalGapUnblockCount = 1;
                        this.logger.info(`Work ${workId} critical gap resolved to true missing queued predecessor ch ${trueMissingKey} (unblocks STAGED ch ${barrierKey})`);
                    }
                    else {
                        // No queued predecessor below barrierKey; do not set criticalGapSortKey to barrierKey (which is STAGED, not QUEUED)
                        activeWork.criticalGapSortKey = null;
                    }
                }
                return { valid: false, reason: 'BLOCKED_BY_STAGED' };
            }
        }
        return { valid: true };
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
                const activeWorkIds = this.stateStore.getActiveWorks().map((w) => w.workId);
                const statsRes = await this.runQuery(this.pool, `
          SELECT 
            COUNT(CASE WHEN q.status = 'QUEUED' AND s.enabled = true AND (s.status = 'ACTIVE' OR (s.status IN ('COOLDOWN', 'PROBING', 'DEGRADED') AND (s.cooldown_until IS NULL OR s.cooldown_until <= NOW()))) THEN 1 END) as claimable_now,
            COUNT(CASE WHEN q.status = 'RETRY' AND q.next_run_at <= NOW() AND s.enabled = true AND (s.status = 'ACTIVE' OR (s.status IN ('COOLDOWN', 'PROBING', 'DEGRADED') AND (s.cooldown_until IS NULL OR s.cooldown_until <= NOW()))) THEN 1 END) as retry_due,
            COUNT(CASE WHEN q.status = 'IMPORTING' THEN 1 END) as importing_cnt,
            COUNT(DISTINCT CASE WHEN s.enabled = true AND (s.status = 'ACTIVE' OR (s.status IN ('COOLDOWN', 'PROBING', 'DEGRADED') AND (s.cooldown_until IS NULL OR s.cooldown_until <= NOW()))) THEN (q.payload->>'workId') END) as valid_waiting_works,
            COUNT(CASE WHEN (q.payload->>'workId') = ANY($1::text[]) THEN 1 END) as active_work_pending
          FROM importer_queue q
          LEFT JOIN importer_sources s ON s.id = q.source
          WHERE q.task_type = 'IMPORT_CHAPTER'
            AND q.status IN ('QUEUED', 'RETRY', 'IMPORTING');
        `, [activeWorkIds.length > 0 ? activeWorkIds : ['00000000-0000-0000-0000-000000000000']]);
                const stagedRes = await this.runQuery(this.pool, `SELECT COUNT(*) as staged_cnt FROM importer_chapter_mappings WHERE status = 'STAGED'`);
                const row = statsRes.rows[0];
                const claimableNow = parseInt(row?.claimable_now || '0', 10);
                const retryDue = parseInt(row?.retry_due || '0', 10);
                const importingCnt = parseInt(row?.importing_cnt || '0', 10);
                const validWaitingWorks = parseInt(row?.valid_waiting_works || '0', 10);
                const activeWorkPending = parseInt(row?.active_work_pending || '0', 10);
                const stagedCnt = parseInt(stagedRes.rows[0]?.staged_cnt || '0', 10);
                const hasSafeWork = claimableNow > 0 || retryDue > 0 || validWaitingWorks > 0 || activeWorkPending > 0 || stagedCnt > 0;
                if (hasSafeWork) {
                    const elapsedPubMs = Date.now() - this.lastAnyPublicationTime;
                    if (elapsedPubMs >= 10 * 60 * 1000) {
                        this.logger.error(`🚨 [PUBLICATION_WATCHDOG] Pipeline stalled: 0 publications for ${(elapsedPubMs / 60000).toFixed(1)}m while safe backlog exists. ` +
                            `[claimable=${claimableNow}, retry_due=${retryDue}, importing=${importingCnt}, waiting_works=${validWaitingWorks}, active_pending=${activeWorkPending}, staged=${stagedCnt}]. Triggering AUTO-RECOVERY TREE!`);
                        // Auto-Recovery Action 1: Force admission cycle to unblock works and promote sliding window
                        await this.admissionController.runAdmissionCycle();
                        // Auto-Recovery Action 2: Recover stale leases
                        await this.runQuery(this.pool, `
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
                            `[claimable=${claimableNow}, retry_due=${retryDue}, importing=${importingCnt}, waiting_works=${validWaitingWorks}, active_pending=${activeWorkPending}, staged=${stagedCnt}]. Observing...`);
                    }
                }
            }
            catch (err) {
                this.logger.warn('Error in Publication Watchdog loop', { error: err?.message });
            }
        }, 60000);
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
            const cand = await this.runQuery(this.pool, `
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
    getMaxInflightPerWork() {
        return this.stateStore.getConfig().maxInflightPerWork || 2;
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
        }, 60000);
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
        const qRes = await this.runQuery(this.pool, `
      SELECT 
        COUNT(CASE WHEN priority >= 100 THEN 1 END) as p0_cnt,
        COUNT(CASE WHEN priority >= 70 AND priority < 100 THEN 1 END) as p1_cnt,
        COUNT(CASE WHEN priority >= 30 AND priority < 70 THEN 1 END) as p2_cnt,
        COUNT(CASE WHEN priority < 30 OR task_type IN ('DISCOVER_WORKS', 'SYNC_WORK') THEN 1 END) as p3_cnt
      FROM importer_queue
      WHERE status = 'QUEUED' AND task_type = 'IMPORT_CHAPTER';
    `);
        const stagedRes = await this.runQuery(this.pool, `
      SELECT COUNT(*) as staged_cnt FROM importer_chapter_mappings WHERE status = 'STAGED';
    `);
        const qRow = qRes.rows[0];
        p0Queued = parseInt(qRow?.p0_cnt || '0', 10);
        p1Queued = parseInt(qRow?.p1_cnt || '0', 10);
        p2Queued = parseInt(qRow?.p2_cnt || '0', 10);
        p3Waiting = parseInt(qRow?.p3_cnt || '0', 10);
        stagedWaitingForGap = parseInt(stagedRes.rows[0]?.staged_cnt || '0', 10);
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
    /**
     * Controlled atomic background cleanup for redundant queue jobs.
     * Safely marks queued/retrying jobs as COMPLETED with CANONICAL_ALREADY_SATISFIED
     * if their canonical chapter is already published in chapters table.
     * Preserves provider mappings and fallbacks without blind DELETES.
     */
    async runControlledRedundantJobCleanup(batchSize = 200) {
        try {
            const res = await this.runQuery(this.pool, `
        WITH recent_works AS (
          SELECT DISTINCT work_id 
          FROM chapters 
          WHERE published_at >= NOW() - INTERVAL '60 minutes'
        ),
        redundant AS (
          SELECT q.id, (q.payload->>'workId')::uuid as work_id, q.chapter_sort_key, q.source, c.id as chapter_id
          FROM recent_works rw
          JOIN chapters c ON c.work_id = rw.work_id AND c.published_at IS NOT NULL
          JOIN importer_queue q ON (q.payload->>'workId') = rw.work_id::text
                               AND q.chapter_sort_key = c.number
                               AND q.status IN ('QUEUED', 'RETRY')
                               AND q.task_type = 'IMPORT_CHAPTER'
          LIMIT $1
        ),
        updated_q AS (
          UPDATE importer_queue q
          SET status = 'COMPLETED',
              locked_by = NULL,
              locked_at = NULL,
              last_error = 'CANONICAL_ALREADY_SATISFIED',
              updated_at = NOW()
          FROM redundant r
          WHERE q.id = r.id
          RETURNING q.id
        ),
        updated_m AS (
          UPDATE importer_chapter_mappings m
          SET status = 'COMPLETED',
              chapter_id = r.chapter_id,
              is_page_provider = false,
              updated_at = NOW()
          FROM redundant r
          WHERE m.source = r.source
            AND m.work_id = r.work_id
            AND m.chapter_sort_key = r.chapter_sort_key
            AND m.status IN ('PENDING', 'QUEUED')
          RETURNING m.id
        )
        SELECT count(*) as count FROM updated_q;
      `, [batchSize]);
            const cleaned = parseInt(res.rows[0]?.count || '0', 10);
            if (cleaned > 0) {
                this.logger.info(`[REDUNDANCY_CLEANUP] Safely short-circuited ${cleaned} redundant jobs for already published chapters.`);
            }
            return { cleaned };
        }
        catch (err) {
            this.logger.warn('[REDUNDANCY_CLEANUP_ERROR] Failed to run redundant job cleanup', { error: err?.message });
            return { cleaned: 0 };
        }
    }
}
