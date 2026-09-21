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
export class AdmissionController {
    stateStore;
    protectiveSentinel;
    logger = new Logger('AdmissionController');
    pool = getYugabytePool();
    isRunning = false;
    loopTimer = null;
    constructor(stateStore, protectiveSentinel) {
        this.stateStore = stateStore;
        this.protectiveSentinel = protectiveSentinel;
    }
    /**
     * Starts the periodic admission background loop (every 5 seconds).
     */
    start() {
        if (this.isRunning)
            return;
        this.isRunning = true;
        this.logger.info('Starting AdmissionController background loop');
        this.scheduleNextCycle(1000);
    }
    stop() {
        this.isRunning = false;
        if (this.loopTimer) {
            clearTimeout(this.loopTimer);
            this.loopTimer = null;
        }
    }
    scheduleNextCycle(delayMs) {
        if (!this.isRunning)
            return;
        this.loopTimer = setTimeout(async () => {
            try {
                await this.runAdmissionCycle();
            }
            catch (err) {
                this.logger.error('Error during admission cycle', { error: err?.message });
            }
            finally {
                this.scheduleNextCycle(5000);
            }
        }, delayMs);
    }
    /**
     * Executes a single admission reconciliation cycle.
     */
    async runAdmissionCycle() {
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
    async reconcileActiveWorks() {
        const activeWorks = this.stateStore.getActiveWorks();
        for (const work of activeWorks) {
            try {
                const client = await this.pool.connect();
                try {
                    // A. Count queued & importing jobs for this work
                    const queueRes = await client.query(`SELECT 
               COUNT(CASE WHEN status = 'QUEUED' THEN 1 END) as queued_cnt,
               COUNT(CASE WHEN status = 'IMPORTING' THEN 1 END) as importing_cnt,
               COUNT(CASE WHEN status = 'PAUSED_BY_STAFF' THEN 1 END) as paused_cnt,
               MIN(CASE WHEN status IN ('QUEUED', 'PAUSED_BY_STAFF') THEN chapter_sort_key END) as min_sort_key
             FROM importer_queue
             WHERE task_type = 'IMPORT_CHAPTER' AND (payload->>'workId') = $1`, [work.workId]);
                    // B. Count published chapters
                    const pubRes = await client.query(`SELECT COUNT(*) as pub_cnt FROM chapters WHERE work_id = $1::uuid AND published_at IS NOT NULL`, [work.workId]);
                    // C. Detect STAGED barrier gaps for this work
                    const stagedRes = await client.query(`SELECT COUNT(*) as staged_cnt, MIN(chapter_sort_key) as min_staged
             FROM importer_chapter_mappings
             WHERE work_id = $1::uuid AND status = 'STAGED'`, [work.workId]);
                    const qRow = queueRes.rows[0];
                    const queuedCnt = parseInt(qRow?.queued_cnt || '0', 10);
                    const importingCnt = parseInt(qRow?.importing_cnt || '0', 10);
                    const pausedCnt = parseInt(qRow?.paused_cnt || '0', 10);
                    const minSortKey = qRow?.min_sort_key ? parseFloat(qRow.min_sort_key) : null;
                    const pubCnt = parseInt(pubRes.rows[0]?.pub_cnt || '0', 10);
                    const stagedCnt = parseInt(stagedRes.rows[0]?.staged_cnt || '0', 10);
                    const minStaged = stagedRes.rows[0]?.min_staged ? parseFloat(stagedRes.rows[0].min_staged) : null;
                    work.queuedChapters = queuedCnt;
                    work.inFlightChapters = importingCnt;
                    work.publishedChapters = pubCnt;
                    work.totalChapters = pubCnt + queuedCnt + importingCnt + pausedCnt;
                    work.frontierSortKey = minSortKey;
                    // Critical gap detection: if we have staged chapters and missing sort key is < minStaged
                    if (stagedCnt > 0 && minSortKey !== null && minStaged !== null && minSortKey < minStaged) {
                        work.criticalGapSortKey = minSortKey;
                        work.criticalGapUnblockCount = stagedCnt;
                    }
                    else {
                        work.criticalGapSortKey = null;
                        work.criticalGapUnblockCount = 0;
                    }
                    // Check if caught up (zero queued, zero importing, zero paused remaining)
                    if (queuedCnt === 0 && importingCnt === 0 && pausedCnt === 0) {
                        work.state = 'CAUGHT_UP';
                        this.logger.info(`Work ${work.workTitle} (${work.workId}) reached CAUGHT_UP state. Vacating active set slot.`, {
                            publishedChapters: pubCnt,
                            lane: work.lane,
                        });
                        this.stateStore.removeActiveWork(work.workId);
                        continue;
                    }
                    work.state = 'FILLING';
                    work.lastActivityAt = new Date().toISOString();
                    this.stateStore.setActiveWork(work);
                }
                finally {
                    client.release();
                }
            }
            catch (err) {
                this.logger.warn(`Failed to reconcile active work ${work.workId}`, { error: err?.message });
            }
        }
    }
    /**
     * Step 2: Replenishes active sets (P1 Backfill and P2 New Works) if slots are free.
     */
    async replenishActiveSets() {
        const config = this.stateStore.getConfig();
        const activeWorks = this.stateStore.getActiveWorks();
        const activeBackfills = activeWorks.filter((w) => w.lane === 'P1');
        const activeNewWorks = activeWorks.filter((w) => w.lane === 'P2');
        const backfillSlotsAvailable = config.maxActiveBackfillWorks - activeBackfills.length;
        const newWorkSlotsAvailable = config.maxActiveNewWorks - activeNewWorks.length;
        // Track active sources for source diversity (Section 81)
        const sourceCounts = new Map();
        for (const w of activeWorks) {
            sourceCounts.set(w.primarySource, (sourceCounts.get(w.primarySource) || 0) + 1);
        }
        const client = await this.pool.connect();
        try {
            // 1. Replenish P1 Backfill Works
            if (backfillSlotsAvailable > 0) {
                const activeIds = activeWorks.map((w) => w.workId);
                const candidatesRes = await client.query(`SELECT (q.payload->>'workId') as work_id,
                  w.title,
                  q.source,
                  COUNT(*) as pending_jobs,
                  COUNT(CASE WHEN q.status = 'QUEUED' THEN 1 END) as queued_count,
                  COUNT(CASE WHEN q.status = 'PAUSED_BY_STAFF' THEN 1 END) as paused_count,
                  MIN(q.chapter_sort_key) as min_sort_key
           FROM importer_queue q
           JOIN works w ON w.id = (q.payload->>'workId')::uuid
           WHERE q.task_type = 'IMPORT_CHAPTER'
             AND q.status IN ('QUEUED', 'RETRY', 'PAUSED_BY_STAFF')
             AND w.published = true
             AND w.latest_chapter_published_at IS NOT NULL
             AND NOT ((q.payload->>'workId') = ANY($1::text[]))
           GROUP BY (q.payload->>'workId'), w.title, q.source
           ORDER BY queued_count DESC, pending_jobs ASC
           LIMIT $2`, [activeIds.length > 0 ? activeIds : ['00000000-0000-0000-0000-000000000000'], backfillSlotsAvailable * 2]);
                let admitted = 0;
                for (const cand of candidatesRes.rows) {
                    if (admitted >= backfillSlotsAvailable)
                        break;
                    const srcCount = sourceCounts.get(cand.source) || 0;
                    // Source diversity check: avoid putting more than 60% of active works on one source if others exist
                    if (srcCount >= 6 && candidatesRes.rows.length > backfillSlotsAvailable) {
                        continue;
                    }
                    const newWork = {
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
                    this.logger.info(`Admitted work into ACTIVE_BACKFILL_WORKS (P1)`, {
                        workId: newWork.workId,
                        title: newWork.workTitle,
                        source: newWork.primarySource,
                        pendingJobs: cand.pending_jobs,
                    });
                }
            }
            // 2. Replenish P2 New Works
            if (newWorkSlotsAvailable > 0) {
                const activeIds = this.stateStore.getActiveWorks().map((w) => w.workId);
                const candidatesRes = await client.query(`SELECT (q.payload->>'workId') as work_id,
                  w.title,
                  q.source,
                  COUNT(*) as pending_jobs,
                  COUNT(CASE WHEN q.status = 'QUEUED' THEN 1 END) as queued_count,
                  COUNT(CASE WHEN q.status = 'PAUSED_BY_STAFF' THEN 1 END) as paused_count,
                  MIN(q.chapter_sort_key) as min_sort_key
           FROM importer_queue q
           JOIN works w ON w.id = (q.payload->>'workId')::uuid
           WHERE q.task_type = 'IMPORT_CHAPTER'
             AND q.status IN ('QUEUED', 'RETRY', 'PAUSED_BY_STAFF')
             AND (w.published IS FALSE OR w.latest_chapter_published_at IS NULL)
             AND NOT ((q.payload->>'workId') = ANY($1::text[]))
           GROUP BY (q.payload->>'workId'), w.title, q.source
           ORDER BY queued_count DESC, pending_jobs ASC
           LIMIT $2`, [activeIds.length > 0 ? activeIds : ['00000000-0000-0000-0000-000000000000'], newWorkSlotsAvailable * 2]);
                let admitted = 0;
                for (const cand of candidatesRes.rows) {
                    if (admitted >= newWorkSlotsAvailable)
                        break;
                    const srcCount = sourceCounts.get(cand.source) || 0;
                    if (srcCount >= 6 && candidatesRes.rows.length > newWorkSlotsAvailable) {
                        continue;
                    }
                    const newWork = {
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
                    this.logger.info(`Admitted work into ACTIVE_NEW_WORKS (P2)`, {
                        workId: newWork.workId,
                        title: newWork.workTitle,
                        source: newWork.primarySource,
                        pendingJobs: cand.pending_jobs,
                    });
                }
            }
        }
        finally {
            client.release();
        }
    }
    /**
     * Step 3: Maintains sliding windows for active works.
     * When an active work has fewer than `slidingWindowMin` queued chapters,
     * promotes the next batch (up to `slidingWindowSize`) from PAUSED_BY_STAFF to QUEUED.
     */
    async maintainSlidingWindows() {
        const config = this.stateStore.getConfig();
        const activeWorks = this.stateStore.getActiveWorks();
        const client = await this.pool.connect();
        try {
            for (const work of activeWorks) {
                if (work.queuedChapters < config.slidingWindowMin) {
                    const needed = config.slidingWindowSize - work.queuedChapters;
                    if (needed <= 0)
                        continue;
                    // Check if this work has critical unblocking gap
                    let targetPriority = work.lane === 'P1' ? 75 : 50;
                    if (work.criticalGapSortKey !== null) {
                        targetPriority = 95; // P1_CRITICAL_GAP boost
                    }
                    // Select the next slice of chapters from PAUSED_BY_STAFF ordered by chapter_sort_key ASC
                    const promoteRes = await client.query(`WITH to_promote AS (
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
             RETURNING q.id, q.chapter_sort_key;`, [work.workId, needed, targetPriority]);
                    if (promoteRes.rows.length > 0) {
                        work.queuedChapters += promoteRes.rows.length;
                        this.stateStore.setActiveWork(work);
                        this.logger.info(`Promoted ${promoteRes.rows.length} chapters into QUEUED for work ${work.workTitle}`, {
                            workId: work.workId,
                            lane: work.lane,
                            priority: targetPriority,
                            chapters: promoteRes.rows.map((r) => r.chapter_sort_key),
                        });
                    }
                }
            }
        }
        finally {
            client.release();
        }
    }
}
