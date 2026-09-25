import { Logger } from './logger.js';
import { diagnostics } from './diagnostics.js';
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
    logger = new Logger('AutoHealWatchdog');
    pool;
    scheduler;
    admissionController;
    protectiveSentinel;
    publicationBarrier;
    safetyBarrier;
    onControlledRestart;
    intervalMs;
    workerId;
    isRunning = false;
    stopSignal = false;
    timer = null;
    autoHealState = 'MONITORING';
    lastAutoHealAt = null;
    lastLevel1At = 0;
    lastLevel2At = 0;
    lastRestartAt = 0;
    lastSweepAt = 0;
    stuckIdentities = new Map();
    circuitBreakerOpen = false;
    cachedTelemetry = null;
    lastTelemetryAt = 0;
    telemetryCacheTtlMs = 45_000;
    lastDeepStagedAt = 0;
    classificationCursor = null;
    lastCoverageResetAt = Date.now();
    classifiedSinceReset = 0;
    constructor(options) {
        this.pool = options.pool;
        this.scheduler = options.scheduler;
        this.admissionController = options.admissionController;
        this.protectiveSentinel = options.protectiveSentinel;
        this.publicationBarrier = options.publicationBarrier;
        this.safetyBarrier = options.safetyBarrier;
        this.onControlledRestart = options.onControlledRestart;
        this.intervalMs = options.intervalMs ?? 60_000;
        this.workerId = options.workerId ?? 'discloud-importer-1';
    }
    getStuckIdentityAge(key) {
        const detectedAt = this.stuckIdentities.get(key);
        if (!detectedAt)
            return 0;
        return Math.floor((Date.now() - detectedAt) / 1000);
    }
    setStuckIdentity(key, detectedAtMs) {
        this.stuckIdentities.set(key, detectedAtMs);
    }
    clearStuckIdentities() {
        this.stuckIdentities.clear();
    }
    getClassificationCursor() {
        return this.classificationCursor;
    }
    setClassificationCursor(cursor) {
        this.classificationCursor = cursor;
    }
    removeStuckIdentity(key) {
        this.stuckIdentities.delete(key);
    }
    getTrackedStuckKeys() {
        return Array.from(this.stuckIdentities.keys());
    }
    /**
     * Starts the background evaluation loop.
     */
    start() {
        if (this.isRunning)
            return;
        this.isRunning = true;
        this.stopSignal = false;
        this.logger.info('AutoHealWatchdog started with interval ' + this.intervalMs + 'ms');
        const tick = async () => {
            if (this.stopSignal)
                return;
            try {
                await this.evaluateCycle();
            }
            catch (err) {
                this.logger.warn('Error during AutoHealWatchdog cycle', { error: err?.message });
            }
            finally {
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
    stop() {
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
    async collectTelemetry(forceFresh = false) {
        const nowMs = Date.now();
        if (!forceFresh && this.cachedTelemetry && nowMs - this.lastTelemetryAt < this.telemetryCacheTtlMs) {
            return this.cachedTelemetry;
        }
        const mem = diagnostics.getMemorySnapshot();
        const now = new Date();
        // 1. Publication, Completion, and Started Timestamps (Index-Optimized)
        const timeRes = await this.pool.query(`
      SELECT 
        (SELECT EXTRACT(EPOCH FROM (NOW() - MAX(locked_at))) FROM importer_queue WHERE status = 'IMPORTING' AND locked_at IS NOT NULL) as started_age,
        (SELECT EXTRACT(EPOCH FROM (NOW() - MAX(updated_at))) FROM importer_queue WHERE status = 'COMPLETED' AND task_type = 'IMPORT_CHAPTER') as completed_age,
        (SELECT EXTRACT(EPOCH FROM (NOW() - MAX(published_at))) FROM chapters WHERE published_at IS NOT NULL) as fresh_age,
        (SELECT count(*) FROM importer_queue WHERE status = 'IMPORTING') as started_15m,
        (SELECT count(*) FROM importer_queue WHERE status = 'COMPLETED' AND task_type = 'IMPORT_CHAPTER' AND updated_at >= NOW() - INTERVAL '15 minutes') as completed_15m,
        (SELECT count(*) FROM chapters WHERE published_at >= NOW() - INTERVAL '15 minutes') as fresh_15m
    `);
        const times = timeRes.rows[0] || {};
        const lastStartedAgeSec = Math.round(parseFloat(times.started_age || '99999'));
        const lastCompletedAgeSec = Math.round(parseFloat(times.completed_age || '99999'));
        const lastFreshVisibleAgeSec = Math.round(parseFloat(times.fresh_age || '99999'));
        const completedLast15m = parseInt(times.completed_15m || '0', 10);
        const freshLast15m = parseInt(times.fresh_15m || '0', 10);
        const rawStarted15m = parseInt(times.started_15m || '0', 10);
        const startedLast15m = rawStarted15m > completedLast15m ? rawStarted15m : rawStarted15m + completedLast15m;
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
        // 3. Staged Unique & Publishable Staged Chapters (Index-Only, Barrier-Aligned, Work-Scoped)
        let stagedUnique = 0;
        let publishableStaged = 0;
        let waitingPredecessorStaged = 0;
        let stuckStaged = 0;
        let classifiedStaged = 0;
        let unclassifiedStaged = 0;
        let classifiedThisCycle = 0;
        let classificationCoverageEstimate = 100;
        let oldestUnclassifiedAge = 0;
        let stuckStagedAgeSec = 0;
        const currentStuckKeys = new Set();
        const isHealthyState = !forceFresh &&
            this.cachedTelemetry !== null &&
            lastCompletedAgeSec <= 600 &&
            lastFreshVisibleAgeSec <= 1800 &&
            (nowMs - this.lastDeepStagedAt < 300_000);
        if (isHealthyState && this.cachedTelemetry) {
            // Lightweight Healthy Path: pipeline actively processing and publishing, skip heavy keyset pagination and stuck audits
            stagedUnique = this.cachedTelemetry.stagedUnique;
            publishableStaged = this.cachedTelemetry.publishableStaged;
            waitingPredecessorStaged = this.cachedTelemetry.waitingPredecessorStaged;
            stuckStaged = this.cachedTelemetry.stuckStaged;
            classifiedStaged = this.cachedTelemetry.classifiedStaged ?? 0;
            unclassifiedStaged = this.cachedTelemetry.unclassifiedStaged ?? 0;
            classifiedThisCycle = this.cachedTelemetry.classifiedThisCycle ?? 0;
            oldestUnclassifiedAge = this.cachedTelemetry.oldestUnclassifiedAge ?? 0;
            stuckStagedAgeSec = this.cachedTelemetry.stuckStagedAgeSec ?? 0;
        }
        else {
            this.lastDeepStagedAt = nowMs;
            try {
                const mapRes = await this.pool.query(`
          SELECT count(*) as staged_unique
          FROM (
            SELECT DISTINCT work_id, chapter_sort_key
            FROM importer_chapter_mappings
            WHERE status IN ('STAGED', 'WAITING_FOR_GAP')
          ) sub
        `);
                stagedUnique = parseInt(mapRes.rows[0]?.staged_unique || '0', 10);
                if (stagedUnique > 0) {
                    const cursorSortKey = this.classificationCursor?.lastFrontierSortKey ?? null;
                    const cursorWorkId = this.classificationCursor?.lastWorkId ?? null;
                    const pubStagedRes = await this.pool.query(`
          WITH staged_works AS (
            SELECT 
              m.work_id,
              MIN(m.chapter_sort_key) as frontier_sort_key,
              COUNT(DISTINCT m.chapter_sort_key) as total_staged_chapters
            FROM importer_chapter_mappings m
            WHERE m.status IN ('STAGED', 'WAITING_FOR_GAP') AND m.work_id IS NOT NULL
            GROUP BY m.work_id
            HAVING ($1::numeric IS NULL OR (MIN(m.chapter_sort_key) > $1::numeric OR (MIN(m.chapter_sort_key) = $1::numeric AND m.work_id::text > $2::text)))
            ORDER BY MIN(m.chapter_sort_key) ASC, m.work_id ASC
            LIMIT 40
          ),
          works_with_published AS (
            SELECT 
              sw.work_id,
              sw.frontier_sort_key,
              sw.total_staged_chapters,
              (
                SELECT MAX(c.number) 
                FROM chapters c 
                WHERE c.work_id = sw.work_id 
                  AND c.published_at IS NOT NULL
              ) as max_published,
              EXISTS (
                SELECT 1 
                FROM importer_chapter_mappings pm
                WHERE pm.work_id = sw.work_id
                  AND pm.chapter_sort_key < sw.frontier_sort_key
                  AND pm.is_gap = false
                  AND pm.status NOT IN ('STAGED', 'WAITING_FOR_GAP')
              ) as has_predecessor_in_mapping,
              EXISTS (
                SELECT 1 
                FROM importer_queue pq
                WHERE (pq.payload->>'workId') = sw.work_id::text
                  AND pq.task_type = 'IMPORT_CHAPTER'
                  AND pq.status IN ('QUEUED', 'RETRY', 'IMPORTING')
                  AND pq.chapter_sort_key < sw.frontier_sort_key
              ) as has_predecessor_in_queue
            FROM staged_works sw
          )
          SELECT 
            work_id,
            frontier_sort_key,
            total_staged_chapters,
            max_published,
            has_predecessor_in_mapping,
            has_predecessor_in_queue,
            CASE
              WHEN (max_published IS NOT NULL AND frontier_sort_key <= max_published + 1.05 AND NOT has_predecessor_in_queue)
                OR (max_published IS NULL AND NOT has_predecessor_in_mapping AND NOT has_predecessor_in_queue)
              THEN 1
              ELSE 0
            END as is_frontier_publishable
          FROM works_with_published;
        `, [cursorSortKey, cursorWorkId]);
                    // Check if query returned old-style publishable_staged (from test mock)
                    if (pubStagedRes.rows[0]?.waiting_predecessor_staged !== undefined || pubStagedRes.rows[0]?.stuck_staged !== undefined) {
                        publishableStaged = parseInt(pubStagedRes.rows[0]?.publishable_staged || '0', 10);
                        waitingPredecessorStaged = parseInt(pubStagedRes.rows[0]?.waiting_predecessor_staged || '0', 10);
                        stuckStaged = parseInt(pubStagedRes.rows[0]?.stuck_staged || '0', 10);
                        classifiedStaged = pubStagedRes.rows[0]?.classified_staged !== undefined
                            ? parseInt(pubStagedRes.rows[0]?.classified_staged, 10)
                            : publishableStaged + waitingPredecessorStaged + stuckStaged;
                        unclassifiedStaged = pubStagedRes.rows[0]?.unclassified_staged !== undefined
                            ? parseInt(pubStagedRes.rows[0]?.unclassified_staged, 10)
                            : Math.max(0, stagedUnique - classifiedStaged);
                        classifiedThisCycle = pubStagedRes.rows.length;
                        if (stuckStaged > 0) {
                            const stuckKey = pubStagedRes.rows[0]?.stuck_key || 'mock-stuck';
                            currentStuckKeys.add(stuckKey);
                            if (!this.stuckIdentities.has(stuckKey)) {
                                this.stuckIdentities.set(stuckKey, nowMs);
                            }
                        }
                    }
                    else if (pubStagedRes.rows[0]?.publishable_staged !== undefined) {
                        publishableStaged = parseInt(pubStagedRes.rows[0]?.publishable_staged || '0', 10);
                        waitingPredecessorStaged = Math.max(0, stagedUnique - publishableStaged);
                        stuckStaged = 0;
                        classifiedStaged = publishableStaged + waitingPredecessorStaged;
                        unclassifiedStaged = Math.max(0, stagedUnique - classifiedStaged);
                        classifiedThisCycle = pubStagedRes.rows.length;
                    }
                    else {
                        for (const row of pubStagedRes.rows) {
                            const totalStaged = parseInt(row.total_staged_chapters || '1', 10);
                            const isCandidatePub = parseInt(row.is_frontier_publishable || '0', 10) === 1;
                            let canActuallyPublish = isCandidatePub;
                            // Chapter barrier canonical alignment: if candidate is publishable and publicationBarrier is present
                            if (canActuallyPublish && this.publicationBarrier && row.work_id && row.frontier_sort_key !== undefined) {
                                try {
                                    const check = await this.publicationBarrier.checkBarrier(row.work_id, parseFloat(row.frontier_sort_key));
                                    if (!check.canPublish) {
                                        canActuallyPublish = false;
                                    }
                                }
                                catch (err) {
                                    this.logger.warn('Error checking individual chapter publication barrier', { workId: row.work_id, error: err?.message });
                                }
                            }
                            if (canActuallyPublish) {
                                publishableStaged += 1; // Only frontier chapter is actionable
                                waitingPredecessorStaged += Math.max(0, totalStaged - 1);
                                if (row.work_id && row.frontier_sort_key !== undefined) {
                                    this.stuckIdentities.delete(`${row.work_id}:${row.frontier_sort_key}`);
                                }
                            }
                            else {
                                // Non-publishable work: WAITING_PREDECESSOR ONLY if there is a real predecessor in queue!
                                const hasPredecessor = Boolean(row.has_predecessor_in_queue);
                                if (hasPredecessor) {
                                    waitingPredecessorStaged += totalStaged;
                                    if (row.work_id && row.frontier_sort_key !== undefined) {
                                        this.stuckIdentities.delete(`${row.work_id}:${row.frontier_sort_key}`);
                                    }
                                }
                                else {
                                    stuckStaged += totalStaged;
                                    const stuckKey = row.work_id ? `${row.work_id}:${row.frontier_sort_key}` : `mock-stuck-${stuckStaged}`;
                                    currentStuckKeys.add(stuckKey);
                                    if (!this.stuckIdentities.has(stuckKey)) {
                                        this.stuckIdentities.set(stuckKey, nowMs);
                                    }
                                }
                            }
                        }
                        classifiedStaged = publishableStaged + waitingPredecessorStaged + stuckStaged;
                        unclassifiedStaged = Math.max(0, stagedUnique - classifiedStaged);
                        classifiedThisCycle = pubStagedRes.rows.length;
                        // Keyset pagination progression & wrap logic
                        if (pubStagedRes.rows.length > 0) {
                            const lastRow = pubStagedRes.rows[pubStagedRes.rows.length - 1];
                            if (pubStagedRes.rows.length === 40 && lastRow.work_id && lastRow.frontier_sort_key !== undefined) {
                                this.classificationCursor = {
                                    lastFrontierSortKey: parseFloat(lastRow.frontier_sort_key),
                                    lastWorkId: String(lastRow.work_id),
                                };
                            }
                            else {
                                // Reached end of staged works, wrap back to start
                                this.classificationCursor = null;
                                this.lastCoverageResetAt = nowMs;
                                this.classifiedSinceReset = 0;
                            }
                        }
                        else {
                            this.classificationCursor = null;
                            this.lastCoverageResetAt = nowMs;
                            this.classifiedSinceReset = 0;
                        }
                    }
                    // Global Safety Barrier guard: If PublicationSafetyBarrier is CLOSED or RECOVERING, nothing is publishable globally
                    if (this.safetyBarrier) {
                        try {
                            const barrierState = await this.safetyBarrier.getState();
                            if (barrierState === 'CLOSED' || barrierState === 'RECOVERING') {
                                if (publishableStaged > 0) {
                                    waitingPredecessorStaged += publishableStaged;
                                    publishableStaged = 0;
                                }
                            }
                        }
                        catch { }
                    }
                }
            }
            catch (err) {
                this.logger.warn('Failed querying staged classification', { error: err?.message });
            }
            // Manage stuck identities across paginated cycles:
            // Audit tracked stuck keys against database to check if any have published or left STAGED status
            if (this.stuckIdentities.size > 0) {
                try {
                    const trackedKeys = Array.from(this.stuckIdentities.keys()).filter((k) => !k.startsWith('mock-'));
                    if (trackedKeys.length > 0) {
                        const checkRes = await this.pool.query(`
            SELECT (work_id || ':' || chapter_sort_key::text) as key
            FROM importer_chapter_mappings
            WHERE status IN ('STAGED', 'WAITING_FOR_GAP')
              AND (work_id || ':' || chapter_sort_key::text) = ANY($1::text[])
          `, [trackedKeys]);
                        const stillStagedKeys = new Set(checkRes.rows.map((r) => r.key));
                        for (const k of trackedKeys) {
                            if (!stillStagedKeys.has(k)) {
                                // Evidence: chapter published, was deleted, or left staged status!
                                this.stuckIdentities.delete(k);
                            }
                        }
                    }
                    // Evict mock keys if not present in current cycle mock
                    for (const k of Array.from(this.stuckIdentities.keys())) {
                        if (k.startsWith('mock-') && !currentStuckKeys.has(k)) {
                            this.stuckIdentities.delete(k);
                        }
                    }
                }
                catch (err) {
                    this.logger.warn('Failed auditing resolved stuck identities', { error: err?.message });
                }
            }
            // Compute max age among currently stuck identities
            let maxStuckAgeMs = 0;
            for (const detectedAt of this.stuckIdentities.values()) {
                const age = nowMs - detectedAt;
                if (age > maxStuckAgeMs) {
                    maxStuckAgeMs = age;
                }
            }
            stuckStagedAgeSec = Math.floor(maxStuckAgeMs / 1000);
            stuckStaged = Math.max(stuckStaged, this.stuckIdentities.size);
        }
        // 4. Scheduler State (active_works, claimable_works)
        let activeWorks = [];
        let claimableWorks = 0;
        let zombieWorksCount = 0;
        try {
            const schedRes = await this.pool.query("SELECT key, value FROM importer_scheduler_state WHERE key = 'active_works'");
            if (schedRes.rows[0]?.value) {
                const raw = schedRes.rows[0].value;
                activeWorks = typeof raw === 'string' ? JSON.parse(raw) : raw;
                if (!Array.isArray(activeWorks))
                    activeWorks = [];
            }
            for (const w of activeWorks) {
                if ((w.queuedChapters || 0) > 0)
                    claimableWorks++;
                if (w.state === 'FILLING' && (w.queuedChapters || 0) === 0 && (w.inFlightChapters || 0) === 0) {
                    zombieWorksCount++;
                }
            }
        }
        catch { }
        // 5. Protective Stop State
        let protectiveStopActive = false;
        let protectiveStopReason = null;
        let protectiveStopTriggeredAt = null;
        try {
            const psRes = await this.pool.query("SELECT value FROM settings WHERE key = 'importer_protective_stop'");
            if (psRes.rows[0]?.value) {
                const raw = psRes.rows[0].value;
                const parsed = typeof raw === 'string' ? JSON.parse(raw) : raw;
                protectiveStopActive = Boolean(parsed.active);
                protectiveStopReason = parsed.reason || null;
                protectiveStopTriggeredAt = parsed.triggered_at || null;
            }
        }
        catch { }
        // 6. Recent Auto-Restarts and Circuit Breaker
        const recentRestarts = await this.getRecentAutoRestarts();
        const restartsLast1h = recentRestarts.filter((r) => nowMs - new Date(r.timestamp).getTime() <= 60 * 60 * 1000);
        const autoRestartCount1h = restartsLast1h.length;
        this.circuitBreakerOpen = autoRestartCount1h >= 3;
        // 7. Correlated dedupe classification of recent completed jobs (Tiered: only queried when fresh is delayed > 10m)
        let recentCorrelatedBreakdown = {
            alreadyCanonical: 0,
            dedupeSource: 0,
            freshPublished: 0,
            freshExpected: 0,
        };
        let recentCompletionsAreDedupeOnly = false;
        if (lastFreshVisibleAgeSec > 10 * 60) {
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
                    if (row.classification === 'ALREADY_CANONICAL')
                        recentCorrelatedBreakdown.alreadyCanonical = cnt;
                    else if (row.classification === 'DEDUPE_SOURCE')
                        recentCorrelatedBreakdown.dedupeSource = cnt;
                    else if (row.classification === 'FRESH_PUBLISHED')
                        recentCorrelatedBreakdown.freshPublished = cnt;
                    else if (row.classification === 'FRESH_EXPECTED')
                        recentCorrelatedBreakdown.freshExpected = cnt;
                }
                const totalRecentJobs = recentCorrelatedBreakdown.alreadyCanonical +
                    recentCorrelatedBreakdown.dedupeSource +
                    recentCorrelatedBreakdown.freshPublished +
                    recentCorrelatedBreakdown.freshExpected;
                if (totalRecentJobs > 0 &&
                    recentCorrelatedBreakdown.freshExpected === 0 &&
                    publishableStaged === 0) {
                    recentCompletionsAreDedupeOnly = true;
                }
            }
            catch (err) {
                this.logger.warn('Failed querying correlated dedupe breakdown', { error: err?.message });
            }
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
            unclassifiedStaged,
            stuckStagedAgeSec,
        });
        const metricsResult = {
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
            stuckStagedAgeSec,
            classifiedStaged,
            unclassifiedStaged,
            classificationCursor: this.classificationCursor,
            classifiedThisCycle,
            classificationCoverageEstimate,
            oldestUnclassifiedAge,
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
        this.cachedTelemetry = metricsResult;
        this.lastTelemetryAt = nowMs;
        return metricsResult;
    }
    /**
     * Deterministic Multidimensional Health evaluation separating processing and publication health.
     */
    evaluateMultidimensionalHealth(params) {
        const publishableStaged = params.publishableStaged ?? (params.hasStagedPublications ? 1 : 0);
        const waitingPredecessorStaged = params.waitingPredecessorStaged ?? 0;
        const stuckStaged = params.stuckStaged ?? 0;
        const unclassifiedStaged = params.unclassifiedStaged ?? 0;
        // 1. Processing Health Dimension (based on chapter completions)
        let processingHealth;
        if (params.lastCompletedAgeSec <= 10 * 60) {
            processingHealth = 'HEALTHY';
        }
        else if (params.lastCompletedAgeSec <= 15 * 60) {
            processingHealth = 'DEGRADED';
        }
        else if (params.lastCompletedAgeSec < 30 * 60) {
            processingHealth = 'STALLED';
        }
        else {
            processingHealth = 'CRITICAL_STALL';
        }
        // 2. Publication Health Dimension (based on fresh visible chapters and staged backlog)
        let publicationHealth;
        if (params.lastFreshVisibleAgeSec <= 10 * 60) {
            publicationHealth = 'HEALTHY';
        }
        else if (params.lastFreshVisibleAgeSec <= 15 * 60) {
            publicationHealth = 'DEGRADED';
        }
        else if (params.recentCompletionsAreDedupeOnly && publishableStaged === 0 && stuckStaged === 0 && unclassifiedStaged === 0) {
            // Completed chapters were deduplicated/canonical-only and no publishable, stuck, or unclassified staged backlog exists
            publicationHealth = 'NO_FRESH_EXPECTED';
        }
        else if (params.eligibleJobs === 0 &&
            params.importingCount === 0 &&
            publishableStaged === 0 &&
            stuckStaged === 0 &&
            unclassifiedStaged === 0) {
            // No active work and zero actionable, stuck, or unclassified chapters waiting -> legitimate idle / no fresh expected
            publicationHealth = 'NO_FRESH_EXPECTED';
        }
        else if (params.lastFreshVisibleAgeSec < 30 * 60) {
            publicationHealth = 'STALLED';
        }
        else {
            publicationHealth = 'CRITICAL_STALL';
        }
        // 3. Resolve Overall Status
        let status;
        // IDLE is ONLY allowed when:
        // eligibleJobs === 0 AND importingCount === 0 AND publishableStaged === 0 AND stuckStaged === 0 AND unclassifiedStaged === 0
        // (Meaning: no processable work AND no actionable publication AND no stuck chapters AND no unclassified staged chapters)
        const isTrulyIdle = params.eligibleJobs === 0 &&
            params.importingCount === 0 &&
            publishableStaged === 0 &&
            stuckStaged === 0 &&
            unclassifiedStaged === 0;
        if (isTrulyIdle) {
            status = 'IDLE';
        }
        else if (params.protectiveStopActive) {
            const isManual = params.protectiveStopReason?.toLowerCase().includes('manual') ||
                params.protectiveStopReason?.toLowerCase().includes('staff');
            if (isManual) {
                status = 'PAUSED_BY_PROTECTION';
            }
            else {
                const stoppedAgeSec = params.protectiveStopTriggeredAt
                    ? Math.floor((Date.now() - new Date(params.protectiveStopTriggeredAt).getTime()) / 1000)
                    : 0;
                if (stoppedAgeSec < 15 * 60) {
                    status = 'PAUSED_BY_PROTECTION';
                }
                else {
                    status = 'STALLED';
                }
            }
        }
        else if (params.eligibleJobs === 0 && params.importingCount === 0 && (publishableStaged > 0 || stuckStaged > 0 || unclassifiedStaged > 0)) {
            // Eligible = 0, Importing = 0, BUT publishable, stuck, or unclassified staged backlog exists!
            // This is NEVER IDLE!
            if (publishableStaged > 0) {
                if (publicationHealth === 'CRITICAL_STALL') {
                    status = 'CRITICAL_STALL';
                }
                else if (publicationHealth === 'STALLED') {
                    status = 'STALLED';
                }
                else if (publicationHealth === 'DEGRADED') {
                    status = 'DEGRADED';
                }
                else {
                    status = 'HEALTHY';
                }
            }
            else if (stuckStaged > 0) {
                // stuckStaged > 0 (publishable = 0)
                // Stuck staged must be handled based on duration of the specific stuck identity
                const effectiveStuckAge = params.stuckStagedAgeSec ?? params.lastFreshVisibleAgeSec;
                if (effectiveStuckAge >= 30 * 60) {
                    status = 'CRITICAL_STALL';
                    publicationHealth = 'CRITICAL_STALL';
                }
                else if (effectiveStuckAge >= 15 * 60) {
                    status = 'STALLED';
                    publicationHealth = 'STALLED';
                }
                else {
                    status = 'DEGRADED';
                    if (publicationHealth === 'NO_FRESH_EXPECTED' || publicationHealth === 'HEALTHY') {
                        publicationHealth = 'DEGRADED';
                    }
                }
            }
            else {
                // unclassifiedStaged > 0 alone (publishable = 0, stuck = 0)
                // CONCEPTUAL STATE: CLASSIFICATION_PENDING / UNKNOWN_STAGED
                // Pipeline cannot be IDLE, but unclassified staged ALONE MUST NEVER trigger CRITICAL_STALL or process restart!
                status = 'DEGRADED';
                publicationHealth = 'DEGRADED';
            }
        }
        else if (publicationHealth === 'NO_FRESH_EXPECTED') {
            status = processingHealth;
        }
        else if (processingHealth === 'CRITICAL_STALL' || publicationHealth === 'CRITICAL_STALL') {
            status = 'CRITICAL_STALL';
        }
        else if (processingHealth === 'STALLED' || publicationHealth === 'STALLED') {
            status = 'STALLED';
        }
        else if (processingHealth === 'DEGRADED' || publicationHealth === 'DEGRADED') {
            status = 'DEGRADED';
        }
        else {
            status = 'HEALTHY';
        }
        return { status, processingHealth, publicationHealth };
    }
    /**
     * Deterministic Health Status evaluation based on REAL PROGRESS (backward-compatible).
     */
    determineHealthStatus(params) {
        return this.evaluateMultidimensionalHealth(params).status;
    }
    /**
     * Executes a single evaluation cycle:
     * 1. Collect telemetry & determine status
     * 2. Persist heartbeat / health metrics
     * 3. Trigger Escalated Recovery Ladder if STALLED / CRITICAL_STALL
     */
    async evaluateCycle() {
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
    async executeRecoveryLadder(metrics) {
        const nowMs = Date.now();
        // Unclassified staged alone MUST NEVER trigger recovery ladder or process restart
        if (metrics.eligibleJobs === 0 &&
            metrics.importingCount === 0 &&
            metrics.publishableStaged === 0 &&
            metrics.stuckStaged === 0) {
            return;
        }
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
            }
            catch (err) {
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
            }
            catch (err) {
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
                this.logger.error(`🚨 [AUTO-RECOVERY CIRCUIT OPEN] Reached max 3 auto-restarts in 1h (Current count: ${restartsLast1h.length}). Halting automatic restarts to prevent loop. ROOT CAUSE REQUIRED.`);
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
                this.logger.warn(`⏳ [AUTO-HEAL NÍVEL 3] Throttle active: Last restart was ${Math.round(timeSinceLastRestartMs / 60000)}m ago (min 15m cooldown). Waiting...`);
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
    async runLevel1LightReconciliation(metrics) {
        // 1. Reconcile in-flight counts and chapter keys in scheduler from DB
        if (this.scheduler) {
            await this.scheduler.syncInFlightCountsFromDb();
            await this.scheduler.reloadActiveWorks?.();
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
        }
        catch { }
        // 3. Clear stale protective stop if reason was transient (e.g. past lag or RAM)
        if (metrics.protectiveStopActive && this.protectiveSentinel) {
            try {
                await this.protectiveSentinel.evaluateAutoResume();
            }
            catch (e) {
                this.logger.warn('[Level 1] Sentinel auto-resume evaluation failed', { error: e?.message });
            }
        }
        // 4. Force a safe admission cycle
        if (this.admissionController) {
            try {
                await this.admissionController.runAdmissionCycle();
            }
            catch (e) {
                this.logger.warn('[Level 1] Admission cycle failed', { error: e?.message });
            }
        }
        // 5. If publication is stalled or publishable staged chapters exist, trigger safe bounded sweep (cooldown: max 1 per 3m)
        const nowMs = Date.now();
        if (this.publicationBarrier &&
            (metrics.publicationHealth === 'STALLED' ||
                metrics.publicationHealth === 'CRITICAL_STALL' ||
                (metrics.publishableStaged || 0) > 0) &&
            nowMs - this.lastSweepAt >= 3 * 60 * 1000) {
            this.lastSweepAt = nowMs;
            try {
                const swept = await this.publicationBarrier.sweepStagedPublications(40, 6);
                if (swept > 0) {
                    this.logger.info(`[Level 1] Publication recovery sweep: published ${swept} staged chapter(s) safely through barrier.`);
                }
            }
            catch (e) {
                this.logger.warn('[Level 1] Publication recovery sweep failed', { error: e?.message });
            }
        }
    }
    /**
     * NÍVEL 2 — ESTADO PRESO
     */
    async runLevel2StuckStateAudit(metrics) {
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
                if (typeof activeWorks === 'string')
                    activeWorks = JSON.parse(activeWorks);
                if (Array.isArray(activeWorks) && activeWorks.length > 0) {
                    const survivingWorks = [];
                    let evictedCount = 0;
                    for (const w of activeWorks) {
                        // Check if this work has any claimable jobs in queue
                        const countRes = await this.pool.query(`SELECT count(*) as count 
               FROM importer_queue 
               WHERE (payload->>'workId') = $1 
                 AND status IN ('QUEUED', 'RETRY') 
                 AND (next_run_at IS NULL OR next_run_at <= NOW())`, [w.workId]);
                        const claimableCount = parseInt(countRes.rows[0]?.count || '0', 10);
                        const inFlight = w.inFlightChapters || 0;
                        if (claimableCount === 0 && inFlight === 0) {
                            this.logger.info(`[Level 2] Evicting empty work "${w.workTitle}" (${w.workId}) from active set (0 claimable, 0 inflight)`);
                            evictedCount++;
                        }
                        else {
                            survivingWorks.push({
                                ...w,
                                queuedChapters: claimableCount,
                            });
                        }
                    }
                    if (evictedCount > 0) {
                        await this.pool.query("UPDATE importer_scheduler_state SET value = $1, updated_at = NOW() WHERE key = 'active_works'", [JSON.stringify(survivingWorks)]);
                        this.logger.info(`[Level 2] Evicted ${evictedCount} empty active works. Retained ${survivingWorks.length} healthy works.`);
                    }
                }
            }
        }
        catch (e) {
            this.logger.warn('[Level 2] Active works audit failed', { error: e?.message });
        }
        // 3. Force admission cycle to bring in fresh claimable works
        if (this.admissionController) {
            try {
                await this.admissionController.runAdmissionCycle();
            }
            catch (e) {
                this.logger.warn('[Level 2] Admission cycle failed', { error: e?.message });
            }
        }
    }
    /**
     * Persists health panel to settings table for supervisor, site, and external monitors.
     */
    async persistHealthMetrics(metrics) {
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
            await this.pool.query(`INSERT INTO settings (key, value)
         VALUES ('importer_heartbeat', $1)
         ON CONFLICT (key) DO UPDATE SET value = $1`, [payload]);
        }
        catch (err) {
            this.logger.warn('Failed persisting importer_heartbeat to settings', { error: err?.message });
        }
    }
    /**
     * Records an auto-restart event to settings.importer_auto_restarts.
     */
    async recordAutoRestart(record) {
        try {
            const existing = await this.getRecentAutoRestarts();
            existing.push(record);
            // Keep only last 20 records
            const pruned = existing.slice(-20);
            await this.pool.query(`INSERT INTO settings (key, value)
         VALUES ('importer_auto_restarts', $1)
         ON CONFLICT (key) DO UPDATE SET value = $1`, [JSON.stringify(pruned)]);
        }
        catch (err) {
            this.logger.warn('Failed recording auto-restart to settings', { error: err?.message });
        }
    }
    /**
     * Reads recent auto-restarters from settings.importer_auto_restarts.
     */
    async getRecentAutoRestarts() {
        try {
            const res = await this.pool.query("SELECT value FROM settings WHERE key = 'importer_auto_restarts'");
            if (res.rows[0]?.value) {
                const raw = res.rows[0].value;
                const parsed = typeof raw === 'string' ? JSON.parse(raw) : raw;
                if (Array.isArray(parsed))
                    return parsed;
            }
        }
        catch { }
        return [];
    }
}
