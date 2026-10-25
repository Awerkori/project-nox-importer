import { Logger } from './logger.js';
export class CoverVisionQueue {
    pool;
    logger = new Logger('CoverVisionQueue');
    schemaReady = false;
    constructor(pool) {
        this.pool = pool;
    }
    async ensureSchema() {
        if (this.schemaReady)
            return;
        try {
            const check = await this.pool.query(`
        SELECT (to_regclass('public.cover_vision_work_tasks') IS NOT NULL) AS ok
      `);
            if (check.rows[0]?.ok) {
                await this.pool.query(`
          CREATE INDEX IF NOT EXISTS idx_cover_vision_tasks_due_v2
            ON public.cover_vision_work_tasks (status, next_retry_at, attempts, updated_at);
        `);
                this.schemaReady = true;
                return;
            }
        }
        catch {
            // Fall through to table creation
        }
        try {
            await this.pool.query(`
        CREATE TABLE IF NOT EXISTS public.cover_vision_work_tasks (
          work_id uuid PRIMARY KEY,
          candidate_covers jsonb NOT NULL,
          status text NOT NULL DEFAULT 'PENDING',
          attempts integer NOT NULL DEFAULT 0,
          max_attempts integer NOT NULL DEFAULT 3,
          inspected_urls text[] NOT NULL DEFAULT '{}'::text[],
          detected_title text,
          attributing_source text,
          confidence numeric(5,4),
          evidence jsonb,
          last_error text,
          created_at timestamptz NOT NULL DEFAULT now(),
          updated_at timestamptz NOT NULL DEFAULT now(),
          next_retry_at timestamptz
        );
        CREATE INDEX IF NOT EXISTS idx_cover_vision_tasks_due_v2
          ON public.cover_vision_work_tasks (status, next_retry_at, attempts, updated_at);
      `);
            this.schemaReady = true;
        }
        catch (err) {
            this.logger.warn('Failed to ensure cover_vision_work_tasks schema', { error: err?.message });
        }
    }
    async enqueueTask(workId, candidateCovers, maxAttempts = 3) {
        if (!workId || !candidateCovers.length)
            return;
        await this.ensureSchema();
        const validIncoming = candidateCovers.filter((c) => c && c.coverUrl && c.source);
        if (!validIncoming.length)
            return;
        // Check existing task for this work
        const existingRes = await this.pool.query(`SELECT status, candidate_covers, inspected_urls
       FROM public.cover_vision_work_tasks
       WHERE work_id = $1::uuid LIMIT 1`, [workId]);
        const existing = existingRes.rows[0];
        if (!existing) {
            // Brand new task - insert directly in PENDING status
            await this.pool.query(`INSERT INTO public.cover_vision_work_tasks (
          work_id, candidate_covers, status, max_attempts, next_retry_at, updated_at
        ) VALUES ($1::uuid, $2::jsonb, 'PENDING', $3, NULL, now())
        ON CONFLICT (work_id) DO NOTHING`, [workId, JSON.stringify(validIncoming), maxAttempts]);
            return;
        }
        // If task has already successfully found PT-BR title, do not re-enqueue
        if (existing.status === 'COMPLETED') {
            return;
        }
        const inspectedSet = new Set(existing.inspected_urls || []);
        const existingCandidates = Array.isArray(existing.candidate_covers) ? existing.candidate_covers : [];
        // Merge candidate covers, keeping unique by coverUrl
        const mergedMap = new Map();
        for (const c of existingCandidates) {
            if (c?.coverUrl)
                mergedMap.set(c.coverUrl, c);
        }
        for (const c of validIncoming) {
            if (c?.coverUrl && !mergedMap.has(c.coverUrl)) {
                mergedMap.set(c.coverUrl, c);
            }
        }
        const mergedCandidates = Array.from(mergedMap.values());
        // Check if there is any candidate cover that has NOT been inspected yet
        const hasUninspectedCovers = mergedCandidates.some((c) => !inspectedSet.has(c.coverUrl));
        if (existing.status === 'NO_PTBR_FOUND' || existing.status === 'FAILED') {
            if (hasUninspectedCovers) {
                // Reopen task to PENDING because fresh, uninspected covers are now available
                await this.pool.query(`UPDATE public.cover_vision_work_tasks
           SET candidate_covers = $1::jsonb,
               status = 'PENDING',
               attempts = 0,
               next_retry_at = NULL,
               last_error = NULL,
               updated_at = now()
           WHERE work_id = $2::uuid`, [JSON.stringify(mergedCandidates), workId]);
            }
            else {
                // All incoming covers are already in inspected_urls; no new work to do
                if (mergedCandidates.length > existingCandidates.length) {
                    await this.pool.query(`UPDATE public.cover_vision_work_tasks
             SET candidate_covers = $1::jsonb,
                 updated_at = now()
             WHERE work_id = $2::uuid`, [JSON.stringify(mergedCandidates), workId]);
                }
            }
            return;
        }
        // If already PENDING or RUNNING:
        // Update candidate_covers with any newly discovered candidates
        await this.pool.query(`UPDATE public.cover_vision_work_tasks
       SET candidate_covers = $1::jsonb,
           updated_at = now()
       WHERE work_id = $2::uuid`, [JSON.stringify(mergedCandidates), workId]);
    }
    async findDueTasks(limit = 2) {
        await this.ensureSchema();
        const res = await this.pool.query(`SELECT
        work_id, candidate_covers, status, attempts, max_attempts,
        inspected_urls, detected_title, attributing_source, confidence,
        evidence, last_error, created_at, updated_at, next_retry_at
      FROM public.cover_vision_work_tasks
      WHERE (
        -- PENDING tasks whose retry delay has passed and haven't exceeded max attempts
        status = 'PENDING'
        AND attempts < max_attempts
        AND (next_retry_at IS NULL OR next_retry_at <= now())
      ) OR (
        -- Abandoned RUNNING tasks from crashed or restarted processes (> 5 minutes old)
        status = 'RUNNING'
        AND attempts < max_attempts
        AND updated_at <= now() - interval '5 minutes'
      )
      ORDER BY
        CASE WHEN status = 'RUNNING' THEN 0 ELSE 1 END ASC,
        COALESCE(next_retry_at, created_at) ASC
      LIMIT $1`, [limit]);
        return res.rows.map((r) => ({
            workId: r.work_id,
            candidateCovers: Array.isArray(r.candidate_covers) ? r.candidate_covers : [],
            status: r.status,
            attempts: Number(r.attempts || 0),
            maxAttempts: Number(r.max_attempts || 3),
            inspectedUrls: Array.isArray(r.inspected_urls) ? r.inspected_urls : [],
            detectedTitle: r.detected_title,
            attributingSource: r.attributing_source,
            confidence: r.confidence ? Number(r.confidence) : null,
            evidence: r.evidence,
            lastError: r.last_error,
            createdAt: r.created_at,
            updatedAt: r.updated_at,
            nextRetryAt: r.next_retry_at,
        }));
    }
    async markRunning(workId) {
        await this.pool.query(`UPDATE public.cover_vision_work_tasks
       SET status = 'RUNNING', updated_at = now()
       WHERE work_id = $1::uuid`, [workId]);
    }
    async markCompleted(workId, result) {
        await this.pool.query(`UPDATE public.cover_vision_work_tasks
       SET status = 'COMPLETED',
           detected_title = $1,
           attributing_source = $2,
           confidence = $3,
           evidence = $4::jsonb,
           inspected_urls = $5::text[],
           next_retry_at = NULL,
           updated_at = now()
       WHERE work_id = $6::uuid`, [
            result.detectedTitle,
            result.attributingSource,
            result.confidence,
            JSON.stringify(result.evidence || {}),
            result.inspectedUrls,
            workId,
        ]);
    }
    async markNoPtBrFound(workId, inspectedUrls) {
        await this.pool.query(`UPDATE public.cover_vision_work_tasks
       SET status = 'NO_PTBR_FOUND',
           attempts = attempts + 1,
           inspected_urls = $1::text[],
           next_retry_at = NULL,
           updated_at = now()
       WHERE work_id = $2::uuid`, [inspectedUrls, workId]);
    }
    async markFailed(workId, error, inspectedUrls, maxAttempts = 3, retryDelayMinutes = 5) {
        await this.pool.query(`UPDATE public.cover_vision_work_tasks
       SET status = CASE WHEN attempts + 1 >= $3 THEN 'FAILED' ELSE 'PENDING' END,
           attempts = attempts + 1,
           last_error = $1,
           inspected_urls = $2::text[],
           next_retry_at = CASE WHEN attempts + 1 >= $3 THEN NULL ELSE now() + ($5 || ' minutes')::interval END,
           updated_at = now()
       WHERE work_id = $4::uuid`, [error, inspectedUrls, maxAttempts, workId, retryDelayMinutes]);
    }
    async getTask(workId) {
        await this.ensureSchema();
        const res = await this.pool.query(`SELECT
        work_id, candidate_covers, status, attempts, max_attempts,
        inspected_urls, detected_title, attributing_source, confidence,
        evidence, last_error, created_at, updated_at, next_retry_at
      FROM public.cover_vision_work_tasks
      WHERE work_id = $1::uuid LIMIT 1`, [workId]);
        if (!res.rows[0])
            return null;
        const r = res.rows[0];
        return {
            workId: r.work_id,
            candidateCovers: Array.isArray(r.candidate_covers) ? r.candidate_covers : [],
            status: r.status,
            attempts: Number(r.attempts || 0),
            maxAttempts: Number(r.max_attempts || 3),
            inspectedUrls: Array.isArray(r.inspected_urls) ? r.inspected_urls : [],
            detectedTitle: r.detected_title,
            attributingSource: r.attributing_source,
            confidence: r.confidence ? Number(r.confidence) : null,
            evidence: r.evidence,
            lastError: r.last_error,
            createdAt: r.created_at,
            updatedAt: r.updated_at,
            nextRetryAt: r.next_retry_at,
        };
    }
}
