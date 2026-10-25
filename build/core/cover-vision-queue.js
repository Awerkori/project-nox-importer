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
        CREATE INDEX IF NOT EXISTS idx_cover_vision_tasks_due
          ON public.cover_vision_work_tasks (status, next_retry_at, created_at);
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
        await this.pool.query(`INSERT INTO public.cover_vision_work_tasks (
        work_id, candidate_covers, status, max_attempts, updated_at
      ) VALUES ($1::uuid, $2::jsonb, 'PENDING', $3, now())
      ON CONFLICT (work_id) DO UPDATE SET
        candidate_covers = EXCLUDED.candidate_covers,
        updated_at = now()
      WHERE public.cover_vision_work_tasks.status IN ('PENDING', 'FAILED', 'NO_PTBR_FOUND')`, [workId, JSON.stringify(candidateCovers), maxAttempts]);
    }
    async findDueTasks(limit = 2) {
        await this.ensureSchema();
        const res = await this.pool.query(`SELECT
        work_id, candidate_covers, status, attempts, max_attempts,
        inspected_urls, detected_title, attributing_source, confidence,
        evidence, last_error, created_at, updated_at, next_retry_at
      FROM public.cover_vision_work_tasks
      WHERE status IN ('PENDING', 'RUNNING')
         OR (status = 'FAILED' AND attempts < max_attempts AND (next_retry_at IS NULL OR next_retry_at <= now()))
      ORDER BY created_at ASC
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
           updated_at = now()
       WHERE work_id = $2::uuid`, [inspectedUrls, workId]);
    }
    async markFailed(workId, error, inspectedUrls, maxAttempts = 3) {
        await this.pool.query(`UPDATE public.cover_vision_work_tasks
       SET status = CASE WHEN attempts + 1 >= $3 THEN 'FAILED' ELSE 'PENDING' END,
           attempts = attempts + 1,
           last_error = $1,
           inspected_urls = $2::text[],
           next_retry_at = now() + interval '5 minutes',
           updated_at = now()
       WHERE work_id = $4::uuid`, [error, inspectedUrls, maxAttempts, workId]);
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
