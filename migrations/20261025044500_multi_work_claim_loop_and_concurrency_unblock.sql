-- PROJECT NOX: Multi-Work Claim Loop & Concurrency Unblock
-- Migration: 20261025044500_multi_work_claim_loop_and_concurrency_unblock.sql
-- Eliminates single-work claim bottleneck: allows up to 20 chapter workers
-- to claim tasks across distinct works without getting starved by in-flight or barrier-delayed works.

CREATE OR REPLACE FUNCTION public.importer_lite_claim_task(p_worker_id text, p_lease interval DEFAULT interval '10 minutes')
RETURNS SETOF public.importer_lite_tasks
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $function$
DECLARE
  r RECORD;
  v_work_key text;
  v_priority smallint;
  v_task_id uuid;
  v_presentable boolean := false;
BEGIN
  -- A. LOOP OVER TOP CANDIDATE WORKS WITH FAIR ROUND-ROBIN & STRICT PRECEDENCE
  -- Precedence: STAFF (0) > P0 (1) > P1 (2) > P2 (3)
  -- Rank: PUBLISH (0) > IMPORT_CHAPTER (1) > CATALOG (2) > DISCOVER (3)
  -- Fairness: last_claimed_at NULLS FIRST (distributes workers across distinct works)
  FOR r IN (
    SELECT rw.work_key, rw.priority
    FROM public.importer_lite_ready_works rw
    WHERE rw.next_run_at <= now()
    ORDER BY rw.priority, rw.operation_rank, rw.last_claimed_at NULLS FIRST, rw.next_run_at, rw.work_key
    LIMIT 20
    FOR UPDATE OF rw SKIP LOCKED
  ) LOOP
    v_work_key := r.work_key;
    v_priority := r.priority;
    v_presentable := false;
    v_task_id := NULL;

    -- B. WORK PRESENTABILITY CHECK (COVER, TITLE, SLUG)
    IF v_work_key LIKE 'work:%' THEN
      SELECT EXISTS (
        SELECT 1
        FROM public.works work
        JOIN public.media cover ON cover.id = work.cover_id
        WHERE work.id = NULLIF(substring(v_work_key from 6), '')::uuid
          AND NULLIF(btrim(work.title), '') IS NOT NULL
          AND NULLIF(btrim(work.slug), '') IS NOT NULL
          AND cover.storage_ready = true AND cover.bytes >= 1500
      ) INTO v_presentable;
    END IF;

    -- C. CANDIDATE TASK SELECTION WITHIN CHOSEN WORK
    -- Strict sequential order:
    -- 1) Prioritize PUBLISH over IMPORT_CHAPTER
    -- 2) Sort chapters strictly by orderKey ascending (e.g. 61 -> 62 -> 63)
    -- 3) Concurrency Isolation: Never dispatch 2 chapters of the same work concurrently
    -- 4) Strict prior-chapter integrity barrier:
    --    Never claim chapter K if prior confirmed chapter J < K has a pending active task (READY)
    --    or is in recent retry (RETRY_WAIT < 15 minutes with attempts remaining).
    --    If prior chapter failed permanently (BLOCKED) or timed out (> 15 minutes), the barrier lifts.
    SELECT candidate.id INTO v_task_id
    FROM public.importer_lite_tasks candidate
    WHERE candidate.work_key = v_work_key
      AND candidate.state = 'READY'
      AND candidate.monitoring = false
      AND candidate.next_run_at <= now()
      AND NOT (candidate.task_type = 'CATALOG' AND candidate.payload ? 'metadataSourceRefresh')
      AND (candidate.task_type NOT IN ('IMPORT_CHAPTER', 'PUBLISH') OR v_presentable)
      AND NOT (
        candidate.task_type IN ('IMPORT_CHAPTER', 'PUBLISH')
        AND (
          EXISTS (
            SELECT 1 FROM public.importer_lite_tasks active_work
            WHERE active_work.work_key = candidate.work_key
              AND active_work.task_type IN ('IMPORT_CHAPTER', 'PUBLISH')
              AND active_work.state = 'RUNNING'
              AND active_work.id <> candidate.id
          )
          OR EXISTS (
            SELECT 1 FROM public.importer_lite_tasks prev
            WHERE prev.work_key = candidate.work_key
              AND prev.task_type IN ('IMPORT_CHAPTER', 'PUBLISH')
              AND prev.id <> candidate.id
              AND NULLIF(prev.payload->>'orderKey', '') < NULLIF(candidate.payload->>'orderKey', '')
              AND (
                prev.state = 'READY'
                OR (prev.state = 'RETRY_WAIT' AND prev.attempts < prev.max_attempts AND prev.updated_at > now() - interval '15 minutes')
              )
          )
        )
      )
    ORDER BY
      CASE
        WHEN candidate.task_type = 'PUBLISH' THEN 0
        WHEN candidate.task_type = 'IMPORT_CHAPTER' THEN 1
        WHEN candidate.task_type = 'CATALOG' AND candidate.priority = 2
          AND candidate.payload ? 'historicalP1Backfill'
          AND COALESCE(candidate.payload->>'deferBehindReadyImport', 'false') <> 'true' THEN 1
        WHEN candidate.task_type = 'CATALOG' THEN 2
        ELSE 3
      END,
      CASE
        WHEN candidate.task_type IN ('PUBLISH', 'IMPORT_CHAPTER')
        THEN NULLIF(candidate.payload->>'orderKey', '')
      END NULLS LAST,
      candidate.priority ASC,
      candidate.next_run_at, candidate.created_at, candidate.id
    LIMIT 1 FOR UPDATE SKIP LOCKED;

    IF v_task_id IS NOT NULL THEN
      -- D. TASK CLAIM FOUND - CLAIM & RETURN IMMEDIATELY
      UPDATE public.importer_lite_ready_works
      SET last_claimed_at = now(),
          next_run_at = CASE 
            WHEN v_work_key LIKE 'work:%' THEN now() + interval '30 seconds'
            ELSE now()
          END,
          updated_at = now()
      WHERE work_key = v_work_key;

      RETURN QUERY UPDATE public.importer_lite_tasks
        SET state = 'RUNNING',
            lease_owner = p_worker_id,
            lease_until = now() + p_lease,
            attempts = attempts + 1,
            updated_at = now()
        WHERE id = v_task_id
        RETURNING *;

      RETURN;
    ELSE
      -- Work has no currently claimable task (e.g. running task, or barrier delay).
      -- Touch last_claimed_at and back off next_run_at by 10s so other works can be claimed.
      UPDATE public.importer_lite_ready_works
      SET last_claimed_at = now(),
          next_run_at = now() + interval '10 seconds',
          updated_at = now()
      WHERE work_key = v_work_key;
    END IF;
  END LOOP;

  RETURN;
END;
$function$;
