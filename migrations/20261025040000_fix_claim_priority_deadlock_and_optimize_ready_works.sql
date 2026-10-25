-- PROJECT NOX: Fix Claim Priority Deadlock & Optimize Ready Work Refresh
-- Migration: 20261025040000_fix_claim_priority_deadlock_and_optimize_ready_works.sql

-- 1. Optimize importer_lite_refresh_ready_work: evaluate work presentability once, not on every candidate task
CREATE OR REPLACE FUNCTION public.importer_lite_refresh_ready_work(p_work_key text)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $function$
DECLARE
  v_priority smallint;
  v_next timestamptz;
  v_operation_rank smallint;
  v_presentable boolean := false;
BEGIN
  IF p_work_key LIKE 'work:%' THEN
    SELECT EXISTS (
      SELECT 1
      FROM public.works work
      JOIN public.media cover ON cover.id = work.cover_id
      WHERE work.id = NULLIF(substring(p_work_key from 6), '')::uuid
        AND NULLIF(btrim(work.title), '') IS NOT NULL
        AND NULLIF(btrim(work.slug), '') IS NOT NULL
        AND cover.storage_ready = true AND cover.bytes >= 1500
    ) INTO v_presentable;
  END IF;

  SELECT priority, next_run_at,
    CASE task_type WHEN 'PUBLISH' THEN 0 WHEN 'IMPORT_CHAPTER' THEN 1 WHEN 'CATALOG' THEN 2 ELSE 3 END
    INTO v_priority, v_next, v_operation_rank
  FROM public.importer_lite_tasks task
  WHERE work_key = p_work_key AND state = 'READY' AND monitoring = false
    AND NOT (task_type = 'CATALOG' AND payload ? 'metadataSourceRefresh')
    AND (task.task_type NOT IN ('IMPORT_CHAPTER', 'PUBLISH') OR v_presentable)
  ORDER BY priority,
    CASE task_type WHEN 'PUBLISH' THEN 0 WHEN 'IMPORT_CHAPTER' THEN 1 WHEN 'CATALOG' THEN 2 ELSE 3 END,
    next_run_at, created_at
  LIMIT 1;

  IF NOT FOUND THEN
    DELETE FROM public.importer_lite_ready_works WHERE work_key = p_work_key;
  ELSE
    INSERT INTO public.importer_lite_ready_works(work_key, priority, operation_rank, next_run_at, updated_at)
    VALUES(p_work_key, v_priority, v_operation_rank, v_next, now())
    ON CONFLICT(work_key) DO UPDATE SET priority = excluded.priority, operation_rank = excluded.operation_rank,
      next_run_at = excluded.next_run_at, updated_at = now();
  END IF;
END;
$function$;

-- 2. Optimize importer_lite_claim_task: remove artificial priority filter within chosen work to prevent deadlock between chapter sequence and priority
CREATE OR REPLACE FUNCTION public.importer_lite_claim_task(p_worker_id text, p_lease interval DEFAULT interval '10 minutes')
RETURNS SETOF public.importer_lite_tasks
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $function$
DECLARE
  v_work_key text;
  v_priority smallint;
  v_task_id uuid;
  v_presentable boolean := false;
BEGIN
  -- A. SELECT NEXT READY WORK WITH FAIR ROUND-ROBIN & STRICT PRECEDENCE
  -- Precedence: STAFF (0) > P0 (1) > P1 (2) > P2 (3)
  -- Rank: PUBLISH (0) > IMPORT_CHAPTER (1) > CATALOG (2) > DISCOVER (3)
  -- Fairness: last_claimed_at NULLS FIRST (distributes workers across distinct works)
  SELECT work_key, priority INTO v_work_key, v_priority
  FROM public.importer_lite_ready_works
  WHERE next_run_at <= now()
  ORDER BY priority, operation_rank, last_claimed_at NULLS FIRST, next_run_at, work_key
  LIMIT 1 FOR UPDATE SKIP LOCKED;

  IF NOT FOUND THEN RETURN; END IF;

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

  IF NOT FOUND THEN
    PERFORM public.importer_lite_refresh_ready_work(v_work_key);
    RETURN;
  END IF;

  RETURN QUERY UPDATE public.importer_lite_tasks
    SET state = 'RUNNING',
        lease_owner = p_worker_id,
        lease_until = now() + p_lease,
        attempts = attempts + 1,
        updated_at = now()
    WHERE id = v_task_id
    RETURNING *;

  UPDATE public.importer_lite_ready_works
  SET last_claimed_at = now(),
      updated_at = now()
  WHERE work_key = v_work_key;

  PERFORM public.importer_lite_refresh_ready_work(v_work_key);
END;
$function$;
