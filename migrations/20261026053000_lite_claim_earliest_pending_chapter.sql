-- PROJECT NOX: make strict chapter ordering bounded for deep P1 backlogs.
--
-- The strict predecessor fence is correct, but the previous candidate query
-- evaluated that fence once for every READY chapter in a selected work. A
-- single 1,114-chapter P1 backlog consequently issued 1,114 remote index
-- probes before it could claim chapter one. Select the earliest pending
-- chapter once instead. The same READY/RETRY_WAIT predicate remains the
-- authority: a recent retry still blocks later chapters; null order keys keep
-- their legacy fallback semantics.

CREATE INDEX IF NOT EXISTS importer_lite_tasks_work_pending_chapter_order_idx
  ON public.importer_lite_tasks (work_key, (NULLIF(payload->>'orderKey', '')), id)
  WHERE task_type IN ('IMPORT_CHAPTER', 'PUBLISH')
    AND state IN ('READY', 'RETRY_WAIT');

CREATE OR REPLACE FUNCTION public.importer_lite_claim_task(p_worker_id text, p_lease interval DEFAULT interval '10 minutes')
RETURNS SETOF public.importer_lite_tasks
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $function$
DECLARE
  r RECORD;
  v_work_key text;
  v_task_id uuid;
  v_presentable boolean := false;
BEGIN
  FOR r IN (
    SELECT rw.work_key
    FROM public.importer_lite_ready_works rw
    WHERE rw.next_run_at <= now()
      AND NOT EXISTS (
        SELECT 1
        FROM public.importer_lite_tasks active_work
        WHERE active_work.work_key = rw.work_key
          AND active_work.task_type IN ('IMPORT_CHAPTER', 'PUBLISH')
          AND active_work.state = 'RUNNING'
      )
    ORDER BY rw.priority, rw.operation_rank, rw.last_claimed_at NULLS FIRST, rw.next_run_at, rw.work_key
    LIMIT 4
    FOR UPDATE OF rw SKIP LOCKED
  ) LOOP
    v_work_key := r.work_key;
    v_presentable := false;
    v_task_id := NULL;

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

    -- Select one canonical frontier for ordered chapter work. `monitoring`
    -- intentionally remains in this CTE: such a task must still fence a later
    -- public chapter even though it is not itself eligible for this claim.
    WITH earliest_pending_chapter AS (
      SELECT candidate.id, candidate.task_type, candidate.priority, candidate.state,
             candidate.next_run_at, candidate.created_at, candidate.monitoring,
             NULLIF(candidate.payload->>'orderKey', '') AS order_key
      FROM public.importer_lite_tasks candidate
      WHERE candidate.work_key = v_work_key
        AND candidate.task_type IN ('IMPORT_CHAPTER', 'PUBLISH')
        AND NULLIF(candidate.payload->>'orderKey', '') IS NOT NULL
        AND (
          candidate.state = 'READY'
          OR (candidate.state = 'RETRY_WAIT' AND candidate.attempts < candidate.max_attempts
              AND candidate.updated_at > now() - interval '15 minutes')
        )
      ORDER BY NULLIF(candidate.payload->>'orderKey', ''),
        CASE WHEN candidate.task_type = 'PUBLISH' THEN 0 ELSE 1 END,
        candidate.priority, candidate.next_run_at, candidate.created_at, candidate.id
      LIMIT 1
    ), candidate_pool AS (
      -- A due, presentable frontier is the only non-null ordered chapter that
      -- can pass the predecessor fence. The active-work test remains a rolling
      -- runtime race fence even though the ready-work row is already locked.
      SELECT pending.id, pending.task_type, pending.priority,
             pending.next_run_at, pending.created_at, pending.order_key,
             false AS historical_p1
      FROM earliest_pending_chapter pending
      WHERE pending.state = 'READY'
        AND pending.monitoring = false
        AND pending.next_run_at <= now()
        AND v_presentable
        AND NOT EXISTS (
          SELECT 1
          FROM public.importer_lite_tasks active_work
          WHERE active_work.work_key = v_work_key
            AND active_work.task_type IN ('IMPORT_CHAPTER', 'PUBLISH')
            AND active_work.state = 'RUNNING'
            AND active_work.id <> pending.id
        )

      UNION ALL

      -- Null/empty order keys historically do not compare as predecessors.
      -- Keep their legacy behavior separate instead of inventing an order.
      SELECT candidate.id, candidate.task_type, candidate.priority,
             candidate.next_run_at, candidate.created_at,
             NULLIF(candidate.payload->>'orderKey', '') AS order_key,
             false AS historical_p1
      FROM public.importer_lite_tasks candidate
      WHERE candidate.work_key = v_work_key
        AND candidate.state = 'READY'
        AND candidate.monitoring = false
        AND candidate.next_run_at <= now()
        AND candidate.task_type IN ('IMPORT_CHAPTER', 'PUBLISH')
        AND NULLIF(candidate.payload->>'orderKey', '') IS NULL
        AND v_presentable
        AND NOT EXISTS (
          SELECT 1
          FROM public.importer_lite_tasks active_work
          WHERE active_work.work_key = v_work_key
            AND active_work.task_type IN ('IMPORT_CHAPTER', 'PUBLISH')
            AND active_work.state = 'RUNNING'
            AND active_work.id <> candidate.id
        )

      UNION ALL

      -- Non-chapter work retains the existing priority and historical P1
      -- ordering. It never bypasses a chapter predecessor barrier.
      SELECT candidate.id, candidate.task_type, candidate.priority,
             candidate.next_run_at, candidate.created_at,
             NULLIF(candidate.payload->>'orderKey', '') AS order_key,
             (candidate.task_type = 'CATALOG' AND candidate.priority = 2
               AND candidate.payload ? 'historicalP1Backfill'
               AND COALESCE(candidate.payload->>'deferBehindReadyImport', 'false') <> 'true') AS historical_p1
      FROM public.importer_lite_tasks candidate
      WHERE candidate.work_key = v_work_key
        AND candidate.state = 'READY'
        AND candidate.monitoring = false
        AND candidate.next_run_at <= now()
        AND NOT (candidate.task_type = 'CATALOG' AND candidate.payload ? 'metadataSourceRefresh')
        AND candidate.task_type NOT IN ('IMPORT_CHAPTER', 'PUBLISH')
    ), selected_candidate AS (
      SELECT id
      FROM candidate_pool
      ORDER BY
        CASE
          WHEN task_type = 'PUBLISH' THEN 0
          WHEN task_type = 'IMPORT_CHAPTER' THEN 1
          WHEN task_type = 'CATALOG' AND priority = 2 AND historical_p1
            THEN 1
          WHEN task_type = 'CATALOG' THEN 2
          ELSE 3
        END,
        CASE WHEN task_type IN ('PUBLISH', 'IMPORT_CHAPTER') THEN order_key END NULLS LAST,
        priority, next_run_at, created_at, id
      LIMIT 1
    )
    SELECT candidate.id INTO v_task_id
    FROM public.importer_lite_tasks candidate
    JOIN selected_candidate selected ON selected.id = candidate.id
    FOR UPDATE OF candidate SKIP LOCKED;

    IF v_task_id IS NOT NULL THEN
      UPDATE public.importer_lite_ready_works
      SET last_claimed_at = now(),
          next_run_at = CASE WHEN v_work_key LIKE 'work:%' THEN now() + interval '30 seconds' ELSE now() END,
          updated_at = now()
      WHERE work_key = v_work_key;

      RETURN QUERY UPDATE public.importer_lite_tasks
      SET state = 'RUNNING', lease_owner = p_worker_id, lease_until = now() + p_lease,
          attempts = attempts + 1, updated_at = now()
      WHERE id = v_task_id
      RETURNING *;
      RETURN;
    END IF;

    UPDATE public.importer_lite_ready_works
    SET last_claimed_at = now(), next_run_at = now() + interval '10 seconds', updated_at = now()
    WHERE work_key = v_work_key;
  END LOOP;
  RETURN;
END;
$function$;
