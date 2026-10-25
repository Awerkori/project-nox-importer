-- PROJECT NOX: bound fair claim probes to the verified YSQL budget.
--
-- The multi-work claim loop correctly escaped a blocked work, but under a
-- 13-connection cluster it locked and inspected up to 20 works per serialized
-- claim. Each inspection includes the strict predecessor barrier, turning an
-- idle-worker recovery into seconds of remote reads. Four probes preserve
-- fairness: an unclaimable work is explicitly deferred for 10 seconds, so the
-- next claim immediately reaches the next distinct work without rescanning it.
--
-- The active-work predicate uses importer_lite_tasks_work_active_chapter_idx
-- introduced by the strict-order migration. It only skips a work that already
-- owns an in-flight chapter/publication lease; it does not change priority,
-- ordering, retries, gaps, or P2 eligibility.

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
          -- Keep the in-work check as a race fence. The ready-work row lock
          -- makes it normally redundant, but a rolling runtime can still
          -- settle a previously leased task between the outer probe and this
          -- task selection.
          EXISTS (
            SELECT 1
            FROM public.importer_lite_tasks active_work
            WHERE active_work.work_key = candidate.work_key
              AND active_work.task_type IN ('IMPORT_CHAPTER', 'PUBLISH')
              AND active_work.state = 'RUNNING'
              AND active_work.id <> candidate.id
          )
          OR EXISTS (
            SELECT 1
            FROM public.importer_lite_tasks prev
            WHERE prev.work_key = candidate.work_key
              AND prev.task_type IN ('IMPORT_CHAPTER', 'PUBLISH')
              AND prev.id <> candidate.id
              AND NULLIF(prev.payload->>'orderKey', '') < NULLIF(candidate.payload->>'orderKey', '')
              AND (
                prev.state = 'READY'
                OR (prev.state = 'RETRY_WAIT' AND prev.attempts < prev.max_attempts
                    AND prev.updated_at > now() - interval '15 minutes')
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
      CASE WHEN candidate.task_type IN ('PUBLISH', 'IMPORT_CHAPTER')
        THEN NULLIF(candidate.payload->>'orderKey', '') END NULLS LAST,
      candidate.priority ASC, candidate.next_run_at, candidate.created_at, candidate.id
    LIMIT 1 FOR UPDATE SKIP LOCKED;

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
