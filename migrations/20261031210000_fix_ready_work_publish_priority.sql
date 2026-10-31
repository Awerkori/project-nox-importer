-- Fix ready_works projection for orderKey-less PUBLISH/IMPORT_CHAPTER tasks:
-- Previously, when a presentable work had BOTH an orderKey-bearing IMPORT_CHAPTER
-- frontier task (READY or RETRY_WAIT) AND a READY legacy/recovered PUBLISH task
-- with payload->>'orderKey' IS NULL, importer_lite_refresh_ready_work only
-- inspected raw_head (orderKey IS NOT NULL) and hid or deleted the READY PUBLISH
-- projection, even though importer_lite_claim_task_focused explicitly admits
-- orderKey IS NULL tasks in candidate_pool. Align refresh_ready_work with
-- claim_task_focused while preserving strict orderKey ordering when orderKey is present.

CREATE OR REPLACE FUNCTION public.importer_lite_refresh_ready_work(p_work_key text)
 RETURNS void
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_now timestamptz := now();
  v_retry_cutoff timestamptz := v_now - interval '15 minutes';
  v_frontier_work_id uuid;
  v_frontier_chapter_key text;
  v_frontier_task_type text;
  v_frontier_state text;
  v_frontier_priority smallint;
  v_frontier_next timestamptz;
  v_frontier_op_rank smallint;
  v_has_frontier boolean := false;
  v_has_candidate boolean := false;
  v_has_null_order_ready boolean := false;
  v_null_priority smallint;
  v_null_next timestamptz;
  v_null_op_rank smallint;
  v_priority smallint;
  v_next timestamptz;
  v_operation_rank smallint;
  v_presentable boolean := false;
  v_has_operational_route boolean := false;
  v_has_due_route boolean := false;
  v_min_route_cooldown timestamptz;
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

  IF v_presentable THEN
    WITH raw_head AS MATERIALIZED (
      SELECT
        task.id,
        task.work_id,
        task.chapter_key,
        task.task_type,
        task.state,
        task.priority,
        task.next_run_at,
        task.created_at,
        task.payload,
        NULLIF(task.payload->>'orderKey', '') AS order_key
      FROM public.importer_lite_tasks task
      WHERE task.work_key = p_work_key
        AND task.monitoring = false
        AND task.task_type IN ('IMPORT_CHAPTER', 'PUBLISH')
        AND task.state IN ('READY', 'RETRY_WAIT')
        AND NULLIF(task.payload->>'orderKey', '') IS NOT NULL
        AND (
          task.state = 'READY'
          OR (task.state = 'RETRY_WAIT' AND task.attempts < task.max_attempts
              AND task.updated_at > v_retry_cutoff)
        )
      ORDER BY NULLIF(task.payload->>'orderKey', ''), task.id
      LIMIT 12
    )
    SELECT
      rh.work_id,
      rh.chapter_key,
      rh.task_type,
      rh.state,
      rh.priority,
      rh.next_run_at,
      (CASE WHEN rh.task_type = 'PUBLISH' THEN 0 ELSE 1 END)::smallint
    INTO
      v_frontier_work_id,
      v_frontier_chapter_key,
      v_frontier_task_type,
      v_frontier_state,
      v_frontier_priority,
      v_frontier_next,
      v_frontier_op_rank
    FROM raw_head rh
    WHERE NOT (
      rh.task_type = 'IMPORT_CHAPTER' AND rh.priority >= 2
      AND COALESCE(rh.payload->>'manifestP1Repair', 'false') = 'true'
      AND public.importer_lite_manifest_p1_route_deferred(rh.work_id, rh.chapter_key)
    )
    ORDER BY rh.order_key,
      CASE WHEN rh.task_type = 'PUBLISH' THEN 0 ELSE 1 END,
      rh.priority, rh.next_run_at, rh.created_at, rh.id
    LIMIT 1;

    v_has_frontier := FOUND;

    SELECT
      task.priority,
      task.next_run_at,
      (CASE WHEN task.task_type = 'PUBLISH' THEN 0 ELSE 1 END)::smallint
    INTO v_null_priority, v_null_next, v_null_op_rank
    FROM public.importer_lite_tasks task
    WHERE task.work_key = p_work_key
      AND task.state = 'READY'
      AND task.monitoring = false
      AND task.task_type IN ('IMPORT_CHAPTER', 'PUBLISH')
      AND NULLIF(task.payload->>'orderKey', '') IS NULL
    ORDER BY task.priority ASC,
      CASE WHEN task.task_type = 'PUBLISH' THEN 0 ELSE 1 END ASC,
      task.next_run_at ASC,
      task.created_at ASC,
      task.id ASC
    LIMIT 1;

    v_has_null_order_ready := FOUND;
  END IF;

  IF v_has_frontier AND v_frontier_state = 'READY' THEN
    v_has_candidate := true;
    v_priority := v_frontier_priority;
    v_next := v_frontier_next;
    v_operation_rank := v_frontier_op_rank;

    IF v_frontier_task_type = 'IMPORT_CHAPTER'
       AND v_frontier_work_id IS NOT NULL
       AND v_frontier_chapter_key IS NOT NULL THEN
      SELECT
        count(*)::int > 0,
        COALESCE(bool_or(
          (provider.cooldown_until IS NULL OR provider.cooldown_until <= v_now)
          AND (health.cooldown_until IS NULL OR health.cooldown_until <= v_now)
        ), false),
        min(GREATEST(
          COALESCE(provider.cooldown_until, '-infinity'::timestamptz),
          COALESCE(health.cooldown_until, '-infinity'::timestamptz)
        ))
      INTO v_has_operational_route, v_has_due_route, v_min_route_cooldown
      FROM public.importer_lite_chapter_sources route
      JOIN public.importer_sources provider ON provider.id = route.source
      LEFT JOIN public.importer_lite_source_health health ON health.source = provider.id
      WHERE route.work_id = v_frontier_work_id
        AND route.canonical_chapter_key = v_frontier_chapter_key
        AND route.confirmed = true
        AND COALESCE(route.has_chapter, true) = true
        AND provider.enabled = true
        AND COALESCE(provider.status, 'ACTIVE') <> 'DISABLED'
        AND COALESCE(provider.chapter_ingestion_enabled, true) = true
        AND COALESCE(health.health, 'HEALTHY') NOT IN ('BROKEN', 'DISABLED')
        AND (provider.blocked_reason IS NULL
          OR provider.blocked_details->>'probe_success' = 'true'
          OR provider.blocked_details->>'recovered_at' IS NOT NULL);

      IF v_has_operational_route AND NOT v_has_due_route
         AND v_min_route_cooldown IS NOT NULL AND v_min_route_cooldown > v_now THEN
        v_next := GREATEST(v_next, v_min_route_cooldown);
      END IF;
    END IF;
  ELSIF v_has_frontier AND v_frontier_next > v_now THEN
    v_has_candidate := true;
    v_priority := v_frontier_priority;
    v_next := v_frontier_next;
    v_operation_rank := v_frontier_op_rank;
  ELSIF v_has_frontier THEN
    SELECT
      task.priority,
      task.next_run_at,
      (CASE
        WHEN task.task_type = 'CATALOG' AND task.priority = 2
          AND task.payload ? 'historicalP1Backfill'
          AND COALESCE(task.payload->>'deferBehindReadyImport', 'false') <> 'true' THEN 1
        WHEN task.task_type = 'CATALOG' THEN 2
        ELSE 3
      END)::smallint
    INTO v_priority, v_next, v_operation_rank
    FROM public.importer_lite_tasks task
    WHERE task.work_key = p_work_key
      AND task.state = 'READY'
      AND task.monitoring = false
      AND NOT (task.task_type = 'CATALOG' AND task.payload ? 'metadataSourceRefresh')
      AND task.task_type NOT IN ('IMPORT_CHAPTER', 'PUBLISH')
    ORDER BY task.priority,
      CASE
        WHEN task.task_type = 'CATALOG' AND task.priority = 2
          AND task.payload ? 'historicalP1Backfill'
          AND COALESCE(task.payload->>'deferBehindReadyImport', 'false') <> 'true' THEN 1
        WHEN task.task_type = 'CATALOG' THEN 2
        ELSE 3
      END,
      task.next_run_at, task.created_at, task.id
    LIMIT 1;

    v_has_candidate := FOUND;
  ELSE
    SELECT
      task.priority,
      task.next_run_at,
      (CASE
        WHEN task.task_type = 'PUBLISH' THEN 0
        WHEN task.task_type = 'IMPORT_CHAPTER' THEN 1
        WHEN task.task_type = 'CATALOG' AND task.priority = 2
          AND task.payload ? 'historicalP1Backfill'
          AND COALESCE(task.payload->>'deferBehindReadyImport', 'false') <> 'true' THEN 1
        WHEN task.task_type = 'CATALOG' THEN 2
        ELSE 3
      END)::smallint
    INTO v_priority, v_next, v_operation_rank
    FROM public.importer_lite_tasks task
    WHERE task.work_key = p_work_key
      AND task.state = 'READY'
      AND task.monitoring = false
      AND NOT (task.task_type = 'CATALOG' AND task.payload ? 'metadataSourceRefresh')
      AND (
        task.task_type NOT IN ('IMPORT_CHAPTER', 'PUBLISH')
        OR (v_presentable AND NULLIF(task.payload->>'orderKey', '') IS NULL)
      )
    ORDER BY task.priority,
      CASE
        WHEN task.task_type = 'PUBLISH' THEN 0
        WHEN task.task_type = 'IMPORT_CHAPTER' THEN 1
        WHEN task.task_type = 'CATALOG' AND task.priority = 2
          AND task.payload ? 'historicalP1Backfill'
          AND COALESCE(task.payload->>'deferBehindReadyImport', 'false') <> 'true' THEN 1
        WHEN task.task_type = 'CATALOG' THEN 2
        ELSE 3
      END,
      task.next_run_at, task.created_at, task.id
    LIMIT 1;

    v_has_candidate := FOUND;
  END IF;

  IF v_has_null_order_ready THEN
    IF NOT v_has_candidate
       OR (v_has_frontier AND v_frontier_state <> 'READY')
       OR v_null_priority < v_priority
       OR (v_null_priority = v_priority AND v_null_op_rank < v_operation_rank)
       OR (v_null_priority = v_priority AND v_null_op_rank = v_operation_rank AND v_null_next < v_next) THEN
      v_has_candidate := true;
      v_priority := v_null_priority;
      v_operation_rank := v_null_op_rank;
      v_next := v_null_next;
    END IF;
  END IF;

  IF NOT v_has_candidate THEN
    DELETE FROM public.importer_lite_ready_works WHERE work_key = p_work_key;
    RETURN;
  END IF;

  INSERT INTO public.importer_lite_ready_works(work_key, priority, operation_rank, next_run_at, updated_at)
  VALUES(p_work_key, v_priority, v_operation_rank, v_next, v_now)
  ON CONFLICT(work_key) DO UPDATE
    SET priority = excluded.priority,
        operation_rank = excluded.operation_rank,
        next_run_at = excluded.next_run_at,
        updated_at = v_now;
END;
$function$;
