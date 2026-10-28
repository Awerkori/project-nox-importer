-- PROJECT NOX: eliminate empty RETRY_WAIT ready-work claims, preserve canonical
-- chapter order across transient source-health cooldowns, and add dynamic P1
-- focus-group + saturated-source-aware claiming.
--
-- Backward compatibility:
--   public.importer_lite_claim_task(text, interval) retains its exact two-arg
--   signature and delegates to public.importer_lite_claim_task_focused(...),
--   allowing zero-downtime rollout while older runtime instances are still live.

CREATE OR REPLACE FUNCTION public.importer_lite_manifest_p1_route_deferred(
  p_work_id uuid,
  p_chapter_key text
)
RETURNS boolean
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = public
AS $function$
  WITH recorded_route AS (
    SELECT route.source AS provider_id
    FROM public.importer_lite_chapter_sources route
    WHERE route.work_id = p_work_id
      AND route.canonical_chapter_key = p_chapter_key
      AND route.confirmed = true
      AND COALESCE(route.has_chapter, true) = true
  ), runnable_route AS (
    SELECT 1
    FROM recorded_route route
    JOIN public.importer_sources provider ON provider.id = route.provider_id
    LEFT JOIN public.importer_lite_source_health health ON health.source = provider.id
    WHERE provider.enabled = true
      AND (COALESCE(provider.status, 'ACTIVE') IN ('ACTIVE', 'DEGRADED', 'RECOVERING')
        OR (provider.status = 'COOLDOWN' AND provider.cooldown_until IS NOT NULL
            AND provider.cooldown_until <= now()))
      AND COALESCE((to_jsonb(provider)->>'chapter_ingestion_enabled')::boolean, true) = true
      AND COALESCE(health.health, 'HEALTHY') NOT IN ('BROKEN', 'DISABLED')
      AND (to_jsonb(provider)->>'blocked_reason' IS NULL
        OR to_jsonb(provider)->'blocked_details'->>'probe_success' = 'true'
        OR to_jsonb(provider)->'blocked_details'->>'recovered_at' IS NOT NULL)
  )
  SELECT EXISTS(SELECT 1 FROM recorded_route)
     AND NOT EXISTS(SELECT 1 FROM runnable_route)
$function$;

CREATE OR REPLACE FUNCTION public.importer_lite_refresh_ready_work(p_work_key text)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $function$
DECLARE
  v_frontier_work_id uuid;
  v_frontier_chapter_key text;
  v_frontier_task_type text;
  v_frontier_state text;
  v_frontier_priority smallint;
  v_frontier_next timestamptz;
  v_frontier_op_rank smallint;
  v_has_frontier boolean := false;
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

  -- Ordered chapter work forms a strict per-work canonical frontier. A recent
  -- RETRY_WAIT predecessor remains in ordered_frontier as a barrier so later
  -- READY chapters of the same work cannot jump ahead, while never projecting
  -- an unclaimable RETRY_WAIT chapter into importer_lite_ready_works.
  IF v_presentable THEN
    SELECT
      task.work_id,
      task.chapter_key,
      task.task_type,
      task.state,
      task.priority,
      task.next_run_at,
      (CASE task.task_type WHEN 'PUBLISH' THEN 0 ELSE 1 END)::smallint
    INTO
      v_frontier_work_id,
      v_frontier_chapter_key,
      v_frontier_task_type,
      v_frontier_state,
      v_frontier_priority,
      v_frontier_next,
      v_frontier_op_rank
    FROM public.importer_lite_tasks task
    WHERE task.work_key = p_work_key
      AND task.monitoring = false
      AND task.task_type IN ('IMPORT_CHAPTER', 'PUBLISH')
      AND NULLIF(task.payload->>'orderKey', '') IS NOT NULL
      AND NOT (
        task.task_type = 'IMPORT_CHAPTER' AND task.priority >= 2
        AND COALESCE(task.payload->>'manifestP1Repair', 'false') = 'true'
        AND public.importer_lite_manifest_p1_route_deferred(task.work_id, task.chapter_key)
      )
      AND (
        task.state = 'READY'
        OR (task.state = 'RETRY_WAIT' AND task.attempts < task.max_attempts
            AND task.updated_at > now() - interval '15 minutes')
      )
    ORDER BY NULLIF(task.payload->>'orderKey', ''),
      CASE task.task_type WHEN 'PUBLISH' THEN 0 ELSE 1 END,
      task.priority, task.next_run_at, task.created_at, task.id
    LIMIT 1;

    v_has_frontier := FOUND;
  END IF;

  IF v_has_frontier AND v_frontier_state = 'READY' THEN
    v_priority := v_frontier_priority;
    v_next := v_frontier_next;
    v_operation_rank := v_frontier_op_rank;

    IF v_frontier_task_type = 'IMPORT_CHAPTER'
       AND v_frontier_work_id IS NOT NULL
       AND v_frontier_chapter_key IS NOT NULL THEN
      SELECT
        count(*)::int > 0,
        COALESCE(bool_or(
          (provider.cooldown_until IS NULL OR provider.cooldown_until <= now())
          AND (health.cooldown_until IS NULL OR health.cooldown_until <= now())
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
        AND COALESCE((to_jsonb(provider)->>'chapter_ingestion_enabled')::boolean, true) = true
        AND COALESCE(health.health, 'HEALTHY') NOT IN ('BROKEN', 'DISABLED')
        AND (to_jsonb(provider)->>'blocked_reason' IS NULL
          OR to_jsonb(provider)->'blocked_details'->>'probe_success' = 'true'
          OR to_jsonb(provider)->'blocked_details'->>'recovered_at' IS NOT NULL);

      IF v_has_operational_route AND NOT v_has_due_route
         AND v_min_route_cooldown IS NOT NULL AND v_min_route_cooldown > now() THEN
        v_next := GREATEST(v_next, v_min_route_cooldown);
      END IF;
    END IF;
  ELSIF v_has_frontier AND v_frontier_next > now() THEN
    -- Keep future RETRY_WAIT frontier deferred until its retry boundary (it is
    -- invisible to next_run_at <= now() claims until due).
    v_priority := v_frontier_priority;
    v_next := v_frontier_next;
    v_operation_rank := v_frontier_op_rank;
  ELSIF v_has_frontier THEN
    -- Frontier is due RETRY_WAIT waiting for reconcile(): block ordered
    -- chapters for this work without leaving a due unclaimable ready-work row.
    SELECT
      task.priority,
      task.next_run_at,
      (CASE task.task_type WHEN 'CATALOG' THEN 2 ELSE 3 END)::smallint
    INTO v_priority, v_next, v_operation_rank
    FROM public.importer_lite_tasks task
    WHERE task.work_key = p_work_key
      AND task.state = 'READY'
      AND task.monitoring = false
      AND NOT (task.task_type = 'CATALOG' AND task.payload ? 'metadataSourceRefresh')
      AND task.task_type NOT IN ('IMPORT_CHAPTER', 'PUBLISH')
    ORDER BY task.priority,
      CASE task.task_type WHEN 'CATALOG' THEN 2 ELSE 3 END,
      task.next_run_at, task.created_at, task.id
    LIMIT 1;

    IF NOT FOUND THEN
      DELETE FROM public.importer_lite_ready_works WHERE work_key = p_work_key;
      RETURN;
    END IF;
  ELSE
    SELECT
      task.priority,
      task.next_run_at,
      (CASE task.task_type WHEN 'PUBLISH' THEN 0 WHEN 'IMPORT_CHAPTER' THEN 1
        WHEN 'CATALOG' THEN 2 ELSE 3 END)::smallint
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
      CASE task.task_type WHEN 'PUBLISH' THEN 0 WHEN 'IMPORT_CHAPTER' THEN 1
        WHEN 'CATALOG' THEN 2 ELSE 3 END,
      task.next_run_at, task.created_at, task.id
    LIMIT 1;

    IF NOT FOUND THEN
      DELETE FROM public.importer_lite_ready_works WHERE work_key = p_work_key;
      RETURN;
    END IF;
  END IF;

  INSERT INTO public.importer_lite_ready_works(work_key, priority, operation_rank, next_run_at, updated_at)
  VALUES(p_work_key, v_priority, v_operation_rank, v_next, now())
  ON CONFLICT(work_key) DO UPDATE
    SET priority = excluded.priority,
        operation_rank = excluded.operation_rank,
        next_run_at = excluded.next_run_at,
        updated_at = now();
END;
$function$;

CREATE OR REPLACE FUNCTION public.importer_lite_claim_task_focused(
  p_worker_id text,
  p_lease interval,
  p_focus_work_keys text[],
  p_prefer_focus boolean,
  p_excluded_sources text[],
  p_chapters_only boolean
)
RETURNS SETOF public.importer_lite_tasks
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $function$
DECLARE
  r RECORD;
  v_work_key text;
  v_task_id uuid;
  v_task_type text;
  v_task_priority smallint;
  v_task_work_id uuid;
  v_task_chapter_key text;
  v_presentable boolean := false;
  v_has_focus boolean := p_focus_work_keys IS NOT NULL AND cardinality(p_focus_work_keys) > 0;
  v_has_excluded boolean := p_excluded_sources IS NOT NULL AND cardinality(p_excluded_sources) > 0;
  v_fallback_task_id uuid := NULL;
  v_fallback_work_key text := NULL;
BEGIN
  FOR r IN (
    SELECT rw.work_key
    FROM public.importer_lite_ready_works rw
    WHERE rw.next_run_at <= now()
      AND (NOT COALESCE(p_chapters_only, false) OR rw.operation_rank <= 1)
      AND NOT EXISTS (
        SELECT 1 FROM public.importer_lite_tasks active_work
        WHERE active_work.work_key = rw.work_key
          AND active_work.task_type IN ('IMPORT_CHAPTER', 'PUBLISH')
          AND active_work.state = 'RUNNING'
      )
    ORDER BY
      rw.priority,
      rw.operation_rank,
      CASE
        WHEN v_has_excluded
          AND rw.priority >= 2
          AND rw.operation_rank = 1
          AND rw.work_key LIKE 'work:%'
          AND EXISTS (
            SELECT 1 FROM public.importer_lite_chapter_sources cs
            WHERE cs.work_id = NULLIF(substring(rw.work_key from 6), '')::uuid
              AND cs.confirmed = true
              AND COALESCE(cs.has_chapter, true) = true
          )
          AND NOT EXISTS (
            SELECT 1
            FROM public.importer_lite_chapter_sources cs
            JOIN public.importer_sources src ON src.id = cs.source
            LEFT JOIN public.importer_lite_source_health sh ON sh.source = src.id
            WHERE cs.work_id = NULLIF(substring(rw.work_key from 6), '')::uuid
              AND cs.confirmed = true
              AND COALESCE(cs.has_chapter, true) = true
              AND NOT (cs.source = ANY(p_excluded_sources))
              AND src.enabled = true
              AND COALESCE(src.status, 'ACTIVE') <> 'DISABLED'
              AND COALESCE((to_jsonb(src)->>'chapter_ingestion_enabled')::boolean, true) = true
              AND COALESCE(sh.health, 'HEALTHY') NOT IN ('BROKEN', 'DISABLED')
              AND (sh.cooldown_until IS NULL OR sh.cooldown_until <= now())
          )
        THEN 1
        ELSE 0
      END,
      CASE
        WHEN v_has_focus AND rw.priority = 2 AND rw.operation_rank = 1 THEN
          CASE
            WHEN COALESCE(p_prefer_focus, true) THEN
              CASE WHEN rw.work_key = ANY(p_focus_work_keys) THEN 0 ELSE 1 END
            ELSE
              CASE WHEN rw.work_key = ANY(p_focus_work_keys) THEN 1 ELSE 0 END
          END
        ELSE 0
      END,
      rw.last_claimed_at NULLS FIRST,
      rw.next_run_at,
      rw.work_key
    LIMIT 8
    FOR UPDATE OF rw SKIP LOCKED
  ) LOOP
    v_work_key := r.work_key;
    v_presentable := false;
    v_task_id := NULL;
    v_task_type := NULL;
    v_task_priority := NULL;
    v_task_work_id := NULL;
    v_task_chapter_key := NULL;

    IF v_work_key LIKE 'work:%' THEN
      SELECT EXISTS (
        SELECT 1 FROM public.works work
        JOIN public.media cover ON cover.id = work.cover_id
        WHERE work.id = NULLIF(substring(v_work_key from 6), '')::uuid
          AND NULLIF(btrim(work.title), '') IS NOT NULL
          AND NULLIF(btrim(work.slug), '') IS NOT NULL
          AND cover.storage_ready = true AND cover.bytes >= 1500
      ) INTO v_presentable;
    END IF;

    WITH earliest_pending_chapter AS (
      SELECT candidate.id, candidate.task_type, candidate.priority, candidate.state,
             candidate.next_run_at, candidate.created_at, candidate.monitoring,
             NULLIF(candidate.payload->>'orderKey', '') AS order_key
      FROM public.importer_lite_tasks candidate
      WHERE candidate.work_key = v_work_key
        AND candidate.task_type IN ('IMPORT_CHAPTER', 'PUBLISH')
        AND NULLIF(candidate.payload->>'orderKey', '') IS NOT NULL
        AND NOT (
          candidate.task_type = 'IMPORT_CHAPTER' AND candidate.priority >= 2
          AND COALESCE(candidate.payload->>'manifestP1Repair', 'false') = 'true'
          AND public.importer_lite_manifest_p1_route_deferred(candidate.work_id, candidate.chapter_key)
        )
        AND (candidate.state = 'READY'
          OR (candidate.state = 'RETRY_WAIT' AND candidate.attempts < candidate.max_attempts
              AND candidate.updated_at > now() - interval '15 minutes'))
      ORDER BY NULLIF(candidate.payload->>'orderKey', ''),
        CASE WHEN candidate.task_type = 'PUBLISH' THEN 0 ELSE 1 END,
        candidate.priority, candidate.next_run_at, candidate.created_at, candidate.id
      LIMIT 1
    ), candidate_pool AS (
      SELECT pending.id, pending.task_type, pending.priority, pending.next_run_at,
             pending.created_at, pending.order_key, false AS historical_p1
      FROM earliest_pending_chapter pending
      WHERE pending.state = 'READY' AND pending.monitoring = false
        AND pending.next_run_at <= now() AND v_presentable
        AND NOT EXISTS (
          SELECT 1 FROM public.importer_lite_tasks active_work
          WHERE active_work.work_key = v_work_key
            AND active_work.task_type IN ('IMPORT_CHAPTER', 'PUBLISH')
            AND active_work.state = 'RUNNING' AND active_work.id <> pending.id
        )
      UNION ALL
      SELECT candidate.id, candidate.task_type, candidate.priority, candidate.next_run_at,
             candidate.created_at, NULLIF(candidate.payload->>'orderKey', ''), false
      FROM public.importer_lite_tasks candidate
      WHERE candidate.work_key = v_work_key AND candidate.state = 'READY'
        AND candidate.monitoring = false AND candidate.next_run_at <= now()
        AND candidate.task_type IN ('IMPORT_CHAPTER', 'PUBLISH')
        AND NULLIF(candidate.payload->>'orderKey', '') IS NULL AND v_presentable
        AND NOT EXISTS (
          SELECT 1 FROM public.importer_lite_tasks active_work
          WHERE active_work.work_key = v_work_key
            AND active_work.task_type IN ('IMPORT_CHAPTER', 'PUBLISH')
            AND active_work.state = 'RUNNING' AND active_work.id <> candidate.id
        )
      UNION ALL
      SELECT candidate.id, candidate.task_type, candidate.priority, candidate.next_run_at,
             candidate.created_at, NULLIF(candidate.payload->>'orderKey', ''),
             (candidate.task_type = 'CATALOG' AND candidate.priority = 2
               AND candidate.payload ? 'historicalP1Backfill'
               AND COALESCE(candidate.payload->>'deferBehindReadyImport', 'false') <> 'true')
      FROM public.importer_lite_tasks candidate
      WHERE NOT COALESCE(p_chapters_only, false)
        AND candidate.work_key = v_work_key AND candidate.state = 'READY'
        AND candidate.monitoring = false AND candidate.next_run_at <= now()
        AND NOT (candidate.task_type = 'CATALOG' AND candidate.payload ? 'metadataSourceRefresh')
        AND candidate.task_type NOT IN ('IMPORT_CHAPTER', 'PUBLISH')
    ), selected_candidate AS (
      SELECT id FROM candidate_pool
      ORDER BY CASE
          WHEN task_type = 'PUBLISH' THEN 0
          WHEN task_type = 'IMPORT_CHAPTER' THEN 1
          WHEN task_type = 'CATALOG' AND priority = 2 AND historical_p1 THEN 1
          WHEN task_type = 'CATALOG' THEN 2 ELSE 3 END,
        CASE WHEN task_type IN ('PUBLISH', 'IMPORT_CHAPTER') THEN order_key END NULLS LAST,
        priority, next_run_at, created_at, id
      LIMIT 1
    )
    SELECT candidate.id, candidate.task_type, candidate.priority, candidate.work_id, candidate.chapter_key
    INTO v_task_id, v_task_type, v_task_priority, v_task_work_id, v_task_chapter_key
    FROM public.importer_lite_tasks candidate
    JOIN selected_candidate selected ON selected.id = candidate.id
    FOR UPDATE OF candidate SKIP LOCKED;

    IF v_task_id IS NOT NULL THEN
      IF v_task_type = 'IMPORT_CHAPTER'
         AND v_task_work_id IS NOT NULL
         AND v_task_chapter_key IS NOT NULL
         AND EXISTS (
           SELECT 1 FROM public.importer_lite_chapter_sources cs
           JOIN public.importer_sources src ON src.id = cs.source
           LEFT JOIN public.importer_lite_source_health sh ON sh.source = src.id
           WHERE cs.work_id = v_task_work_id
             AND cs.canonical_chapter_key = v_task_chapter_key
             AND cs.confirmed = true
             AND COALESCE(cs.has_chapter, true) = true
             AND src.enabled = true
             AND COALESCE(src.status, 'ACTIVE') <> 'DISABLED'
             AND COALESCE((to_jsonb(src)->>'chapter_ingestion_enabled')::boolean, true) = true
             AND COALESCE(sh.health, 'HEALTHY') NOT IN ('BROKEN', 'DISABLED')
         )
         AND NOT EXISTS (
           SELECT 1 FROM public.importer_lite_chapter_sources cs
           JOIN public.importer_sources src ON src.id = cs.source
           LEFT JOIN public.importer_lite_source_health sh ON sh.source = src.id
           WHERE cs.work_id = v_task_work_id
             AND cs.canonical_chapter_key = v_task_chapter_key
             AND cs.confirmed = true
             AND COALESCE(cs.has_chapter, true) = true
             AND src.enabled = true
             AND COALESCE(src.status, 'ACTIVE') <> 'DISABLED'
             AND COALESCE((to_jsonb(src)->>'chapter_ingestion_enabled')::boolean, true) = true
             AND COALESCE(sh.health, 'HEALTHY') NOT IN ('BROKEN', 'DISABLED')
             AND (sh.cooldown_until IS NULL OR sh.cooldown_until <= now())
         ) THEN
        PERFORM public.importer_lite_refresh_ready_work(v_work_key);
        CONTINUE;
      END IF;

      IF v_has_excluded
         AND v_task_type = 'IMPORT_CHAPTER'
         AND v_task_priority >= 2
         AND v_task_work_id IS NOT NULL
         AND v_task_chapter_key IS NOT NULL
         AND EXISTS (
           SELECT 1 FROM public.importer_lite_chapter_sources cs
           WHERE cs.work_id = v_task_work_id
             AND cs.canonical_chapter_key = v_task_chapter_key
             AND cs.confirmed = true
             AND COALESCE(cs.has_chapter, true) = true
         )
         AND NOT EXISTS (
           SELECT 1 FROM public.importer_lite_chapter_sources cs
           JOIN public.importer_sources src ON src.id = cs.source
           LEFT JOIN public.importer_lite_source_health sh ON sh.source = src.id
           WHERE cs.work_id = v_task_work_id
             AND cs.canonical_chapter_key = v_task_chapter_key
             AND cs.confirmed = true
             AND COALESCE(cs.has_chapter, true) = true
             AND NOT (cs.source = ANY(p_excluded_sources))
             AND src.enabled = true
             AND COALESCE(src.status, 'ACTIVE') <> 'DISABLED'
             AND COALESCE((to_jsonb(src)->>'chapter_ingestion_enabled')::boolean, true) = true
             AND COALESCE(sh.health, 'HEALTHY') NOT IN ('BROKEN', 'DISABLED')
             AND (sh.cooldown_until IS NULL OR sh.cooldown_until <= now())
         ) THEN
        IF v_fallback_task_id IS NULL THEN
          v_fallback_task_id := v_task_id;
          v_fallback_work_key := v_work_key;
        END IF;
        UPDATE public.importer_lite_ready_works
        SET last_claimed_at = now(), updated_at = now()
        WHERE work_key = v_work_key;
        CONTINUE;
      END IF;

      UPDATE public.importer_lite_ready_works
      SET last_claimed_at = now(),
          next_run_at = CASE WHEN v_work_key LIKE 'work:%' THEN now() + interval '30 seconds' ELSE now() END,
          updated_at = now()
      WHERE work_key = v_work_key;
      RETURN QUERY UPDATE public.importer_lite_tasks
      SET state = 'RUNNING', lease_owner = p_worker_id, lease_until = now() + p_lease,
          attempts = attempts + 1, updated_at = now()
      WHERE id = v_task_id RETURNING *;
      RETURN;
    END IF;

    PERFORM public.importer_lite_refresh_ready_work(v_work_key);
    UPDATE public.importer_lite_ready_works
    SET last_claimed_at = now(),
        next_run_at = GREATEST(next_run_at, now() + interval '10 seconds'),
        updated_at = now()
    WHERE work_key = v_work_key;
  END LOOP;

  IF v_fallback_task_id IS NOT NULL THEN
    UPDATE public.importer_lite_ready_works
    SET last_claimed_at = now(),
        next_run_at = CASE WHEN v_fallback_work_key LIKE 'work:%' THEN now() + interval '30 seconds' ELSE now() END,
        updated_at = now()
    WHERE work_key = v_fallback_work_key;
    RETURN QUERY UPDATE public.importer_lite_tasks
    SET state = 'RUNNING', lease_owner = p_worker_id, lease_until = now() + p_lease,
        attempts = attempts + 1, updated_at = now()
    WHERE id = v_fallback_task_id AND state = 'READY' RETURNING *;
    RETURN;
  END IF;

  RETURN;
END;
$function$;

CREATE OR REPLACE FUNCTION public.importer_lite_claim_task(
  p_worker_id text,
  p_lease interval DEFAULT interval '10 minutes'
)
RETURNS SETOF public.importer_lite_tasks
LANGUAGE sql
SECURITY DEFINER
SET search_path = public
AS $function$
  SELECT * FROM public.importer_lite_claim_task_focused(
    p_worker_id,
    p_lease,
    NULL,
    true,
    NULL,
    false
  );
$function$;

CREATE OR REPLACE FUNCTION public.importer_lite_yield_task(
  p_task_id uuid,
  p_worker_id text,
  p_delay_ms integer
)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $function$
DECLARE
  v_work_key text;
BEGIN
  UPDATE public.importer_lite_tasks
     SET state = 'READY',
         attempts = GREATEST(attempts - 1, 0),
         lease_owner = NULL,
         lease_until = NULL,
         next_run_at = GREATEST(next_run_at, now() + (p_delay_ms * interval '1 millisecond')),
         updated_at = now()
   WHERE id = p_task_id
     AND state = 'RUNNING'
     AND lease_owner = p_worker_id
  RETURNING work_key INTO v_work_key;

  IF NOT FOUND THEN
    RETURN false;
  END IF;

  PERFORM public.importer_lite_refresh_ready_work(v_work_key);
  RETURN true;
END;
$function$;

CREATE OR REPLACE FUNCTION public.importer_lite_finish_task(
  p_task_id uuid,
  p_worker_id text,
  p_state text,
  p_reason text,
  p_next_run_at timestamptz,
  p_checkpoint jsonb,
  p_priority smallint,
  p_monitoring boolean,
  p_work_id uuid,
  p_reset_attempts boolean
)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $function$
DECLARE
  v_work_key text;
BEGIN
  UPDATE public.importer_lite_tasks
     SET state = p_state,
         reason = p_reason,
         next_run_at = COALESCE(p_next_run_at, next_run_at),
         base_priority = COALESCE(p_priority, base_priority),
         priority = CASE WHEN EXISTS (
           SELECT 1 FROM public.importer_staff_requests s
           WHERE s.work_id = COALESCE(p_work_id, importer_lite_tasks.work_id)
             AND s.status IN ('QUEUED', 'IMPORTING', 'RETRYING', 'ACTIVE')
         ) THEN 0 ELSE COALESCE(p_priority, base_priority) END,
         checkpoint = checkpoint || COALESCE(p_checkpoint, '{}'::jsonb),
         monitoring = COALESCE(p_monitoring, monitoring),
         work_id = COALESCE(p_work_id, work_id),
         attempts = CASE WHEN COALESCE(p_reset_attempts, false) THEN 0 ELSE attempts END,
         lease_owner = NULL,
         lease_until = NULL,
         updated_at = now()
   WHERE id = p_task_id
     AND state = 'RUNNING'
     AND lease_owner = p_worker_id
     AND lease_until > now()
  RETURNING work_key INTO v_work_key;

  IF NOT FOUND THEN
    RETURN false;
  END IF;

  PERFORM public.importer_lite_refresh_ready_work(v_work_key);
  RETURN true;
END;
$function$;

REVOKE ALL ON FUNCTION public.importer_lite_claim_task_focused(text, interval, text[], boolean, text[], boolean) FROM PUBLIC;
REVOKE ALL ON FUNCTION public.importer_lite_yield_task(uuid, text, integer) FROM PUBLIC;
REVOKE ALL ON FUNCTION public.importer_lite_finish_task(uuid, text, text, text, timestamptz, jsonb, smallint, boolean, uuid, boolean) FROM PUBLIC;
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'anon') THEN
    REVOKE ALL ON FUNCTION public.importer_lite_claim_task_focused(text, interval, text[], boolean, text[], boolean) FROM anon;
    REVOKE ALL ON FUNCTION public.importer_lite_yield_task(uuid, text, integer) FROM anon;
    REVOKE ALL ON FUNCTION public.importer_lite_finish_task(uuid, text, text, text, timestamptz, jsonb, smallint, boolean, uuid, boolean) FROM anon;
  END IF;
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'authenticated') THEN
    REVOKE ALL ON FUNCTION public.importer_lite_claim_task_focused(text, interval, text[], boolean, text[], boolean) FROM authenticated;
    REVOKE ALL ON FUNCTION public.importer_lite_yield_task(uuid, text, integer) FROM authenticated;
    REVOKE ALL ON FUNCTION public.importer_lite_finish_task(uuid, text, text, text, timestamptz, jsonb, smallint, boolean, uuid, boolean) FROM authenticated;
  END IF;
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'service_role') THEN
    GRANT EXECUTE ON FUNCTION public.importer_lite_claim_task_focused(text, interval, text[], boolean, text[], boolean) TO service_role;
    GRANT EXECUTE ON FUNCTION public.importer_lite_yield_task(uuid, text, integer) TO service_role;
    GRANT EXECUTE ON FUNCTION public.importer_lite_finish_task(uuid, text, text, text, timestamptz, jsonb, smallint, boolean, uuid, boolean) TO service_role;
  END IF;
END;
$$;

-- Purge due stale chapter ready-work projections left behind by RETRY_WAIT frontiers.
DELETE FROM public.importer_lite_ready_works rw
WHERE rw.operation_rank IN (0, 1)
  AND rw.next_run_at <= now()
  AND NOT EXISTS (
    SELECT 1
    FROM public.importer_lite_tasks t
    WHERE t.work_key = rw.work_key
      AND t.state = 'READY'
      AND t.monitoring = false
      AND t.task_type IN ('IMPORT_CHAPTER', 'PUBLISH')
  );
