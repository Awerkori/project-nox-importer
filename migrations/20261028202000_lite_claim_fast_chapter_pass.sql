-- Fast two-pass chapter claim + historical P1 non-deferred operation_rank fix:
-- 1. Restores operation_rank = 1 for non-deferred historicalP1Backfill CATALOG
--    tasks in importer_lite_refresh_ready_work while keeping deferred ones at 2.
-- 2. Scans only STAFF/P0/P1 operation_rank <= 1 works in Pass 1 via a clean
--    single-query index scan (no UNION ALL or PK re-join that scrambles ORDER BY),
--    so the 9,000+ P2/deferred CATALOG rows in importer_lite_ready_works are
--    never sorted or probed during normal chapter admission.

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

    IF NOT FOUND THEN
      DELETE FROM public.importer_lite_ready_works WHERE work_key = p_work_key;
      RETURN;
    END IF;
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
  v_pass int;
  v_candidate_keys text[];
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
  FOR v_pass IN 1..2 LOOP
    v_fallback_task_id := NULL;
    v_fallback_work_key := NULL;

    IF v_pass = 1 THEN
      SELECT array_agg(cand.work_key)
      INTO v_candidate_keys
      FROM (
        SELECT rw.work_key
        FROM public.importer_lite_ready_works rw
        WHERE rw.priority <= 2
          AND rw.operation_rank <= 1
          AND rw.next_run_at <= now()
          AND NOT EXISTS (
            SELECT 1 FROM public.importer_lite_tasks active_work
            WHERE active_work.work_key = rw.work_key
              AND active_work.task_type IN ('IMPORT_CHAPTER', 'PUBLISH')
              AND active_work.state IN ('RUNNING', 'READY', 'RETRY_WAIT')
              AND active_work.state = 'RUNNING'
          )
        ORDER BY
          rw.priority ASC,
          rw.operation_rank ASC,
          CASE
            WHEN v_has_focus AND rw.priority = 2 AND rw.operation_rank = 1 THEN
              CASE
                WHEN COALESCE(p_prefer_focus, true) THEN
                  CASE WHEN rw.work_key = ANY(p_focus_work_keys) THEN 0 ELSE 1 END
                ELSE
                  CASE WHEN rw.work_key = ANY(p_focus_work_keys) THEN 1 ELSE 0 END
              END
            ELSE 0
          END ASC,
          rw.last_claimed_at ASC NULLS FIRST,
          rw.next_run_at ASC,
          rw.work_key ASC
        LIMIT 16
      ) cand;
    ELSE
      IF COALESCE(p_chapters_only, false) THEN
        SELECT array_agg(cand2.work_key)
        INTO v_candidate_keys
        FROM (
          SELECT rw.work_key
          FROM public.importer_lite_ready_works rw
          WHERE rw.priority > 2
            AND rw.operation_rank <= 1
            AND rw.next_run_at <= now()
            AND NOT EXISTS (
              SELECT 1 FROM public.importer_lite_tasks active_work
              WHERE active_work.work_key = rw.work_key
                AND active_work.task_type IN ('IMPORT_CHAPTER', 'PUBLISH')
                AND active_work.state IN ('RUNNING', 'READY', 'RETRY_WAIT')
                AND active_work.state = 'RUNNING'
            )
          ORDER BY
            rw.priority ASC,
            rw.operation_rank ASC,
            rw.last_claimed_at ASC NULLS FIRST,
            rw.next_run_at ASC,
            rw.work_key ASC
          LIMIT 12
        ) cand2;
      ELSE
        SELECT array_agg(cand2.work_key)
        INTO v_candidate_keys
        FROM (
          SELECT rw.work_key
          FROM public.importer_lite_ready_works rw
          WHERE rw.next_run_at <= now()
            AND NOT (rw.priority <= 2 AND rw.operation_rank <= 1)
          ORDER BY
            rw.priority ASC,
            rw.operation_rank ASC,
            rw.last_claimed_at ASC NULLS FIRST,
            rw.next_run_at ASC,
            rw.work_key ASC
          LIMIT 12
        ) cand2;
      END IF;
    END IF;

    IF v_candidate_keys IS NULL OR cardinality(v_candidate_keys) = 0 THEN
      CONTINUE;
    END IF;

    FOREACH v_work_key IN ARRAY v_candidate_keys LOOP
      PERFORM 1 FROM public.importer_lite_ready_works
      WHERE work_key = v_work_key
      FOR UPDATE SKIP LOCKED;
      IF NOT FOUND THEN
        CONTINUE;
      END IF;

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
              AND active_work.state IN ('RUNNING', 'READY', 'RETRY_WAIT')
              AND active_work.state = 'RUNNING'
              AND active_work.id <> pending.id
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
              AND active_work.state IN ('RUNNING', 'READY', 'RETRY_WAIT')
              AND active_work.state = 'RUNNING'
              AND active_work.id <> candidate.id
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
        ORDER BY
          priority ASC,
          CASE
            WHEN task_type = 'PUBLISH' THEN 0
            WHEN task_type = 'IMPORT_CHAPTER' THEN 1
            WHEN task_type = 'CATALOG' AND priority = 2 AND historical_p1 THEN 1
            WHEN task_type = 'CATALOG' THEN 2
            ELSE 3
          END ASC,
          CASE WHEN task_type IN ('PUBLISH', 'IMPORT_CHAPTER') THEN order_key END NULLS LAST,
          next_run_at ASC,
          created_at ASC,
          id ASC
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
            next_run_at = CASE
              WHEN v_work_key LIKE 'work:%' AND v_task_type IN ('IMPORT_CHAPTER', 'PUBLISH') THEN now() + interval '30 seconds'
              ELSE now()
            END,
            updated_at = now()
        WHERE work_key = v_work_key;
        RETURN QUERY UPDATE public.importer_lite_tasks
        SET state = 'RUNNING', lease_owner = p_worker_id, lease_until = now() + p_lease,
            attempts = attempts + 1, updated_at = now()
        WHERE id = v_task_id RETURNING *;
        IF v_task_type NOT IN ('IMPORT_CHAPTER', 'PUBLISH') THEN
          PERFORM public.importer_lite_refresh_ready_work(v_work_key);
        END IF;
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
  END LOOP;

  RETURN;
END;
$function$;
