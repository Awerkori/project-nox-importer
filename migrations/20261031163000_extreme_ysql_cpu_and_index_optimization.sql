-- Extreme YugabyteDB YSQL CPU, Index & Claim Optimization (Round 2):
-- 1. Ensure native policy/block columns exist on importer_sources across all environments.
-- 2. Drop 10 unused/duplicate/obsolete secondary LSM indexes to reduce write amplification on the colocated tablet.
-- 3. Create covering/partial indexes (importer_lite_tasks_running_chapter_work_idx, importer_lite_tasks_p1_catalog_active_work_idx, covering importer_lite_tasks_ready_work_idx).
-- 4. Eliminate to_jsonb(provider) row conversions in hot stored functions.
-- 5. Enable DocDB Storage Filter & LIMIT pushdown in importer_lite_refresh_ready_work and importer_lite_claim_task_focused by binding timestamptz variables and using a materialized index-order raw_head window.

ALTER TABLE public.importer_sources
  ADD COLUMN IF NOT EXISTS catalog_discovery_enabled boolean NOT NULL DEFAULT true,
  ADD COLUMN IF NOT EXISTS chapter_ingestion_enabled boolean NOT NULL DEFAULT true,
  ADD COLUMN IF NOT EXISTS blocked_reason text NULL,
  ADD COLUMN IF NOT EXISTS blocked_details jsonb NULL;

DROP INDEX IF EXISTS public.importer_lite_ready_works_p1_active_idx2;
DROP INDEX IF EXISTS public.importer_lite_ready_works_claim_rank_v2_idx;
DROP INDEX IF EXISTS public.importer_lite_tasks_cross_source_active_idx;
DROP INDEX IF EXISTS public.importer_lite_tasks_metadata_source_updated_idx;
DROP INDEX IF EXISTS public.importer_lite_tasks_source_cooldown_work_idx;
DROP INDEX IF EXISTS public.importer_lite_tasks_v23_active_idx;
DROP INDEX IF EXISTS public.importer_lite_tasks_cross_source_deferred_idx;
DROP INDEX IF EXISTS public.importer_lite_metadata_enrichment_v5_backlog_hint_idx;
DROP INDEX IF EXISTS public.importer_work_mappings_v5_active_source_work_idx;
DROP INDEX IF EXISTS public.importer_work_mappings_v5_active_work_idx;

CREATE INDEX IF NOT EXISTS importer_lite_tasks_running_chapter_work_idx
ON public.importer_lite_tasks (work_key ASC)
INCLUDE (id, priority, lease_until, monitoring)
WHERE state = 'RUNNING' AND task_type IN ('IMPORT_CHAPTER', 'PUBLISH');

CREATE INDEX IF NOT EXISTS importer_lite_tasks_p1_catalog_active_work_idx
ON public.importer_lite_tasks (work_key ASC)
WHERE state IN ('READY', 'RUNNING', 'RETRY_WAIT')
  AND monitoring = false
  AND task_type = 'CATALOG'
  AND base_priority = 2
  AND NOT (payload ? 'metadataEnrichment');

CREATE INDEX IF NOT EXISTS importer_lite_tasks_ready_work_idx
ON public.importer_lite_tasks (work_key ASC, priority ASC, next_run_at ASC, created_at ASC)
INCLUDE (id, work_id, chapter_key, task_type, monitoring)
WHERE state = 'READY';

CREATE OR REPLACE FUNCTION public.importer_lite_manifest_p1_route_deferred(p_work_id uuid, p_chapter_key text)
 RETURNS boolean
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
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
      AND COALESCE(provider.chapter_ingestion_enabled, true) = true
      AND COALESCE(health.health, 'HEALTHY') NOT IN ('BROKEN', 'DISABLED')
      AND (provider.blocked_reason IS NULL
        OR provider.blocked_details->>'probe_success' = 'true'
        OR provider.blocked_details->>'recovered_at' IS NOT NULL)
  )
  SELECT EXISTS(SELECT 1 FROM recorded_route)
     AND NOT EXISTS(SELECT 1 FROM runnable_route)
$function$;

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
  VALUES(p_work_key, v_priority, v_operation_rank, v_next, v_now)
  ON CONFLICT(work_key) DO UPDATE
    SET priority = excluded.priority,
        operation_rank = excluded.operation_rank,
        next_run_at = excluded.next_run_at,
        updated_at = v_now;
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
  v_now timestamptz := now();
  v_retry_cutoff timestamptz := v_now - interval '15 minutes';
  v_refresh_budget int := 2;
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
  v_has_confirmed boolean := false;
  v_has_enabled_route boolean := false;
  v_has_runnable_route boolean := false;
  v_has_unexcluded_runnable_route boolean := false;
  v_excluded_skips int := 0;
BEGIN
  FOR v_pass IN 1..2 LOOP
    v_fallback_task_id := NULL;
    v_fallback_work_key := NULL;
    v_excluded_skips := 0;

    IF v_pass = 1 THEN
      SELECT array_agg(cand.work_key)
      INTO v_candidate_keys
      FROM (
        SELECT rw.work_key
        FROM public.importer_lite_ready_works rw
        WHERE rw.priority <= 2
          AND rw.operation_rank <= 1
          AND rw.next_run_at <= v_now
          AND NOT EXISTS (
            SELECT 1 FROM public.importer_lite_tasks active_work
            WHERE active_work.work_key = rw.work_key
              AND active_work.task_type IN ('IMPORT_CHAPTER', 'PUBLISH')
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
        LIMIT 24
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
            AND rw.next_run_at <= v_now
            AND NOT EXISTS (
              SELECT 1 FROM public.importer_lite_tasks active_work
              WHERE active_work.work_key = rw.work_key
                AND active_work.task_type IN ('IMPORT_CHAPTER', 'PUBLISH')
                AND active_work.state = 'RUNNING'
            )
          ORDER BY
            rw.priority ASC,
            rw.operation_rank ASC,
            rw.last_claimed_at ASC NULLS FIRST,
            rw.next_run_at ASC,
            rw.work_key ASC
          LIMIT 16
        ) cand2;
      ELSE
        SELECT array_agg(cand2.work_key)
        INTO v_candidate_keys
        FROM (
          SELECT rw.work_key
          FROM public.importer_lite_ready_works rw
          WHERE rw.next_run_at <= v_now
            AND NOT (rw.priority <= 2 AND rw.operation_rank <= 1)
          ORDER BY
            rw.priority ASC,
            rw.operation_rank ASC,
            rw.last_claimed_at ASC NULLS FIRST,
            rw.next_run_at ASC,
            rw.work_key ASC
          LIMIT 16
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

      WITH raw_head AS MATERIALIZED (
        SELECT candidate.id, candidate.work_id, candidate.chapter_key, candidate.task_type,
               candidate.priority, candidate.state, candidate.next_run_at, candidate.created_at,
               candidate.monitoring, candidate.payload,
               NULLIF(candidate.payload->>'orderKey', '') AS order_key
        FROM public.importer_lite_tasks candidate
        WHERE candidate.work_key = v_work_key
          AND candidate.task_type IN ('IMPORT_CHAPTER', 'PUBLISH')
          AND candidate.state IN ('READY', 'RETRY_WAIT')
          AND NULLIF(candidate.payload->>'orderKey', '') IS NOT NULL
          AND (candidate.state = 'READY'
            OR (candidate.state = 'RETRY_WAIT' AND candidate.attempts < candidate.max_attempts
                AND candidate.updated_at > v_retry_cutoff))
        ORDER BY NULLIF(candidate.payload->>'orderKey', ''), candidate.id
        LIMIT 12
      ), earliest_pending_chapter AS (
        SELECT rh.id, rh.task_type, rh.priority, rh.state,
               rh.next_run_at, rh.created_at, rh.monitoring, rh.order_key
        FROM raw_head rh
        WHERE NOT (
          rh.task_type = 'IMPORT_CHAPTER' AND rh.priority >= 2
          AND COALESCE(rh.payload->>'manifestP1Repair', 'false') = 'true'
          AND public.importer_lite_manifest_p1_route_deferred(rh.work_id, rh.chapter_key)
        )
        ORDER BY rh.order_key,
          CASE WHEN rh.task_type = 'PUBLISH' THEN 0 ELSE 1 END,
          rh.priority, rh.next_run_at, rh.created_at, rh.id
        LIMIT 1
      ), candidate_pool AS (
        SELECT pending.id, pending.task_type, pending.priority, pending.next_run_at,
               pending.created_at, pending.order_key, false AS historical_p1
        FROM earliest_pending_chapter pending
        WHERE pending.state = 'READY' AND pending.monitoring = false
          AND pending.next_run_at <= v_now AND v_presentable
          AND NOT EXISTS (
            SELECT 1 FROM public.importer_lite_tasks active_work
            WHERE active_work.work_key = v_work_key
              AND active_work.task_type IN ('IMPORT_CHAPTER', 'PUBLISH')
              AND active_work.state = 'RUNNING'
              AND active_work.id <> pending.id
          )
        UNION ALL
        SELECT candidate.id, candidate.task_type, candidate.priority, candidate.next_run_at,
               candidate.created_at, NULLIF(candidate.payload->>'orderKey', ''), false
        FROM public.importer_lite_tasks candidate
        WHERE candidate.work_key = v_work_key AND candidate.state = 'READY'
          AND candidate.monitoring = false AND candidate.next_run_at <= v_now
          AND candidate.task_type IN ('IMPORT_CHAPTER', 'PUBLISH')
          AND NULLIF(candidate.payload->>'orderKey', '') IS NULL AND v_presentable
          AND NOT EXISTS (
            SELECT 1 FROM public.importer_lite_tasks active_work
            WHERE active_work.work_key = v_work_key
              AND active_work.task_type IN ('IMPORT_CHAPTER', 'PUBLISH')
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
          AND candidate.monitoring = false AND candidate.next_run_at <= v_now
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
           AND v_task_chapter_key IS NOT NULL THEN
          SELECT
            COALESCE(bool_or(true), false),
            COALESCE(bool_or(
              src.enabled = true
              AND COALESCE(src.status, 'ACTIVE') <> 'DISABLED'
              AND COALESCE(src.chapter_ingestion_enabled, true)
              AND COALESCE(sh.health, 'HEALTHY') NOT IN ('BROKEN', 'DISABLED')
            ), false),
            COALESCE(bool_or(
              src.enabled = true
              AND COALESCE(src.status, 'ACTIVE') <> 'DISABLED'
              AND COALESCE(src.chapter_ingestion_enabled, true)
              AND COALESCE(sh.health, 'HEALTHY') NOT IN ('BROKEN', 'DISABLED')
              AND (sh.cooldown_until IS NULL OR sh.cooldown_until <= v_now)
            ), false),
            COALESCE(bool_or(
              src.enabled = true
              AND COALESCE(src.status, 'ACTIVE') <> 'DISABLED'
              AND COALESCE(src.chapter_ingestion_enabled, true)
              AND COALESCE(sh.health, 'HEALTHY') NOT IN ('BROKEN', 'DISABLED')
              AND (sh.cooldown_until IS NULL OR sh.cooldown_until <= v_now)
              AND NOT (v_has_excluded AND cs.source = ANY(p_excluded_sources))
            ), false)
          INTO
            v_has_confirmed,
            v_has_enabled_route,
            v_has_runnable_route,
            v_has_unexcluded_runnable_route
          FROM public.importer_lite_chapter_sources cs
          LEFT JOIN public.importer_sources src ON src.id = cs.source
          LEFT JOIN public.importer_lite_source_health sh ON sh.source = cs.source
          WHERE cs.work_id = v_task_work_id
            AND cs.canonical_chapter_key = v_task_chapter_key
            AND cs.confirmed = true
            AND COALESCE(cs.has_chapter, true) = true;

          IF v_has_enabled_route AND NOT v_has_runnable_route THEN
            IF v_refresh_budget > 0 THEN
              v_refresh_budget := v_refresh_budget - 1;
              PERFORM public.importer_lite_refresh_ready_work(v_work_key);
            ELSE
              UPDATE public.importer_lite_ready_works
              SET last_claimed_at = v_now,
                  next_run_at = GREATEST(next_run_at, v_now + interval '15 seconds'),
                  updated_at = v_now
              WHERE work_key = v_work_key;
            END IF;
            CONTINUE;
          END IF;

          IF v_has_excluded
             AND v_task_priority >= 2
             AND v_has_confirmed
             AND NOT v_has_unexcluded_runnable_route THEN
            IF v_fallback_task_id IS NULL THEN
              v_fallback_task_id := v_task_id;
              v_fallback_work_key := v_work_key;
            END IF;
            v_excluded_skips := v_excluded_skips + 1;
            IF v_excluded_skips >= 8
               AND v_fallback_task_id IS NOT NULL
               AND NOT (v_has_focus AND COALESCE(p_prefer_focus, true) AND v_work_key = ANY(p_focus_work_keys)) THEN
              EXIT;
            END IF;
            CONTINUE;
          END IF;
        END IF;

        UPDATE public.importer_lite_ready_works
        SET last_claimed_at = v_now,
            next_run_at = CASE
              WHEN v_work_key LIKE 'work:%' AND v_task_type IN ('IMPORT_CHAPTER', 'PUBLISH') THEN v_now + interval '30 seconds'
              ELSE v_now
            END,
            updated_at = v_now
        WHERE work_key = v_work_key;
        RETURN QUERY UPDATE public.importer_lite_tasks
        SET state = 'RUNNING', lease_owner = p_worker_id, lease_until = v_now + p_lease,
            attempts = attempts + 1, updated_at = v_now
        WHERE id = v_task_id RETURNING *;
        IF v_task_type NOT IN ('IMPORT_CHAPTER', 'PUBLISH') THEN
          PERFORM public.importer_lite_refresh_ready_work(v_work_key);
        END IF;
        RETURN;
      END IF;

      IF v_refresh_budget > 0 THEN
        v_refresh_budget := v_refresh_budget - 1;
        PERFORM public.importer_lite_refresh_ready_work(v_work_key);
      END IF;
      UPDATE public.importer_lite_ready_works
      SET last_claimed_at = v_now,
          next_run_at = GREATEST(next_run_at, v_now + interval '10 seconds'),
          updated_at = v_now
      WHERE work_key = v_work_key;
    END LOOP;

    IF v_fallback_task_id IS NOT NULL THEN
      UPDATE public.importer_lite_ready_works
      SET last_claimed_at = v_now,
          next_run_at = CASE WHEN v_fallback_work_key LIKE 'work:%' THEN v_now + interval '30 seconds' ELSE v_now END,
          updated_at = v_now
      WHERE work_key = v_fallback_work_key;
      RETURN QUERY UPDATE public.importer_lite_tasks
      SET state = 'RUNNING', lease_owner = p_worker_id, lease_until = v_now + p_lease,
          attempts = attempts + 1, updated_at = v_now
      WHERE id = v_fallback_task_id AND state = 'READY' RETURNING *;
      RETURN;
    END IF;
  END LOOP;

  RETURN;
END;
$function$;

-- 7. Update public.index_work_search() to include accent-folded tokens in works.search_text
--    and index published works with ybgin trigram index + stale search_text partial index.
CREATE OR REPLACE FUNCTION public.index_work_search()
RETURNS trigger
LANGUAGE plpgsql
AS $function$
DECLARE
  v_base text;
  v_folded text;
BEGIN
  v_base := trim(coalesce(new.title, '') || ' ' || array_to_string(coalesce(new.aliases, ARRAY[]::text[]), ' '));
  v_folded := translate(lower(v_base), 'áàâãäéèêëíìîïóòôõöúùûüçñ', 'aaaaaeeeeiiiiooooouuuucn');
  IF v_folded <> lower(v_base) THEN
    new.search_text := v_base || ' ' || v_folded;
  ELSE
    new.search_text := v_base;
  END IF;
  RETURN new;
END;
$function$;

CREATE INDEX IF NOT EXISTS idx_works_search_text_trgm
  ON public.works USING ybgin (search_text gin_trgm_ops)
  WHERE published = true;

CREATE INDEX IF NOT EXISTS idx_works_stale_search_text
  ON public.works (id)
  WHERE published = true AND search_text IS NULL;

