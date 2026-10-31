-- Optimize YSQL CPU and claim latency:
-- 1. Clear stale global source cooldowns triggered by single-chapter content errors (INVALID_PAGE_MANIFEST, PAGE_HTTP_404, CHAPTER_CONTENT_INVALID_PLACEHOLDER).
-- 2. Bound candidate horizon in importer_lite_claim_task_focused (LIMIT 64 on Pass 1, LIMIT 32 on Pass 2).
-- 3. Collapse 4 separate importer_lite_chapter_sources EXISTS/NOT EXISTS queries per loop iteration into 1 single bool_or() pass.
-- 4. Remove redundant Raft WAL UPDATE on importer_lite_ready_works when skipping a saturated source in p_excluded_sources, and bound non-focus excluded skips to 10 once a fallback task is captured.

UPDATE public.importer_lite_source_health
SET cooldown_until = NULL,
    updated_at = now()
WHERE health = 'DEGRADED'
  AND cooldown_until IS NOT NULL
  AND last_error ~ '^(INVALID_PAGE_MANIFEST|PAGE_HTTP_404|CHAPTER_CONTENT_INVALID_PLACEHOLDER|CATALOG_SOURCE_TIMEOUT|METADATA_SOURCE_TIMEOUT)';

CREATE INDEX IF NOT EXISTS importer_lite_tasks_historical_p1_backfill_idx
ON public.importer_lite_tasks (state ASC, next_run_at ASC, created_at ASC, id ASC)
WHERE task_type = 'CATALOG'
  AND monitoring = false
  AND priority = 2
  AND state IN ('READY', 'RUNNING', 'RETRY_WAIT')
  AND (payload ? 'historicalP1Backfill');

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
        LIMIT 64
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
          LIMIT 32
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
          LIMIT 32
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
           AND v_task_chapter_key IS NOT NULL THEN
          SELECT
            COALESCE(bool_or(true), false),
            COALESCE(bool_or(
              src.enabled = true
              AND COALESCE(src.status, 'ACTIVE') <> 'DISABLED'
              AND COALESCE((to_jsonb(src)->>'chapter_ingestion_enabled')::boolean, true)
              AND COALESCE(sh.health, 'HEALTHY') NOT IN ('BROKEN', 'DISABLED')
            ), false),
            COALESCE(bool_or(
              src.enabled = true
              AND COALESCE(src.status, 'ACTIVE') <> 'DISABLED'
              AND COALESCE((to_jsonb(src)->>'chapter_ingestion_enabled')::boolean, true)
              AND COALESCE(sh.health, 'HEALTHY') NOT IN ('BROKEN', 'DISABLED')
              AND (sh.cooldown_until IS NULL OR sh.cooldown_until <= now())
            ), false),
            COALESCE(bool_or(
              src.enabled = true
              AND COALESCE(src.status, 'ACTIVE') <> 'DISABLED'
              AND COALESCE((to_jsonb(src)->>'chapter_ingestion_enabled')::boolean, true)
              AND COALESCE(sh.health, 'HEALTHY') NOT IN ('BROKEN', 'DISABLED')
              AND (sh.cooldown_until IS NULL OR sh.cooldown_until <= now())
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
            PERFORM public.importer_lite_refresh_ready_work(v_work_key);
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
            IF v_excluded_skips >= 10
               AND v_fallback_task_id IS NOT NULL
               AND NOT (v_has_focus AND COALESCE(p_prefer_focus, true) AND v_work_key = ANY(p_focus_work_keys)) THEN
              EXIT;
            END IF;
            CONTINUE;
          END IF;
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
