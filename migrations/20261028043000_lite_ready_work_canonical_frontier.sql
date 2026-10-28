-- PROJECT NOX: project each work at its executable canonical frontier.
--
-- A later P0 chapter must not elevate an earlier P1 prerequisite from the
-- same work.  The canonical barrier still requires that P1 chapter first;
-- advertising the work as P0 meanwhile made the bounded claim probe revisit
-- dependent work and left independent P1 sources idle.  Materialise the
-- same earliest ordered task used by importer_lite_claim_task instead.
--
-- This changes only the disposable ready-work projection.  Durable tasks,
-- chapter order, priority values and publication rows remain untouched.

-- A confirmed historical P1 task remains a canonical barrier while at least
-- one of its recorded routes can run. If every recorded route is under a
-- durable provider cooldown/block, it is an unavailable gap rather than a
-- reason to hold a newer P0 release (or unrelated works) hostage. Tasks with
-- no recorded route preserve legacy behaviour and are still barriers.
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
    WHERE route.work_id=p_work_id
      AND route.canonical_chapter_key=p_chapter_key
      AND route.confirmed=true
      AND COALESCE(route.has_chapter,true)=true
  ), runnable_route AS (
    SELECT 1
    FROM recorded_route route
    JOIN public.importer_sources provider ON provider.id=route.provider_id
    LEFT JOIN public.importer_lite_source_health health ON health.source=provider.id
    WHERE provider.enabled=true
      AND (COALESCE(provider.status,'ACTIVE') IN ('ACTIVE','DEGRADED','RECOVERING')
        OR (provider.status='COOLDOWN' AND provider.cooldown_until IS NOT NULL
            AND provider.cooldown_until<=now()))
      AND COALESCE((to_jsonb(provider)->>'chapter_ingestion_enabled')::boolean,true)=true
      AND COALESCE(health.health,'HEALTHY') NOT IN ('BROKEN','DISABLED')
      AND (health.cooldown_until IS NULL OR health.cooldown_until<=now())
      AND (to_jsonb(provider)->>'blocked_reason' IS NULL
        OR to_jsonb(provider)->'blocked_details'->>'probe_success'='true'
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

  -- Ordered chapter work is a single canonical frontier.  Include a recent
  -- RETRY_WAIT predecessor so the projection sleeps until its durable retry
  -- boundary instead of repeatedly claiming a later READY chapter that may
  -- not legally advance.  A permanently failed or stale retry intentionally
  -- remains outside this frontier, matching the existing claim barrier.
  WITH ordered_frontier AS (
    SELECT task.priority,task.next_run_at,
      CASE task.task_type WHEN 'PUBLISH' THEN 0 ELSE 1 END AS operation_rank
    FROM public.importer_lite_tasks task
    WHERE task.work_key=p_work_key
      AND task.monitoring=false
      AND task.task_type IN ('IMPORT_CHAPTER','PUBLISH')
      AND NULLIF(task.payload->>'orderKey','') IS NOT NULL
      AND v_presentable
      AND NOT (
        task.task_type='IMPORT_CHAPTER' AND task.priority>=2
        AND COALESCE(task.payload->>'manifestP1Repair','false')='true'
        AND public.importer_lite_manifest_p1_route_deferred(task.work_id,task.chapter_key)
      )
      AND (
        task.state='READY'
        OR (task.state='RETRY_WAIT' AND task.attempts<task.max_attempts
            AND task.updated_at>now()-interval '15 minutes')
      )
    ORDER BY NULLIF(task.payload->>'orderKey',''),
      CASE task.task_type WHEN 'PUBLISH' THEN 0 ELSE 1 END,
      task.priority,task.next_run_at,task.created_at,task.id
    LIMIT 1
  ), fallback AS (
    -- Non-chapter work and legacy null-order identities retain their prior
    -- precedence.  This branch is reached only when no canonical frontier is
    -- currently constraining the work.
    SELECT task.priority,task.next_run_at,
      CASE task.task_type WHEN 'PUBLISH' THEN 0 WHEN 'IMPORT_CHAPTER' THEN 1
        WHEN 'CATALOG' THEN 2 ELSE 3 END AS operation_rank
    FROM public.importer_lite_tasks task
    WHERE task.work_key=p_work_key
      AND task.state='READY'
      AND task.monitoring=false
      AND NOT (task.task_type='CATALOG' AND task.payload ? 'metadataSourceRefresh')
      AND (
        task.task_type NOT IN ('IMPORT_CHAPTER','PUBLISH')
        OR (v_presentable AND NULLIF(task.payload->>'orderKey','') IS NULL)
      )
    ORDER BY task.priority,
      CASE task.task_type WHEN 'PUBLISH' THEN 0 WHEN 'IMPORT_CHAPTER' THEN 1
        WHEN 'CATALOG' THEN 2 ELSE 3 END,
      task.next_run_at,task.created_at,task.id
    LIMIT 1
  )
  SELECT COALESCE(frontier.priority,other.priority),
         COALESCE(frontier.next_run_at,other.next_run_at),
         COALESCE(frontier.operation_rank,other.operation_rank)
    INTO v_priority,v_next,v_operation_rank
  FROM (SELECT * FROM ordered_frontier) frontier
  FULL JOIN (SELECT * FROM fallback) other ON true;

  IF NOT FOUND THEN
    DELETE FROM public.importer_lite_ready_works WHERE work_key=p_work_key;
  ELSE
    INSERT INTO public.importer_lite_ready_works(work_key,priority,operation_rank,next_run_at,updated_at)
    VALUES(p_work_key,v_priority,v_operation_rank,v_next,now())
    ON CONFLICT(work_key) DO UPDATE
      SET priority=excluded.priority,operation_rank=excluded.operation_rank,
          next_run_at=excluded.next_run_at,updated_at=now();
  END IF;
END;
$function$;

-- Keep the bounded claim path aligned with the ready-work projection above.
-- Otherwise an unavailable P1 gap could be excluded from the projection but
-- selected again as the work-local earliest chapter, recreating the exact
-- starvation this migration removes.
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
        SELECT 1 FROM public.importer_lite_tasks active_work
        WHERE active_work.work_key=rw.work_key
          AND active_work.task_type IN ('IMPORT_CHAPTER','PUBLISH')
          AND active_work.state='RUNNING'
      )
    ORDER BY rw.priority,rw.operation_rank,rw.last_claimed_at NULLS FIRST,rw.next_run_at,rw.work_key
    LIMIT 4
    FOR UPDATE OF rw SKIP LOCKED
  ) LOOP
    v_work_key := r.work_key;
    v_presentable := false;
    v_task_id := NULL;
    IF v_work_key LIKE 'work:%' THEN
      SELECT EXISTS (
        SELECT 1 FROM public.works work
        JOIN public.media cover ON cover.id=work.cover_id
        WHERE work.id=NULLIF(substring(v_work_key from 6),'')::uuid
          AND NULLIF(btrim(work.title),'') IS NOT NULL
          AND NULLIF(btrim(work.slug),'') IS NOT NULL
          AND cover.storage_ready=true AND cover.bytes>=1500
      ) INTO v_presentable;
    END IF;

    WITH earliest_pending_chapter AS (
      SELECT candidate.id,candidate.task_type,candidate.priority,candidate.state,
             candidate.next_run_at,candidate.created_at,candidate.monitoring,
             NULLIF(candidate.payload->>'orderKey','') AS order_key
      FROM public.importer_lite_tasks candidate
      WHERE candidate.work_key=v_work_key
        AND candidate.task_type IN ('IMPORT_CHAPTER','PUBLISH')
        AND NULLIF(candidate.payload->>'orderKey','') IS NOT NULL
        AND NOT (
          candidate.task_type='IMPORT_CHAPTER' AND candidate.priority>=2
          AND COALESCE(candidate.payload->>'manifestP1Repair','false')='true'
          AND public.importer_lite_manifest_p1_route_deferred(candidate.work_id,candidate.chapter_key)
        )
        AND (candidate.state='READY'
          OR (candidate.state='RETRY_WAIT' AND candidate.attempts<candidate.max_attempts
              AND candidate.updated_at>now()-interval '15 minutes'))
      ORDER BY NULLIF(candidate.payload->>'orderKey',''),
        CASE WHEN candidate.task_type='PUBLISH' THEN 0 ELSE 1 END,
        candidate.priority,candidate.next_run_at,candidate.created_at,candidate.id
      LIMIT 1
    ), candidate_pool AS (
      SELECT pending.id,pending.task_type,pending.priority,pending.next_run_at,
             pending.created_at,pending.order_key,false AS historical_p1
      FROM earliest_pending_chapter pending
      WHERE pending.state='READY' AND pending.monitoring=false
        AND pending.next_run_at<=now() AND v_presentable
        AND NOT EXISTS (
          SELECT 1 FROM public.importer_lite_tasks active_work
          WHERE active_work.work_key=v_work_key
            AND active_work.task_type IN ('IMPORT_CHAPTER','PUBLISH')
            AND active_work.state='RUNNING' AND active_work.id<>pending.id
        )
      UNION ALL
      SELECT candidate.id,candidate.task_type,candidate.priority,candidate.next_run_at,
             candidate.created_at,NULLIF(candidate.payload->>'orderKey',''),false
      FROM public.importer_lite_tasks candidate
      WHERE candidate.work_key=v_work_key AND candidate.state='READY'
        AND candidate.monitoring=false AND candidate.next_run_at<=now()
        AND candidate.task_type IN ('IMPORT_CHAPTER','PUBLISH')
        AND NULLIF(candidate.payload->>'orderKey','') IS NULL AND v_presentable
        AND NOT EXISTS (
          SELECT 1 FROM public.importer_lite_tasks active_work
          WHERE active_work.work_key=v_work_key
            AND active_work.task_type IN ('IMPORT_CHAPTER','PUBLISH')
            AND active_work.state='RUNNING' AND active_work.id<>candidate.id
        )
      UNION ALL
      SELECT candidate.id,candidate.task_type,candidate.priority,candidate.next_run_at,
             candidate.created_at,NULLIF(candidate.payload->>'orderKey',''),
             (candidate.task_type='CATALOG' AND candidate.priority=2
               AND candidate.payload ? 'historicalP1Backfill'
               AND COALESCE(candidate.payload->>'deferBehindReadyImport','false')<>'true')
      FROM public.importer_lite_tasks candidate
      WHERE candidate.work_key=v_work_key AND candidate.state='READY'
        AND candidate.monitoring=false AND candidate.next_run_at<=now()
        AND NOT (candidate.task_type='CATALOG' AND candidate.payload ? 'metadataSourceRefresh')
        AND candidate.task_type NOT IN ('IMPORT_CHAPTER','PUBLISH')
    ), selected_candidate AS (
      SELECT id FROM candidate_pool
      ORDER BY CASE
          WHEN task_type='PUBLISH' THEN 0
          WHEN task_type='IMPORT_CHAPTER' THEN 1
          WHEN task_type='CATALOG' AND priority=2 AND historical_p1 THEN 1
          WHEN task_type='CATALOG' THEN 2 ELSE 3 END,
        CASE WHEN task_type IN ('PUBLISH','IMPORT_CHAPTER') THEN order_key END NULLS LAST,
        priority,next_run_at,created_at,id
      LIMIT 1
    )
    SELECT candidate.id INTO v_task_id
    FROM public.importer_lite_tasks candidate
    JOIN selected_candidate selected ON selected.id=candidate.id
    FOR UPDATE OF candidate SKIP LOCKED;

    IF v_task_id IS NOT NULL THEN
      UPDATE public.importer_lite_ready_works
      SET last_claimed_at=now(),
          next_run_at=CASE WHEN v_work_key LIKE 'work:%' THEN now()+interval '30 seconds' ELSE now() END,
          updated_at=now()
      WHERE work_key=v_work_key;
      RETURN QUERY UPDATE public.importer_lite_tasks
      SET state='RUNNING',lease_owner=p_worker_id,lease_until=now()+p_lease,
          attempts=attempts+1,updated_at=now()
      WHERE id=v_task_id RETURNING *;
      RETURN;
    END IF;

    UPDATE public.importer_lite_ready_works
    SET last_claimed_at=now(),next_run_at=now()+interval '10 seconds',updated_at=now()
    WHERE work_key=v_work_key;
  END LOOP;
  RETURN;
END;
$function$;
