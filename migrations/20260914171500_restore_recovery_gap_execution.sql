CREATE OR REPLACE FUNCTION public.importer_acquire_job(p_worker_id text, p_lease_duration interval DEFAULT '00:05:00'::interval, p_source text DEFAULT NULL::text, p_task_type text DEFAULT NULL::text)
 RETURNS TABLE(id uuid, task_type text, source text, priority integer, payload jsonb, dedupe_key text, status text, attempts integer, max_attempts integer, locked_by text, locked_at timestamp with time zone, lease_expires_at timestamp with time zone, next_run_at timestamp with time zone, last_error text, chapter_sort_key numeric)
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_focus_request_id uuid;
  v_focus_work_id uuid;
  v_focus_created_at timestamptz;
  v_focus_status text;
  v_has_pending_jobs boolean;
  v_has_pending_mappings boolean;
  v_has_completed_work boolean;
  v_job_id uuid;
  v_barrier_state text;
BEGIN
  IF NOT public.importer_admission_available(p_task_type) THEN RETURN; END IF;
  -- 0. Check PublicationSafetyBarrier state from settings
  SELECT value INTO v_barrier_state
  FROM public.settings
  WHERE key = 'publication_safety_barrier';

  -- 1. Check if there is an active focus work (Absolute Priority)
  SELECT sr.id, sr.work_id, sr.created_at, sr.status
  INTO v_focus_request_id, v_focus_work_id, v_focus_created_at, v_focus_status
  FROM public.importer_staff_requests sr
  WHERE sr.status in ('QUEUED', 'IMPORTING', 'RETRYING')
  ORDER BY sr.created_at desc
  LIMIT 1;

  IF v_focus_request_id is not null THEN
    SELECT exists (
      SELECT 1 FROM public.importer_queue q
      WHERE (q.payload->>'workId')::text = v_focus_work_id::text
        AND q.status in ('QUEUED', 'RETRY', 'IMPORTING')
    ) INTO v_has_pending_jobs;

    SELECT exists (
      SELECT 1 FROM public.importer_chapter_mappings m
      WHERE m.work_id = v_focus_work_id
        AND m.status in ('PENDING', 'IMPORTING', 'STAGED')
    ) INTO v_has_pending_mappings;

    SELECT (c.published_at is not null) INTO v_has_completed_work
    FROM public.chapters c
    WHERE c.work_id = v_focus_work_id
    ORDER BY c.created_at desc
    LIMIT 1;

    IF not v_has_pending_jobs and not v_has_pending_mappings and coalesce(v_has_completed_work, false) THEN
      UPDATE public.importer_staff_requests
      SET status = 'COMPLETED', updated_at = now()
      WHERE id = v_focus_request_id;

      v_focus_request_id := null;
      v_focus_work_id := null;
    ELSIF v_focus_status = 'QUEUED' THEN
      UPDATE public.importer_staff_requests
      SET status = 'IMPORTING', updated_at = now()
      WHERE public.importer_staff_requests.id = v_focus_request_id
        AND public.importer_staff_requests.status = 'QUEUED';
    END IF;
  END IF;

  -- 2. Select next job using CTE candidate selection (Fast Lane + Recovery Lane)
  SELECT q.id INTO v_job_id
  FROM public.importer_queue q
  WHERE q.id = (
    WITH staged_works AS (
      SELECT m.work_id, max(m.chapter_sort_key) as max_staged_sort_key
      FROM public.importer_chapter_mappings m
      WHERE m.status = 'STAGED'
      GROUP BY m.work_id
    ),
    recovery_candidates AS (
      -- PUBLICATION_RECOVERY_GAP Lane: Gap chapters that unblock STAGED chapters
      -- This lane remains active even when v_barrier_state = 'CLOSED' to break deadlocks!
      SELECT
        q_rec.id,
        q_rec.task_type,
        q_rec.source,
        85 as effective_priority,
        q_rec.payload,
        q_rec.chapter_sort_key,
        q_rec.created_at,
        q_rec.next_run_at
      FROM staged_works sw
      JOIN public.importer_queue q_rec ON (
        (q_rec.payload->>'workId')::text = sw.work_id::text
        AND q_rec.chapter_sort_key < sw.max_staged_sort_key
        AND q_rec.task_type = 'IMPORT_CHAPTER'
        AND q_rec.status IN ('QUEUED', 'RETRY')
        AND q_rec.next_run_at <= now()
      )
      WHERE (p_source IS NULL OR q_rec.source = p_source)
        AND (p_task_type IS NULL OR p_task_type = 'IMPORT_CHAPTER')
        AND (v_focus_work_id IS NULL OR (q_rec.payload->>'workId')::text = v_focus_work_id::text)
        AND NOT exists (
          SELECT 1 FROM public.importer_sources s
          WHERE s.id = q_rec.source
            AND (s.enabled = false OR s.status IN ('PAUSED', 'DISABLED', 'UPSTREAM_BLOCKED') OR (s.cooldown_until IS NOT NULL AND s.cooldown_until > now()))
        )
      LIMIT 10
    ),
    normal_candidates AS (
      -- Normal queue processing (respects barrier CLOSED)
      SELECT
        q_norm.id,
        q_norm.task_type,
        q_norm.source,
        CASE
          WHEN v_focus_work_id is not null and (q_norm.payload->>'workId')::text = v_focus_work_id::text THEN 100
          WHEN p_task_type = 'DISCOVERY' AND q_norm.task_type = 'DISCOVER_WORKS' THEN 95
          WHEN p_task_type = 'DISCOVERY' AND q_norm.task_type = 'SYNC_WORK' THEN 90
          ELSE q_norm.priority
        END as effective_priority,
        q_norm.payload,
        q_norm.chapter_sort_key,
        q_norm.created_at,
        q_norm.next_run_at
      FROM public.importer_queue q_norm
      WHERE q_norm.status in ('QUEUED', 'RETRY')
        AND q_norm.next_run_at <= now()
        AND (
          v_focus_work_id is null
          or (q_norm.payload->>'workId')::text = v_focus_work_id::text
        )
        AND (p_source is null or q_norm.source = p_source)
        AND (
          p_task_type is null
          or (p_task_type = 'DISCOVERY' and q_norm.task_type in ('DISCOVER_WORKS', 'SYNC_WORK'))
          or q_norm.task_type = p_task_type
        )
        AND (
          coalesce(v_barrier_state, 'CLOSED') IN ('OPEN','CAUTION')
          OR q_norm.task_type in ('DISCOVER_WORKS', 'SYNC_WORK')
        )
        AND not exists (
          SELECT 1 FROM public.importer_sources s
          WHERE s.id = q_norm.source
            AND (s.enabled = false or s.status in ('PAUSED', 'DISABLED', 'UPSTREAM_BLOCKED') or (s.cooldown_until is not null and s.cooldown_until > now()))
        )
      ORDER BY q_norm.priority DESC, q_norm.next_run_at ASC
      LIMIT 25
    ),
    combined_candidates AS (
      SELECT * FROM recovery_candidates
      UNION ALL
      SELECT * FROM normal_candidates
    )
    SELECT cand.id
    FROM combined_candidates cand
    ORDER BY
      cand.effective_priority DESC,
      CASE WHEN cand.chapter_sort_key IS NOT NULL THEN cand.chapter_sort_key ELSE 999999 END ASC,
      cand.created_at ASC
    LIMIT 1
  )
  FOR UPDATE OF q SKIP LOCKED;

  -- 2b. Fallback: Check expired leases only if no queued job found
  IF v_job_id IS NULL THEN
    SELECT q.id INTO v_job_id
    FROM public.importer_queue q
    WHERE q.id = (
      SELECT q_cand.id
      FROM public.importer_queue q_cand
      WHERE q_cand.status = 'IMPORTING'
        AND q_cand.lease_expires_at < now()
        AND (q_cand.task_type IN ('DISCOVER_WORKS','SYNC_WORK') OR coalesce(v_barrier_state,'CLOSED') IN ('OPEN','CAUTION') OR EXISTS (
          SELECT 1 FROM importer_chapter_mappings m WHERE m.status='STAGED'
          AND m.work_id::text=q_cand.payload->>'workId' AND m.chapter_sort_key>q_cand.chapter_sort_key
        ))
        AND (p_source is null or q_cand.source = p_source)
        AND (
          p_task_type is null
          or (p_task_type = 'DISCOVERY' and q_cand.task_type in ('DISCOVER_WORKS', 'SYNC_WORK'))
          or q_cand.task_type = p_task_type
        )
      ORDER BY q_cand.lease_expires_at ASC
      LIMIT 1
    )
    FOR UPDATE OF q SKIP LOCKED;
  END IF;

  IF v_job_id IS NULL THEN
    RETURN;
  END IF;

  -- 3. Lock the selected job atomically
  RETURN QUERY
  UPDATE public.importer_queue
  SET
    status = 'IMPORTING',
    locked_by = p_worker_id,
    locked_at = now(),
    lease_expires_at = now() + p_lease_duration,
    attempts = public.importer_queue.attempts + 1,
    updated_at = now()
  WHERE public.importer_queue.id = v_job_id
  RETURNING
    public.importer_queue.id,
    public.importer_queue.task_type,
    public.importer_queue.source,
    public.importer_queue.priority,
    public.importer_queue.payload,
    public.importer_queue.dedupe_key,
    public.importer_queue.status,
    public.importer_queue.attempts,
    public.importer_queue.max_attempts,
    public.importer_queue.locked_by,
    public.importer_queue.locked_at,
    public.importer_queue.lease_expires_at,
    public.importer_queue.next_run_at,
    public.importer_queue.last_error,
    public.importer_queue.chapter_sort_key;
END;
$function$
;
