-- Migration: Queue Fairness & Barrier Fixes
-- This ensures that the Recovery Lane picks jobs fairly across ALL stalled works
-- by limiting the selection to 1 job per work before ordering.

CREATE OR REPLACE FUNCTION public.importer_acquire_job(
  p_worker_id text,
  p_lease_duration interval DEFAULT '5 minutes'::interval,
  p_source text DEFAULT NULL::text,
  p_task_type text DEFAULT NULL::text
) RETURNS TABLE(
  id uuid,
  task_type text,
  source text,
  priority integer,
  payload jsonb,
  dedupe_key text,
  status public.importer_job_status,
  attempts integer,
  max_attempts integer,
  locked_by text,
  locked_at timestamp with time zone,
  lease_expires_at timestamp with time zone,
  next_run_at timestamp with time zone,
  last_error text,
  chapter_sort_key real
)
LANGUAGE plpgsql
AS $function$
DECLARE
  v_job_id uuid;
  v_focus_request_id uuid;
  v_focus_work_id uuid;
  v_focus_created_at timestamp with time zone;
  v_focus_status text;
  v_has_pending_jobs boolean;
  v_has_pending_mappings boolean;
  v_has_completed_work boolean;
  v_barrier_state text;
BEGIN
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
        AND q.status in ('QUEUED', 'IMPORTING', 'RETRY')
    ) INTO v_has_pending_jobs;

    SELECT exists (
      SELECT 1 FROM public.importer_chapter_mappings m
      WHERE m.work_id = v_focus_work_id
        AND m.status in ('PENDING', 'IMPORTING', 'STAGED')
        AND m.is_gap = false
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
    END IF;
  END IF;

  -- Read barrier setting
  SELECT value INTO v_barrier_state FROM public.settings WHERE key = 'publication_safety_barrier';

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
    recovery_candidates_raw AS (
      SELECT
        q_rec.id,
        q_rec.task_type,
        q_rec.source,
        85 as effective_priority,
        q_rec.payload,
        q_rec.chapter_sort_key,
        q_rec.created_at,
        q_rec.next_run_at,
        ROW_NUMBER() OVER(PARTITION BY (q_rec.payload->>'workId') ORDER BY q_rec.chapter_sort_key ASC) as rn
      FROM staged_works sw
      JOIN public.importer_queue q_rec ON (
        (q_rec.payload->>'workId')::text = sw.work_id::text
        AND q_rec.chapter_sort_key < sw.max_staged_sort_key
        AND q_rec.task_type = 'IMPORT_CHAPTER'
        AND q_rec.status IN ('QUEUED', 'RETRY')
        AND q_rec.next_run_at <= now()
      )
      WHERE (p_source IS NULL OR q_rec.source = p_source)
    ),
    recovery_candidates AS (
      -- Only take the FIRST missing chapter for each work to ensure fair round-robin recovery
      SELECT id, task_type, source, effective_priority, payload, chapter_sort_key, created_at, next_run_at
      FROM recovery_candidates_raw
      WHERE rn = 1
    ),
    normal_candidates AS (
      SELECT
        q_norm.id,
        q_norm.task_type,
        q_norm.source,
        q_norm.priority as effective_priority,
        q_norm.payload,
        q_norm.chapter_sort_key,
        q_norm.created_at,
        q_norm.next_run_at
      FROM public.importer_queue q_norm
      JOIN public.importer_sources s ON (s.id = q_norm.source)
      WHERE q_norm.status IN ('QUEUED', 'RETRY')
        AND q_norm.next_run_at <= now()
        AND (p_source IS NULL OR q_norm.source = p_source)
        AND (p_task_type IS NULL OR q_norm.task_type = p_task_type)
        AND s.enabled = true
        AND s.status NOT IN ('DISABLED', 'EXCLUDED_BY_POLICY')
        AND (
          v_focus_request_id is null OR
          (q_norm.payload->>'workId')::text = v_focus_work_id::text OR
          (q_norm.task_type = 'SYNC_WORK' AND (q_norm.payload->>'sourceWorkId')::text IN (
             SELECT wm.source_work_id FROM public.importer_work_mappings wm WHERE wm.work_id = v_focus_work_id
          ))
        )
        AND (
          -- Stop picking standard chapters if the barrier is CLOSED and the job is NOT a staff request
          coalesce(v_barrier_state, 'CLOSED') = 'OPEN'
          OR q_norm.task_type != 'IMPORT_CHAPTER'
          OR v_focus_request_id is not null
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
      cand.created_at ASC
    LIMIT 1
  );

  -- 3. If no new job is available, try to recover an expired lease (without breaking absolute priority)
  IF v_job_id is null THEN
    SELECT q_cand.id INTO v_job_id
    FROM public.importer_queue q_cand
    WHERE q_cand.status = 'IMPORTING'
      AND q_cand.lease_expires_at < now()
      AND (
        v_focus_request_id is null OR
        (q_cand.payload->>'workId')::text = v_focus_work_id::text
      )
      AND (
        p_source is null
        or q_cand.source = p_source
      )
      AND (
        p_task_type is null
        or (p_task_type = 'DISCOVERY' and q_cand.task_type in ('DISCOVER_WORKS', 'SYNC_WORK'))
        or q_cand.task_type = p_task_type
      )
    ORDER BY q_cand.lease_expires_at ASC
    LIMIT 1
    FOR UPDATE OF q_cand SKIP LOCKED;
  END IF;

  -- 4. Mark job as leased and return it
  IF v_job_id is not null THEN
    RETURN QUERY
    UPDATE public.importer_queue
    SET
      status = 'IMPORTING',
      locked_by = p_worker_id,
      locked_at = now(),
      lease_expires_at = now() + p_lease_duration
    WHERE id = v_job_id
    RETURNING *;
  END IF;

  RETURN;
END;
$function$;
