-- Migration: 20260913170000_unquarantine_discloud_importer.sql
-- Removes the legacy quarantine check for discloud-importer-1 in importer_acquire_job.
-- This restores 24/7 autonomous ingestion for the Discloud production worker.

CREATE OR REPLACE FUNCTION public.importer_acquire_job(
  p_worker_id text,
  p_lease_duration interval default interval '5 minutes',
  p_source text default null,
  p_task_type text default null
)
RETURNS TABLE (
  id uuid,
  task_type text,
  source text,
  priority integer,
  payload jsonb,
  dedupe_key text,
  status text,
  attempts integer,
  max_attempts integer,
  locked_by text,
  locked_at timestamptz,
  lease_expires_at timestamptz,
  next_run_at timestamptz,
  last_error text,
  chapter_sort_key numeric
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $fn$
DECLARE
  v_focus_request_id uuid;
  v_focus_work_id uuid;
  v_focus_created_at timestamptz;
  v_focus_status text;
  v_has_pending_jobs boolean;
  v_has_pending_mappings boolean;
  v_has_completed_work boolean;
  v_job_id uuid;
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

  -- 2. Select next job with candidate batching (eliminates O(N) correlated subqueries on 50k rows)
  SELECT q.id INTO v_job_id
  FROM public.importer_queue q
  WHERE q.id = (
    SELECT cand_batch.id
    FROM (
      SELECT q_cand.id, q_cand.task_type, q_cand.priority, q_cand.payload, q_cand.chapter_sort_key, q_cand.created_at
      FROM public.importer_queue q_cand
      WHERE (
        (q_cand.status in ('QUEUED', 'RETRY') and q_cand.next_run_at <= now())
        or
        (q_cand.status = 'IMPORTING' and q_cand.lease_expires_at < now())
      )
      AND (
        v_focus_work_id is null
        or
        (q_cand.payload->>'workId')::text = v_focus_work_id::text
      )
      AND (p_source is null or q_cand.source = p_source)
      AND (
        p_task_type is null
        or (p_task_type = 'DISCOVERY' and q_cand.task_type in ('DISCOVER_WORKS', 'SYNC_WORK'))
        or q_cand.task_type = p_task_type
      )
      AND not exists (
        SELECT 1 FROM public.importer_sources s
        WHERE s.id = q_cand.source
          AND (s.enabled = false or s.status in ('PAUSED', 'DISABLED', 'UPSTREAM_BLOCKED') or (s.cooldown_until is not null and s.cooldown_until > now()))
      )
      ORDER BY q_cand.priority DESC, q_cand.next_run_at ASC
      LIMIT 25
    ) cand_batch
    ORDER BY
      CASE
        WHEN v_focus_work_id is not null and (cand_batch.payload->>'workId')::text = v_focus_work_id::text THEN 100
        WHEN p_task_type = 'DISCOVERY' AND cand_batch.task_type = 'DISCOVER_WORKS' THEN 90
        WHEN p_task_type = 'DISCOVERY' AND cand_batch.task_type = 'SYNC_WORK' THEN 80
        WHEN cand_batch.task_type = 'IMPORT_CHAPTER'
         AND cand_batch.payload->>'workId' is not null
         AND cand_batch.chapter_sort_key is not null
         AND exists (
           SELECT 1
           FROM public.importer_chapter_mappings staged
           WHERE staged.work_id = (cand_batch.payload->>'workId')::uuid
             AND staged.status = 'STAGED'
             AND staged.chapter_sort_key > cand_batch.chapter_sort_key
         ) THEN 50
        ELSE 0
      END DESC,
      cand_batch.priority DESC,
      CASE WHEN cand_batch.chapter_sort_key IS NOT NULL THEN cand_batch.chapter_sort_key ELSE 999999 END ASC,
      cand_batch.created_at ASC
    LIMIT 1
  )
  FOR UPDATE OF q SKIP LOCKED;

  IF v_job_id IS NULL THEN
    RETURN;
  END IF;

  RETURN QUERY
  UPDATE public.importer_queue q
  SET
    status = 'IMPORTING',
    locked_by = p_worker_id,
    locked_at = now(),
    lease_expires_at = now() + p_lease_duration,
    attempts = q.attempts + 1,
    last_error = null,
    updated_at = now()
  WHERE q.id = v_job_id
  RETURNING
    q.id,
    q.task_type,
    q.source,
    q.priority,
    q.payload,
    q.dedupe_key,
    q.status,
    q.attempts,
    q.max_attempts,
    q.locked_by,
    q.locked_at,
    q.lease_expires_at,
    q.next_run_at,
    q.last_error,
    q.chapter_sort_key;
END;
$fn$;
