-- ==============================================================================
-- Project Nox Importer - Migration 20260913010000:
-- Dedicated Discovery Lane, Stale Discovery Consolidation, and Fair Scheduling
--
-- 1. Extend importer_queue status check constraint with 'SUPERSEDED'
-- 2. Consolidate accumulated stale DISCOVER_WORKS jobs
-- 3. Upgrade importer_acquire_job stored procedure with p_task_type support
-- ==============================================================================

-- 1. Extend importer_queue status check constraint with 'SUPERSEDED'
ALTER TABLE public.importer_queue
  DROP CONSTRAINT IF EXISTS importer_queue_status_check;

ALTER TABLE public.importer_queue
  ADD CONSTRAINT importer_queue_status_check
  CHECK (status IN (
    'QUEUED', 'IMPORTING', 'COMPLETED', 'FAILED', 'RETRY',
    'PAUSED_BY_STAFF', 'CANCELLED_BY_STAFF', 'BLOCKED_BY_UPSTREAM', 'SUPERSEDED'
  ));

-- 2. Consolidate accumulated stale DISCOVER_WORKS jobs (> 1 hour old or redundant)
-- Mark stale queued discoveries older than 1 hour as SUPERSEDED
UPDATE public.importer_queue
SET
  status = 'SUPERSEDED',
  last_error = 'superseded_by_discovery_lane_consolidation',
  updated_at = now()
WHERE task_type = 'DISCOVER_WORKS'
  AND status = 'QUEUED'
  AND created_at < now() - interval '1 hour';

-- For any remaining QUEUED discoveries, keep only the newest 1 per source
WITH ranked_discoveries AS (
  SELECT id,
         ROW_NUMBER() OVER (PARTITION BY source ORDER BY created_at DESC) as rn
  FROM public.importer_queue
  WHERE task_type = 'DISCOVER_WORKS'
    AND status = 'QUEUED'
)
UPDATE public.importer_queue
SET
  status = 'SUPERSEDED',
  last_error = 'superseded_duplicate_discovery',
  updated_at = now()
WHERE id IN (
  SELECT id FROM ranked_discoveries WHERE rn > 1
);

-- 3. Upgrade importer_acquire_job stored procedure with p_task_type support
DROP FUNCTION IF EXISTS public.importer_acquire_job(text, interval, text);

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
AS $$
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
  -- Quarantine stalled legacy worker instance
  IF p_worker_id = 'discloud-importer-1' THEN
    RETURN;
  END IF;

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

  -- 2. Select next job respecting focus mode, source filter, and task type filter
  SELECT q.id INTO v_job_id
  FROM public.importer_queue q
  WHERE (
    (q.status in ('QUEUED', 'RETRY') and q.next_run_at <= now())
    or
    (q.status = 'IMPORTING' and q.lease_expires_at < now())
  )
  AND (
    v_focus_work_id is null
    or
    (q.payload->>'workId')::text = v_focus_work_id::text
  )
  AND (p_source is null or q.source = p_source)
  AND (
    p_task_type is null
    or (p_task_type = 'DISCOVERY' and q.task_type in ('DISCOVER_WORKS', 'SYNC_WORK'))
    or q.task_type = p_task_type
  )
  AND not exists (
    SELECT 1 FROM public.importer_sources s
    WHERE s.id = q.source
      AND (s.enabled = false or s.status in ('PAUSED', 'DISABLED', 'UPSTREAM_BLOCKED') or (s.cooldown_until is not null and s.cooldown_until > now()))
  )
  ORDER BY
    CASE
      WHEN v_focus_work_id is not null and (q.payload->>'workId')::text = v_focus_work_id::text THEN 100
      WHEN p_task_type = 'DISCOVERY' AND q.task_type = 'DISCOVER_WORKS' THEN 90
      WHEN p_task_type = 'DISCOVERY' AND q.task_type = 'SYNC_WORK' THEN 80
      WHEN q.task_type = 'IMPORT_CHAPTER'
       AND q.payload->>'workId' is not null
       AND q.chapter_sort_key is not null
       AND exists (
         SELECT 1
         FROM public.importer_chapter_mappings staged
         WHERE staged.work_id = (q.payload->>'workId')::uuid
           AND staged.status = 'STAGED'
           AND staged.chapter_sort_key > q.chapter_sort_key
       )
       AND not exists (
         SELECT 1
         FROM public.importer_queue q2
         WHERE q2.task_type = 'IMPORT_CHAPTER'
           AND (q2.payload->>'workId')::text = q.payload->>'workId'
           AND q2.status in ('QUEUED', 'RETRY', 'IMPORTING')
           AND q2.id != q.id
           AND q2.chapter_sort_key < q.chapter_sort_key
       )
       AND not exists (
         SELECT 1
         FROM public.importer_chapter_mappings m2
         LEFT JOIN public.chapters c2 on c2.id = m2.chapter_id
         WHERE m2.work_id = (q.payload->>'workId')::uuid
           AND m2.chapter_sort_key < q.chapter_sort_key
           AND (c2.published_at is null or m2.status != 'COMPLETED')
       ) THEN 50
      ELSE 0
    END DESC,
    q.priority DESC,
    CASE WHEN q.chapter_sort_key IS NOT NULL THEN q.chapter_sort_key ELSE 999999 END ASC,
    q.created_at ASC
  LIMIT 1
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
$$;

REVOKE ALL ON FUNCTION public.importer_acquire_job(text, interval, text, text) FROM public, anon, authenticated;
GRANT EXECUTE ON FUNCTION public.importer_acquire_job(text, interval, text, text) TO service_role;
