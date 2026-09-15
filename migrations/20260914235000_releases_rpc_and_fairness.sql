-- Migration: 20260914235000_releases_rpc_and_fairness.sql
-- Part 1: Efficient home releases RPC (12 works × 3 chapters, no time window)
-- Part 2: Fix importer_acquire_job for per-work fairness among normal works

-- ============================================================
-- 1. Home Releases RPC
-- Returns the 12 canonical works most recently published,
-- each with their 3 most recent published chapters.
-- No time-based TTL. Works stay until displaced by newer ones.
-- ============================================================
CREATE OR REPLACE FUNCTION public.get_recent_releases(p_limit integer DEFAULT 12, p_chapters_per_work integer DEFAULT 3)
RETURNS TABLE (
  work_id uuid,
  work_slug text,
  work_title text,
  work_kind text,
  work_cover_id uuid,
  work_content_rating text,
  latest_published_at timestamptz,
  chapter_id uuid,
  chapter_number numeric,
  chapter_title text,
  chapter_published_at timestamptz
)
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = public
AS $$
  WITH recent_works AS (
    SELECT
      c.work_id,
      MAX(c.published_at) AS latest_published_at
    FROM public.chapters c
    INNER JOIN public.works w ON w.id = c.work_id
    WHERE c.published_at IS NOT NULL
      AND w.published = true
    GROUP BY c.work_id
    ORDER BY MAX(c.published_at) DESC
    LIMIT p_limit
  ),
  ranked_chapters AS (
    SELECT
      c.id AS chapter_id,
      c.work_id,
      c.number AS chapter_number,
      c.title AS chapter_title,
      c.published_at AS chapter_published_at,
      ROW_NUMBER() OVER (PARTITION BY c.work_id ORDER BY c.published_at DESC) AS rn
    FROM public.chapters c
    INNER JOIN recent_works rw ON rw.work_id = c.work_id
    WHERE c.published_at IS NOT NULL
  )
  SELECT
    rw.work_id,
    w.slug AS work_slug,
    w.title AS work_title,
    w.kind AS work_kind,
    w.cover_id AS work_cover_id,
    w.content_rating AS work_content_rating,
    rw.latest_published_at,
    rc.chapter_id,
    rc.chapter_number,
    rc.chapter_title,
    rc.chapter_published_at
  FROM recent_works rw
  INNER JOIN public.works w ON w.id = rw.work_id
  INNER JOIN ranked_chapters rc ON rc.work_id = rw.work_id AND rc.rn <= p_chapters_per_work
  ORDER BY rw.latest_published_at DESC, rw.work_id, rc.chapter_published_at DESC;
$$;

REVOKE ALL ON FUNCTION public.get_recent_releases(integer, integer) FROM public, anon, authenticated;
GRANT EXECUTE ON FUNCTION public.get_recent_releases(integer, integer) TO anon, authenticated, service_role;

-- ============================================================
-- 2. Fix importer_acquire_job:
--    a. Normal works: per-work fairness via ROW_NUMBER window,
--       pick 1 candidate per work then best overall
--    b. Priority work (staff request): consumes all eligible slots
--       BUT: if priority work has no eligible job, allow normals
-- ============================================================
CREATE OR REPLACE FUNCTION public.importer_acquire_job(
  p_worker_id text,
  p_lease_duration interval DEFAULT interval '5 minutes',
  p_source text DEFAULT NULL,
  p_task_type text DEFAULT NULL
)
RETURNS TABLE(
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
  locked_at timestamp with time zone,
  lease_expires_at timestamp with time zone,
  next_run_at timestamp with time zone,
  last_error text,
  chapter_sort_key numeric
)
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
  v_focus_has_eligible boolean;
BEGIN
  IF NOT public.importer_admission_available(p_task_type) THEN RETURN; END IF;

  SELECT value INTO v_barrier_state
  FROM public.settings
  WHERE key = 'publication_safety_barrier';

  -- Resolve active staff request (priority work)
  SELECT sr.id, sr.work_id, sr.created_at, sr.status
  INTO v_focus_request_id, v_focus_work_id, v_focus_created_at, v_focus_status
  FROM public.importer_staff_requests sr
  WHERE sr.status IN ('QUEUED', 'IMPORTING', 'RETRYING')
  ORDER BY sr.created_at DESC
  LIMIT 1;

  IF v_focus_request_id IS NOT NULL THEN
    SELECT EXISTS (
      SELECT 1 FROM public.importer_queue q
      WHERE (q.payload->>'workId')::text = v_focus_work_id::text
        AND q.status IN ('QUEUED', 'IMPORTING', 'RETRY')
    ) INTO v_has_pending_jobs;

    SELECT EXISTS (
      SELECT 1 FROM public.importer_chapter_mappings m
      WHERE m.work_id = v_focus_work_id
        AND m.status IN ('PENDING', 'IMPORTING', 'STAGED')
        AND m.is_gap = false
    ) INTO v_has_pending_mappings;

    SELECT (c.published_at IS NOT NULL) INTO v_has_completed_work
    FROM public.chapters c
    WHERE c.work_id = v_focus_work_id
    ORDER BY c.created_at DESC
    LIMIT 1;

    IF NOT v_has_pending_jobs AND NOT v_has_pending_mappings AND COALESCE(v_has_completed_work, false) THEN
      UPDATE public.importer_staff_requests
      SET status = 'COMPLETED', updated_at = now()
      WHERE public.importer_staff_requests.id = v_focus_request_id;
      v_focus_request_id := NULL;
      v_focus_work_id := NULL;
    ELSIF v_focus_status = 'QUEUED' THEN
      UPDATE public.importer_staff_requests
      SET status = 'IMPORTING', updated_at = now()
      WHERE public.importer_staff_requests.id = v_focus_request_id
        AND public.importer_staff_requests.status = 'QUEUED';
    END IF;

    -- Check if priority work has any IMMEDIATELY eligible job
    -- (not just pending in mappings, but actually ready in queue)
    IF v_focus_work_id IS NOT NULL THEN
      SELECT EXISTS (
        SELECT 1 FROM public.importer_queue q
        WHERE (q.payload->>'workId')::text = v_focus_work_id::text
          AND q.status IN ('QUEUED', 'RETRY')
          AND q.next_run_at <= now()
          AND NOT EXISTS (
            SELECT 1 FROM public.importer_sources s
            WHERE s.id = q.source
              AND (s.enabled = false OR s.status IN ('PAUSED', 'DISABLED', 'UPSTREAM_BLOCKED')
                   OR (s.cooldown_until IS NOT NULL AND s.cooldown_until > now()))
          )
      ) INTO v_focus_has_eligible;
    ELSE
      v_focus_has_eligible := false;
    END IF;
  ELSE
    v_focus_has_eligible := false;
  END IF;

  WITH staged_works AS (
    SELECT m.work_id, max(m.chapter_sort_key) AS max_staged_sort_key
    FROM public.importer_chapter_mappings m
    WHERE m.status = 'STAGED'
    GROUP BY m.work_id
  ),
  inflight_counts AS (
    SELECT (q_in.payload->>'workId') AS work_id, count(*) AS active_jobs
    FROM public.importer_queue q_in
    WHERE q_in.status = 'IMPORTING'
    GROUP BY q_in.payload->>'workId'
  ),
  -- Recovery lane: jobs that are behind existing STAGED chapters for same work
  recovery_candidates_raw AS (
    SELECT
      q_rec.id,
      q_rec.task_type,
      q_rec.source,
      85 AS effective_priority,
      q_rec.payload,
      q_rec.chapter_sort_key,
      q_rec.created_at,
      q_rec.next_run_at,
      ROW_NUMBER() OVER(PARTITION BY (q_rec.payload->>'workId') ORDER BY q_rec.chapter_sort_key ASC) AS rn,
      COALESCE(ic.active_jobs, 0) AS current_inflight
    FROM staged_works sw
    JOIN public.importer_queue q_rec ON (
      (q_rec.payload->>'workId')::text = sw.work_id::text
      AND q_rec.chapter_sort_key < sw.max_staged_sort_key
      AND q_rec.task_type = 'IMPORT_CHAPTER'
      AND q_rec.status IN ('QUEUED', 'RETRY')
      AND q_rec.next_run_at <= now()
    )
    LEFT JOIN inflight_counts ic ON ic.work_id = (q_rec.payload->>'workId')::text
    WHERE (p_source IS NULL OR q_rec.source = p_source)
      AND (p_task_type IS NULL OR p_task_type = 'IMPORT_CHAPTER')
      -- Priority focus: if focus work has eligible jobs, restrict recovery lane to it
      -- If focus work has NO eligible jobs, allow all works (fallback to normals)
      AND (
        v_focus_work_id IS NULL
        OR NOT v_focus_has_eligible
        OR (q_rec.payload->>'workId')::text = v_focus_work_id::text
      )
      AND NOT EXISTS (
        SELECT 1 FROM public.importer_sources s
        WHERE s.id = q_rec.source
          AND (s.enabled = false OR s.status IN ('PAUSED', 'DISABLED', 'UPSTREAM_BLOCKED')
               OR (s.cooldown_until IS NOT NULL AND s.cooldown_until > now()))
      )
  ),
  recovery_candidates AS (
    SELECT rcr.id, rcr.task_type, rcr.source, rcr.effective_priority, rcr.payload,
           rcr.chapter_sort_key, rcr.created_at, rcr.next_run_at, rcr.current_inflight
    FROM recovery_candidates_raw rcr
    WHERE rcr.rn = 1
  ),
  -- Normal candidates with per-work fairness:
  -- When priority work has eligible jobs → restrict to that work
  -- When no priority work (or it's blocked) → apply per-work round-robin (1 candidate per work)
  normal_candidates_raw AS (
    SELECT
      q_norm.id,
      q_norm.task_type,
      q_norm.source,
      CASE
        WHEN v_focus_work_id IS NOT NULL AND (q_norm.payload->>'workId')::text = v_focus_work_id::text THEN 150
        WHEN p_task_type = 'DISCOVERY' AND q_norm.task_type = 'DISCOVER_WORKS' THEN 95
        WHEN p_task_type = 'DISCOVERY' AND q_norm.task_type = 'SYNC_WORK' THEN 90
        ELSE q_norm.priority
      END AS effective_priority,
      q_norm.payload,
      q_norm.chapter_sort_key,
      q_norm.created_at,
      q_norm.next_run_at,
      0::bigint AS current_inflight,
      -- Per-work fairness for normal works: rank each work's jobs by sort_key
      -- This ensures we pick the best job from each work, not 25 from the same work
      ROW_NUMBER() OVER(
        PARTITION BY
          CASE
            -- Priority work gets its own partition (not grouped with normals)
            WHEN v_focus_work_id IS NOT NULL AND (q_norm.payload->>'workId')::text = v_focus_work_id::text THEN 'PRIORITY:' || (q_norm.payload->>'workId')
            WHEN q_norm.task_type IN ('DISCOVER_WORKS', 'SYNC_WORK') THEN 'META:' || q_norm.source
            ELSE 'WORK:' || COALESCE(q_norm.payload->>'workId', q_norm.id::text)
          END
        ORDER BY q_norm.priority DESC, q_norm.chapter_sort_key ASC NULLS LAST, q_norm.created_at ASC
      ) AS work_rn
    FROM public.importer_queue q_norm
    WHERE q_norm.status IN ('QUEUED', 'RETRY')
      AND q_norm.next_run_at <= now()
      -- Priority enforcement: if priority work has eligible jobs, only pick from it
      AND (
        v_focus_work_id IS NULL
        OR NOT v_focus_has_eligible
        OR (q_norm.payload->>'workId')::text = v_focus_work_id::text
        OR q_norm.task_type IN ('DISCOVER_WORKS', 'SYNC_WORK')
      )
      AND (p_source IS NULL OR q_norm.source = p_source)
      AND (
        p_task_type IS NULL
        OR (p_task_type = 'DISCOVERY' AND q_norm.task_type IN ('DISCOVER_WORKS', 'SYNC_WORK'))
        OR q_norm.task_type = p_task_type
      )
      AND (
        COALESCE(v_barrier_state, 'CLOSED') IN ('OPEN', 'CAUTION')
        OR q_norm.task_type IN ('DISCOVER_WORKS', 'SYNC_WORK')
      )
      AND NOT EXISTS (
        SELECT 1 FROM public.importer_sources s
        WHERE s.id = q_norm.source
          AND (s.enabled = false OR s.status IN ('PAUSED', 'DISABLED', 'UPSTREAM_BLOCKED')
               OR (s.cooldown_until IS NOT NULL AND s.cooldown_until > now()))
      )
  ),
  -- For normal mode (no priority work eligible): pick best 1 candidate per work
  -- For priority mode: allow all jobs from that work
  normal_candidates AS (
    SELECT ncr.id, ncr.task_type, ncr.source, ncr.effective_priority, ncr.payload,
           ncr.chapter_sort_key, ncr.created_at, ncr.next_run_at, ncr.current_inflight
    FROM normal_candidates_raw ncr
    WHERE
      -- Priority work: no per-work cap (take all eligible slots)
      (v_focus_has_eligible AND v_focus_work_id IS NOT NULL
       AND (ncr.payload->>'workId')::text = v_focus_work_id::text)
      -- Normal fairness: only 1 candidate per work partition
      OR ncr.work_rn = 1
  ),
  combined_candidates AS (
    SELECT * FROM recovery_candidates
    UNION ALL
    SELECT * FROM normal_candidates
  )
  SELECT q.id INTO v_job_id
  FROM combined_candidates cand
  JOIN public.importer_queue q ON q.id = cand.id
  ORDER BY
    cand.effective_priority DESC,
    cand.current_inflight ASC,
    CASE WHEN cand.chapter_sort_key IS NOT NULL THEN cand.chapter_sort_key ELSE 999999 END ASC,
    cand.created_at ASC
  LIMIT 1
  FOR UPDATE OF q SKIP LOCKED;

  -- Fallback: reclaim stale IMPORTING leases
  IF v_job_id IS NULL THEN
    SELECT q.id INTO v_job_id
    FROM public.importer_queue q
    WHERE q.id = (
      SELECT q_cand.id
      FROM public.importer_queue q_cand
      WHERE q_cand.status = 'IMPORTING'
        AND q_cand.lease_expires_at < now()
        AND (q_cand.task_type IN ('DISCOVER_WORKS', 'SYNC_WORK') OR COALESCE(v_barrier_state, 'CLOSED') IN ('OPEN', 'CAUTION') OR EXISTS (
          SELECT 1 FROM importer_chapter_mappings m WHERE m.status = 'STAGED'
          AND m.work_id::text = q_cand.payload->>'workId' AND m.chapter_sort_key > q_cand.chapter_sort_key
        ))
        AND (p_source IS NULL OR q_cand.source = p_source)
        AND (p_task_type IS NULL OR q_cand.task_type = p_task_type)
        AND q_cand.attempts < q_cand.max_attempts
      ORDER BY q_cand.lease_expires_at ASC
      LIMIT 1
    )
    FOR UPDATE SKIP LOCKED;

    IF v_job_id IS NOT NULL THEN
      UPDATE public.importer_queue
      SET attempts = attempts + 1,
          status = 'RETRY',
          last_error = 'Lease reclaimed after expiry',
          updated_at = now()
      WHERE public.importer_queue.id = v_job_id;
    END IF;
  END IF;

  IF v_job_id IS NULL THEN RETURN; END IF;

  UPDATE public.importer_queue
  SET
    status = 'IMPORTING',
    locked_by = p_worker_id,
    locked_at = now(),
    lease_expires_at = now() + p_lease_duration,
    attempts = CASE WHEN status = 'QUEUED' THEN attempts + 1 ELSE attempts END,
    updated_at = now()
  WHERE public.importer_queue.id = v_job_id;

  RETURN QUERY
  SELECT q.id, q.task_type, q.source, q.priority, q.payload, q.dedupe_key,
         q.status, q.attempts, q.max_attempts, q.locked_by, q.locked_at,
         q.lease_expires_at, q.next_run_at, q.last_error, q.chapter_sort_key
  FROM public.importer_queue q
  WHERE q.id = v_job_id;
END;
$function$;

REVOKE ALL ON FUNCTION public.importer_acquire_job(text, interval, text, text) FROM public, anon, authenticated;
GRANT EXECUTE ON FUNCTION public.importer_acquire_job(text, interval, text, text) TO service_role;
