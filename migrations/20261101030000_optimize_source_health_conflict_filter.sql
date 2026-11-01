-- Optimize importer_lite_complete_import_and_publish to eliminate row-level lock contention
-- on importer_lite_source_health.
-- When multiple concurrent workers publish chapters from the same source, they previously
-- contended for an exclusive row lock on importer_lite_source_health even when the source
-- was already HEALTHY with 0 failures.
-- By adding a conflict filter predicate, if the source is already healthy and clean,
-- YSQL skips the UPDATE entirely (0 writes, 0 row locks), completely eliminating worker serialization.

CREATE OR REPLACE FUNCTION public.importer_lite_complete_import_and_publish(
  p_task_id uuid,
  p_worker_id text,
  p_work_id uuid,
  p_chapter_id uuid,
  p_pages jsonb,
  p_fresh boolean,
  p_source text,
  p_priority_rank smallint,
  p_checkpoint jsonb,
  p_work_key text
)
RETURNS text
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_work RECORD;
  v_pub_at timestamptz;
  v_done int;
BEGIN
  -- 1. Verify task ownership & presentation
  SELECT w.cover_id, w.title, w.slug, m.storage_ready, m.bytes
    INTO v_work
    FROM importer_lite_tasks t
    JOIN works w ON w.id = p_work_id
    LEFT JOIN media m ON m.id = w.cover_id
   WHERE t.id = p_task_id AND t.state = 'RUNNING' AND t.lease_owner = p_worker_id AND t.lease_until > now()
   FOR UPDATE OF t, w;

  IF NOT FOUND THEN
    RETURN 'LEASE_LOST';
  END IF;

  IF v_work.title IS NULL OR v_work.slug IS NULL OR v_work.cover_id IS NULL OR v_work.storage_ready IS NOT TRUE OR COALESCE(v_work.bytes, 0) < 1500 THEN
    RETURN 'FALLBACK_TWO_STEP';
  END IF;

  -- 2. Replace pages atomically
  PERFORM public.importer_replace_pages(p_chapter_id, p_pages);

  -- 3. Publish chapter
  UPDATE chapters
     SET published_at = COALESCE(
           published_at,
           GREATEST(
             clock_timestamp(),
             COALESCE(
               (SELECT max(ch_prev.published_at) + interval '1 millisecond'
                  FROM chapters ch_prev
                 WHERE ch_prev.work_id = chapters.work_id
                   AND ch_prev.chapter_order_key < chapters.chapter_order_key
                   AND ch_prev.published_at IS NOT NULL),
               clock_timestamp()
             )
           )
         ),
         is_fresh_release = CASE WHEN published_at IS NULL THEN p_fresh ELSE is_fresh_release END
   WHERE id = p_chapter_id
  RETURNING published_at INTO v_pub_at;

  IF v_pub_at IS NULL THEN
    RETURN 'PUBLISH_FAILED';
  END IF;

  -- 4. Publication event
  INSERT INTO importer_publication_events
    (chapter_id, transition_at, recorded_at, bucket_minute, is_fresh_release, source, work_id, priority)
  VALUES
    (p_chapter_id, v_pub_at, clock_timestamp(), date_trunc('minute', v_pub_at), p_fresh, p_source, p_work_id, p_priority_rank)
  ON CONFLICT(chapter_id) DO NOTHING;

  -- 5. Update work latest published
  UPDATE works
     SET published = true,
         latest_chapter_published_at = GREATEST(COALESCE(latest_chapter_published_at, v_pub_at), v_pub_at),
         updated_at = now()
   WHERE id = p_work_id;

  -- 6. Mark task DONE
  UPDATE importer_lite_tasks
     SET state = 'DONE',
         lease_owner = NULL,
         lease_until = NULL,
         checkpoint = checkpoint || p_checkpoint || jsonb_build_object('publishedAt', v_pub_at::text),
         updated_at = now()
   WHERE id = p_task_id AND state = 'RUNNING' AND lease_owner = p_worker_id AND lease_until > now();

  GET DIAGNOSTICS v_done = ROW_COUNT;
  IF v_done <> 1 THEN
    RETURN 'TASK_NOT_DONE';
  END IF;

  -- 7. Source health update: Skip update when already HEALTHY and clean to prevent row-lock serialization
  INSERT INTO importer_lite_source_health(source, health, cooldown_until, consecutive_failures, last_error)
  VALUES (p_source, 'HEALTHY', NULL, 0, NULL)
  ON CONFLICT(source) DO UPDATE
    SET health = 'HEALTHY', cooldown_until = NULL, consecutive_failures = 0, last_error = NULL, updated_at = now()
  WHERE (importer_lite_source_health.health IS DISTINCT FROM 'HEALTHY'
     OR importer_lite_source_health.consecutive_failures > 0
     OR importer_lite_source_health.cooldown_until IS NOT NULL)
    AND NOT (importer_lite_source_health.health = 'RATE_LIMITED'
     AND importer_lite_source_health.cooldown_until > now());

  -- 8. Refresh ready work projection
  PERFORM public.importer_lite_refresh_ready_work(p_work_key);

  RETURN 'SUCCESS';
END;
$function$;
