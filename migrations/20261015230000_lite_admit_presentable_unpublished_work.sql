-- A work becomes publicly published atomically with its first successfully
-- published chapter. Requiring works.published before admitting that first
-- chapter creates a circular gate: fully presentable newly discovered works
-- can never reach the publication transaction. Keep the presentation fence
-- (title, slug, stored cover) but do not require a prior public chapter.
BEGIN;

CREATE OR REPLACE FUNCTION public.importer_lite_refresh_ready_work(p_work_key text)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path=public AS $$
DECLARE v_priority smallint; v_next timestamptz; v_operation_rank smallint;
BEGIN
  SELECT priority,next_run_at,
    CASE task_type WHEN 'PUBLISH' THEN 0 WHEN 'IMPORT_CHAPTER' THEN 1 WHEN 'CATALOG' THEN 2 ELSE 3 END
    INTO v_priority,v_next,v_operation_rank
  FROM public.importer_lite_tasks task
  WHERE work_key=p_work_key AND state='READY' AND monitoring=false
    AND NOT (task_type='CATALOG' AND payload ? 'metadataSourceRefresh')
    AND (task.task_type NOT IN ('IMPORT_CHAPTER','PUBLISH') OR EXISTS (
      SELECT 1
      FROM public.works work
      JOIN public.media cover ON cover.id=work.cover_id
      WHERE work.id=task.work_id
        AND NULLIF(btrim(work.title),'') IS NOT NULL
        AND NULLIF(btrim(work.slug),'') IS NOT NULL
        AND cover.storage_ready=true AND cover.bytes>=1500
    ))
  ORDER BY priority,
    CASE task_type WHEN 'PUBLISH' THEN 0 WHEN 'IMPORT_CHAPTER' THEN 1 WHEN 'CATALOG' THEN 2 ELSE 3 END,
    next_run_at,created_at
  LIMIT 1;
  IF NOT FOUND THEN
    DELETE FROM public.importer_lite_ready_works WHERE work_key=p_work_key;
  ELSE
    INSERT INTO public.importer_lite_ready_works(work_key,priority,operation_rank,next_run_at,updated_at)
    VALUES(p_work_key,v_priority,v_operation_rank,v_next,now())
    ON CONFLICT(work_key) DO UPDATE SET priority=excluded.priority,operation_rank=excluded.operation_rank,
      next_run_at=excluded.next_run_at,updated_at=now();
  END IF;
END;
$$;

CREATE OR REPLACE FUNCTION public.importer_lite_claim_task(p_worker_id text, p_lease interval DEFAULT interval '10 minutes')
RETURNS SETOF public.importer_lite_tasks LANGUAGE plpgsql SECURITY DEFINER SET search_path=public AS $$
DECLARE v_work_key text; v_task_id uuid;
BEGIN
  SELECT work_key INTO v_work_key
  FROM public.importer_lite_ready_works
  WHERE next_run_at<=now()
  ORDER BY priority,operation_rank,last_claimed_at NULLS FIRST,next_run_at,work_key
  LIMIT 1 FOR UPDATE SKIP LOCKED;
  IF NOT FOUND THEN RETURN; END IF;

  SELECT candidate.id INTO v_task_id
  FROM public.importer_lite_tasks candidate
  WHERE candidate.work_key=v_work_key AND candidate.state='READY'
    AND candidate.monitoring=false AND candidate.next_run_at<=now()
    AND NOT (candidate.task_type='CATALOG' AND candidate.payload ? 'metadataSourceRefresh')
    AND (candidate.task_type NOT IN ('IMPORT_CHAPTER','PUBLISH') OR EXISTS (
      SELECT 1
      FROM public.works work
      JOIN public.media cover ON cover.id=work.cover_id
      WHERE work.id=candidate.work_id
        AND NULLIF(btrim(work.title),'') IS NOT NULL
        AND NULLIF(btrim(work.slug),'') IS NOT NULL
        AND cover.storage_ready=true AND cover.bytes>=1500
    ))
  ORDER BY
    CASE
      WHEN candidate.priority=1 AND candidate.task_type IN ('IMPORT_CHAPTER','PUBLISH')
        AND EXISTS (
          SELECT 1
          FROM public.importer_lite_chapter_sources newer
          JOIN public.importer_lite_chapter_sources gap
            ON gap.work_id=newer.work_id AND gap.confirmed=true AND gap.has_chapter=true
             AND gap.order_key < newer.order_key
          JOIN public.importer_lite_tasks gap_task
            ON gap_task.work_id=gap.work_id AND gap_task.chapter_key=gap.canonical_chapter_key
             AND (gap_task.task_type='IMPORT_CHAPTER' AND
                    (gap_task.payload ? 'manifestP1Repair' OR gap_task.payload ? 'historicalManifestRepair')
               OR gap_task.task_type='PUBLISH' AND EXISTS (
                    SELECT 1 FROM public.importer_lite_tasks source_import
                    WHERE source_import.idempotency_key='lite:chapter:' || gap.work_id::text || ':' || gap.canonical_chapter_key
                      AND (source_import.payload ? 'manifestP1Repair' OR source_import.payload ? 'historicalManifestRepair')
               ))
             AND gap_task.state IN ('READY','RUNNING')
          JOIN public.importer_sources route ON route.id=gap.source AND route.enabled=true
          LEFT JOIN public.importer_lite_source_health health ON health.source=gap.source
          WHERE newer.work_id=candidate.work_id
            AND newer.canonical_chapter_key=candidate.chapter_key
            AND newer.confirmed=true AND newer.has_chapter=true
            AND NOT EXISTS (SELECT 1 FROM public.importer_lite_chapters present
              WHERE present.work_id=gap.work_id AND present.canonical_chapter_key=gap.canonical_chapter_key)
            AND gap_task.priority=2
            AND (COALESCE(route.status,'ACTIVE') IN ('ACTIVE','DEGRADED','RECOVERING')
              OR (route.status='COOLDOWN' AND route.cooldown_until IS NOT NULL AND route.cooldown_until<=now()))
            AND (to_jsonb(route)->>'blocked_reason' IS NULL
              OR to_jsonb(route)->'blocked_details'->>'probe_success'='true'
              OR to_jsonb(route)->'blocked_details'->>'recovered_at' IS NOT NULL)
            AND COALESCE(health.health,'HEALTHY') NOT IN ('BROKEN','DISABLED')
            AND (health.cooldown_until IS NULL OR health.cooldown_until<=now())
        ) THEN 3
      ELSE candidate.priority
    END,
    CASE
      WHEN candidate.task_type='PUBLISH' THEN 0
      WHEN candidate.task_type='IMPORT_CHAPTER' THEN 1
      WHEN candidate.task_type='CATALOG' AND candidate.priority=2
        AND candidate.payload ? 'historicalP1Backfill'
        AND COALESCE(candidate.payload->>'deferBehindReadyImport','false') <> 'true' THEN 1
      WHEN candidate.task_type='CATALOG' THEN 2
      ELSE 3
    END,
    CASE WHEN candidate.task_type='IMPORT_CHAPTER' THEN NULLIF(candidate.payload->>'orderKey','') END NULLS LAST,
    candidate.next_run_at,candidate.created_at,candidate.id
  LIMIT 1 FOR UPDATE SKIP LOCKED;

  IF NOT FOUND THEN
    PERFORM public.importer_lite_refresh_ready_work(v_work_key);
    RETURN;
  END IF;
  RETURN QUERY UPDATE public.importer_lite_tasks
    SET state='RUNNING',lease_owner=p_worker_id,lease_until=now()+p_lease,
      attempts=attempts+1,updated_at=now()
    WHERE id=v_task_id RETURNING *;
  UPDATE public.importer_lite_ready_works SET last_claimed_at=now(),updated_at=now() WHERE work_key=v_work_key;
  PERFORM public.importer_lite_refresh_ready_work(v_work_key);
END;
$$;

-- The former projection cannot contain newly presentable-but-unpublished
-- works. Rebuild this small executable projection once; task history and
-- source evidence remain untouched.
DELETE FROM public.importer_lite_ready_works;
DO $$
DECLARE v_work_key text;
BEGIN
  FOR v_work_key IN
    SELECT DISTINCT work_key FROM public.importer_lite_tasks
    WHERE state='READY' AND monitoring=false
  LOOP
    PERFORM public.importer_lite_refresh_ready_work(v_work_key);
  END LOOP;
END;
$$;

COMMIT;
