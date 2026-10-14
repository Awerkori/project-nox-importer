-- When several confirmed historical chapters for one work are admitted in the
-- same manifest batch, their created_at timestamps tie. Do not let an
-- implementation-dependent row order publish later chapters before earlier
-- manifest-proven gaps. Work-level fairness remains unchanged; this orders
-- only tasks after a work has already been selected.
BEGIN;

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
  SELECT id INTO v_task_id FROM public.importer_lite_tasks
  WHERE work_key=v_work_key AND state='READY' AND monitoring=false AND next_run_at<=now()
  ORDER BY priority,
    CASE
      WHEN task_type='PUBLISH' THEN 0
      WHEN task_type='IMPORT_CHAPTER' THEN 1
      WHEN task_type='CATALOG' AND priority=2 AND payload ? 'historicalP1Backfill'
        AND COALESCE(payload->>'deferBehindReadyImport','false') <> 'true' THEN 1
      WHEN task_type='CATALOG' THEN 2
      ELSE 3
    END,
    next_run_at,
    CASE WHEN task_type='IMPORT_CHAPTER' THEN NULLIF(payload->>'orderKey','') END NULLS LAST,
    created_at,id
  LIMIT 1 FOR UPDATE SKIP LOCKED;
  IF NOT FOUND THEN
    PERFORM public.importer_lite_refresh_ready_work(v_work_key);
    RETURN;
  END IF;
  RETURN QUERY UPDATE public.importer_lite_tasks
  SET state='RUNNING',lease_owner=p_worker_id,lease_until=now()+p_lease,attempts=attempts+1,updated_at=now()
  WHERE id=v_task_id RETURNING *;
  UPDATE public.importer_lite_ready_works SET last_claimed_at=now(),updated_at=now() WHERE work_key=v_work_key;
  PERFORM public.importer_lite_refresh_ready_work(v_work_key);
END;
$$;

COMMIT;
