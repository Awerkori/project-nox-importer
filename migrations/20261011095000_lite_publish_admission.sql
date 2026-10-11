-- Ensure chapters with complete durable media are finalized before further
-- same-priority downloads.  This is additive and preserves the absolute
-- STAFF -> P0 -> P1 -> P2 priority ordering.
BEGIN;

ALTER TABLE public.importer_lite_ready_works
  ADD COLUMN IF NOT EXISTS operation_rank smallint NOT NULL DEFAULT 1;

CREATE INDEX IF NOT EXISTS importer_lite_ready_works_claim_rank_idx
  ON public.importer_lite_ready_works (priority, operation_rank, next_run_at, last_claimed_at);

CREATE OR REPLACE FUNCTION public.importer_lite_refresh_ready_work(p_work_key text)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path=public AS $$
DECLARE v_priority smallint; v_next timestamptz; v_operation_rank smallint;
BEGIN
  SELECT priority, next_run_at,
    CASE task_type WHEN 'PUBLISH' THEN 0 WHEN 'IMPORT_CHAPTER' THEN 1 WHEN 'CATALOG' THEN 2 ELSE 3 END
    INTO v_priority, v_next, v_operation_rank
  FROM public.importer_lite_tasks
  WHERE work_key=p_work_key AND state='READY' AND monitoring=false
  ORDER BY priority,
    CASE task_type WHEN 'PUBLISH' THEN 0 WHEN 'IMPORT_CHAPTER' THEN 1 WHEN 'CATALOG' THEN 2 ELSE 3 END,
    next_run_at, created_at
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
  SELECT id INTO v_task_id FROM public.importer_lite_tasks
  WHERE work_key=v_work_key AND state='READY' AND monitoring=false AND next_run_at<=now()
  ORDER BY priority,
    CASE task_type WHEN 'PUBLISH' THEN 0 WHEN 'IMPORT_CHAPTER' THEN 1 WHEN 'CATALOG' THEN 2 ELSE 3 END,
    next_run_at,created_at LIMIT 1 FOR UPDATE SKIP LOCKED;
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

-- One bounded rebuild of the materialized projection at migration time. It is
-- not in the claim path and only touches current ready-work rows.
DO $$
DECLARE v_work_key text;
BEGIN
  FOR v_work_key IN SELECT work_key FROM public.importer_lite_ready_works LOOP
    PERFORM public.importer_lite_refresh_ready_work(v_work_key);
  END LOOP;
END;
$$;

COMMIT;
