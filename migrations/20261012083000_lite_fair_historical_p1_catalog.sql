-- Historical P1 catalog work is the bounded producer of confirmed chapter
-- manifests from mapped sources.  Keeping it behind every P1 import made a
-- large, already-materialised backlog prevent other healthy sources from ever
-- contributing new P1 work.  Give only this small, bounded catalog reserve the
-- same admission rank as P1 imports; work-level round-robin still decides the
-- next work and PUBLISH remains ahead of both.
--
-- This does not alter STAFF -> P0 -> P1 -> P2.  It also does not add source
-- joins or scans to the claim path: source diversity is supplied by the
-- existing bounded, per-source historical catalog admission.
BEGIN;

CREATE OR REPLACE FUNCTION public.importer_lite_refresh_ready_work(p_work_key text)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path=public AS $$
DECLARE v_priority smallint; v_next timestamptz; v_operation_rank smallint;
BEGIN
  SELECT priority, next_run_at,
    CASE
      WHEN task_type='PUBLISH' THEN 0
      WHEN task_type='IMPORT_CHAPTER' THEN 1
      WHEN task_type='CATALOG' AND priority=2 AND payload ? 'historicalP1Backfill' THEN 1
      WHEN task_type='CATALOG' THEN 2
      ELSE 3
    END
    INTO v_priority, v_next, v_operation_rank
  FROM public.importer_lite_tasks
  WHERE work_key=p_work_key AND state='READY' AND monitoring=false
  ORDER BY priority,
    CASE
      WHEN task_type='PUBLISH' THEN 0
      WHEN task_type='IMPORT_CHAPTER' THEN 1
      WHEN task_type='CATALOG' AND priority=2 AND payload ? 'historicalP1Backfill' THEN 1
      WHEN task_type='CATALOG' THEN 2
      ELSE 3
    END,
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
  SELECT id INTO v_task_id FROM public.importer_lite_tasks
  WHERE work_key=v_work_key AND state='READY' AND monitoring=false AND next_run_at<=now()
  ORDER BY priority,
    CASE
      WHEN task_type='PUBLISH' THEN 0
      WHEN task_type='IMPORT_CHAPTER' THEN 1
      WHEN task_type='CATALOG' AND priority=2 AND payload ? 'historicalP1Backfill' THEN 1
      WHEN task_type='CATALOG' THEN 2
      ELSE 3
    END,
    next_run_at,created_at
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

-- This affects only the small ready-work projection, never chapter/media
-- records. Rebuilding makes already queued bounded catalog tasks eligible
-- immediately after deployment rather than waiting for an unrelated update.
DO $$
DECLARE v_work_key text;
BEGIN
  FOR v_work_key IN SELECT work_key FROM public.importer_lite_ready_works WHERE priority=2 LOOP
    PERFORM public.importer_lite_refresh_ready_work(v_work_key);
  END LOOP;
END;
$$;

-- Public publication events are the durable attribution point for fairness
-- metrics. Legacy rows intentionally remain unattributed; no historical data
-- is rewritten just to make a dashboard appear balanced.
ALTER TABLE public.importer_publication_events
  ADD COLUMN IF NOT EXISTS source text NULL,
  ADD COLUMN IF NOT EXISTS work_id uuid NULL,
  ADD COLUMN IF NOT EXISTS priority smallint NULL;
CREATE INDEX IF NOT EXISTS importer_publication_events_source_window_idx
  ON public.importer_publication_events (transition_at DESC, source);

COMMIT;
