-- A discovered work has no canonical work_id until its first CATALOG pass
-- completes.  Under a permanently healthy P1 chapter reserve, ordinary P2
-- claims can otherwise leave that first pass READY indefinitely.  Give only
-- this narrow, unmapped onboarding shape one durable serial claim lane.  It
-- does not claim chapters, source metadata refetches, or mapped backfills.
BEGIN;

CREATE INDEX IF NOT EXISTS importer_lite_new_work_catalog_ready_idx
  ON public.importer_lite_tasks (created_at,id)
  WHERE state='READY' AND task_type='CATALOG' AND work_id IS NULL
    AND monitoring=false;

CREATE OR REPLACE FUNCTION public.importer_lite_claim_new_work_catalog(
  p_worker_id text,
  p_lease interval DEFAULT interval '90 seconds'
)
RETURNS SETOF public.importer_lite_tasks
LANGUAGE plpgsql SECURITY DEFINER SET search_path=public AS $$
DECLARE v_task_id uuid;
BEGIN
  SELECT task.id INTO v_task_id
  FROM public.importer_lite_tasks task
  WHERE task.task_type='CATALOG'
    AND task.work_id IS NULL
    AND task.state='READY'
    AND task.monitoring=false
    AND task.next_run_at<=now()
    -- These shapes have their own bounded metadata lanes.  This lane is only
    -- a first real catalog pass discovered from an upstream source.
    AND NOT (task.payload ? 'metadataEnrichment')
    AND NOT (task.payload ? 'metadataSourceRefresh')
  ORDER BY task.created_at,task.id
  LIMIT 1 FOR UPDATE SKIP LOCKED;

  IF NOT FOUND THEN RETURN; END IF;

  RETURN QUERY UPDATE public.importer_lite_tasks
    SET state='RUNNING',lease_owner=p_worker_id,lease_until=now()+p_lease,
      attempts=attempts+1,updated_at=now()
    WHERE id=v_task_id
    RETURNING *;
END;
$$;

REVOKE ALL ON FUNCTION public.importer_lite_claim_new_work_catalog(text,interval) FROM PUBLIC;
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname='anon') THEN
    REVOKE ALL ON FUNCTION public.importer_lite_claim_new_work_catalog(text,interval) FROM anon;
  END IF;
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname='authenticated') THEN
    REVOKE ALL ON FUNCTION public.importer_lite_claim_new_work_catalog(text,interval) FROM authenticated;
  END IF;
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname='service_role') THEN
    GRANT EXECUTE ON FUNCTION public.importer_lite_claim_new_work_catalog(text,interval) TO service_role;
  END IF;
END;
$$;

COMMIT;
