BEGIN;

-- A native rolling deploy can leave a source-metadata request RUNNING after
-- its owner exits. The dedicated claimant must recover that expired lease;
-- otherwise the V5 producer excludes it from capacity while this lane never
-- executes it again.
CREATE OR REPLACE FUNCTION public.importer_lite_claim_source_metadata_task(
  p_worker_id text,p_lease interval DEFAULT interval '2 minutes'
)
RETURNS SETOF public.importer_lite_tasks LANGUAGE plpgsql SECURITY DEFINER SET search_path=public AS $$
DECLARE v_task_id uuid; v_work_key text;
BEGIN
  SELECT id,work_key INTO v_task_id,v_work_key
  FROM public.importer_lite_tasks
  WHERE task_type='CATALOG' AND monitoring=false
    AND payload ? 'metadataSourceRefresh'
    AND (
      (state IN ('READY','RETRY_WAIT') AND next_run_at<=now())
      OR (state='RUNNING' AND lease_until<now())
    )
  ORDER BY next_run_at,created_at,id
  LIMIT 1 FOR UPDATE SKIP LOCKED;
  IF NOT FOUND THEN RETURN; END IF;
  RETURN QUERY UPDATE public.importer_lite_tasks
    SET state='RUNNING',lease_owner=p_worker_id,lease_until=now()+p_lease,
      attempts=attempts+1,updated_at=now()
    WHERE id=v_task_id RETURNING *;
  PERFORM public.importer_lite_refresh_ready_work(v_work_key);
END;
$$;

COMMIT;
