BEGIN;

-- A source metadata refresh that failed with an external condition is durable
-- in RETRY_WAIT until next_run_at. The dedicated lane must claim it when due;
-- otherwise the refill producer counts it against its two-task limit forever
-- while the claimant only searches READY tasks.
CREATE OR REPLACE FUNCTION public.importer_lite_claim_source_metadata_task(
  p_worker_id text,p_lease interval DEFAULT interval '2 minutes'
)
RETURNS SETOF public.importer_lite_tasks LANGUAGE plpgsql SECURITY DEFINER SET search_path=public AS $$
DECLARE v_task_id uuid; v_work_key text;
BEGIN
  SELECT id,work_key INTO v_task_id,v_work_key
  FROM public.importer_lite_tasks
  WHERE task_type='CATALOG' AND state IN ('READY','RETRY_WAIT') AND monitoring=false
    AND next_run_at<=now() AND payload ? 'metadataSourceRefresh'
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
