-- A serial onboarding lane must not spend each turn claiming an old work from
-- a provider already in cooldown.  That was enough to make a healthy newly
-- discovered work wait behind dozens of immediately-deferred rows.  Keep the
-- lane narrow, but choose only sources that catalog() can actually contact.
BEGIN;

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
  JOIN public.importer_sources source ON source.id=task.payload->>'source'
  LEFT JOIN public.importer_lite_source_health health ON health.source=source.id
  WHERE task.task_type='CATALOG'
    AND task.work_id IS NULL
    AND task.state='READY'
    AND task.monitoring=false
    AND task.next_run_at<=now()
    AND source.enabled=true
    AND (COALESCE(source.status,'ACTIVE') IN ('ACTIVE','DEGRADED','RECOVERING')
      OR (source.status='COOLDOWN' AND source.cooldown_until IS NOT NULL AND source.cooldown_until<=now()))
    AND COALESCE(health.health,'HEALTHY') NOT IN ('BROKEN','DISABLED')
    AND (source.cooldown_until IS NULL OR source.cooldown_until<=now())
    AND (health.cooldown_until IS NULL OR health.cooldown_until<=now())
    AND NOT (task.payload ? 'metadataEnrichment')
    AND NOT (task.payload ? 'metadataSourceRefresh')
  ORDER BY task.created_at,task.id
  LIMIT 1 FOR UPDATE OF task SKIP LOCKED;

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
