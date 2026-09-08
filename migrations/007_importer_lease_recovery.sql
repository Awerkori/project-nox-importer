-- ==============================================================================
-- PROJECT NOX IMPORTER - MIGRATION 007: Generic Lease Recovery
-- ==============================================================================

begin;

-- Stored function for atomic crash-safe lease recovery
create or replace function public.importer_recover_stalled_leases()
returns table (
  recovered_count integer,
  failed_count integer
)
language plpgsql
security definer
set search_path = public
as $$
declare
  v_recovered integer := 0;
  v_failed integer := 0;
begin
  -- 1. Mark as FAILED those that reached or exceeded max_attempts
  with failed_jobs as (
    update public.importer_queue
    set
      status = 'FAILED',
      locked_by = null,
      locked_at = null,
      lease_expires_at = null,
      last_error = coalesce(last_error || ' | ', '') || 'Lease expired after max attempts (' || attempts || '/' || max_attempts || ')',
      updated_at = now()
    where status = 'IMPORTING'
      and lease_expires_at < now()
      and attempts >= max_attempts
    returning id
  )
  select count(*)::integer into v_failed from failed_jobs;

  -- 2. Reset back to QUEUED those with attempts remaining
  with requeued_jobs as (
    update public.importer_queue
    set
      status = 'QUEUED',
      locked_by = null,
      locked_at = null,
      lease_expires_at = null,
      next_run_at = now(),
      updated_at = now()
    where status = 'IMPORTING'
      and lease_expires_at < now()
      and attempts < max_attempts
    returning id
  )
  select count(*)::integer into v_recovered from requeued_jobs;

  return query select v_recovered, v_failed;
end;
$$;

revoke all on function public.importer_recover_stalled_leases() from public, anon, authenticated;
grant execute on function public.importer_recover_stalled_leases() to service_role;

commit;
