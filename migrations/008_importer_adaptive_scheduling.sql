-- ==============================================================================
-- Project Nox Importer - Migration 008: Dynamic Blocker Priority Scheduling
-- Calculates priority 90 dynamically for chapters blocking STAGED chapters
-- without mutating or persisting artificial priority boosts in the table.
-- ==============================================================================

create or replace function public.importer_acquire_job(
  p_worker_id text,
  p_lease_duration interval default interval '5 minutes',
  p_source text default null
)
returns table (
  id uuid,
  task_type text,
  source text,
  priority integer,
  payload jsonb,
  dedupe_key text,
  status text,
  attempts integer,
  max_attempts integer,
  locked_by text,
  locked_at timestamptz,
  lease_expires_at timestamptz,
  next_run_at timestamptz,
  last_error text,
  chapter_sort_key numeric
)
language plpgsql
security definer
set search_path = public
as $$
declare
  v_job_id uuid;
begin
  -- Select candidate job: ready to run OR expired lease from crashed worker
  -- Respecting: dynamic operational priority desc, deterministic chapter_sort_key asc, next_run_at asc
  select q.id into v_job_id
  from public.importer_queue q
  left join public.importer_sources s on s.id = q.source
  where (
    (q.status in ('QUEUED', 'RETRY') and q.next_run_at <= now())
    or
    (q.status = 'IMPORTING' and q.lease_expires_at < now())
  )
  and (p_source is null or q.source = p_source)
  and (s.id is null or (s.enabled = true and s.status != 'PAUSED' and (s.cooldown_until is null or s.cooldown_until <= now())))
  order by
    case
      -- Se for um predecessor bloqueando capítulos STAGED para a mesma obra: prioridade MÁXIMA 90 dinâmica
      when q.task_type = 'IMPORT_CHAPTER'
       and q.payload->>'workId' is not null
       and q.chapter_sort_key is not null
       and exists (
         select 1
         from public.importer_chapter_mappings m
         where m.status = 'STAGED'
           and m.work_id = (q.payload->>'workId')::uuid
           and m.chapter_sort_key > q.chapter_sort_key
       ) then 90
      else q.priority
    end desc,
    q.chapter_sort_key asc nulls last,
    q.next_run_at asc,
    q.created_at asc
  limit 1
  for update skip locked;

  if v_job_id is not null then
    return query
    update public.importer_queue
    set
      status = 'IMPORTING',
      locked_by = p_worker_id,
      locked_at = now(),
      lease_expires_at = now() + p_lease_duration,
      attempts = public.importer_queue.attempts + 1,
      updated_at = now()
    where public.importer_queue.id = v_job_id
    returning
      public.importer_queue.id,
      public.importer_queue.task_type,
      public.importer_queue.source,
      public.importer_queue.priority,
      public.importer_queue.payload,
      public.importer_queue.dedupe_key,
      public.importer_queue.status,
      public.importer_queue.attempts,
      public.importer_queue.max_attempts,
      public.importer_queue.locked_by,
      public.importer_queue.locked_at,
      public.importer_queue.lease_expires_at,
      public.importer_queue.next_run_at,
      public.importer_queue.last_error,
      public.importer_queue.chapter_sort_key;
  end if;
end;
$$;

revoke all on function public.importer_acquire_job(text, interval, text) from public, anon, authenticated;
grant execute on function public.importer_acquire_job(text, interval, text) to service_role;
