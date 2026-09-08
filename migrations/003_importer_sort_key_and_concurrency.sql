-- ==============================================================================
-- PROJECT NOX IMPORTER - MIGRATION 003: Deterministic Sorting & Source Concurrency
-- ==============================================================================

begin;

-- 1. Add chapter_sort_key to importer_queue
alter table public.importer_queue
  add column if not exists chapter_sort_key numeric(10, 4);

-- Index for deterministic ordering by sort key
create index if not exists importer_queue_sort_idx
  on public.importer_queue(source, priority desc, chapter_sort_key asc nulls last, next_run_at asc);

-- 2. Update importer_acquire_job stored procedure with source filtering and deterministic sort key
drop function if exists public.importer_acquire_job(text, interval);

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
  -- Respecting: operational priority desc, deterministic chapter_sort_key asc, next_run_at asc
  select q.id into v_job_id
  from public.importer_queue q
  where (
    (q.status in ('QUEUED', 'RETRY') and q.next_run_at <= now())
    or
    (q.status = 'IMPORTING' and q.lease_expires_at < now())
  )
  and (p_source is null or q.source = p_source)
  order by
    q.priority desc,
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

-- 3. Seed Mango Toons in importer_sources (initially PAUSED)
insert into public.importer_sources (id, name, base_url, enabled, status, rate_limit_per_second, sync_interval_minutes, config)
values (
  'mangotoons',
  'Mango Toons',
  'https://api.mangotoons.com',
  false,
  'PAUSED',
  2.00,
  30,
  jsonb_build_object(
    'api_url', 'https://api.mangotoons.com/api',
    'cdn_url', 'https://cdn.mangotoons.com'
  )
)
on conflict (id) do update set
  name = excluded.name,
  base_url = excluded.base_url,
  status = excluded.status,
  updated_at = now();

commit;
