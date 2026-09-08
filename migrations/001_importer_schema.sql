-- ==============================================================================
-- PROJECT NOX IMPORTER - DATABASE SCHEMA (001_importer_schema.sql)
--
-- Autonomous 24/7 crawler/importer state, persistent queue, mappings & locks.
-- Strictly internal schema: RLS enabled, revoked from anon and authenticated.
-- ==============================================================================

begin;

-- 1. Importer Sources
create table if not exists public.importer_sources (
  id text primary key,
  name text not null,
  base_url text not null,
  enabled boolean not null default true,
  rate_limit_per_second numeric(4,2) not null default 2.00,
  sync_interval_minutes integer not null default 30,
  last_sync_at timestamptz,
  config jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

-- 2. Work Mappings (Deduplication & External Identifiers)
create table if not exists public.importer_work_mappings (
  id uuid primary key default gen_random_uuid(),
  source text not null references public.importer_sources(id) on delete cascade,
  source_work_id text not null,
  work_id uuid references public.works(id) on delete set null,
  source_slug text not null,
  source_title text not null,
  sync_status text not null default 'SYNCED' check(sync_status in ('SYNCED', 'AMBIGUOUS', 'IGNORED', 'FAILED')),
  metadata jsonb not null default '{}'::jsonb,
  last_synced_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique(source, source_work_id)
);

create index if not exists importer_work_mappings_work_id_idx on public.importer_work_mappings(work_id);
create index if not exists importer_work_mappings_status_idx on public.importer_work_mappings(sync_status);

-- 3. Chapter Mappings
create table if not exists public.importer_chapter_mappings (
  id uuid primary key default gen_random_uuid(),
  source text not null references public.importer_sources(id) on delete cascade,
  source_chapter_id text not null,
  chapter_id uuid references public.chapters(id) on delete cascade,
  work_mapping_id uuid not null references public.importer_work_mappings(id) on delete cascade,
  chapter_number numeric(8,2) not null,
  page_count integer not null default 0,
  status text not null default 'COMPLETED' check(status in ('PENDING', 'IMPORTING', 'COMPLETED', 'FAILED', 'VERIFICATION_FAILED')),
  last_error text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique(source, source_chapter_id)
);

create index if not exists importer_chapter_mappings_chap_idx on public.importer_chapter_mappings(chapter_id);
create index if not exists importer_chapter_mappings_work_map_idx on public.importer_chapter_mappings(work_mapping_id);

-- 4. Importer Queue (Atomic Lease/Locking & Crash-Safe Processing)
create table if not exists public.importer_queue (
  id uuid primary key default gen_random_uuid(),
  task_type text not null check(task_type in ('DISCOVER_WORKS', 'SYNC_WORK', 'IMPORT_CHAPTER')),
  source text not null references public.importer_sources(id) on delete cascade,
  priority integer not null default 10,
  payload jsonb not null default '{}'::jsonb,
  dedupe_key text not null unique,
  status text not null default 'QUEUED' check(status in ('QUEUED', 'IMPORTING', 'COMPLETED', 'FAILED', 'RETRY')),
  attempts integer not null default 0,
  max_attempts integer not null default 5,
  locked_by text,
  locked_at timestamptz,
  lease_expires_at timestamptz,
  next_run_at timestamptz not null default now(),
  last_error text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists importer_queue_fetch_idx on public.importer_queue(status, next_run_at, priority desc);
create index if not exists importer_queue_lease_idx on public.importer_queue(status, lease_expires_at);

-- 5. Importer Checkpoints (Crawl cursors & pagination markers)
create table if not exists public.importer_checkpoints (
  id uuid primary key default gen_random_uuid(),
  source text not null unique references public.importer_sources(id) on delete cascade,
  cursor_value text,
  last_checked_at timestamptz not null default now(),
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

-- ==============================================================================
-- Atomic Queue Stored Functions
-- ==============================================================================

-- Acquire next available job atomically using FOR UPDATE SKIP LOCKED
create or replace function public.importer_acquire_job(
  p_worker_id text,
  p_lease_duration interval default interval '5 minutes'
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
  last_error text
)
language plpgsql
security definer
set search_path = public
as $$
declare
  v_job_id uuid;
begin
  -- Select candidate job: ready to run OR expired lease from crashed worker
  select q.id into v_job_id
  from public.importer_queue q
  where (
    (q.status in ('QUEUED', 'RETRY') and q.next_run_at <= now())
    or
    (q.status = 'IMPORTING' and q.lease_expires_at < now())
  )
  order by q.priority desc, q.next_run_at asc
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
      public.importer_queue.last_error;
  end if;
end;
$$;

-- Heartbeat lease renewal
create or replace function public.importer_renew_lease(
  p_job_id uuid,
  p_worker_id text,
  p_lease_duration interval default interval '5 minutes'
)
returns boolean
language plpgsql
security definer
set search_path = public
as $$
begin
  update public.importer_queue
  set
    lease_expires_at = now() + p_lease_duration,
    updated_at = now()
  where id = p_job_id
    and locked_by = p_worker_id
    and status = 'IMPORTING';

  return found;
end;
$$;

-- Release job (completion, retry with backoff, or permanent failure)
create or replace function public.importer_release_job(
  p_job_id uuid,
  p_worker_id text,
  p_status text,
  p_error text default null,
  p_retry_delay interval default null
)
returns boolean
language plpgsql
security definer
set search_path = public
as $$
declare
  v_next_run timestamptz;
begin
  if p_status = 'RETRY' then
    v_next_run := now() + coalesce(p_retry_delay, interval '1 minute');
  else
    v_next_run := now();
  end if;

  update public.importer_queue
  set
    status = p_status,
    locked_by = null,
    locked_at = null,
    lease_expires_at = null,
    last_error = p_error,
    next_run_at = v_next_run,
    updated_at = now()
  where id = p_job_id
    and (locked_by = p_worker_id or locked_by is null);

  return found;
end;
$$;

-- ==============================================================================
-- Security & Permissions (Strict internal-only isolation)
-- ==============================================================================

alter table public.importer_sources enable row level security;
alter table public.importer_work_mappings enable row level security;
alter table public.importer_chapter_mappings enable row level security;
alter table public.importer_queue enable row level security;
alter table public.importer_checkpoints enable row level security;

-- Revoke all access from public, anon, authenticated
revoke all on public.importer_sources from public, anon, authenticated;
revoke all on public.importer_work_mappings from public, anon, authenticated;
revoke all on public.importer_chapter_mappings from public, anon, authenticated;
revoke all on public.importer_queue from public, anon, authenticated;
revoke all on public.importer_checkpoints from public, anon, authenticated;

revoke all on function public.importer_acquire_job(text, interval) from public, anon, authenticated;
revoke all on function public.importer_renew_lease(uuid, text, interval) from public, anon, authenticated;
revoke all on function public.importer_release_job(uuid, text, text, text, interval) from public, anon, authenticated;

-- Grant all only to service_role
grant all on public.importer_sources to service_role;
grant all on public.importer_work_mappings to service_role;
grant all on public.importer_chapter_mappings to service_role;
grant all on public.importer_queue to service_role;
grant all on public.importer_checkpoints to service_role;

grant execute on function public.importer_acquire_job(text, interval) to service_role;
grant execute on function public.importer_renew_lease(uuid, text, interval) to service_role;
grant execute on function public.importer_release_job(uuid, text, text, text, interval) to service_role;

-- Seed default pilot source
insert into public.importer_sources (id, name, base_url, enabled, rate_limit_per_second, sync_interval_minutes, config)
values (
  'nexus',
  'Nexus Mangas',
  'https://www.nexusmangas.com',
  true,
  2.00,
  30,
  jsonb_build_object(
    'api_url', 'https://supabase.nexusmangas.com/rest/v1',
    'functions_url', 'https://supabase.nexusmangas.com/functions/v1',
    'cdn_url', 'https://cdn.nexusmangas.com'
  )
)
on conflict (id) do update set
  name = excluded.name,
  base_url = excluded.base_url,
  config = excluded.config,
  updated_at = now();

commit;
