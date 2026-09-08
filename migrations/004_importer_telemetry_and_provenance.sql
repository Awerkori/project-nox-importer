-- ==============================================================================
-- PROJECT NOX IMPORTER - DATABASE MIGRATION (004_importer_telemetry_and_provenance.sql)
--
-- 1. Metadata Provenance & RBAC Manual Edit Protection on public.works
-- 2. Internal Telemetry Snapshots & Job Metrics Tables (RLS enabled, service_role only)
-- 3. Automated Telemetry Pruning Function
-- ==============================================================================

begin;

-- 1. Metadata Provenance on public.works
alter table public.works add column if not exists metadata_provenance jsonb not null default '{}'::jsonb;

-- Trigger to track manual edits by authorized human Editor/Admin
create or replace function public.trig_works_track_manual_edits()
returns trigger language plpgsql security definer set search_path = '' as $$
declare
  v_uid uuid := auth.uid();
  v_is_editor boolean := false;
  v_prov jsonb := coalesce(NEW.metadata_provenance, '{}'::jsonb);
begin
  -- Strictly require an authenticated user AND editorial role (ADMIN or EDITOR)
  if v_uid is not null then
    select coalesce(public.is_editor(), false) into v_is_editor;
    if v_is_editor then
      if NEW.title is distinct from OLD.title then
        v_prov := jsonb_set(v_prov, '{title}', jsonb_build_object('source', 'manual', 'updated_at', now(), 'actor_id', v_uid));
      end if;
      if NEW.synopsis is distinct from OLD.synopsis then
        v_prov := jsonb_set(v_prov, '{synopsis}', jsonb_build_object('source', 'manual', 'updated_at', now(), 'actor_id', v_uid));
      end if;
      if NEW.description is distinct from OLD.description then
        v_prov := jsonb_set(v_prov, '{description}', jsonb_build_object('source', 'manual', 'updated_at', now(), 'actor_id', v_uid));
      end if;
      if NEW.cover_id is distinct from OLD.cover_id then
        v_prov := jsonb_set(v_prov, '{cover}', jsonb_build_object('source', 'manual', 'updated_at', now(), 'actor_id', v_uid));
      end if;
      if NEW.author is distinct from OLD.author then
        v_prov := jsonb_set(v_prov, '{author}', jsonb_build_object('source', 'manual', 'updated_at', now(), 'actor_id', v_uid));
      end if;
      if NEW.artist is distinct from OLD.artist then
        v_prov := jsonb_set(v_prov, '{artist}', jsonb_build_object('source', 'manual', 'updated_at', now(), 'actor_id', v_uid));
      end if;
      if NEW.kind is distinct from OLD.kind then
        v_prov := jsonb_set(v_prov, '{kind}', jsonb_build_object('source', 'manual', 'updated_at', now(), 'actor_id', v_uid));
      end if;
      if NEW.status is distinct from OLD.status then
        v_prov := jsonb_set(v_prov, '{status}', jsonb_build_object('source', 'manual', 'updated_at', now(), 'actor_id', v_uid));
      end if;
      if NEW.year is distinct from OLD.year then
        v_prov := jsonb_set(v_prov, '{year}', jsonb_build_object('source', 'manual', 'updated_at', now(), 'actor_id', v_uid));
      end if;
      NEW.metadata_provenance := v_prov;
    end if;
  end if;
  return NEW;
end;
$$;

drop trigger if exists works_track_manual_edits_trig on public.works;
create trigger works_track_manual_edits_trig
before update on public.works
for each row execute function public.trig_works_track_manual_edits();

-- 2. Telemetry Snapshots
create table if not exists public.importer_telemetry (
  id uuid primary key default gen_random_uuid(),
  worker_id text not null,
  rss_mb integer not null,
  heap_used_mb integer not null,
  heap_total_mb integer not null,
  external_mb integer not null,
  array_buffers_mb integer not null,
  event_loop_lag_ms integer not null,
  concurrency integer not null,
  active_jobs integer not null,
  cycle_action text not null,
  cycle_reason text,
  created_at timestamptz not null default now()
);

-- 3. Fine-Grained Chapter Job Metrics
create table if not exists public.importer_job_metrics (
  id uuid primary key default gen_random_uuid(),
  worker_id text not null,
  source text not null,
  work_id uuid references public.works(id) on delete cascade,
  chapter_id uuid references public.chapters(id) on delete cascade,
  chapter_number numeric(10, 4) not null,
  page_count integer not null,
  total_bytes bigint not null default 0,
  duration_ms integer not null,
  download_ms integer not null default 0,
  upload_ms integer not null default 0,
  db_ms integer not null default 0,
  status text not null,
  error_message text,
  created_at timestamptz not null default now()
);

-- Security: Enable RLS and revoke all from public, anon, authenticated
alter table public.importer_telemetry enable row level security;
alter table public.importer_job_metrics enable row level security;

revoke all on public.importer_telemetry from public, anon, authenticated;
revoke all on public.importer_job_metrics from public, anon, authenticated;

grant all on public.importer_telemetry to service_role;
grant all on public.importer_job_metrics to service_role;

create index if not exists importer_telemetry_created_at_idx on public.importer_telemetry(created_at desc);
create index if not exists importer_job_metrics_created_at_idx on public.importer_job_metrics(created_at desc);

-- 4. Pruning / Retention Routine (keep 24h of telemetry and 7 days of job metrics)
create or replace function public.importer_prune_telemetry(p_telemetry_hours integer default 24, p_job_metrics_days integer default 7)
returns void language plpgsql security definer set search_path = '' as $$
begin
  delete from public.importer_telemetry where created_at < now() - (p_telemetry_hours || ' hours')::interval;
  delete from public.importer_job_metrics where created_at < now() - (p_job_metrics_days || ' days')::interval;
end;
$$;

revoke all on function public.importer_prune_telemetry(integer, integer) from public, anon, authenticated;
grant execute on function public.importer_prune_telemetry(integer, integer) to service_role;

commit;
