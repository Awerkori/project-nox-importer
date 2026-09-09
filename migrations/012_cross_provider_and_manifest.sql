-- ============================================================================
-- Migration: 20260909050000_cross_provider_and_manifest.sql
-- Description: Cross-Provider Work Matching, Canonical Manifest & Health Tracking
-- ============================================================================

-- 1. Extend importer_work_mappings with matching confidence and provenance
alter table public.importer_work_mappings
  add column if not exists confidence_score numeric default 1.0,
  add column if not exists is_primary boolean default false,
  add column if not exists match_method text default 'EXACT';

-- 2. Work Health Tracking Table
create table if not exists public.importer_work_health (
  work_id uuid primary key references public.works(id) on delete cascade,
  health_status text not null check (health_status in ('HEALTHY', 'INCOMPLETE', 'RECONCILING', 'BLOCKED', 'UNVERIFIED')),
  total_known_chapters int not null default 0,
  total_imported_chapters int not null default 0,
  missing_start boolean not null default false,
  first_chapter_number numeric null,
  latest_chapter_number numeric null,
  gaps jsonb not null default '[]'::jsonb,
  unresolved_gaps jsonb not null default '[]'::jsonb,
  providers_summary jsonb not null default '{}'::jsonb,
  last_reconciled_at timestamptz not null default now(),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists idx_importer_work_health_status
  on public.importer_work_health(health_status);

create index if not exists idx_importer_work_health_reconciled
  on public.importer_work_health(last_reconciled_at);

-- 3. Canonical Chapter Manifest Table
create table if not exists public.importer_chapter_manifest (
  id uuid primary key default gen_random_uuid(),
  work_id uuid not null references public.works(id) on delete cascade,
  chapter_number numeric not null,
  chapter_sort_key bigint not null,
  chapter_title text null,
  status text not null check (status in ('PUBLISHED', 'QUEUED', 'STAGED', 'UNRESOLVED_GAP', 'SKIPPED')),
  selected_source text null,
  available_sources jsonb not null default '[]'::jsonb,
  page_count int null,
  last_checked_at timestamptz not null default now(),
  constraint uq_importer_chapter_manifest unique (work_id, chapter_sort_key)
);

create index if not exists idx_importer_chapter_manifest_work_sort
  on public.importer_chapter_manifest(work_id, chapter_sort_key);

create index if not exists idx_importer_chapter_manifest_status
  on public.importer_chapter_manifest(work_id, status);

-- 4. Enable RLS and permissions
alter table public.importer_work_health enable row level security;
alter table public.importer_chapter_manifest enable row level security;

-- Policies for importer_work_health
drop policy if exists work_health_select on public.importer_work_health;
create policy work_health_select on public.importer_work_health
  for select to authenticated using (public.is_editor());

-- Policies for importer_chapter_manifest
drop policy if exists chapter_manifest_select on public.importer_chapter_manifest;
create policy chapter_manifest_select on public.importer_chapter_manifest
  for select to authenticated using (public.is_editor());

grant select on public.importer_work_health to authenticated;
grant select on public.importer_chapter_manifest to authenticated;
grant all on public.importer_work_health to service_role;
grant all on public.importer_chapter_manifest to service_role;

-- 5. RPC to trigger manual or automated cross-provider reconciliation request
create or replace function public.importer_request_reconciliation(p_work_id uuid)
returns jsonb
language plpgsql
security definer
set search_path = public, extensions
as $$
declare
  v_role text;
  v_title text;
begin
  select public.current_role() into v_role;
  if v_role not in ('EDITOR', 'ADMIN') then
    raise exception 'Apenas membros da staff editorial podem solicitar reconciliação.';
  end if;

  select title into v_title from public.works where id = p_work_id;
  if v_title is null then
    return jsonb_build_object('success', false, 'message', 'Obra não encontrada');
  end if;

  -- Update or insert health status as RECONCILING
  insert into public.importer_work_health (
    work_id,
    health_status,
    last_reconciled_at,
    updated_at
  ) values (
    p_work_id,
    'RECONCILING',
    now(),
    now()
  ) on conflict (work_id) do update set
    health_status = 'RECONCILING',
    updated_at = now();

  return jsonb_build_object(
    'success', true,
    'work_id', p_work_id,
    'title', v_title,
    'message', 'Reconciliação multi-fonte solicitada com sucesso.'
  );
end;
$$;

grant execute on function public.importer_request_reconciliation(uuid) to authenticated;
grant execute on function public.importer_request_reconciliation(uuid) to service_role;
