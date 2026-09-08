-- ==============================================================================
-- PROJECT NOX IMPORTER - MIGRATION 002: Source Status & Cooldown
-- ==============================================================================

begin;

-- 1. Add status column (ACTIVE, PAUSED, COOLDOWN, DISABLED)
alter table public.importer_sources
  add column if not exists status text not null default 'ACTIVE'
  check (status in ('ACTIVE', 'PAUSED', 'COOLDOWN', 'DISABLED'));

-- 2. Add cooldown_until timestamp for 429/rate-limiting backoff
alter table public.importer_sources
  add column if not exists cooldown_until timestamptz;

-- 3. Seed the 4 new adapters in PAUSED state (enabled = false, status = 'PAUSED')
insert into public.importer_sources (id, name, base_url, enabled, status, rate_limit_per_second, sync_interval_minutes, config)
values
  (
    'mangaflix',
    'MangaFlix',
    'https://mangaflix.net',
    false,
    'PAUSED',
    2.00,
    30,
    jsonb_build_object(
      'api_url', 'https://api.mangaflix.net/v1'
    )
  ),
  (
    'manhastro',
    'Manhastro',
    'https://manhastro.net',
    false,
    'PAUSED',
    2.00,
    30,
    jsonb_build_object(
      'api_url', 'https://api2.manhastro.net'
    )
  ),
  (
    'toonlivre',
    'Toon Livre',
    'https://toonlivre.net',
    false,
    'PAUSED',
    2.00,
    30,
    jsonb_build_object(
      'api_url', 'https://toonlivre.net/api'
    )
  ),
  (
    'kuro',
    'Kuro Mangas',
    'https://kuromangas.com',
    false,
    'PAUSED',
    2.00,
    30,
    jsonb_build_object(
      'api_url', 'https://kuromangas.com/api',
      'cdn_url', 'https://cdn.kuromangas.com'
    )
  )
on conflict (id) do update set
  name = excluded.name,
  base_url = excluded.base_url,
  status = excluded.status,
  updated_at = now();

commit;
