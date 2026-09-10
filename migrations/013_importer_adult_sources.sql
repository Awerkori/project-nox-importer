-- ==============================================================================
-- Migration: 013_importer_adult_sources.sql
-- Description: Register 8 adult (+18 / Adulto / Pornhwa) sources into importer_sources
-- ==============================================================================

insert into public.importer_sources (
  id,
  name,
  base_url,
  enabled,
  status,
  rate_limit_per_second,
  sync_interval_minutes,
  config
)
values
  ('acervohentai', 'Acervo Hentai', 'https://acervohentai.com', true, 'ACTIVE', 2.00, 15, '{"adult": true}'::jsonb),
  ('blackoutcomics', 'Blackout Comics', 'https://blackoutcomics.com', true, 'ACTIVE', 2.00, 15, '{"adult": true}'::jsonb),
  ('hanamiheaven', 'Hanami Heaven', 'https://hanamiheaven.com', true, 'ACTIVE', 2.00, 15, '{"adult": true}'::jsonb),
  ('hipercool', 'HipercooL', 'https://lerhentais.com', true, 'ACTIVE', 2.00, 15, '{"adult": true}'::jsonb),
  ('inkapk', 'Inkapk', 'https://inkapk.com', true, 'ACTIVE', 2.00, 15, '{"adult": true}'::jsonb),
  ('instahentai', 'InstaHentai', 'https://instahentai.com', true, 'ACTIVE', 2.00, 15, '{"adult": true}'::jsonb),
  ('megahentai', 'MegaHentai', 'https://megahentai.com', true, 'ACTIVE', 2.00, 15, '{"adult": true}'::jsonb),
  ('tiamanhwa', 'Tia Manhwa', 'https://tiamanhwa.com', true, 'ACTIVE', 2.00, 15, '{"adult": true}'::jsonb)
on conflict (id) do update set
  name = excluded.name,
  base_url = excluded.base_url,
  enabled = excluded.enabled,
  status = excluded.status,
  rate_limit_per_second = excluded.rate_limit_per_second,
  sync_interval_minutes = excluded.sync_interval_minutes,
  config = excluded.config,
  updated_at = now();
