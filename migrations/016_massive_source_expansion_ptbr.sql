-- Migration: 20260913160000_massive_source_expansion_ptbr.sql
-- Massive expansion of functional PT-BR manga sources from fonte-extensoes repository
-- Adds 10 thoroughly verified and certified sources to public.importer_sources.

INSERT INTO public.importer_sources (id, name, base_url, enabled, status, rate_limit_per_second)
VALUES 
  ('taimumangas', 'TaimuMangas', 'https://beta.taimumangas.com', true, 'ACTIVE', 4.0),
  ('euphoriascan', 'Euphoria Scan', 'https://euphoriascan.com', true, 'ACTIVE', 3.0),
  ('fleurblanche', 'Fleur Blanche', 'https://fbsquadx.com', true, 'ACTIVE', 3.0),
  ('littletyrant', 'Little Tyrant', 'https://tiraninha.world', true, 'ACTIVE', 3.0),
  ('mangalivreto', 'Manga Livre.to', 'https://mangalivre.to', true, 'ACTIVE', 3.0),
  ('montetai', 'Monte Tai', 'https://montetaiscanlator.xyz', true, 'ACTIVE', 3.0),
  ('nebulosascan', 'Nebulosa Scan', 'https://nebulosascan.com', true, 'ACTIVE', 3.0),
  ('nocturnesummer', 'Nocturne Summer', 'https://nocfsb.com', true, 'ACTIVE', 3.0),
  ('tankouhentai', 'Tankou Hentai', 'https://tankouhentai.com', true, 'ACTIVE', 3.0),
  ('cafecomyaoi', 'Café com Yaoi', 'https://cafecomyaoi.com.br', true, 'ACTIVE', 3.0)
ON CONFLICT (id) DO UPDATE SET
  name = EXCLUDED.name,
  base_url = EXCLUDED.base_url,
  enabled = true,
  status = 'ACTIVE',
  rate_limit_per_second = EXCLUDED.rate_limit_per_second,
  updated_at = now();

-- Ensure policy-excluded sources remain permanently excluded
UPDATE public.importer_sources
SET enabled = false, status = 'EXCLUDED_BY_POLICY', blocked_reason = 'POLICY_EXCLUSION_PERMANENT', updated_at = now()
WHERE id IN ('nexus_toons', 'toonlivre');
