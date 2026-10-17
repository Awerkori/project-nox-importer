-- The importer also performs this reconciliation at boot so a native
-- Discloud deploy cannot leave a validated adapter absent from
-- importer_sources.  Keep this migration for environments that apply the SQL
-- migration history independently.
BEGIN;

INSERT INTO public.importer_sources (
  id, name, base_url, enabled, status, rate_limit_per_second,
  sync_interval_minutes, chapter_ingestion_enabled,
  catalog_discovery_enabled, config
) VALUES
  ('brscans', 'BR Scans', 'https://brscans.com', true, 'ACTIVE', 1.00, 30, true, false, jsonb_build_object('sourceAuditRelease', '2026-10-17')),
  ('mangastop', 'MangaStop', 'https://mangastop.net', true, 'ACTIVE', 1.00, 30, true, false, jsonb_build_object('sourceAuditRelease', '2026-10-17')),
  ('hentaiteca', 'HentaiTeca', 'https://hentaiteca.online', true, 'ACTIVE', 1.00, 30, true, false, jsonb_build_object('sourceAuditRelease', '2026-10-17')),
  ('imperiodabritannia', 'Sagrado Império da Britannia', 'https://api.imperiodabritannia.net/api', true, 'ACTIVE', 1.00, 30, true, false, jsonb_build_object('sourceAuditRelease', '2026-10-17'))
ON CONFLICT (id) DO NOTHING;

-- Restored adapters use current endpoints and are admitted only for verified
-- mappings/onboarding; broad catalog discovery remains intentionally off.
WITH recovered(id, name, base_url) AS (
  VALUES
    ('acervohentai', 'Acervo Hentai', 'https://acervohentai.com'),
    ('amuy', 'AMUY Scan', 'https://www.apenasmaisumyaoi.com'),
    ('arthurscan', 'Arthur Scan', 'https://arthurscan.xyz'),
    ('geasscomics', 'Geass Comics', 'https://geasscomics.xyz'),
    ('inkapk', 'Inkapk', 'https://inkapk.net'),
    ('mangaonline', 'Manga Online', 'https://mangaonline.love'),
    ('manhastro', 'Manhastro', 'https://manhastro.net'),
    ('nexus', 'Nexus Mangas', 'https://www.nexusmangas.com'),
    ('nocturnesummer', 'Nocturne Summer', 'https://nocfsb.com'),
    ('pointzerotoons', 'Kitsune Yako', 'https://kitsuneyako.com'),
    ('tankouhentai', 'Tankou Hentai', 'https://tankouhentai.com'),
    ('yaoifanclub', 'Yaoi Fan Club', 'https://yaoifanclub.com'),
    ('yuriverso', 'Yuri on Air', 'https://yurionair.top')
)
UPDATE public.importer_sources AS source
   SET name = recovered.name,
       base_url = recovered.base_url,
       enabled = true,
       status = 'ACTIVE',
       chapter_ingestion_enabled = true,
       catalog_discovery_enabled = false,
       cooldown_until = NULL,
       blocked_reason = NULL,
       blocked_details = '{}'::jsonb,
       config = COALESCE(source.config, '{}'::jsonb) || jsonb_build_object('sourceAuditActivation', '2026-10-17'),
       updated_at = now()
  FROM recovered
 WHERE source.id = recovered.id
   AND COALESCE(source.config, '{}'::jsonb)->>'sourceAuditActivation' IS DISTINCT FROM '2026-10-17';

UPDATE public.importer_sources
   SET name = 'Kuro Mangas', base_url = 'https://kuromangas.com',
       enabled = true, status = 'ACTIVE', chapter_ingestion_enabled = true,
       catalog_discovery_enabled = false, cooldown_until = NULL,
       blocked_reason = NULL, blocked_details = '{}'::jsonb,
       config = COALESCE(config, '{}'::jsonb) || jsonb_build_object('sourceAuditRecovery', '2026-10-17'),
       updated_at = now()
 WHERE id = 'kuro'
   AND COALESCE(config, '{}'::jsonb)->>'sourceAuditRecovery' IS DISTINCT FROM '2026-10-17';

UPDATE public.importer_sources
   SET enabled = false, status = 'DISABLED', chapter_ingestion_enabled = false,
       catalog_discovery_enabled = false, cooldown_until = NULL,
       blocked_reason = 'Retired after source audit: no sustainable public Reader route',
       blocked_details = jsonb_build_object('sourceAuditRetired', '2026-10-17'),
       config = COALESCE(config, '{}'::jsonb) || jsonb_build_object('sourceAuditRetired', '2026-10-17'),
       updated_at = now()
 WHERE id IN ('megahentai', 'osakascan')
   AND COALESCE(config, '{}'::jsonb)->>'sourceAuditRetired' IS DISTINCT FROM '2026-10-17';

COMMIT;
