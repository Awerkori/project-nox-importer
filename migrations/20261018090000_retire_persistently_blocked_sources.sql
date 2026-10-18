-- Disable only sources that failed the complete production admission gate on
-- 2026-10-18. This migration never deletes public content or operational
-- mappings; scripts/retire-blocked-sources-production.ts archives those rows
-- atomically after the deployed runtime is verified.
BEGIN;

WITH retired(id, reason) AS (
  VALUES
    ('acervohentai', 'AUDIT_2026_10_JS_CHALLENGE'),
    ('amuy', 'AUDIT_2026_10_JS_CHALLENGE'),
    ('arthurscan', 'AUDIT_2026_10_JS_CHALLENGE'),
    ('geasscomics', 'AUDIT_2026_10_JS_CHALLENGE'),
    ('inkapk', 'AUDIT_2026_10_JS_CHALLENGE'),
    ('mangaonline', 'AUDIT_2026_10_JS_CHALLENGE'),
    ('mangastop', 'AUDIT_2026_10_JS_CHALLENGE'),
    ('pointzerotoons', 'AUDIT_2026_10_JS_CHALLENGE'),
    ('vegitoons', 'AUDIT_2026_10_API_BLOCK'),
    ('yaoifanclub', 'AUDIT_2026_10_JS_CHALLENGE'),
    ('yuriverso', 'AUDIT_2026_10_JS_CHALLENGE')
)
UPDATE public.importer_sources AS source
   SET enabled = false,
       status = 'DISABLED',
       chapter_ingestion_enabled = false,
       catalog_discovery_enabled = false,
       cooldown_until = NULL,
       blocked_reason = 'Retired after production source audit: ' || retired.reason,
       blocked_details = jsonb_build_object(
         'sourceRetirement', '2026-10-18',
         'reason', retired.reason,
         'audit', 'production-admission-gate'
       ),
       config = COALESCE(source.config, '{}'::jsonb) || jsonb_build_object(
         'sourceRetirement', '2026-10-18',
         'sourceRetirementReason', retired.reason
       ),
       updated_at = now()
  FROM retired
 WHERE source.id = retired.id;

COMMIT;
