-- Expand importer_sources status constraint to include all valid states
ALTER TABLE importer_sources
  DROP CONSTRAINT IF EXISTS importer_sources_status_check;

ALTER TABLE importer_sources
  ADD CONSTRAINT importer_sources_status_check
  CHECK (status IN (
    'ACTIVE',
    'PAUSED',
    'COOLDOWN',
    'DISABLED',
    'UPSTREAM_BLOCKED',
    'EXCLUDED_BY_POLICY',
    'TEMPORARILY_UNAVAILABLE',
    'PERMANENT_404',
    'DEGRADED',
    'RECOVERING'
  ));

-- Update mangaonline: domain changed to mangaonline.love (currently unreachable)
UPDATE importer_sources
SET
  status = 'TEMPORARILY_UNAVAILABLE',
  blocked_reason = 'Domain migrated',
  blocked_details = jsonb_build_object('reason', 'mangaonline.red redirects to mangaonline.love which is currently unreachable (verified 2026-09-14)'),
  cooldown_until = NOW() + INTERVAL '24 hours',
  updated_at = NOW()
WHERE id = 'mangaonline';

-- Make sure mangaonline adapter is not generating new jobs while unavailable
UPDATE importer_sources
SET enabled = false
WHERE id = 'mangaonline' AND status = 'TEMPORARILY_UNAVAILABLE';
