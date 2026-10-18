-- Permanently retire Kuro from operational importer state while preserving an
-- immutable, queryable audit trail before its mappings and queued work are
-- removed by the retirement runner.  Canonical works, chapters, pages and
-- media are intentionally outside the scope of this migration.
BEGIN;

CREATE TABLE IF NOT EXISTS public.importer_source_retirement_runs (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  source text NOT NULL,
  reason text NOT NULL,
  started_at timestamptz NOT NULL DEFAULT now(),
  completed_at timestamptz NULL,
  counts jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS importer_source_retirement_runs_source_idx
  ON public.importer_source_retirement_runs (source, completed_at DESC);

-- A generic snapshot keeps every pre-removal column (including future
-- additive columns) without coupling historical provenance to live foreign
-- keys. `original_id` makes every removed row individually traceable.
CREATE TABLE IF NOT EXISTS public.importer_retired_source_records (
  source text NOT NULL,
  record_type text NOT NULL,
  original_id text NOT NULL,
  retirement_run_id uuid NOT NULL
    REFERENCES public.importer_source_retirement_runs(id) ON DELETE RESTRICT,
  retired_at timestamptz NOT NULL DEFAULT now(),
  record jsonb NOT NULL,
  PRIMARY KEY (source, record_type, original_id)
);
CREATE INDEX IF NOT EXISTS importer_retired_source_records_run_idx
  ON public.importer_retired_source_records (retirement_run_id, record_type);

-- Deployment does not run destructive cleanup. It only makes the source
-- unschedulable immediately; the audited runner performs archival and
-- deletion after confirming no lease is active.
UPDATE public.importer_sources
   SET enabled = false,
       status = 'DISABLED',
       chapter_ingestion_enabled = false,
       catalog_discovery_enabled = false,
       cooldown_until = NULL,
       blocked_reason = 'Retired: recurring Cloudflare authentication challenge has no sustainable importer route',
       blocked_details = jsonb_build_object(
         'sourceRetirement', '2026-10-18',
         'reason', 'KURO_EXTERNAL_CLOUDFLARE_LOGIN_CHALLENGE'
       ),
       config = COALESCE(config, '{}'::jsonb) || jsonb_build_object(
         'sourceRetirement', '2026-10-18',
         'sourceRetirementReason', 'KURO_EXTERNAL_CLOUDFLARE_LOGIN_CHALLENGE'
       ),
       updated_at = now()
 WHERE id = 'kuro';

COMMIT;
