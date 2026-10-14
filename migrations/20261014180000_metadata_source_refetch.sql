BEGIN;

-- Durable per-mapping classification for the bounded, source-backed catalog
-- metadata pass. It is separate from the adapter payload so raw provider
-- metadata and editorial annotations keep their existing ownership.
CREATE TABLE IF NOT EXISTS public.importer_lite_metadata_enrichment (
  source text NOT NULL REFERENCES public.importer_sources(id) ON DELETE RESTRICT,
  source_work_id text NOT NULL,
  work_id uuid NOT NULL REFERENCES public.works(id) ON DELETE RESTRICT,
  aliases_state text NOT NULL DEFAULT 'PENDING'
    CHECK (aliases_state IN ('PENDING','ENRICHED','NO_ALIASES','EXTERNAL_BLOCKED','PARSER_ERROR')),
  tags_state text NOT NULL DEFAULT 'PENDING'
    CHECK (tags_state IN ('PENDING','ENRICHED','NO_TAGS','EXTERNAL_BLOCKED','PARSER_ERROR')),
  aliases_added integer NOT NULL DEFAULT 0 CHECK (aliases_added >= 0),
  tags_added integer NOT NULL DEFAULT 0 CHECK (tags_added >= 0),
  attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
  last_checked_at timestamptz NULL,
  next_retry_at timestamptz NULL,
  last_error text NULL,
  version integer NOT NULL DEFAULT 2,
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (source, source_work_id)
);
CREATE INDEX IF NOT EXISTS importer_lite_metadata_enrichment_pending_idx
  ON public.importer_lite_metadata_enrichment(updated_at, source)
  WHERE aliases_state='PENDING' OR tags_state='PENDING';
CREATE INDEX IF NOT EXISTS importer_lite_metadata_enrichment_recovery_idx
  ON public.importer_lite_metadata_enrichment(next_retry_at, source)
  WHERE aliases_state='EXTERNAL_BLOCKED' OR tags_state='EXTERNAL_BLOCKED';

REVOKE ALL ON public.importer_lite_metadata_enrichment FROM PUBLIC;
DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname='anon') THEN
    EXECUTE 'REVOKE ALL ON public.importer_lite_metadata_enrichment FROM anon';
  END IF;
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname='authenticated') THEN
    EXECUTE 'REVOKE ALL ON public.importer_lite_metadata_enrichment FROM authenticated';
  END IF;
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname='service_role') THEN
    EXECUTE 'GRANT ALL ON public.importer_lite_metadata_enrichment TO service_role';
  END IF;
END $$;

COMMIT;
