BEGIN;

-- The metadata producer admits at most two P2 source refetches.  Its source
-- frontier must therefore be durable: scanning all providers merely to select
-- two mappings was itself enough to create YSQL waiters.  Advancing this
-- cursor after each bounded probe guarantees that an exhausted source cannot
-- starve later sources and that a restart resumes safely.
CREATE TABLE IF NOT EXISTS public.importer_lite_metadata_source_cursor (
  cursor_name text PRIMARY KEY,
  last_source text NULL,
  updated_at timestamptz NOT NULL DEFAULT now()
);

INSERT INTO public.importer_lite_metadata_source_cursor(cursor_name,last_source)
VALUES ('source_refetch',NULL)
ON CONFLICT(cursor_name) DO NOTHING;

REVOKE ALL ON public.importer_lite_metadata_source_cursor FROM PUBLIC;
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname='anon') THEN
    REVOKE ALL ON public.importer_lite_metadata_source_cursor FROM anon;
  END IF;
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname='authenticated') THEN
    REVOKE ALL ON public.importer_lite_metadata_source_cursor FROM authenticated;
  END IF;
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname='service_role') THEN
    GRANT ALL ON public.importer_lite_metadata_source_cursor TO service_role;
  END IF;
END;
$$;

COMMIT;
