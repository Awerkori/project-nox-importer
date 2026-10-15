BEGIN;

-- One durable row per work records the product transitions that matter for
-- discovery/search. These are deliberately transitions, not a sum of mapping
-- deltas: several mappings can enrich the same work and must count once.
CREATE TABLE IF NOT EXISTS public.importer_lite_metadata_product_metrics (
  work_id uuid PRIMARY KEY REFERENCES public.works(id) ON DELETE CASCADE,
  aliases_activated_at timestamptz NULL,
  tags_activated_at timestamptz NULL,
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE OR REPLACE FUNCTION public.importer_lite_record_alias_activation()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_activated boolean := false;
BEGIN
  IF cardinality(COALESCE(NEW.aliases, ARRAY[]::text[])) > 0 THEN
    IF TG_OP = 'INSERT' THEN
      v_activated := true;
    ELSIF cardinality(COALESCE(OLD.aliases, ARRAY[]::text[])) = 0 THEN
      v_activated := true;
    END IF;
  END IF;
  IF v_activated THEN
    INSERT INTO public.importer_lite_metadata_product_metrics(work_id,aliases_activated_at,updated_at)
    VALUES(NEW.id,now(),now())
    ON CONFLICT(work_id) DO UPDATE SET
      aliases_activated_at=COALESCE(importer_lite_metadata_product_metrics.aliases_activated_at,excluded.aliases_activated_at),
      updated_at=now();
  END IF;
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS importer_lite_metadata_alias_activation ON public.works;
CREATE TRIGGER importer_lite_metadata_alias_activation
AFTER INSERT OR UPDATE OF aliases ON public.works
FOR EACH ROW EXECUTE FUNCTION public.importer_lite_record_alias_activation();

CREATE OR REPLACE FUNCTION public.importer_lite_record_tag_activation()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
BEGIN
  -- The importer writes explicit upstream taxonomy with system_generated=true.
  -- A staff association never contributes to this importer product metric.
  IF NEW.system_generated
    AND NOT EXISTS (
      SELECT 1 FROM public.work_tags existing
      WHERE existing.work_id=NEW.work_id AND existing.tag_id<>NEW.tag_id
    ) THEN
    INSERT INTO public.importer_lite_metadata_product_metrics(work_id,tags_activated_at,updated_at)
    VALUES(NEW.work_id,now(),now())
    ON CONFLICT(work_id) DO UPDATE SET
      tags_activated_at=COALESCE(importer_lite_metadata_product_metrics.tags_activated_at,excluded.tags_activated_at),
      updated_at=now();
  END IF;
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS importer_lite_metadata_tag_activation ON public.work_tags;
CREATE TRIGGER importer_lite_metadata_tag_activation
AFTER INSERT ON public.work_tags
FOR EACH ROW EXECUTE FUNCTION public.importer_lite_record_tag_activation();

REVOKE ALL ON public.importer_lite_metadata_product_metrics FROM PUBLIC;
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname='anon') THEN
    REVOKE ALL ON public.importer_lite_metadata_product_metrics FROM anon;
  END IF;
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname='authenticated') THEN
    REVOKE ALL ON public.importer_lite_metadata_product_metrics FROM authenticated;
  END IF;
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname='service_role') THEN
    GRANT ALL ON public.importer_lite_metadata_product_metrics TO service_role;
  END IF;
END;
$$;

COMMIT;
