-- Chapter identity repair: only whole numeric display variants are equivalent.
--
-- The legacy importer stored `28.00` with a legacy key and no language.  Lite
-- correctly treats decimal text as opaque, but that made a new pt-BR source
-- materialize `28` beside the old Reader chapter.  This migration retires
-- only objectively corroborated public pairs; it never touches fractions,
-- specials, explicit alternate languages or editions.

CREATE TABLE IF NOT EXISTS public.importer_chapter_identity_merges (
  retired_chapter_id uuid PRIMARY KEY REFERENCES public.chapters(id) ON DELETE RESTRICT,
  canonical_chapter_id uuid NOT NULL REFERENCES public.chapters(id) ON DELETE RESTRICT,
  work_id uuid NOT NULL REFERENCES public.works(id) ON DELETE RESTRICT,
  reason text NOT NULL,
  canonical_display_number text NOT NULL,
  retired_display_number text NOT NULL,
  canonical_page_count integer NOT NULL,
  retired_page_count integer NOT NULL,
  canonical_published_at timestamptz NULL,
  retired_published_at timestamptz NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  CHECK (retired_chapter_id <> canonical_chapter_id)
);
CREATE INDEX IF NOT EXISTS importer_chapter_identity_merges_canonical_idx
  ON public.importer_chapter_identity_merges (canonical_chapter_id);

-- This predicate deliberately leaves ambiguous pairs untouched.  `und` is a
-- missing legacy language, not an explicit translation; all explicit
-- language/edition combinations retain their independent Reader identity.
WITH candidates AS (
  SELECT modern.id AS canonical_chapter_id,legacy.id AS retired_chapter_id,
    modern.work_id,modern.display_number AS canonical_display_number,
    legacy.display_number AS retired_display_number,
    modern.published_at AS canonical_published_at,legacy.published_at AS retired_published_at
  FROM public.chapters modern
  JOIN public.chapters legacy ON legacy.work_id=modern.work_id
    AND regexp_replace(legacy.display_number,'\.0+$','')=modern.display_number
  WHERE modern.published_at IS NOT NULL
    AND legacy.published_at IS NOT NULL
    AND modern.display_number ~ '^[0-9]+$'
    AND legacy.display_number ~ '^[0-9]+\.0+$'
    AND modern.canonical_chapter_key ~ '^pt-br\|default\|chapter\|[0-9]+\|-$'
    AND legacy.canonical_chapter_key LIKE 'legacy:%'
    AND modern.chapter_kind='chapter' AND legacy.chapter_kind='chapter'
    AND modern.edition='default' AND legacy.edition='default'
    AND modern.language='pt-BR' AND legacy.language='und'
    AND lower(modern.title)=lower(legacy.title)
), page_counts AS (
  SELECT p.chapter_id,count(*)::integer AS page_count
  FROM public.pages p
  JOIN (
    SELECT canonical_chapter_id AS chapter_id FROM candidates
    UNION
    SELECT retired_chapter_id AS chapter_id FROM candidates
  ) candidate_pages ON candidate_pages.chapter_id=p.chapter_id
  GROUP BY p.chapter_id
)
INSERT INTO public.importer_chapter_identity_merges (
  retired_chapter_id,canonical_chapter_id,work_id,reason,
  canonical_display_number,retired_display_number,
  canonical_page_count,retired_page_count,canonical_published_at,retired_published_at
)
SELECT candidate.retired_chapter_id,candidate.canonical_chapter_id,candidate.work_id,
  'WHOLE_NUMBER_ZERO_DECIMAL_LEGACY',candidate.canonical_display_number,candidate.retired_display_number,
  coalesce(canonical_pages.page_count,0),coalesce(retired_pages.page_count,0),
  candidate.canonical_published_at,candidate.retired_published_at
FROM candidates candidate
LEFT JOIN page_counts canonical_pages ON canonical_pages.chapter_id=candidate.canonical_chapter_id
LEFT JOIN page_counts retired_pages ON retired_pages.chapter_id=candidate.retired_chapter_id
ON CONFLICT (retired_chapter_id) DO NOTHING;

-- Preserve every old row and every page/media reference for audit and direct
-- Reader recovery.  Only source mappings and the public projection move to
-- the canonical chapter; the retired row is never deleted.
UPDATE public.importer_chapter_mappings mapping
SET chapter_id=merge.canonical_chapter_id
FROM public.importer_chapter_identity_merges merge
WHERE merge.reason='WHOLE_NUMBER_ZERO_DECIMAL_LEGACY'
  AND mapping.chapter_id=merge.retired_chapter_id;

-- If Lite had already projected a legacy mapping, keep the canonical mapping
-- and remove only its redundant projection row before retargeting any lone
-- projection.  No chapter, page or media row is deleted here.
DELETE FROM public.importer_lite_chapters retired_projection
USING public.importer_chapter_identity_merges merge,
  public.importer_lite_chapters canonical_projection
WHERE merge.reason='WHOLE_NUMBER_ZERO_DECIMAL_LEGACY'
  AND retired_projection.chapter_id=merge.retired_chapter_id
  AND canonical_projection.work_id=merge.work_id
  AND canonical_projection.chapter_id=merge.canonical_chapter_id;

UPDATE public.importer_lite_chapters projection
SET chapter_id=merge.canonical_chapter_id
FROM public.importer_chapter_identity_merges merge
WHERE merge.reason='WHOLE_NUMBER_ZERO_DECIMAL_LEGACY'
  AND projection.chapter_id=merge.retired_chapter_id;

UPDATE public.importer_lite_chapter_sources source
SET canonical_chapter_key=modern.canonical_chapter_key,updated_at=now()
FROM public.importer_chapter_identity_merges merge
JOIN public.chapters modern ON modern.id=merge.canonical_chapter_id
WHERE merge.reason='WHOLE_NUMBER_ZERO_DECIMAL_LEGACY'
  AND source.work_id=merge.work_id
  AND source.canonical_chapter_key='legacy:' || merge.retired_chapter_id::text;

UPDATE public.chapters legacy
SET published_at=NULL,is_fresh_release=false
FROM public.importer_chapter_identity_merges merge
WHERE merge.reason='WHOLE_NUMBER_ZERO_DECIMAL_LEGACY'
  AND legacy.id=merge.retired_chapter_id
  AND legacy.published_at IS NOT NULL;

-- Re-key pending durable Lite metadata from `28.00` to `28`.  Non-zero
-- fractions are not matched by this expression.  A source identity remains
-- intact, and an already-existing canonical task wins over a duplicate
-- waiting task without interrupting a RUNNING import.
UPDATE public.importer_lite_chapter_sources source
SET canonical_chapter_key=regexp_replace(source.canonical_chapter_key,'\.0+(\|.*)$','\1'),updated_at=now()
WHERE source.canonical_chapter_key ~ '^[^|]+\|[^|]+\|chapter\|[0-9]+\.0+\|-$';

DELETE FROM public.importer_lite_chapters zero_projection
USING public.importer_lite_chapters whole_projection
WHERE zero_projection.canonical_chapter_key ~ '^[^|]+\|[^|]+\|chapter\|[0-9]+\.0+\|-$'
  AND whole_projection.work_id=zero_projection.work_id
  AND whole_projection.canonical_chapter_key=regexp_replace(zero_projection.canonical_chapter_key,'\.0+(\|.*)$','\1');

UPDATE public.importer_lite_chapters projection
SET canonical_chapter_key=regexp_replace(projection.canonical_chapter_key,'\.0+(\|.*)$','\1')
WHERE projection.canonical_chapter_key ~ '^[^|]+\|[^|]+\|chapter\|[0-9]+\.0+\|-$';

UPDATE public.importer_lite_tasks task
SET chapter_key=regexp_replace(task.chapter_key,'\.0+(\|.*)$','\1'),
  idempotency_key='lite:chapter:' || task.work_id::text || ':' || regexp_replace(task.chapter_key,'\.0+(\|.*)$','\1'),
  updated_at=now()
WHERE task.task_type='IMPORT_CHAPTER'
  AND task.state <> 'RUNNING'
  AND task.chapter_key ~ '^[^|]+\|[^|]+\|chapter\|[0-9]+\.0+\|-$'
  AND NOT EXISTS (
    SELECT 1 FROM public.importer_lite_tasks canonical_task
    WHERE canonical_task.idempotency_key='lite:chapter:' || task.work_id::text || ':' || regexp_replace(task.chapter_key,'\.0+(\|.*)$','\1')
  );

UPDATE public.importer_lite_tasks duplicate_task
SET state='DONE',lease_owner=NULL,lease_until=NULL,
  reason='CANONICAL_ZERO_DECIMAL_COALESCED',updated_at=now()
WHERE duplicate_task.task_type='IMPORT_CHAPTER'
  AND duplicate_task.state <> 'RUNNING'
  AND duplicate_task.chapter_key ~ '^[^|]+\|[^|]+\|chapter\|[0-9]+\.0+\|-$'
  AND EXISTS (
    SELECT 1 FROM public.importer_lite_tasks canonical_task
    WHERE canonical_task.id <> duplicate_task.id
      AND canonical_task.idempotency_key='lite:chapter:' || duplicate_task.work_id::text || ':' || regexp_replace(duplicate_task.chapter_key,'\.0+(\|.*)$','\1')
  );
