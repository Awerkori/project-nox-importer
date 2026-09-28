-- Alternative-source jobs are intentionally retained for resilience. Without a
-- database fence, two runners can claim different alternatives for the same
-- canonical work/chapter before in-memory accounting sees the first one,
-- duplicating page downloads and Telegram uploads.
--
-- The partial predicate excludes completed/history rows. CONCURRENTLY keeps the
-- operational queue available during rollout.
CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS idx_importer_queue_one_importing_canonical_chapter
  ON public.importer_queue ((payload ->> 'workId'), chapter_sort_key)
  WHERE task_type = 'IMPORT_CHAPTER'
    AND status = 'IMPORTING'
    AND (payload ? 'workId')
    AND chapter_sort_key IS NOT NULL;
