-- Keep the diagnostic backlog probe on a narrow partial index. This predicate
-- deliberately excludes completed/published chapters and all non-chapter work.
-- The runtime query adds the two-hour lower bound and LIMIT 1, allowing YSQL
-- to stop at the first matching index entry instead of scanning DONE history.

CREATE INDEX IF NOT EXISTS importer_lite_tasks_progress_unpublished_idx
  ON public.importer_lite_tasks (updated_at ASC)
  WHERE state = 'DONE'
    AND task_type = 'IMPORT_CHAPTER'
    AND checkpoint ? 'mediaIds'
    AND NOT checkpoint ? 'publishedAt';
