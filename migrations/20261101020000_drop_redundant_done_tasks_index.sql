-- Drop redundant partial index on importer_lite_tasks.
-- Note: YugabyteDB YSQL does NOT support DROP INDEX CONCURRENTLY (throws feature not supported).
-- Standard DROP INDEX in YugabyteDB is an online distributed catalog update.
-- The identical index importer_lite_tasks_publish_recovery_idx remains active and fully covers this path.
DROP INDEX IF EXISTS public.importer_lite_tasks_progress_unpublished_idx;
