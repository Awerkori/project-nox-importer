-- The previous covering index was evaluated in production and not selected by
-- YSQL for the cooldown-recovery plan.  Keep migration history immutable while
-- ensuring fresh and existing installations converge without its write cost.
DROP INDEX IF EXISTS public.importer_lite_tasks_source_cooldown_due_cover_idx;
