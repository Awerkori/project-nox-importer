-- Bound the common focused-claim anti-join without changing its correctness
-- contract.  The fast horizon is accepted only when it already yields the
-- complete 32-work claim window; otherwise the exact historical query runs.
-- This avoids turning temporary in-flight work into starvation.
DO $migration$
DECLARE
  fn text;
  old_selection text := $old$
      SELECT array_agg(cand.work_key)
      INTO v_candidate_keys
      FROM (
        SELECT rw.work_key
        FROM public.importer_lite_ready_works rw
        WHERE rw.priority <= 2
          AND rw.operation_rank <= 1
          AND rw.next_run_at <= v_now
          AND NOT EXISTS (
            SELECT 1 FROM public.importer_lite_tasks active_work
            WHERE active_work.work_key = rw.work_key
              AND active_work.task_type IN ('IMPORT_CHAPTER', 'PUBLISH')
              AND active_work.state = 'RUNNING'
          )
        ORDER BY
          rw.priority ASC,
          rw.operation_rank ASC,
          CASE
            WHEN v_has_focus AND rw.priority = 2 AND rw.operation_rank = 1 THEN
              CASE
                WHEN COALESCE(p_prefer_focus, true) THEN
                  COALESCE(array_position(p_focus_work_keys, rw.work_key), 9999)
                ELSE
                  CASE WHEN rw.work_key = ANY(p_focus_work_keys) THEN 9999 ELSE 0 END
              END
            ELSE 0
          END ASC,
          rw.last_claimed_at ASC NULLS FIRST,
          rw.next_run_at ASC,
          rw.work_key ASC
        LIMIT 32
      ) cand;
$old$;
  new_selection text := $new$
      WITH fast_horizon AS MATERIALIZED (
        SELECT rw.work_key,rw.priority,rw.operation_rank,rw.last_claimed_at,rw.next_run_at
        FROM public.importer_lite_ready_works rw
        WHERE rw.priority <= 2
          AND rw.operation_rank <= 1
          AND rw.next_run_at <= v_now
        ORDER BY
          rw.priority ASC,
          rw.operation_rank ASC,
          CASE
            WHEN v_has_focus AND rw.priority = 2 AND rw.operation_rank = 1 THEN
              CASE
                WHEN COALESCE(p_prefer_focus, true) THEN
                  COALESCE(array_position(p_focus_work_keys, rw.work_key), 9999)
                ELSE
                  CASE WHEN rw.work_key = ANY(p_focus_work_keys) THEN 9999 ELSE 0 END
              END
            ELSE 0
          END ASC,
          rw.last_claimed_at ASC NULLS FIRST,
          rw.next_run_at ASC,
          rw.work_key ASC
        LIMIT 128
      ), fast_candidates AS (
        SELECT h.work_key
        FROM fast_horizon h
        WHERE NOT EXISTS (
          SELECT 1 FROM public.importer_lite_tasks active_work
          WHERE active_work.work_key = h.work_key
            AND active_work.task_type IN ('IMPORT_CHAPTER', 'PUBLISH')
            AND active_work.state = 'RUNNING'
        )
        ORDER BY h.priority,h.operation_rank,h.last_claimed_at ASC NULLS FIRST,h.next_run_at,h.work_key
        LIMIT 32
      )
      SELECT array_agg(cand.work_key)
      INTO v_candidate_keys
      FROM fast_candidates cand;

      -- If the ordered horizon is depleted by in-flight work, retain the
      -- original unbounded candidate search rather than hiding eligible work.
      IF COALESCE(cardinality(v_candidate_keys), 0) < 32 THEN
        SELECT array_agg(cand.work_key)
        INTO v_candidate_keys
        FROM (
          SELECT rw.work_key
          FROM public.importer_lite_ready_works rw
          WHERE rw.priority <= 2
            AND rw.operation_rank <= 1
            AND rw.next_run_at <= v_now
            AND NOT EXISTS (
              SELECT 1 FROM public.importer_lite_tasks active_work
              WHERE active_work.work_key = rw.work_key
                AND active_work.task_type IN ('IMPORT_CHAPTER', 'PUBLISH')
                AND active_work.state = 'RUNNING'
            )
          ORDER BY
            rw.priority ASC,
            rw.operation_rank ASC,
            CASE
              WHEN v_has_focus AND rw.priority = 2 AND rw.operation_rank = 1 THEN
                CASE
                  WHEN COALESCE(p_prefer_focus, true) THEN
                    COALESCE(array_position(p_focus_work_keys, rw.work_key), 9999)
                  ELSE
                    CASE WHEN rw.work_key = ANY(p_focus_work_keys) THEN 9999 ELSE 0 END
                END
              ELSE 0
            END ASC,
            rw.last_claimed_at ASC NULLS FIRST,
            rw.next_run_at ASC,
            rw.work_key ASC
          LIMIT 32
        ) cand;
      END IF;
$new$;
BEGIN
  SELECT pg_get_functiondef(
    'public.importer_lite_claim_task_focused(text,interval,text[],boolean,text[],boolean)'::regprocedure
  ) INTO fn;
  IF fn IS NULL OR position(old_selection IN fn) = 0 THEN
    RAISE EXCEPTION 'focused claim function does not match the reviewed baseline';
  END IF;
  EXECUTE replace(fn, old_selection, new_selection);
END;
$migration$;
