import fs from 'fs';

const p = '../project-nox-manga/supabase/migrations/20260915020000_perfect_gap_and_fairness.sql';
let sql = fs.readFileSync(p, 'utf8');

// The flawed part:
// SELECT q.id INTO v_job_id FROM public.importer_queue q WHERE q.id = ( SELECT cand_batch.id FROM ( ... ) cand_batch ORDER BY ... LIMIT 1 ) FOR UPDATE OF q SKIP LOCKED;

const target = `  SELECT q.id INTO v_job_id
  FROM public.importer_queue q
  WHERE q.id = (
    SELECT cand_batch.id
    FROM (`;

const replacement = `  SELECT q.id INTO v_job_id
  FROM (
    SELECT cand_batch.id, cand_batch.payload, cand_batch.task_type, cand_batch.chapter_sort_key, cand_batch.priority, cand_batch.created_at
    FROM (`;

sql = sql.replace(target, replacement);

const target2 = `    LIMIT 1
  )
  FOR UPDATE OF q SKIP LOCKED;`;

const replacement2 = `    ) cand_batch
    ORDER BY
      CASE WHEN v_focus_work_id is not null and (cand_batch.payload->>'workId')::text = v_focus_work_id::text THEN 100000 ELSE 0 END DESC,
      CASE WHEN p_task_type = 'DISCOVERY' AND cand_batch.task_type = 'DISCOVER_WORKS' THEN 10000 ELSE 0 END DESC,
      CASE WHEN p_task_type = 'DISCOVERY' AND cand_batch.task_type = 'SYNC_WORK' THEN 9000 ELSE 0 END DESC,
      (
        cand_batch.priority 
        + CASE WHEN cand_batch.task_type = 'IMPORT_CHAPTER' 
                AND cand_batch.payload->>'workId' is not null 
                AND cand_batch.chapter_sort_key is not null 
                AND exists (
                  SELECT 1 FROM public.importer_chapter_mappings staged
                  WHERE staged.work_id = (cand_batch.payload->>'workId')::uuid
                    AND staged.status = 'STAGED'
                    AND staged.chapter_sort_key > cand_batch.chapter_sort_key
                ) THEN 500 ELSE 0 END
        - CASE WHEN cand_batch.payload->>'workId' is not null THEN (
            SELECT count(*) * 1000
            FROM public.importer_queue active_q
            WHERE active_q.status = 'IMPORTING' 
              AND active_q.task_type = 'IMPORT_CHAPTER'
              AND (active_q.payload->>'workId')::text = (cand_batch.payload->>'workId')::text
          ) ELSE 0 END
      ) DESC,
      CASE WHEN cand_batch.chapter_sort_key IS NOT NULL THEN cand_batch.chapter_sort_key ELSE 999999 END ASC,
      cand_batch.created_at ASC
  ) sorted_cands
  JOIN public.importer_queue q ON q.id = sorted_cands.id
  FOR UPDATE OF q SKIP LOCKED
  LIMIT 1;`;

// But wait, the original query already had the ORDER BY inside the subquery! I need to replace the whole block carefully.
