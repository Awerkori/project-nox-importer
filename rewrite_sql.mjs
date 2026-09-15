import fs from 'fs';

const p = '../project-nox-manga/supabase/migrations/20260915020000_perfect_gap_and_fairness.sql';
let sql = fs.readFileSync(p, 'utf8');

const regex = /SELECT q\.id INTO v_job_id\s*FROM public\.importer_queue q\s*WHERE q\.id = \(\s*SELECT cand_batch\.id\s*FROM \(([\s\S]*?)LIMIT 1\s*\)\s*FOR UPDATE OF q SKIP LOCKED;/;

const match = sql.match(regex);
if (!match) {
  console.log("No match found!");
} else {
  // We extract the UNION ALL part from match[1], excluding the final ORDER BY
  const innerPart = match[1];
  
  // Actually, we can just rewrite the whole block since we know exactly what it is.
  const newBlock = `
  SELECT q.id INTO v_job_id
  FROM (
    SELECT cand_batch.id, cand_batch.payload, cand_batch.task_type, cand_batch.chapter_sort_key, cand_batch.priority, cand_batch.created_at
    FROM (
      SELECT q_cand.id, q_cand.task_type, q_cand.priority, q_cand.payload, q_cand.chapter_sort_key, q_cand.created_at
      FROM public.importer_queue q_cand
      WHERE q_cand.status in ('QUEUED', 'RETRY') AND q_cand.priority >= 100 AND q_cand.next_run_at <= now()
        AND (v_focus_work_id is null or (q_cand.payload->>'workId')::text = v_focus_work_id::text)
        AND (p_source is null or q_cand.source = p_source)
        AND (p_task_type is null or (p_task_type = 'DISCOVERY' and q_cand.task_type in ('DISCOVER_WORKS', 'SYNC_WORK')) or q_cand.task_type = p_task_type)
        AND (coalesce(v_barrier_state, 'OPEN') != 'CLOSED' OR q_cand.task_type in ('DISCOVER_WORKS', 'SYNC_WORK'))
        AND not exists (SELECT 1 FROM public.importer_sources s WHERE s.id = q_cand.source AND (s.enabled = false or s.status in ('PAUSED', 'DISABLED', 'UPSTREAM_BLOCKED') or (s.cooldown_until is not null and s.cooldown_until > now())))
      ORDER BY q_cand.priority DESC, q_cand.next_run_at ASC LIMIT 25
      UNION ALL
      SELECT q_cand.id, q_cand.task_type, q_cand.priority, q_cand.payload, q_cand.chapter_sort_key, q_cand.created_at
      FROM public.importer_queue q_cand
      WHERE q_cand.status in ('QUEUED', 'RETRY') AND q_cand.priority >= 80 AND q_cand.priority < 100 AND q_cand.next_run_at <= now()
        AND (v_focus_work_id is null or (q_cand.payload->>'workId')::text = v_focus_work_id::text)
        AND (p_source is null or q_cand.source = p_source)
        AND (p_task_type is null or (p_task_type = 'DISCOVERY' and q_cand.task_type in ('DISCOVER_WORKS', 'SYNC_WORK')) or q_cand.task_type = p_task_type)
        AND (coalesce(v_barrier_state, 'OPEN') != 'CLOSED' OR q_cand.task_type in ('DISCOVER_WORKS', 'SYNC_WORK'))
        AND not exists (SELECT 1 FROM public.importer_sources s WHERE s.id = q_cand.source AND (s.enabled = false or s.status in ('PAUSED', 'DISABLED', 'UPSTREAM_BLOCKED') or (s.cooldown_until is not null and s.cooldown_until > now())))
      ORDER BY q_cand.priority DESC, q_cand.next_run_at ASC LIMIT 15
      UNION ALL
      SELECT q_cand.id, q_cand.task_type, q_cand.priority, q_cand.payload, q_cand.chapter_sort_key, q_cand.created_at
      FROM public.importer_queue q_cand
      WHERE q_cand.status in ('QUEUED', 'RETRY') AND q_cand.priority < 80 AND q_cand.next_run_at <= now()
        AND (v_focus_work_id is null or (q_cand.payload->>'workId')::text = v_focus_work_id::text)
        AND (p_source is null or q_cand.source = p_source)
        AND (p_task_type is null or (p_task_type = 'DISCOVERY' and q_cand.task_type in ('DISCOVER_WORKS', 'SYNC_WORK')) or q_cand.task_type = p_task_type)
        AND (coalesce(v_barrier_state, 'OPEN') != 'CLOSED' OR q_cand.task_type in ('DISCOVER_WORKS', 'SYNC_WORK'))
        AND not exists (SELECT 1 FROM public.importer_sources s WHERE s.id = q_cand.source AND (s.enabled = false or s.status in ('PAUSED', 'DISABLED', 'UPSTREAM_BLOCKED') or (s.cooldown_until is not null and s.cooldown_until > now())))
      ORDER BY q_cand.priority DESC, q_cand.next_run_at ASC LIMIT 10
    ) cand_batch
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

  sql = sql.replace(match[0], newBlock);
  fs.writeFileSync(p, sql);
  console.log("Patched SQL successfully!");
}
