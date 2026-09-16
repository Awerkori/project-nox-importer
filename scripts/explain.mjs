import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';
dotenv.config();
const sb = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY);
async function check() {
  const { data, error } = await sb.rpc('execute_sql', { sql: `
    EXPLAIN ANALYZE
    WITH staged_works AS (
      SELECT work_id, MAX(chapter_sort_key) as max_staged_sort_key
      FROM public.importer_chapter_mappings
      WHERE status = 'STAGED'
      GROUP BY work_id
    )
    SELECT q_cand.id
    FROM staged_works sw
    JOIN public.importer_queue q_cand 
      ON (q_cand.payload->>'workId') = sw.work_id::text
    WHERE q_cand.status in ('QUEUED', 'RETRY') 
      AND q_cand.next_run_at <= now()
      AND q_cand.task_type = 'IMPORT_CHAPTER'
      AND q_cand.chapter_sort_key < sw.max_staged_sort_key
    ORDER BY q_cand.chapter_sort_key ASC LIMIT 20
  ` });
  console.log(data || error);
}
check();
