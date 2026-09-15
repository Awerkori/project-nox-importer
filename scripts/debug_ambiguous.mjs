import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';
dotenv.config();

const url = process.env.SUPABASE_URL;
const key = process.env.SUPABASE_SERVICE_ROLE_KEY;
const sb = createClient(url, key);

async function main() {
  const query = `
    SELECT q.id
    FROM (
      SELECT cand_batch.id, cand_batch.payload, cand_batch.task_type, cand_batch.chapter_sort_key, cand_batch.priority, cand_batch.created_at
      FROM (
        ( SELECT q_cand.id, q_cand.task_type, q_cand.priority, q_cand.payload, q_cand.chapter_sort_key, q_cand.created_at
          FROM public.importer_queue q_cand
          WHERE q_cand.status in ('QUEUED', 'RETRY') AND q_cand.priority >= 100 AND q_cand.next_run_at <= now()
          ORDER BY q_cand.priority DESC, q_cand.next_run_at ASC LIMIT 25 )
        UNION ALL
        ( SELECT q_cand.id, q_cand.task_type, q_cand.priority, q_cand.payload, q_cand.chapter_sort_key, q_cand.created_at
          FROM public.importer_queue q_cand
          WHERE q_cand.status in ('QUEUED', 'RETRY') AND q_cand.priority >= 80 AND q_cand.priority < 100 AND q_cand.next_run_at <= now()
          ORDER BY q_cand.priority DESC, q_cand.next_run_at ASC LIMIT 15 )
      ) cand_batch
      ORDER BY cand_batch.priority DESC
    ) sorted_cands
    JOIN public.importer_queue q ON q.id = sorted_cands.id
    LIMIT 1;
  `;
  
  const { data, error } = await sb.rpc('exec_sql', { sql: query }).catch(e => ({ error: e }));
  console.log('Error from simple test:', error);
}
main().catch(console.error);
