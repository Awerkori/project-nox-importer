import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';
dotenv.config();

const url = process.env.SUPABASE_URL;
const key = process.env.SUPABASE_SERVICE_ROLE_KEY;
const sb = createClient(url, key);

async function main() {
  // Let's find exactly why importer_acquire_job returns 0.
  // We'll select matching jobs directly!
  const { data: cands } = await sb.from('importer_queue')
    .select('id, task_type, priority, payload, chapter_sort_key, next_run_at, source')
    .in('status', ['QUEUED', 'RETRY'])
    .lte('next_run_at', new Date().toISOString())
    .eq('source', 'mangotoons')
    .eq('task_type', 'IMPORT_CHAPTER')
    .gte('priority', 80)
    .limit(10);
    
  console.log('Candidates matching filter:', cands?.length);
  
  if (cands && cands.length > 0) {
    console.log('Sample candidate:', cands[0]);
  }
}
main().catch(console.error);
