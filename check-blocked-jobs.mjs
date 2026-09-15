import { createClient } from '@supabase/supabase-js';
import { readFileSync } from 'fs';
const envRaw = readFileSync('/home/awerkori/.Projects/project-nox-importer/.env', 'utf8');
const env = {};
for (const line of envRaw.split('\n')) { 
  const m = line.match(/^([A-Z_]+)=(.*)$/); 
  if (m) env[m[1]] = m[2].trim(); 
}
const supabase = createClient(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY);
async function run() {
  const { data: stagedWorks } = await supabase.from('importer_chapter_mappings').select('work_id, chapter_sort_key').eq('status', 'STAGED');
  const counts = {};
  for(const c of stagedWorks) counts[c.work_id] = Math.max(counts[c.work_id] || 0, c.chapter_sort_key);
  
  const workIds = Object.keys(counts).slice(0, 5);
  for(const wid of workIds) {
      const maxStaged = counts[wid];
      const { data: qJobs } = await supabase.from('importer_queue').select('id, status, task_type, chapter_sort_key, next_run_at').eq('task_type', 'IMPORT_CHAPTER').eq('payload->>workId', wid).lt('chapter_sort_key', maxStaged);
      console.log(`Work ${wid} max STAGED: ${maxStaged}, Queued blocking jobs:`, qJobs);
  }
}
run();
