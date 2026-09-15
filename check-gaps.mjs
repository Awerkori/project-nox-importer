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
  const { data: stagedWorks } = await supabase.from('importer_chapter_mappings')
    .select('work_id, chapter_sort_key').eq('status', 'STAGED');
    
  const maxStaged = {};
  for (const m of stagedWorks) {
    if (!maxStaged[m.work_id] || m.chapter_sort_key > maxStaged[m.work_id]) {
      maxStaged[m.work_id] = m.chapter_sort_key;
    }
  }
  
  for (const workId of Object.keys(maxStaged)) {
    const { count } = await supabase.from('importer_queue').select('*', {count: 'exact', head: true})
      .eq('status', 'QUEUED')
      .eq('task_type', 'IMPORT_CHAPTER')
      .contains('payload', {workId: workId})
      .lt('chapter_sort_key', maxStaged[workId]);
    console.log(`Work ${workId}: ${count} gaps`);
  }
}
run();
