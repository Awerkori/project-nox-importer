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
  const { data: staged } = await supabase.from('importer_chapter_mappings').select('work_id, chapter_sort_key').eq('status', 'STAGED');
  const distinct = [...new Set(staged.map(s => s.work_id))];
  
  const { data: qJobs } = await supabase.from('importer_queue').select('id, payload, chapter_sort_key').in('status', ['QUEUED', 'RETRY']).lte('next_run_at', new Date().toISOString());
  
  const qFiltered = qJobs.filter(j => distinct.includes(j.payload?.workId));
  qFiltered.sort((a,b) => a.chapter_sort_key - b.chapter_sort_key);
  console.log('Top 10 lowest chapter_sort_key jobs for staged works:');
  console.log(qFiltered.slice(0, 10));
}
run();
