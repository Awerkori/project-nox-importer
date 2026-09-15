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
  const maxStaged = {};
  for(const s of staged) maxStaged[s.work_id] = Math.max(maxStaged[s.work_id]||0, s.chapter_sort_key);
  
  let allQueue = [];
  let lastId = '00000000-0000-0000-0000-000000000000';
  while(true) {
    const { data: qJobs } = await supabase.from('importer_queue').select('id, chapter_sort_key, payload, status').gt('id', lastId).in('status', ['QUEUED', 'RETRY']).lte('next_run_at', new Date().toISOString()).order('id').limit(1000);
    if(qJobs.length === 0) break;
    allQueue = allQueue.concat(qJobs);
    lastId = qJobs[qJobs.length-1].id;
  }
  
  const results = [];
  for(const q of allQueue) {
    const wid = q.payload?.workId;
    if (wid && maxStaged[wid] && q.chapter_sort_key < maxStaged[wid]) {
      results.push({id: q.id, key: q.chapter_sort_key, wid: wid});
    }
  }
  results.sort((a,b) => a.key - b.key);
  console.log(results.slice(0, 10));
}
run();
