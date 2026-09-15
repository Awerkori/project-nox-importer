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
  console.log('Fetching queued IMPORT_CHAPTER jobs...');
  let totalJobs = 0;
  let mismatchedJobs = 0;
  
  // We can't fetch 100k jobs in memory easily, let's fetch a sample or do a server-side RPC if possible.
  // Instead, let's just fetch mappings and check.
  const { data: mappings } = await supabase.from('importer_work_mappings').select('id, work_id');
  const mappingDict = {};
  for (const m of mappings) mappingDict[m.id] = m.work_id;

  let page = 0;
  while(true) {
    const { data: jobs, error } = await supabase.from('importer_queue')
      .select('id, payload')
      .eq('task_type', 'IMPORT_CHAPTER')
      .eq('status', 'QUEUED')
      .range(page*1000, (page+1)*1000 - 1);
      
    if (error) { console.error(error); break; }
    if (jobs.length === 0) break;
    
    for (const job of jobs) {
      totalJobs++;
      const wMid = job.payload?.workMappingId;
      const wId = job.payload?.workId;
      if (wMid && mappingDict[wMid] && mappingDict[wMid] !== wId) {
         mismatchedJobs++;
      }
    }
    page++;
    if (page % 5 === 0) console.log(`Processed ${page*1000} jobs...`);
  }
  
  console.log(`Total QUEUED IMPORT_CHAPTER jobs: ${totalJobs}`);
  console.log(`Jobs needing workId remap: ${mismatchedJobs}`);
}
run();
