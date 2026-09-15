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
  console.log('Fetching all work ids...');
  const { data: works } = await supabase.from('works').select('id');
  const validWorkIds = new Set(works.map(w => w.id));

  let page = 0;
  let invalidJobs = 0;
  while(true) {
    const { data: jobs, error } = await supabase.from('importer_queue')
      .select('id, payload')
      .eq('task_type', 'IMPORT_CHAPTER')
      .eq('status', 'QUEUED')
      .range(page*1000, (page+1)*1000 - 1);
      
    if (error) { console.error(error); break; }
    if (jobs.length === 0) break;
    
    for (const job of jobs) {
      const wId = job.payload?.workId;
      if (wId && !validWorkIds.has(wId)) {
         invalidJobs++;
         // console.log(`Job ${job.id} points to invalid workId ${wId}`);
      }
    }
    page++;
  }
  
  console.log(`Jobs pointing to non-existent works: ${invalidJobs}`);
}
run();
