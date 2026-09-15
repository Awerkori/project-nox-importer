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
  console.log('Fetching all importer_work_mappings...');
  let mappings = [];
  let page = 0;
  while(true) {
    const { data } = await supabase.from('importer_work_mappings').select('id, work_id').range(page*1000, (page+1)*1000-1);
    if (!data || data.length === 0) break;
    mappings.push(...data);
    page++;
  }
  
  const mappingDict = {};
  for (const m of mappings) mappingDict[m.id] = m.work_id;

  console.log(`Found ${mappings.length} mappings. Scanning queue for jobs to remap...`);

  let updatedJobs = 0;
  page = 0;
  while(true) {
    const { data: jobs, error } = await supabase.from('importer_queue')
      .select('id, payload')
      .eq('task_type', 'IMPORT_CHAPTER')
      .eq('status', 'QUEUED')
      .range(page*1000, (page+1)*1000 - 1);
      
    if (error) { console.error(error); break; }
    if (!jobs || jobs.length === 0) break;
    
    for (const job of jobs) {
      const wMid = job.payload?.workMappingId;
      const currentWId = job.payload?.workId;
      const canonicalWId = mappingDict[wMid];
      
      if (canonicalWId && currentWId !== canonicalWId) {
         // Remap!
         const newPayload = { ...job.payload, workId: canonicalWId };
         await supabase.from('importer_queue').update({ payload: newPayload }).eq('id', job.id);
         updatedJobs++;
      }
    }
    page++;
    if (page % 5 === 0) console.log(`Processed ${page*1000} jobs. Updated so far: ${updatedJobs}`);
  }
  
  console.log(`Finished processing queue. Total jobs remapped to canonical works: ${updatedJobs}`);
}
run();
