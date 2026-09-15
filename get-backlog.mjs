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
  const { data: q } = await supabase.from('importer_queue').select('task_type, status, priority');
  
  let fresh = 0;
  let recovery = 0;
  let historical = 0;
  let retry = 0;
  let completed = 0;
  let failed = 0;
  
  for (const job of q) {
     if (job.status === 'QUEUED' || job.status === 'RUNNING') {
         if (job.priority >= 100) fresh++;
         else if (job.priority > 10 && job.priority < 100) recovery++;
         else historical++;
     } else if (job.status === 'RETRY') {
         retry++;
     } else if (job.status === 'COMPLETED') {
         completed++;
     } else if (job.status === 'FAILED') {
         failed++;
     }
  }
  
  console.log('--- BACKLOG INVENTORY ---');
  console.log(`Fresh: ${fresh}`);
  console.log(`Recovery: ${recovery}`);
  console.log(`Historical: ${historical}`);
  console.log(`Retry: ${retry}`);
  console.log(`Completed: ${completed}`);
  console.log(`Failed: ${failed}`);
  
  // Storage ready / staged
  const { count: stored } = await supabase.from('chapters').select('*', {count: 'exact', head:true}).not('storage_url', 'is', null);
  const { count: published } = await supabase.from('chapters').select('*', {count: 'exact', head:true}).eq('status', 'PUBLISHED');
  
  console.log(`Stored chapters: ${stored}`);
  console.log(`Published chapters: ${published}`);
}
run();
