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
  const { data: recentJobs } = await supabase.from('importer_queue')
    .select('*')
    .in('status', ['COMPLETED', 'FAILED'])
    .order('updated_at', { ascending: false })
    .limit(20);
    
  let skipCount = 0;
  let completeCount = 0;
  let failCount = 0;
  
  for (const j of recentJobs) {
    if (j.status === 'COMPLETED') {
      completeCount++;
      // Check if it was skipped due to canonical
      // Since skipped jobs finish as COMPLETED instantly without downloading,
      // their duration or log might indicate it. Actually, just check if they are COMPLETED.
    } else if (j.status === 'FAILED') {
      failCount++;
    }
  }
  
  console.log(`Recent Jobs -> Completed: ${completeCount}, Failed: ${failCount}`);
  
  // Also verify that the jobs we remapped earlier for Magic Emperor have been picked up and COMPLETED without errors.
  const targetIds = ['4b1f452c-b223-404c-83f4-e0d626563397', '7eb3e4e4-32be-4fb3-9ffd-4b1206701dd5', '27242595-44b5-4683-83c6-9b47f9bf6e11'];
  const { data: magicJobs } = await supabase.from('importer_queue')
    .select('id, status, task_type')
    .in('payload->>workId', targetIds)
    .limit(10);
    
  console.log('Sample remapped jobs statuses:');
  for (const mj of (magicJobs || [])) {
    console.log(`- Job ${mj.id}: ${mj.task_type} -> ${mj.status}`);
  }
}
run();
