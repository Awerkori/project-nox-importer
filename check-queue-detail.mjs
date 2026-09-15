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
  const { data: queued } = await supabase.from('importer_queue').select('status, task_type').limit(10000);
  const counts = {};
  for(const j of queued) {
      const k = `${j.task_type} - ${j.status}`;
      counts[k] = (counts[k] || 0) + 1;
  }
  console.log('Sample counts:', counts);
  
  const { count: totalQueued } = await supabase.from('importer_queue').select('*', {count: 'exact', head: true}).eq('status', 'QUEUED');
  console.log('Total QUEUED:', totalQueued);
}
run();
