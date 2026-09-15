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
  const { data: q } = await supabase.from('importer_queue').select('task_type, status, priority').limit(5000);
  
  const types = {};
  for (const job of q) {
      const k = `${job.task_type} - ${job.status}`;
      types[k] = (types[k] || 0) + 1;
  }
  console.log(types);
}
run();
