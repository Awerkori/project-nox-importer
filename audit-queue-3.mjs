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
  const { data: jobs } = await supabase.from('importer_queue')
    .select('id, payload')
    .eq('task_type', 'IMPORT_CHAPTER')
    .eq('status', 'QUEUED')
    .limit(10);
    
  console.log(JSON.stringify(jobs, null, 2));
}
run();
