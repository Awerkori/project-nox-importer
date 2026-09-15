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
  const { data: job } = await supabase.from('importer_queue').select('id, next_run_at, status').eq('status', 'QUEUED').limit(1);
  console.log('Next run at:', job[0]?.next_run_at, 'Current time:', new Date().toISOString());
  
  const { data: metrics } = await supabase.from('importer_telemetry').select('*').order('created_at', {ascending: false}).limit(1);
  console.log('Telemetry:', metrics[0]);
}
run();
