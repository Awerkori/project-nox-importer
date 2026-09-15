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
  const { data: q } = await supabase.from('importer_telemetry').select('*').order('created_at', {ascending: false}).limit(5);
  for (const t of q) {
      console.log(`[${t.created_at}] Concurrency: ${t.concurrency}, Active: ${t.active_jobs}, Reason: ${t.cycle_reason}`);
  }
}
run();
