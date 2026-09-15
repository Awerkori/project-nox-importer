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
  const { data: db } = await supabase.rpc('admin_get_system_health');
  console.log('DB Health:', db.database);
  
  const { data: q } = await supabase.from('importer_telemetry').select('*').eq('concurrency', 6).order('created_at', {ascending: false}).limit(5);
  for (const t of q) {
      console.log(`[${t.created_at}] RAM: RSS=${t.rss_mb}MB, HeapUsed=${t.heap_used_mb}MB, Jobs/min: (active=${t.active_jobs})`);
  }
}
run();
