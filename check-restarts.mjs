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
  const { data } = await supabase
    .from('importer_telemetry')
    .select('*')
    .eq('worker_id', 'discloud-importer-1')
    .order('created_at', { ascending: false })
    .limit(120); // last hour
  
  // count actual restarts by checking if cycle_reason resets to '1/3' WITHOUT being just a normal row id change
  // actually, let's just look at the created_at gaps or memory drops
  const cycleStarts = data.filter(r => r.cycle_reason.includes('(1/3 cycles'));
  console.log("Telemetry rows:", data.length);
  console.log("Memory range:", Math.min(...data.map(d=>d.rss_mb)), "-", Math.max(...data.map(d=>d.rss_mb)));
  console.log("Concurrency range:", Math.min(...data.map(d=>d.concurrency)), "-", Math.max(...data.map(d=>d.concurrency)));
  console.log("Event loop lag max:", Math.max(...data.map(d=>d.event_loop_lag_ms)));
}
run();
