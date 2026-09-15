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
  console.log("Acquiring 5 jobs...");
  const works = [];
  for (let i = 0; i < 5; i++) {
     const { data, error } = await supabase.rpc('importer_acquire_job', { p_worker_id: 'test-fairness-worker', p_lease_duration: '1 minute' });
     if (error) {
       console.error("RPC Error:", error);
       return;
     }
     if (data && data.length > 0) {
        works.push(data[0].payload.workId);
     } else {
        works.push('NONE');
     }
  }
  console.log("Acquired works in order:");
  console.log(works);
}
run();
