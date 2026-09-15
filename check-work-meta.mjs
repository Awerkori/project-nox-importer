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
  const workId = 'e1b4bce1-5fbd-4fdf-8930-ca4afc26771d';
  const { data: mappings } = await supabase.from('importer_work_mappings').select('source, metadata').eq('work_id', workId);
  console.log(mappings);
}
run();
