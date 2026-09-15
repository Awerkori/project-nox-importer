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
  const { data } = await supabase.from('importer_queue').select('id, status, attempts, created_at, locked_at, task_type').eq('status', 'IMPORTING');
  console.log('IMPORTING jobs:', data.length);
  console.log(data);
  
  const { data: staged } = await supabase.from('importer_chapter_mappings').select('*', {count: 'exact', head: true}).eq('status', 'STAGED');
  console.log('STAGED Mappings:', staged);
}
run();
