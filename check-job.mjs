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
  const { data: job } = await supabase.from('importer_queue').select('*').eq('id', '93a05d8b-1321-403a-8d49-b72daca6b09e').single();
  console.log(job);
  
  const { data: tags } = await supabase.from('work_tags').select('tags(name)').eq('work_id', job.payload.workId);
  console.log('Tags after a few more seconds:', tags?.map(t => t.tags?.name));
}
run();
