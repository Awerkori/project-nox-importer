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
  const { data: importing } = await supabase.from('importer_queue').select('id, status, payload').eq('status', 'IMPORTING').limit(10);
  const { count: pending } = await supabase.from('importer_queue').select('*', {count: 'exact', head: true}).eq('status', 'QUEUED');
  const { data: lastCompleted } = await supabase.from('importer_queue_archive').select('completed_at').order('completed_at', {ascending: false}).limit(1);
  console.log(`Currently IMPORTING: ${importing?.length}`);
  for (const j of (importing || [])) console.log(` - Work: ${j.payload?.workId}`);
  console.log(`QUEUED count: ${pending}`);
  console.log(`Last Completed Job: ${lastCompleted?.[0]?.completed_at}`);
}
run();
