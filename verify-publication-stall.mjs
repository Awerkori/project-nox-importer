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
  const { data: lastChap } = await supabase.from('chapters').select('published_at, work_id').not('published_at', 'is', null).order('published_at', {ascending: false}).limit(5);
  
  console.log("Recent Publications:");
  for (const c of (lastChap || [])) {
     console.log(`- ${c.published_at} (Work: ${c.work_id})`);
  }
}
run();
