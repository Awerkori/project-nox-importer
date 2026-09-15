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
  const { data: q } = await supabase.from('importer_chapter_mappings').select('work_id, chapter_number, status, is_gap').eq('status', 'STAGED');
  const counts = {};
  for(const c of q) counts[c.work_id] = (counts[c.work_id] || 0) + 1;
  console.log('Total STAGED mappings:', q.length);
  console.log('Work counts:', Object.values(counts).sort((a,b)=>b-a));
  
  const { data: blocked } = await supabase.from('importer_chapter_mappings').select('status, is_gap').eq('work_id', Object.keys(counts)[0] || null).neq('status', 'STAGED').neq('status', 'COMPLETED');
  console.log('First work blocked by (statuses):', blocked);
}
run();
