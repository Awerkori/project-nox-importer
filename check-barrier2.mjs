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
  const { data: staged } = await supabase.from('importer_chapter_mappings').select('chapter_sort_key').eq('work_id', 'e74aa68d-149f-400f-b65f-c2535e04845b').eq('status', 'STAGED').order('chapter_sort_key', {ascending: true}).limit(1);
  console.log("Lowest STAGED:", staged);
  
  if (staged && staged.length > 0) {
    const { data } = await supabase.rpc('importer_check_publication_barrier', {
        p_work_id: 'e74aa68d-149f-400f-b65f-c2535e04845b',
        p_target_sort_key: staged[0].chapter_sort_key,
      });
    console.log("Barrier check:", data);
  }
}
run();
