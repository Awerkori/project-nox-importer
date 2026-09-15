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
  const { data } = await supabase.from('importer_chapter_mappings').select('status, is_gap').eq('work_id', '4b1f452c-b223-404c-83f4-e0d626563397').eq('chapter_sort_key', 383.1);
  console.log(data);
  const { data: q } = await supabase.from('importer_queue').select('status').eq('chapter_sort_key', 383.1).filter('payload->>workId', 'eq', '4b1f452c-b223-404c-83f4-e0d626563397');
  console.log(q);
}
run();
