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
  const { data } = await supabase.from('importer_queue').select('id, task_type, chapter_sort_key').eq('task_type', 'IMPORT_CHAPTER').limit(100);
  const nullCount = data.filter(d => d.chapter_sort_key === null).length;
  console.log(`IMPORT_CHAPTER jobs with null chapter_sort_key: ${nullCount} / ${data.length}`);
}
run();
