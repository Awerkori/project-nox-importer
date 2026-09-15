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
  const { data } = await supabase.from('importer_queue')
    .select('id, chapter_sort_key, status, next_run_at')
    .eq('payload->>workId', '4b1f452c-b223-404c-83f4-e0d626563397')
    .lt('chapter_sort_key', 803)
    .in('status', ['QUEUED', 'RETRY'])
    .lte('next_run_at', new Date().toISOString())
    .limit(10);
  console.log('Valid recovery candidates:', data);
}
run();
