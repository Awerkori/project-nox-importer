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
  const { data, error } = await supabase.from('tags').select('*').limit(10);
  console.log('Tags sample:', data);
  const { data: cols } = await supabase.rpc('get_columns_for_table', { p_table_name: 'tags' }).catch(() => ({data: 'no rpc'}));
  console.log('Columns:', cols);
}
run();
