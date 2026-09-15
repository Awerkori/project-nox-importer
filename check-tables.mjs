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
  try {
     const { data } = await supabase.from('chapter_pages').select('*').limit(1);
     console.log('chapter_pages:', Object.keys(data[0]||{}));
  } catch (e) {}
  try {
     const { data } = await supabase.from('media').select('*').limit(1);
     console.log('media:', Object.keys(data[0]||{}));
  } catch (e) {}
}
run();
