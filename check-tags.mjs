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
  const { data, error } = await supabase.from('tags').select('*').limit(200);
  console.log('Total tags:', data.length);
  for (const t of data) {
    if (['yaoi', 'yuri', 'hentai', 'adulto', '+18', 'bl', 'gl', 'manhwa', 'pornhwa'].includes(t.slug)) {
       console.log('Found:', t);
    }
  }
}
run();
