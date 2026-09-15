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
  const { data: cols } = await supabase.from('pages').select('*').limit(1).catch(() => ({data: []}));
  console.log(cols && cols.length ? Object.keys(cols[0]) : 'no pages table');
  
  const { data: media } = await supabase.from('chapter_media').select('*').limit(1).catch(() => ({data: []}));
  console.log(media && media.length ? Object.keys(media[0]) : 'no chapter_media table');
}
run();
