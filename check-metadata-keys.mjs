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
  const { data: mappings } = await supabase.from('importer_work_mappings').select('metadata').not('metadata', 'is', null).limit(100);
  const keys = new Set();
  for (const m of mappings) {
    if (m.metadata) {
       for (const k of Object.keys(m.metadata)) {
          if (k.toLowerCase().includes('genr') || k.toLowerCase().includes('tag') || k.toLowerCase().includes('cat')) keys.add(k);
       }
    }
  }
  console.log('Metadata tag keys:', Array.from(keys));
}
run();
