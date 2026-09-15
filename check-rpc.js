import { createClient } from '@supabase/supabase-js';
import fs from 'fs';
const envRaw = fs.readFileSync('/home/awerkori/.Projects/project-nox-importer/.env', 'utf8');
const env = {};
for (const line of envRaw.split('\n')) { 
  const m = line.match(/^([A-Z_]+)=(.*)$/); 
  if (m) env[m[1]] = m[2].trim(); 
}
const supabase = createClient(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY);

async function run() {
  const { data, error } = await supabase.from('schema_migrations').select('version').order('version', { ascending: false }).limit(5);
  console.log('Migrations:', data);
}
run();
