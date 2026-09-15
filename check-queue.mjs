import { createClient } from '@supabase/supabase-js';
import { readFileSync } from 'fs';
const envRaw = readFileSync('/home/awerkori/.Projects/project-nox-importer/.env', 'utf8');
const env = {};
for (const line of envRaw.split('\n')) { 
  const m = line.match(/^([A-Z_]+)=(.*)$/); 
  if (m) env[m[1]] = m[2].trim(); 
}
const s = createClient(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY);

const { count, error } = await s.from('importer_queue').select('*', { count: 'exact', head: true }).eq('status', 'QUEUED');
console.log('Total QUEUED jobs:', count);
