import { createClient } from '@supabase/supabase-js';
import { readFileSync } from 'fs';
const envRaw = readFileSync('/home/awerkori/.Projects/project-nox-importer/.env', 'utf8');
const env = {};
for (const line of envRaw.split('\n')) { 
  const m = line.match(/^([A-Z_]+)=(.*)$/); 
  if (m) env[m[1]] = m[2].trim(); 
}
const s = createClient(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY);

const { data: row } = await s.from('works').select('aliases, kind, author').limit(1);
console.log('works aliases type:', typeof row[0]?.aliases, Array.isArray(row[0]?.aliases) ? 'Array' : 'Not Array', row[0]?.aliases);
