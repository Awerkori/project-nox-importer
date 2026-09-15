import { createClient } from '@supabase/supabase-js';
import { readFileSync } from 'fs';
const envRaw = readFileSync('/home/awerkori/.Projects/project-nox-importer/.env', 'utf8');
const env = {};
for (const line of envRaw.split('\n')) { 
  const m = line.match(/^([A-Z_]+)=(.*)$/); 
  if (m) env[m[1]] = m[2].trim(); 
}
const s = createClient(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY);

function safeArrayLiteral(arr) {
  return `{${arr.map(t => `"${t.replace(/"/g, '""')}"`).join(',')}}`;
}

const titles = ['Magic Emperor', 'weird "title", with comma'];
console.log(safeArrayLiteral(titles));

const { data, error } = await s.from('works')
  .select('id')
  .or(`aliases.ov.${safeArrayLiteral(titles)}`);
  
console.log(error ? error : data);
