import { createClient } from '@supabase/supabase-js';
import { readFileSync } from 'fs';
const envRaw = readFileSync('/home/awerkori/.Projects/project-nox-importer/.env', 'utf8');
const env = {};
for (const line of envRaw.split('\n')) { 
  const m = line.match(/^([A-Z_]+)=(.*)$/); 
  if (m) env[m[1]] = m[2].trim(); 
}
const s = createClient(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY);

const candidateTitles = ['Magic Emperor'];
const candidateSlugs = ['imperador-magico'];

const [res1, res2] = await Promise.all([
  s.from('works').select('id, title').in('slug', candidateSlugs),
  s.from('works').select('id, title').overlaps('aliases', candidateTitles)
]);

console.log('bySlug:', res1.data);
console.log('byAlias:', res2.data);
