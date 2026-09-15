import { createClient } from '@supabase/supabase-js';
import { readFileSync } from 'fs';
const envRaw = readFileSync('/home/awerkori/.Projects/project-nox-importer/.env', 'utf8');
const env = {};
for (const line of envRaw.split('\n')) { 
  const m = line.match(/^([A-Z_]+)=(.*)$/); 
  if (m) env[m[1]] = m[2].trim(); 
}
const s = createClient(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY);

const candidateTitles = ['Magic Emperor', 'Imperador Mágico', 'Demonic Emperor'];
const candidateSlugs = ['magic-emperor', 'imperador-magico', 'demonic-emperor'];

// Use ov (overlap) for aliases
// and ilike for title matching if possible, but title.in might be enough if we also check aliases.
// Wait, we can't do title.in easily case-insensitive. But `title.ilike` is supported in `.or()` but we can't do ilike ANY.
// Since we have candidateSlugs, slug.in is extremely powerful because slug is already normalized!

const { data, error } = await s.from('works')
  .select('id, title, slug, aliases, author, synopsis')
  .or(`slug.in.(${candidateSlugs.join(',')}),aliases.ov.{${candidateTitles.map(t => `"${t}"`).join(',')}}`)
  .limit(10);
  
console.log(error ? error : data);
