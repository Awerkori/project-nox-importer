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
  const q1 = await supabase.rpc('explain_query', { query: `EXPLAIN ANALYZE SELECT id, title, slug, author, aliases, synopsis, kind FROM works WHERE slug IN ('magic-emperor', 'imperador-magico')` }).catch(() => null);
  const q2 = await supabase.rpc('explain_query', { query: `EXPLAIN ANALYZE SELECT id, title, slug, author, aliases, synopsis, kind FROM works WHERE aliases && ARRAY['Magic Emperor', 'Imperador Mágico']` }).catch(() => null);
  
  if (!q1) {
    console.log("No explain_query rpc available. Let's just trust PostgREST indexes. We know 'slug' has a UNIQUE index, so slug IN is fast. 'aliases' is a JSONB or Text Array.");
  }
}
run();
