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
  const { data: tags } = await supabase.from('tags').select('slug');
  const slugCounts = {};
  let dups = 0;
  for (const t of tags) {
     slugCounts[t.slug] = (slugCounts[t.slug] || 0) + 1;
     if (slugCounts[t.slug] > 1) dups++;
  }
  console.log(`Duplicate tags created: ${dups}`);
  
  // Check duplicate work_tags
  const { data: wtags } = await supabase.from('work_tags').select('work_id, tag_id');
  const wtagSet = new Set();
  let wDups = 0;
  for (const wt of wtags) {
     const k = `${wt.work_id}-${wt.tag_id}`;
     if (wtagSet.has(k)) wDups++;
     wtagSet.add(k);
  }
  console.log(`Duplicate work_tags links: ${wDups}`);
}
run();
