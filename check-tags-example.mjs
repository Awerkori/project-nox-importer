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
  const { data: tag } = await supabase.from('tags').select('id').eq('slug', 'yaoi').single();
  const { data: wt } = await supabase.from('work_tags').select('work_id, works(title)').eq('tag_id', tag.id).limit(1);
  const workId = wt[0].work_id;
  console.log(`Work: ${wt[0].works.title}`);
  
  const { data: mappings } = await supabase.from('importer_work_mappings').select('source, metadata').eq('work_id', workId);
  for (const m of mappings) {
     console.log(`Source: ${m.source} | Metadata:`, m.metadata?.genres || m.metadata?.generos || m.metadata?.categoria || m.metadata?.tags || 'Specialized Source Default applied');
  }
  
  const { data: currentTags } = await supabase.from('work_tags').select('tags(name)').eq('work_id', workId);
  console.log('Project Nox Tags:', currentTags.map(t => t.tags.name));
}
run();
