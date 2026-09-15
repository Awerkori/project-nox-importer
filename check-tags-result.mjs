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
  const { data: tags } = await supabase.from('tags').select('id, name, slug');
  
  const tagCounts = {};
  for (const t of tags) {
     const { count } = await supabase.from('work_tags').select('*', {count: 'exact', head: true}).eq('tag_id', t.id);
     tagCounts[t.slug] = count;
  }
  
  console.log('--- TAG COUNTS ---');
  ['yaoi', 'hentai', 'pornhwa', 'manhwa', 'manga', 'manhua', 'yuri'].forEach(s => {
      console.log(`${s.toUpperCase()}: ${tagCounts[s] || 0}`);
  });
  
  const createdTags = tags.filter(t => !['acao', 'aventura', 'fantasia', 'drama', 'romance', 'comedia', 'sobrenatural', 'escolar', 'psicologico', 'misterio'].includes(t.slug));
  console.log(`Other tags created: ${createdTags.length}`);
  
  let missingCount = 0;
  let page = 0;
  while(true) {
    const { data: works, error } = await supabase.from('works').select('id, work_tags(tag_id)').range(page*1000, (page+1)*1000-1);
    if (!works || works.length === 0) break;
    for (const w of works) {
       if (!w.work_tags || w.work_tags.length === 0) missingCount++;
    }
    page++;
  }
  console.log(`Works STILL missing tags: ${missingCount}`);
}
run();
