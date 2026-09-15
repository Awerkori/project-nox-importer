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
  let missingCount = 0;
  let totalWorks = 0;
  let page = 0;
  while(true) {
    const { data: works, error } = await supabase.from('works').select('id, work_tags(tag_id)').range(page*1000, (page+1)*1000-1);
    if (!works || works.length === 0) break;
    totalWorks += works.length;
    for (const w of works) {
       if (!w.work_tags || w.work_tags.length === 0) missingCount++;
    }
    page++;
  }
  console.log(`Works total: ${totalWorks}. Works missing tags: ${missingCount}`);
}
run();
