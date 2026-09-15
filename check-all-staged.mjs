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
  const { data: staged } = await supabase.from('importer_chapter_mappings').select('work_id, chapter_sort_key').eq('status', 'STAGED');
  const distinct = [...new Set(staged.map(s => s.work_id))];
  
  let canPublishCount = 0;
  for (const wid of distinct) {
    const minStagedKey = Math.min(...staged.filter(s => s.work_id === wid).map(s => s.chapter_sort_key));
    const { data } = await supabase.rpc('importer_check_publication_barrier', {
      p_work_id: wid,
      p_target_sort_key: minStagedKey
    });
    if (data && data.length > 0 && data[0].can_publish) {
      console.log(`Work ${wid} CAN PUBLISH chapter ${minStagedKey}`);
      canPublishCount++;
    } else {
      console.log(`Work ${wid} BLOCKED at ${minStagedKey}:`, data[0].reason);
    }
  }
  console.log('Total works that can publish:', canPublishCount);
}
run();
