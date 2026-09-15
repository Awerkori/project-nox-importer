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
  const { data } = await supabase.from('importer_chapter_mappings')
    .select('work_id, chapter_sort_key, is_gap')
    .eq('status', 'STAGED');
    
  const byWork = {};
  for (const row of (data||[])) {
    if (!byWork[row.work_id]) byWork[row.work_id] = [];
    byWork[row.work_id].push(row);
  }
  
  for (const workId in byWork) {
    const chapters = byWork[workId].sort((a,b) => a.chapter_sort_key - b.chapter_sort_key);
    // find first gap
    const firstGapIdx = chapters.findIndex(c => c.is_gap);
    const ready = firstGapIdx === -1 ? chapters.length : firstGapIdx;
    if (ready > 0) {
      console.log(`Work ${workId}: ${ready} ready chapters (First gap at idx ${firstGapIdx})`);
    }
  }
}
run();
