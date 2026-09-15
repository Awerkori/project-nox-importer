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
  const { data: cancelled } = await supabase.from('importer_queue').select('id, payload, chapter_sort_key').eq('status', 'CANCELLED_BY_STAFF').eq('task_type', 'IMPORT_CHAPTER');
  console.log(`Found ${cancelled.length} CANCELLED_BY_STAFF chapter jobs.`);
  
  const blocks = [];
  for (const job of cancelled) {
    const workId = job.payload.workId;
    const sortKey = job.chapter_sort_key || job.payload.chapterNumber;
    
    // Check if this work has STAGED chapters after this one
    const { count } = await supabase.from('importer_chapter_mappings')
      .select('id', {count: 'exact', head: true})
      .eq('work_id', workId)
      .eq('status', 'STAGED')
      .gt('chapter_sort_key', sortKey);
      
    if (count > 0) {
      blocks.push({ workId, sortKey, blockedStagedCount: count });
    }
  }
  
  console.log("Blocking Cancellations:");
  console.table(blocks);
}
run();
