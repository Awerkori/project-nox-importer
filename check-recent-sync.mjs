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
  const { data: recentJobs } = await supabase.from('importer_queue')
    .select('id, updated_at, payload, task_type')
    .eq('status', 'COMPLETED')
    .order('updated_at', { ascending: false })
    .limit(5);
    
  for (const j of recentJobs) {
     console.log(`Job ${j.id} [${j.task_type}] updated at ${j.updated_at} for Work ${j.payload.workId}`);
     const { data: tags } = await supabase.from('work_tags').select('tags(name)').eq('work_id', j.payload.workId);
     console.log('  Tags:', tags?.map(t => t.tags?.name));
  }
}
run();
