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
  console.log('Checking recent completed jobs...');
  // We want to see if any recent job triggered a tag update.
  const { data: recentJobs } = await supabase.from('importer_queue')
    .select('*')
    .eq('status', 'COMPLETED')
    .order('updated_at', { ascending: false })
    .limit(5);
    
  if (recentJobs && recentJobs.length > 0) {
     console.log('Process is ONLINE and completing jobs!');
     console.log(`Last job updated at: ${recentJobs[0].updated_at}`);
  } else {
     console.log('No recent completed jobs found yet...');
  }
  
  // To prove that the new tag logic works on a REAL job, we can look at the latest work_tags added.
  const { data: recentTags } = await supabase.from('work_tags')
    .select('work_id, tag_id, created_at, tags(name, slug), works(title, kind)')
    .order('created_at', { ascending: false })
    .limit(5);
    
  console.log('\n--- RECENT TAGS CREATED/LINKED IN PROD ---');
  for (const wt of (recentTags || [])) {
     console.log(`Work: ${wt.works?.title} | Kind: ${wt.works?.kind} | Tag: ${wt.tags?.name} (${wt.tags?.slug}) | Linked At: ${wt.created_at}`);
  }
}
run();
