import { createClient } from '@supabase/supabase-js';
import { readFileSync } from 'fs';

const envRaw = readFileSync('/home/awerkori/.Projects/project-nox-importer/.env', 'utf8');
const env: any = {};
for (const line of envRaw.split('\n')) { 
  const m = line.match(/^([A-Z_]+)=(.*)$/); 
  if (m) env[m[1]] = m[2].trim(); 
}
const supabase = createClient(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY);

async function runTest() {
  const targetIds = ['4b1f452c-b223-404c-83f4-e0d626563397']; // Imperador Mágico

  const { data: jobs } = await supabase.from('importer_queue')
    .select('*')
    .eq('task_type', 'IMPORT_CHAPTER')
    .eq('status', 'QUEUED')
    .in('payload->>workId', targetIds)
    .limit(3);

  if (!jobs || jobs.length === 0) { console.log('No jobs for Imperador Mágico.'); return; }
  
  for (const job of jobs) {
    console.log(`\nTesting job: ${job.id} for Work: ${job.payload.workId} Chapter: ${job.payload.chapterNumber}`);
    
    const { data: alreadyPub } = await supabase
      .from('chapters')
      .select('id, number, title')
      .eq('work_id', job.payload.workId)
      .eq('number', job.payload.chapterNumber)
      .not('published_at', 'is', null)
      .maybeSingle();
      
    if (alreadyPub) {
        console.log('Chapter already published by concurrent source, linking mapping and skipping duplicate download');
        console.log(`Page redownload: 0 / explain (Job skipped due to alreadyPub logic)`);
        console.log(`Canonical chapter reused: YES (${alreadyPub.id})`);
    } else {
        console.log('Chapter not found in canonical! Would download.');
    }
  }
}
runTest();
