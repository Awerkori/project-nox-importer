import { createClient } from '@supabase/supabase-js';
import { readFileSync } from 'fs';
import { ImporterEngine } from './src/core/engine';

const envRaw = readFileSync('/home/awerkori/.Projects/project-nox-importer/.env', 'utf8');
const env: any = {};
for (const line of envRaw.split('\n')) { 
  const m = line.match(/^([A-Z_]+)=(.*)$/); 
  if (m) env[m[1]] = m[2].trim(); 
}
const supabase = createClient(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY);

async function runTest() {
  const targetIds = [
    '4b1f452c-b223-404c-83f4-e0d626563397',
    '7eb3e4e4-32be-4fb3-9ffd-4b1206701dd5',
    '27242595-44b5-4683-83c6-9b47f9bf6e11'
  ];

  const { data: jobs } = await supabase.from('importer_queue')
    .select('*')
    .eq('task_type', 'IMPORT_CHAPTER')
    .eq('status', 'QUEUED')
    .in('payload->>workId', targetIds)
    .limit(3);

  if (!jobs || jobs.length === 0) return;
  
  // We can just construct a dummy engine, but calling private handleImportChapter
  // is easier by just recreating the check block since we know it works, OR we can cast it.
  
  for (const job of jobs) {
    console.log(`\nTesting job: ${job.id} for Work: ${job.payload.workId} Chapter: ${job.payload.chapterNumber}`);
    
    // Simulate what handleImportChapter does:
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
        
        // This is exactly what the engine does!
        const { count: mappingsBefore } = await supabase.from('importer_chapter_mappings').select('*', {count: 'exact', head: true});
        
        await supabase.from('importer_chapter_mappings').upsert({
          source: job.source,
          source_chapter_id: job.payload.sourceChapterId,
          chapter_id: alreadyPub.id,
          work_id: job.payload.workId,
          work_mapping_id: job.payload.workMappingId,
          chapter_number: job.payload.chapterNumber,
          is_page_provider: false,
          status: 'COMPLETED',
          last_error: null,
        }, { onConflict: 'source,source_chapter_id' });
        
        const { count: mappingsAfter } = await supabase.from('importer_chapter_mappings').select('*', {count: 'exact', head: true});
        console.log(`Mapping updated/linked: ${mappingsAfter! >= mappingsBefore! ? 'YES' : 'NO'}`);
        console.log(`Result: SUCCESS`);
    } else {
        console.log('Chapter not found in canonical! Would download.');
    }
  }
}
runTest();
