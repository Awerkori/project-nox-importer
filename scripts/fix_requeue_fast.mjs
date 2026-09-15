import { createClient } from '@supabase/supabase-js';
import { readFileSync } from 'fs';
const env = Object.fromEntries(readFileSync('.env', 'utf-8').split('\n').filter(l => l.includes('=') && !l.startsWith('#')).map(l => { const i = l.indexOf('='); return [l.slice(0,i), l.slice(i+1)]; }));
const sb = createClient(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY);

async function run() {
  const { data: q } = await sb.from('importer_queue').select('id, payload, chapter_sort_key, source').like('dedupe_key', 'requeue:%');
  if (!q) return;
  console.log(`Found ${q.length} requeued jobs`);
  
  let fixed = 0;
  for (const job of q) {
    if (!job.payload.sourceChapterId) {
      const { data: m } = await sb.from('importer_chapter_mappings')
        .select('chapter_number, source_chapter_id, work_mapping_id')
        .eq('work_id', job.payload.workId)
        .eq('chapter_sort_key', job.chapter_sort_key)
        .eq('source', job.source)
        .single();
      
      if (m) {
        const newPayload = {
          ...job.payload,
          chapterNumber: m.chapter_number,
          sourceChapterId: m.source_chapter_id,
          workMappingId: m.work_mapping_id
        };
        await sb.from('importer_queue').update({ payload: newPayload }).eq('id', job.id);
        fixed++;
      }
    }
  }
  console.log(`Fixed ${fixed} payloads.`);
}
run();
