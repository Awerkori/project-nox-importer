import { createClient } from '@supabase/supabase-js';
import { readFileSync } from 'fs';
const env = Object.fromEntries(readFileSync('.env', 'utf-8').split('\n').filter(l => l.includes('=') && !l.startsWith('#')).map(l => l.split('=')));
const sb = createClient(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY);
async function run() {
  const { data: w } = await sb.from('works').select('id, slug').ilike('title', '%Omniscient%').single();
  const { data: mappings } = await sb.from('importer_work_mappings').select('source, source_work_id').eq('work_id', w.id);
  console.log("Mappings:", mappings);
  
  for (const m of mappings) {
    const dedupeKey = `${m.source}:work:${m.source_work_id}`;
    await sb.from('importer_queue').upsert({
       task_type: 'SYNC_WORK',
       source: m.source,
       dedupe_key: dedupeKey,
       payload: { sourceWorkId: m.source_work_id, slug: w.slug },
       priority: 80,
       status: 'QUEUED'
    }, { onConflict: 'dedupe_key' });
    console.log("Enqueued SYNC_WORK for", m.source);
  }
}
run();
