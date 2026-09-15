import { createClient } from '@supabase/supabase-js';
import { readFileSync } from 'fs';
const env = Object.fromEntries(readFileSync('.env', 'utf-8').split('\n').filter(l => l.includes('=') && !l.startsWith('#')).map(l => l.split('=')));
const sb = createClient(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY);
async function run() {
  const m = { work_id: '04626908-f556-4380-a601-8cc7e21fb751', id: '97575643-60be-415d-a5d5-647360d62dd9', chapter_sort_key: 48 };
  const targetSortKey = 49;
  
  // Set job status to COMPLETED temporarily to bypass the queue check
  await sb.from('importer_queue').update({ status: 'COMPLETED' }).eq('payload->>workId', m.work_id).eq('chapter_sort_key', 48);
  
  // Test with is_gap = true
  await sb.from('importer_chapter_mappings').update({ is_gap: true }).eq('id', m.id);
  const { data: rpc1 } = await sb.rpc('importer_check_publication_barrier', {
    p_work_id: m.work_id,
    p_target_sort_key: targetSortKey
  });
  console.log("Barrier check with is_gap=true AND no active job:", rpc1);
  
  // Test with is_gap = false
  await sb.from('importer_chapter_mappings').update({ is_gap: false }).eq('id', m.id);
  const { data: rpc2 } = await sb.rpc('importer_check_publication_barrier', {
    p_work_id: m.work_id,
    p_target_sort_key: targetSortKey
  });
  console.log("Barrier check with is_gap=false AND no active job:", rpc2);
  
  // Restore state
  await sb.from('importer_queue').update({ status: 'QUEUED' }).eq('payload->>workId', m.work_id).eq('chapter_sort_key', 48);
  await sb.from('importer_chapter_mappings').update({ is_gap: true }).eq('id', m.id);
}
run();
