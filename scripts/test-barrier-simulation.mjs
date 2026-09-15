import { createClient } from '@supabase/supabase-js';
import { readFileSync } from 'fs';
const env = Object.fromEntries(readFileSync('.env', 'utf-8').split('\n').filter(l => l.includes('=') && !l.startsWith('#')).map(l => l.split('=')));
const sb = createClient(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY);
async function run() {
  // We'll update the first failed mapping in the db to have is_gap = false
  const { data: mappings } = await sb
    .from('importer_chapter_mappings')
    .select('id, chapter_number, chapter_sort_key, work_id, is_gap')
    .eq('status', 'FAILED')
    .eq('is_gap', true)
    .limit(1);
    
  if (mappings.length > 0) {
    const m = mappings[0];
    console.log("Found failed mapping currently set to is_gap = true:", m);
    
    // Now let's test check_publication_barrier RPC if we try to publish chapter_sort_key + 1
    const targetSortKey = Number(m.chapter_sort_key) + 1;
    
    // First, with is_gap = true, the barrier should allow it (if no other blocking elements)
    const { data: rpc1 } = await sb.rpc('importer_check_publication_barrier', {
      p_work_id: m.work_id,
      p_target_sort_key: targetSortKey
    });
    console.log("Barrier check with is_gap=true:", rpc1);
    
    // Now set it to is_gap = false (simulating my new code)
    await sb.from('importer_chapter_mappings').update({ is_gap: false }).eq('id', m.id);
    
    // Check barrier again
    const { data: rpc2 } = await sb.rpc('importer_check_publication_barrier', {
      p_work_id: m.work_id,
      p_target_sort_key: targetSortKey
    });
    console.log("Barrier check with is_gap=false:", rpc2);
    
    // Reset it back
    await sb.from('importer_chapter_mappings').update({ is_gap: true }).eq('id', m.id);
  }
}
run();
