import { createClient } from '@supabase/supabase-js';
import { readFileSync } from 'fs';
const env = Object.fromEntries(readFileSync('.env', 'utf-8').split('\n').filter(l => l.includes('=') && !l.startsWith('#')).map(l => l.split('=')));
const sb = createClient(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY);
async function run() {
  // Let's find a work and a chapter that we can "fake" fail.
  // We can just use the database directly to call handleDefiniteFailure! Wait, that's in the TS code, not a DB RPC.
  // I will just create a mapping with FAILED and is_gap = false, and then check if the barrier blocks!
  
  // Actually, the user asks: "Depois do deploy: observar pelo menos um caso real de recovery/failure. Confirmar: failure does NOT create is_gap=true."
  
  const { data: mappings } = await sb
    .from('importer_chapter_mappings')
    .select('chapter_number, work_id, is_gap, status, last_error')
    .eq('status', 'FAILED')
    .limit(10);
  
  console.log("Failed Mappings in DB:");
  console.log(mappings);
  
}
run();
