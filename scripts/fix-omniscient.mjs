import { createClient } from '@supabase/supabase-js';
import { readFileSync } from 'fs';
const env = Object.fromEntries(readFileSync('.env', 'utf-8').split('\n').filter(l => l.includes('=') && !l.startsWith('#')).map(l => l.split('=')));
const sb = createClient(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY);
async function run() {
  const { data: w } = await sb.from('works').select('*').ilike('title', '%Omniscient%').single();
  if (w) {
    console.log("Current status:", w.status, "kind:", w.kind);
    // Let's reset kind and status to UNKNOWN manually if they were defaults without provenance
    await sb.from('works').update({
       kind: 'UNKNOWN',
       status: 'UNKNOWN'
    }).eq('id', w.id);
    console.log("Updated to UNKNOWN to allow next sync to pull true values.");
  }
}
run();
