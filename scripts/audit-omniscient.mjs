import { createClient } from '@supabase/supabase-js';
import { readFileSync } from 'fs';
const env = Object.fromEntries(readFileSync('.env', 'utf-8').split('\n').filter(l => l.includes('=') && !l.startsWith('#')).map(l => l.split('=')));
const sb = createClient(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY);
async function run() {
  const { data } = await sb.from('works').select('id, title, slug, kind, status').ilike('title', '%Omniscient%');
  console.log(data);
  for (const w of data) {
    const { data: ch } = await sb.from('chapters').select('number').eq('work_id', w.id).order('number', { ascending: true });
    console.log(`Chapters for ${w.title}:`, ch.map(c => c.number));
    
    // Check if we should update it back to UNKNOWN or MANHWA.
    // If it's Omniscient Reader's Viewpoint, the correct type is Manhwa (Korean) and Status is Ongoing.
    // I can query the provenance.
    const { data: wprov } = await sb.from('works').select('metadata_provenance').eq('id', w.id).single();
    console.log(`Provenance for ${w.title}:`, wprov);
  }
}
run();
