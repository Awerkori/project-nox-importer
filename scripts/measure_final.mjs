import { createClient } from '@supabase/supabase-js';
import { readFileSync } from 'fs';
const env = Object.fromEntries(readFileSync('.env', 'utf-8').split('\n').filter(l => l.includes('=') && !l.startsWith('#')).map(l => { const i = l.indexOf('='); return [l.slice(0,i), l.slice(i+1)]; }));
const sb = createClient(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY);

async function run() {
  const { data: mappings } = await sb.from('importer_chapter_mappings')
    .select('work_id, chapter_number, status, updated_at, works(title)')
    .eq('status', 'COMPLETED')
    .order('updated_at', { ascending: false })
    .limit(20);
    
  console.log("Recent COMPLETED mappings:");
  for (const m of mappings) {
    console.log(`- ${m.works?.title} ch ${m.chapter_number} at ${m.updated_at}`);
  }
}
run();
