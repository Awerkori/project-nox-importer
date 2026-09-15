import { createClient } from '@supabase/supabase-js';
import { readFileSync } from 'fs';
const env = Object.fromEntries(readFileSync('.env', 'utf-8').split('\n').filter(l => l.includes('=') && !l.startsWith('#')).map(l => l.split('=')));
const sb = createClient(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY);
async function run() {
  const { data } = await sb.from('importer_queue').select('id, status, chapter_sort_key').eq('payload->>workId', '04626908-f556-4380-a601-8cc7e21fb751').eq('chapter_sort_key', 48);
  console.log(data);
}
run();
