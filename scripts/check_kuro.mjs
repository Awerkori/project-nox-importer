import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';
dotenv.config();
const sb = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY);
async function run() {
  const { data } = await sb.from('importer_queue').select('id, status, priority, next_run_at, source').eq('source', 'kuro').in('status', ['QUEUED', 'RETRY']).order('priority', { ascending: false }).limit(5);
  console.log('Kuro jobs:', data);
}
run();
