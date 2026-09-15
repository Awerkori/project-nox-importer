import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';
dotenv.config();
const sb = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY);
async function run() {
  const { count } = await sb.from('importer_queue').select('*', { count: 'estimated', head: true }).eq('status', 'QUEUED');
  const { data } = await sb.from('importer_queue').select('id, status, next_run_at').eq('status', 'QUEUED').limit(5);
  console.log('Queued Estimated:', count);
  console.log('Sample Queued:', data);
}
run();
