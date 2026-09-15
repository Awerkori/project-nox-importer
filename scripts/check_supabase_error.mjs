import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';
dotenv.config();
const sb = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY);
async function run() {
  const { data, error, count } = await sb.from('importer_queue').select('*', { count: 'exact', head: true }).eq('status', 'QUEUED');
  console.log('Error:', error);
  console.log('Count:', count);
}
run();
