import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';
dotenv.config();
const sb = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY);
async function run() {
  const { data } = await sb.from('importer_queue').select('id, status, locked_by, locked_at').eq('status', 'IMPORTING');
  console.log('Total Importing:', data ? data.length : 0);
  console.log('Sample:', data?.slice(0, 5));
}
run();
