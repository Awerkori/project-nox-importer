import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';
dotenv.config();
const sb = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY);
async function run() {
  const { data } = await sb.from('importer_queue').select('id, source, status, updated_at').eq('status', 'COMPLETED').order('updated_at', { ascending: false }).limit(5);
  console.log(data);
}
run();
