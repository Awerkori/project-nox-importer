import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';
dotenv.config();
const sb = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY);
async function run() {
  const { data } = await sb.from('importer_queue').select('source, status, last_error, attempts').eq('task_type', 'IMPORT_CHAPTER').order('updated_at', { ascending: false }).limit(20);
  console.log(data);
}
run();
