import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';
dotenv.config();
const sb = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY);
async function run() {
  await sb.from('importer_queue').update({ status: 'RETRY', locked_by: null, locked_at: null, next_run_at: new Date(Date.now() + 30*60*1000).toISOString() }).eq('status', 'IMPORTING').eq('source', 'manhastro');
  console.log("Cleared stuck manhastro jobs.");
}
run();
