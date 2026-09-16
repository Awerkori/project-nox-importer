import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';
dotenv.config();
const sb = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY);
async function run() {
  const thirtySecsAgo = new Date(Date.now() - 30000).toISOString();
  await sb.from('importer_queue').update({ status: 'RETRY', locked_by: null, locked_at: null, next_run_at: new Date(Date.now() + 1000).toISOString() }).eq('status', 'IMPORTING').lt('locked_at', thirtySecsAgo);
  console.log("Cleared stuck jobs.");
}
run();
