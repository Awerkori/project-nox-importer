import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';
dotenv.config();
const sb = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY);
async function run() {
  const { error } = await sb.from('importer_sources').update({ status: 'UPSTREAM_BLOCKED' }).eq('id', 'manhastro');
  console.log(error || 'Blocked manhastro!');
  
  // also release the stuck jobs so they can be picked up by fallback sources!
  const { error: e2 } = await sb.from('importer_queue').update({ status: 'QUEUED', attempts: 0, locked_by: null, locked_at: null }).eq('source', 'manhastro').eq('status', 'IMPORTING');
  console.log(e2 || 'Released manhastro stuck jobs!');
}
run();
