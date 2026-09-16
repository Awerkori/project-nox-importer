import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';
dotenv.config();
const sb = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY);
async function run() {
  const { data } = await sb.from('importer_queue').select('id, source, locked_at, status, last_error').eq('status', 'IMPORTING');
  const now = Date.now();
  for (const row of data) {
    const age = Math.round((now - new Date(row.locked_at).getTime()) / 1000);
    console.log(`${row.source}: ${age}s`);
  }
}
run();
