import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';
dotenv.config();
const sb = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY);
async function run() {
  const { data, error } = await sb.rpc('exec_sql', { query: "SELECT pid, state, query, wait_event_type, wait_event FROM pg_stat_activity WHERE state != 'idle' AND query NOT LIKE '%pg_stat_activity%';" });
  console.log('Error:', error);
  console.log('Data:', data);
}
run();
