import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';
dotenv.config();
const sb = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY);
async function run() {
  const { data, error } = await sb.rpc('exec_sql', { 
    query: "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE query LIKE '%importer_acquire_job%' AND pid <> pg_backend_pid();" 
  });
  console.log('Error:', error);
  console.log('Data:', data);
}
run();
