import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';
dotenv.config();

const sb = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY);
async function run() {
  const { data, error } = await sb.rpc('exec_sql', { query: `EXPLAIN ANALYZE SELECT * FROM importer_acquire_job(1);` });
  if (error) console.error(error);
  else console.log(data);
}
run();
