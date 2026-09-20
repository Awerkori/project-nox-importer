import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';
dotenv.config();
const sb = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY);
async function run() {
  const { data, error } = await sb.rpc('exec_sql', { query: "SELECT prosrc FROM pg_proc WHERE proname = 'importer_acquire_job';" });
  console.log(data?.[0]?.prosrc?.includes('staged_works') ? "FIX APPLIED!" : "NOT APPLIED");
}
run();
