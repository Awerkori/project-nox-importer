import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';
dotenv.config();
const sb = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY);
async function run() {
  const { data } = await sb.from('importer_telemetry').select('created_at, concurrency, active_jobs').order('created_at', { ascending: false }).limit(3);
  console.log(data);
}
run();
