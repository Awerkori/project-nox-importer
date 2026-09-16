import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';
dotenv.config();
const sb = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY);
async function run() {
  const { data } = await sb.from('importer_telemetry').select('last_heartbeat_at, active_jobs, worker_id, status').order('last_heartbeat_at', { ascending: false }).limit(5);
  console.log(data);
}
run();
