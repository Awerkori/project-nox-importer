import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';
dotenv.config();
const sb = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY);
async function run() {
  const { data } = await sb.from('importer_queue')
    .select('id, locked_at, lease_expires_at, attempts')
    .eq('status', 'IMPORTING')
    .lt('lease_expires_at', new Date().toISOString());
  console.log('Stale Jobs:', data.length);
  if (data.length > 0) console.log(data[0]);
}
run();
