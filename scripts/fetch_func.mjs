import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';
dotenv.config();

const url = process.env.SUPABASE_URL;
const key = process.env.SUPABASE_SERVICE_ROLE_KEY;
const sb = createClient(url, key);

async function main() {
  const { data: proc } = await sb.rpc('exec_sql', { sql: `SELECT prosrc FROM pg_proc WHERE proname = 'importer_acquire_job'` });
  console.log(proc[0].prosrc);
}
main().catch(console.error);
