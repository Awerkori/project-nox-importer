import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';
dotenv.config();

const sb = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY);

async function check() {
  const { data, error } = await sb.from('pg_proc').select('proname, prosrc').eq('proname', 'importer_acquire_job').limit(1);
  if (error) { console.log(error); } else { console.log(data?.[0]?.prosrc); }
}
check();
