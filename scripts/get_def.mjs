import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';
dotenv.config();

const url = process.env.SUPABASE_URL;
const key = process.env.SUPABASE_SERVICE_ROLE_KEY;
const sb = createClient(url, key);

async function main() {
  const { data, error } = await sb.rpc('admin_get_system_health');
  
  // Can we create a temp function to get the definition?
  const res = await fetch(`${url}/rest/v1/rpc/exec_sql`, {
    method: 'POST',
    headers: { 'apikey': key, 'Authorization': `Bearer ${key}`, 'Content-Type': 'application/json' },
    body: JSON.stringify({ sql: "SELECT prosrc FROM pg_proc WHERE proname = 'importer_acquire_job'" })
  });
  const text = await res.text();
  console.log(text);
}
main().catch(console.error);
