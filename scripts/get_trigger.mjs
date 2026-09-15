import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';
dotenv.config();

const url = process.env.SUPABASE_URL;
const key = process.env.SUPABASE_SERVICE_ROLE_KEY;
const sb = createClient(url, key);

async function main() {
  const { data, error } = await sb.rpc('execute_sql_query', { query: "SELECT tgname, pg_get_triggerdef(oid) FROM pg_trigger WHERE tgname LIKE '%latest_chapter%'" });
  console.log(data, error);
}
main().catch(console.error);
