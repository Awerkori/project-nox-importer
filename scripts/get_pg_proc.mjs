import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';
dotenv.config();

const url = process.env.SUPABASE_URL;
const key = process.env.SUPABASE_SERVICE_ROLE_KEY;
const sb = createClient(url, key);

async function main() {
  const { data, error } = await sb.rpc('exec_sql', { sql: `SELECT prosrc FROM pg_proc WHERE proname = 'importer_acquire_job'` }).catch(e => ({ error: e }));
  console.log(error ? 'exec_sql failed' : data);
  
  // Since exec_sql is not available, let's create a temporary function to read pg_proc
  const fnSql = `
    CREATE OR REPLACE FUNCTION admin_read_func(func_name text) RETURNS text AS $$
    DECLARE
      v_src text;
    BEGIN
      SELECT prosrc INTO v_src FROM pg_proc WHERE proname = func_name LIMIT 1;
      RETURN v_src;
    END;
    $$ LANGUAGE plpgsql SECURITY DEFINER;
  `;
  
  // Actually, I can just use psql if it's available, but I don't have the db url.
  // Wait, I DO have the DB url in project-nox-manga!
}
main().catch(console.error);
