import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';
dotenv.config();
const sb = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY);
const sql = `
CREATE OR REPLACE FUNCTION importer_acquire_job(p_worker_id text, p_lease_duration interval, p_source text DEFAULT NULL::text, p_task_type text DEFAULT NULL::text) RETURNS TABLE(id uuid, task_type text, source text, priority integer, payload jsonb, dedupe_key text, status text, attempts integer, max_attempts integer, locked_by text, locked_at timestamp with time zone, lease_expires_at timestamp with time zone, next_run_at timestamp with time zone, last_error text, chapter_sort_key numeric)
    LANGUAGE plpgsql
    AS $$
BEGIN
  RETURN;
END;
$$;
`;
async function run() {
  const { data, error } = await sb.rpc('exec_sql', { query: sql });
  console.log('Error:', error);
  console.log('Data:', data);
}
run();
