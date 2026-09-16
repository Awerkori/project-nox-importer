import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';
dotenv.config();

const sb = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY);

async function check() {
  console.log("Creating index...");
  const { error } = await sb.rpc('exec_sql', { query: `CREATE INDEX IF NOT EXISTS idx_queue_workid ON public.importer_queue ((payload->>'workId')) WHERE status IN ('QUEUED', 'RETRY');` });
  console.log("Index error:", error);
}
check();
