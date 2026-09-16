import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';
dotenv.config();

const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY);

async function runTests() {
  console.log("Running simple DB query (select count from works)...");
  let start = Date.now();
  let res = await supabase.from('works').select('id', { count: 'exact', head: true });
  console.log(`DB SELECT Time: ${Date.now() - start}ms | Error: ${res.error?.message}`);

  console.log("Running simple HTTP PostgREST (health check)...");
  start = Date.now();
  let httpRes = await fetch(`${process.env.SUPABASE_URL}/rest/v1/`, { headers: { apikey: process.env.SUPABASE_ANON_KEY }});
  console.log(`HTTP Time: ${Date.now() - start}ms | Status: ${httpRes.status}`);
  
  console.log("Checking active importer workers...");
  start = Date.now();
  let resWorkers = await supabase.from('importer_jobs').select('id, status, worker_id, updated_at').eq('status', 'processing');
  console.log(`Workers processing: ${resWorkers.data?.length || 0}`);
}
runTests();
