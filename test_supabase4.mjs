import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';
dotenv.config();

const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY);

async function runTests() {
  let start = Date.now();
  let resRpc = await supabase.rpc('importer_acquire_job', { 
    p_worker_id: 'test-diagnostic',
    p_lease_duration: '5 minutes',
    p_source: null,
    p_task_type: null
  });
  console.log(`RPC acquire_job Time: ${Date.now() - start}ms | Error: ${resRpc.error?.message}`);
}
runTests();
