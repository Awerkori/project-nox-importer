import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';
dotenv.config();

const sb = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY);

async function check() {
  console.log("Measuring importer_acquire_job...");
  const start = Date.now();
  const { data, error } = await sb.rpc('importer_acquire_job', { p_worker_id: 'test', p_lease_duration: '5 minutes' });
  const latency = Date.now() - start;
  console.log(`Latency: ${latency}ms`);
  if (error) console.log(error);
  
  if (data && data.length > 0) {
     // release the job immediately
     await sb.from('importer_queue').update({ status: 'RETRY', locked_by: null }).eq('id', data[0].id);
  }
}
check();
