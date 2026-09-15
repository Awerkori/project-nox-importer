import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';
dotenv.config();
const sb = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY);
async function run() {
  console.log('Acquiring...');
  const { data, error } = await sb.rpc('importer_acquire_job', {
    p_worker_id: 'test_worker',
    p_lease_duration: '1 minute'
  });
  console.log('Error:', error);
  console.log('Data:', data);
  if (data && data.length > 0) {
    await sb.from('importer_queue').update({ status: 'QUEUED' }).eq('id', data[0].id);
  }
}
run();
