import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';
dotenv.config();

const url = process.env.SUPABASE_URL;
const key = process.env.SUPABASE_SERVICE_ROLE_KEY;
const sb = createClient(url, key);

async function main() {
  const { data: d1 } = await sb.rpc('importer_acquire_job', { p_worker_id: 'test', p_lease_duration: '1 minute', p_source: 'mangotoons', p_task_type: 'IMPORT_CHAPTER' });
  console.log('Test acquire 1:', d1);
  
  const { data: d2 } = await sb.rpc('importer_acquire_job', { p_worker_id: 'test', p_lease_duration: '1 minute', p_source: 'mangotoons', p_task_type: 'IMPORT_CHAPTER' });
  console.log('Test acquire 2:', d2);
  
  // Clean up
  if (d1?.length) await sb.from('importer_queue').update({ status: 'QUEUED', locked_by: null }).eq('id', d1[0].id);
  if (d2?.length) await sb.from('importer_queue').update({ status: 'QUEUED', locked_by: null }).eq('id', d2[0].id);
}
main().catch(console.error);
