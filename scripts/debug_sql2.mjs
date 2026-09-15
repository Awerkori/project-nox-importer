import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';
dotenv.config();

const url = process.env.SUPABASE_URL;
const key = process.env.SUPABASE_SERVICE_ROLE_KEY;
const sb = createClient(url, key);

async function main() {
  const { data: test1 } = await sb.rpc('importer_acquire_job', { p_worker_id: 'test', p_lease_duration: '1 minute', p_source: 'mangotoons', p_task_type: 'IMPORT_CHAPTER' });
  console.log('RPC Call:', test1);
}
main().catch(console.error);
