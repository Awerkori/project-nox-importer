import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';
dotenv.config();

const url = process.env.SUPABASE_URL;
const key = process.env.SUPABASE_SERVICE_ROLE_KEY;
const sb = createClient(url, key);

async function main() {
  const { data: q } = await sb.from('importer_queue').select('id, next_run_at, status, task_type').eq('status', 'QUEUED').limit(10);
  console.log('Sample QUEUED jobs:');
  console.log(q);
  
  const now = new Date();
  console.log('Current time:', now.toISOString());
}
main().catch(console.error);
