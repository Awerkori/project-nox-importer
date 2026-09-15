import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';
dotenv.config();

const url = process.env.SUPABASE_URL;
const key = process.env.SUPABASE_SERVICE_ROLE_KEY;
const sb = createClient(url, key);

async function main() {
  const { data: errors } = await sb.from('importer_queue')
    .select('id, status, last_error, updated_at, task_type')
    .not('last_error', 'is', null)
    .order('updated_at', { ascending: false })
    .limit(10);
    
  console.log('Recent errors in queue:');
  console.log(errors);
}
main().catch(console.error);
