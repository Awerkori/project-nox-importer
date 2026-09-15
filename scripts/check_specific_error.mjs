import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';
dotenv.config();

const url = process.env.SUPABASE_URL;
const key = process.env.SUPABASE_SERVICE_ROLE_KEY;
const sb = createClient(url, key);

async function main() {
  const { data: retryJobs } = await sb.from('importer_queue')
    .select('id, last_error, payload')
    .eq('task_type', 'IMPORT_CHAPTER')
    .ilike('last_error', '%work_mapping_id%')
    .limit(10);
    
  for (const j of retryJobs || []) {
    console.log(`Job ${j.id}: workMappingId = ${j.payload.workMappingId}`);
  }
}
main().catch(console.error);
