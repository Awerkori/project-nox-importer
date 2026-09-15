import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';
dotenv.config();

const url = process.env.SUPABASE_URL;
const key = process.env.SUPABASE_SERVICE_ROLE_KEY;
const sb = createClient(url, key);

async function main() {
  const { data: retryJobs } = await sb.from('importer_queue')
    .select('id, last_error, payload')
    .eq('status', 'RETRY')
    .eq('task_type', 'IMPORT_CHAPTER')
    .limit(50);
    
  let missingMappingCount = 0;
  for (const j of retryJobs || []) {
    if (j.last_error?.includes('work_mapping_id')) {
      missingMappingCount++;
      // Check if this job has workMappingId in payload
      // console.log(j.payload);
    }
  }
  console.log(`Out of ${retryJobs?.length} RETRY jobs, ${missingMappingCount} failed due to work_mapping_id`);
  if (retryJobs && retryJobs.length > 0) {
    console.log('Sample payload:', retryJobs[0].payload);
  }
}
main().catch(console.error);
