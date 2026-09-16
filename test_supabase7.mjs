import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';
dotenv.config();

const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY);

async function runTests() {
  let resPending = await supabase.from('importer_jobs').select('id').eq('status', 'pending');
  console.log(`Jobs pending: ${resPending.data?.length} | Error: ${resPending.error?.message}`);
  
  let resProcessing = await supabase.from('importer_jobs').select('id, worker_id, updated_at').eq('status', 'processing');
  console.log(`Jobs processing: ${resProcessing.data?.length} | Error: ${resProcessing.error?.message}`);
  if (resProcessing.data?.length > 0) console.log(resProcessing.data);
  
  let resTotal = await supabase.from('importer_jobs').select('id').limit(1);
  console.log(`Any jobs? ${resTotal.data?.length}`);
}
runTests();
