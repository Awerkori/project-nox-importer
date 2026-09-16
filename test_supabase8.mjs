import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';
dotenv.config();

const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY);

async function runTests() {
  let resPending = await supabase.from('importer_queue').select('id').eq('status', 'pending');
  console.log(`Jobs pending: ${resPending.data?.length} | Error: ${resPending.error?.message}`);
  
  let resProcessing = await supabase.from('importer_queue').select('id, locked_by').eq('status', 'processing');
  console.log(`Jobs processing: ${resProcessing.data?.length} | Error: ${resProcessing.error?.message}`);
  if (resProcessing.data?.length > 0) {
    // Count unique workers
    const workers = new Set(resProcessing.data.map(j => j.locked_by));
    console.log(`Active workers: ${workers.size}`);
    console.log(`Jobs per worker: ${resProcessing.data.length / workers.size}`);
  }
}
runTests();
