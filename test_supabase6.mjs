import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';
dotenv.config();

const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY);

async function runTests() {
  let resPending = await supabase.from('importer_jobs').select('id', { count: 'exact', head: true }).eq('status', 'pending');
  console.log(`Jobs pending: ${resPending.count} | Error: ${resPending.error?.message}`);
  
  let resProcessing = await supabase.from('importer_jobs').select('id', { count: 'exact', head: true }).eq('status', 'processing');
  console.log(`Jobs processing: ${resProcessing.count} | Error: ${resProcessing.error?.message}`);
}
runTests();
