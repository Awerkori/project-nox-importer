import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';
dotenv.config();

const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY);
async function run() {
  const { count: failedQueue } = await supabase.from('importer_queue').select('id', { count: 'exact', head: true }).eq('status', 'FAILED');
  console.log(`Queue FAILED: ${failedQueue}`);
}
run();
