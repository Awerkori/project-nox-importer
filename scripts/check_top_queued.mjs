import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';
dotenv.config();
const sb = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY);
async function run() {
  const { data } = await sb.from('importer_queue').select('source').in('status', ['QUEUED', 'RETRY']).order('priority', { ascending: false }).order('next_run_at', { ascending: true }).limit(500);
  const counts = data.reduce((acc, row) => {
    acc[row.source] = (acc[row.source] || 0) + 1;
    return acc;
  }, {});
  console.log(counts);
}
run();
