import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';
dotenv.config();
const sb = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY);
async function run() {
  const { data } = await sb.from('importer_queue').select('source, priority, task_type').eq('status', 'QUEUED').order('priority', { ascending: false }).limit(100);
  const counts = data.reduce((acc, row) => {
    acc[row.source] = (acc[row.source] || 0) + 1;
    return acc;
  }, {});
  console.log('Top 100 QUEUED Priorities:', counts);
  console.log('Highest Priority:', data[0]?.priority);
}
run();
