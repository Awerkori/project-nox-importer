import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';
dotenv.config();
const sb = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY);
async function run() {
  const { data } = await sb.from('importer_queue').select('source, task_type').eq('status', 'IMPORTING');
  const counts = data.reduce((acc, row) => {
    const key = `${row.source}:${row.task_type}`;
    acc[key] = (acc[key] || 0) + 1;
    return acc;
  }, {});
  console.log(counts);
}
run();
