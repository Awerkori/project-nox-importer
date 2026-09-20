import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';
dotenv.config();
const sb = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY);
async function run() {
  const { data } = await sb.from('importer_queue').select('source').eq('status', 'IMPORTING');
  const counts = data.reduce((acc, row) => {
    acc[row.source] = (acc[row.source] || 0) + 1;
    return acc;
  }, {});
  console.log('IMPORTING Source Distribution:', counts);
  
  const { data: d2 } = await sb.from('importer_queue').select('source').eq('status', 'QUEUED').limit(500);
  const counts2 = d2.reduce((acc, row) => {
    acc[row.source] = (acc[row.source] || 0) + 1;
    return acc;
  }, {});
  console.log('QUEUED Source Distribution (Sample 500):', counts2);
}
run();
