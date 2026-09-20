import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';
dotenv.config();
const sb = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY);
async function run() {
  const { data } = await sb.from('importer_queue').select('id, source, payload').eq('status', 'IMPORTING').eq('source', 'manhastro');
  for (const row of data) {
    console.log(`Job ${row.id}: fallbacks =`, row.payload.fallbackSources);
  }
}
run();
