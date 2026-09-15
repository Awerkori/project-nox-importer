import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';
dotenv.config();

const url = process.env.SUPABASE_URL;
const key = process.env.SUPABASE_SERVICE_ROLE_KEY;
const sb = createClient(url, key);

async function main() {
  const { data: q } = await sb.from('importer_queue').select('source').eq('status', 'QUEUED').limit(5000);
  const counts = {};
  for (const r of q) {
    counts[r.source] = (counts[r.source] || 0) + 1;
  }
  console.log(counts);
}
main().catch(console.error);
