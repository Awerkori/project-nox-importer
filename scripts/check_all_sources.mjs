import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';
dotenv.config();

const url = process.env.SUPABASE_URL;
const key = process.env.SUPABASE_SERVICE_ROLE_KEY;
const sb = createClient(url, key);

async function main() {
  const { data: qs } = await sb.from('importer_queue').select('source, status');
  const stats = {};
  for (const q of qs) {
    if (!stats[q.source]) stats[q.source] = { QUEUED: 0, COMPLETED: 0, RETRY: 0, IMPORTING: 0 };
    stats[q.source][q.status] = (stats[q.source][q.status] || 0) + 1;
  }
  console.log(stats);
}
main().catch(console.error);
