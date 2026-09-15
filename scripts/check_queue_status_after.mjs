import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';
dotenv.config();

const url = process.env.SUPABASE_URL;
const key = process.env.SUPABASE_SERVICE_ROLE_KEY;
const sb = createClient(url, key);

async function main() {
  const { data: qs } = await sb.from('importer_queue').select('status', { count: 'exact' }).eq('source', 'mangotoons');
  console.log('Mangotoons jobs total:', qs.length);
  
  const statuses = {};
  for (const q of qs) {
    statuses[q.status] = (statuses[q.status] || 0) + 1;
  }
  console.log('Statuses:', statuses);
}
main().catch(console.error);
