import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';
dotenv.config();

const url = process.env.SUPABASE_URL;
const key = process.env.SUPABASE_SERVICE_ROLE_KEY;
const sb = createClient(url, key);

async function main() {
  const { data: q } = await sb.from('importer_queue').select('priority').eq('status', 'QUEUED').limit(100);
  console.log('Sample priorities of QUEUED jobs:', [...new Set(q.map(r => r.priority))]);
}
main().catch(console.error);
