import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';
dotenv.config();

const url = process.env.SUPABASE_URL;
const key = process.env.SUPABASE_SERVICE_ROLE_KEY;
const sb = createClient(url, key);

async function main() {
  const statuses = ['QUEUED', 'IMPORTING', 'STAGED', 'FAILED', 'RETRY'];
  console.log('Queue counts:');
  for (const s of statuses) {
    const { count } = await sb.from('importer_queue').select('*', { count: 'exact', head: true }).eq('status', s);
    console.log(`  ${s}: ${count}`);
  }
}
main().catch(console.error);
