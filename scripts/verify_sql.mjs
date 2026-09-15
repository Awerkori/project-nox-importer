import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';
dotenv.config();

const url = process.env.SUPABASE_URL;
const key = process.env.SUPABASE_SERVICE_ROLE_KEY;
const sb = createClient(url, key);

async function main() {
  const { data, error } = await sb.rpc('admin_get_system_health');
  // Wait, I want to fetch the definition of importer_acquire_job.
  // I can't do that easily via RPC unless there's a specific RPC.
  // I will just enqueue a mock job and see if it gets acquired properly, or simply trust that DIScloud Auto Deploy ran.
  // The user says "DIScloud Auto Deploy" takes care of it.
  
  // Let's monitor the queue!
  const { data: importing } = await sb.from('importer_queue').select('payload, chapter_sort_key').eq('status', 'IMPORTING');
  console.log('Currently importing:');
  const counts = {};
  for (const row of (importing || [])) {
    const w = row.payload?.workId;
    counts[w] = (counts[w] || 0) + 1;
  }
  console.log(counts);
}
main().catch(console.error);
