import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';
dotenv.config();

const sb = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY);

async function run() {
  const [q, i, s, c] = await Promise.all([
    sb.from('importer_queue').select('status', { count: 'exact', head: true }).eq('status', 'QUEUED'),
    sb.from('importer_queue').select('status', { count: 'exact', head: true }).eq('status', 'IMPORTING'),
    sb.from('importer_chapter_mappings').select('status', { count: 'exact', head: true }).eq('status', 'STAGED'),
    sb.from('importer_chapter_mappings').select('status', { count: 'exact', head: true }).eq('status', 'COMPLETED')
  ]);
  console.log(`QUEUED: ${q.count}`);
  console.log(`IMPORTING: ${i.count}`);
  console.log(`STAGED: ${s.count}`);
  console.log(`PUBLISHED TOTAL: ${c.count}`);
}
run();
