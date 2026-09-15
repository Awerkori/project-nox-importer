import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';
dotenv.config();
const sb = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY);

async function run() {
  const tenMinsAgo = new Date(Date.now() - 10 * 60000).toISOString();
  const [{ data: ch }, { count: q_tot }, { count: q_imp }, { count: q_stg }] = await Promise.all([
    sb.from('chapters').select('id').gte('published_at', tenMinsAgo),
    sb.from('importer_queue').select('*', { count: 'exact', head: true }).eq('status', 'QUEUED'),
    sb.from('importer_queue').select('*', { count: 'exact', head: true }).eq('status', 'IMPORTING'),
    sb.from('importer_chapter_mappings').select('*', { count: 'exact', head: true }).eq('status', 'STAGED'),
  ]);
  console.log(`Published (10m): ${ch?.length}`);
  console.log(`Queued: ${q_tot}`);
  console.log(`Importing: ${q_imp}`);
  console.log(`Staged: ${q_stg}`);
}
run();
