import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';
dotenv.config();
const sb = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY);
async function run() {
  const c1 = await sb.from('importer_chapter_mappings').select('status', { count: 'exact', head: true }).eq('status', 'COMPLETED');
  const start = c1.count;
  await new Promise(r => setTimeout(r, 30000));
  const c2 = await sb.from('importer_chapter_mappings').select('status', { count: 'exact', head: true }).eq('status', 'COMPLETED');
  const end = c2.count;
  console.log(`Start: ${start}, End: ${end}`);
  console.log('Published/min:', (end - start) * 2);
}
run();
