import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';
dotenv.config();

const url = process.env.SUPABASE_URL;
const key = process.env.SUPABASE_SERVICE_ROLE_KEY;
const sb = createClient(url, key);

async function main() {
  const { error } = await sb.from('importer_chapter_mappings').insert({
    source: 'test',
    source_chapter_id: 'test',
    work_id: 'e1b4bce1-5fbd-4fdf-8930-ca4afc26771d',
    chapter_number: 1,
    chapter_sort_key: 1,
    status: 'QUEUED'
  });
  console.log(error);
}
main().catch(console.error);
