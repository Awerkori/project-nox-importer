import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';
dotenv.config();

const url = process.env.SUPABASE_URL;
const key = process.env.SUPABASE_SERVICE_ROLE_KEY;
const sb = createClient(url, key);

async function main() {
  const { data: schema } = await sb.rpc('exec_sql', { sql: `
    SELECT column_name, is_nullable
    FROM information_schema.columns
    WHERE table_name = 'importer_chapter_mappings' AND column_name = 'work_mapping_id';
  `}).catch(() => ({ data: null }));
  
  // Actually, I can just query the error directly!
  console.log('Error happens here! Let us see if work_mapping_id is null on the table level.');
  
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
