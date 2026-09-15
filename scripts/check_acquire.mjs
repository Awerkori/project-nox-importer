import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';
dotenv.config();

const url = process.env.SUPABASE_URL;
const key = process.env.SUPABASE_SERVICE_ROLE_KEY;
const sb = createClient(url, key);

async function main() {
  const sources = ['kuro', 'mangotoons', 'nexus', 'manhastro', 'wpgallery'];
  for (const src of sources) {
    const { data, error } = await sb.rpc('importer_acquire_job', {
      p_worker_id: 'test-1',
      p_lease_duration: '5 minutes',
      p_source: src,
      p_task_type: 'IMPORT_CHAPTER'
    });
    console.log(`Acquire for ${src}:`, error ? `ERROR: ${error.message}` : `${data?.length || 0} jobs`);
  }
}
main().catch(console.error);
