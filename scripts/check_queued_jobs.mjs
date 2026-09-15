import { createClient } from '@supabase/supabase-js';
import { readFileSync } from 'fs';
const env = Object.fromEntries(readFileSync('.env', 'utf-8').split('\n').filter(l => l.includes('=') && !l.startsWith('#')).map(l => { const i = l.indexOf('='); return [l.slice(0,i), l.slice(i+1)]; }));
const sb = createClient(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY);
async function run() {
  const { data: jobs } = await sb.from('importer_queue').select('id, status, locked_by').eq('task_type', 'IMPORT_CHAPTER').eq('status', 'QUEUED');
  console.log(`There are ${jobs?.length || 0} IMPORT_CHAPTER jobs QUEUED`);
  if (jobs && jobs.length > 0) {
    const { data: acq } = await sb.rpc('importer_acquire_job', { p_worker_id: 'test' });
    console.log("Test acquire returned:", acq.length);
  }
}
run();
