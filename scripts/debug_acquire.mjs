import { createClient } from '@supabase/supabase-js';
import { readFileSync } from 'fs';
const env = Object.fromEntries(readFileSync('.env', 'utf-8').split('\n').filter(l => l.includes('=') && !l.startsWith('#')).map(l => { const i = l.indexOf('='); return [l.slice(0,i), l.slice(i+1)]; }));
const sb = createClient(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY);

async function run() {
  const { data: eligible } = await sb.from('importer_queue').select('id, task_type, source, status').eq('status', 'QUEUED').limit(5);
  console.log("Some eligible jobs:", eligible);
  
  const { data: q1 } = await sb.rpc('importer_acquire_job', { p_worker_id: 'test' });
  console.log("Acquire 1:", q1);
}
run();
