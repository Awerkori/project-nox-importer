import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';
dotenv.config();

const url = process.env.SUPABASE_URL;
const key = process.env.SUPABASE_SERVICE_ROLE_KEY;
const sb = createClient(url, key);

async function main() {
  const now = Date.now();
  
  // 1. Check last published chapter
  const { data: lastPub } = await sb.from('chapters')
    .select('number, published_at, work_id')
    .not('published_at', 'is', null)
    .order('published_at', { ascending: false })
    .limit(1);
    
  console.log('=== STALL CHECK ===');
  if (lastPub && lastPub.length > 0) {
    const pubTime = new Date(lastPub[0].published_at).getTime();
    const diffMins = Math.round((now - pubTime) / 60000);
    console.log(`Last published chapter: ${diffMins} minutes ago (at ${lastPub[0].published_at})`);
  } else {
    console.log('No published chapters found.');
  }

  // 2. Check last staged (to see if STAGED is advancing but PUBLISHED is not)
  const { data: lastStaged } = await sb.from('importer_queue')
    .select('updated_at')
    .eq('status', 'STAGED')
    .order('updated_at', { ascending: false })
    .limit(1);
  
  if (lastStaged && lastStaged.length > 0) {
    const stgTime = new Date(lastStaged[0].updated_at).getTime();
    const diffMins = Math.round((now - stgTime) / 60000);
    console.log(`Last STAGED job updated: ${diffMins} minutes ago (at ${lastStaged[0].updated_at})`);
  }
  
  // 3. Count statuses
  const { data: qStats } = await sb.from('importer_queue')
    .select('status, task_type')
    .eq('task_type', 'IMPORT_CHAPTER');
    
  const counts = {};
  for (const r of (qStats || [])) {
    counts[r.status] = (counts[r.status] || 0) + 1;
  }
  console.log('\nQueue breakdown:', counts);

  // 4. Verify the DB function (importer_acquire_job)
  const { data: funcDef } = await sb.rpc('admin_get_system_health').catch(() => ({}));
  // We can just verify it by fetching it from pg_proc
  const { data: proc } = await sb.rpc('exec_sql', { sql: `SELECT prosrc FROM pg_proc WHERE proname = 'importer_acquire_job'` }).catch(() => ({ data: null }));
  if (proc) {
    console.log('\nFunction verified from pg_proc. Contains fairness penalty?', proc[0].prosrc.includes('v_active_jobs_count * 1000') ? 'YES' : 'NO');
  }

}
main().catch(console.error);
