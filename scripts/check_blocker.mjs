import { createClient } from '@supabase/supabase-js';
import { readFileSync } from 'fs';
const env = Object.fromEntries(readFileSync('.env', 'utf-8').split('\n').filter(l => l.includes('=') && !l.startsWith('#')).map(l => { const i = l.indexOf('='); return [l.slice(0,i), l.slice(i+1)]; }));
const sb = createClient(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY);

const workId = 'e74aa68d-149f-400f-b65f-c2535e04845b';

// Check the blocker chapters 101-102
const { data: blockers } = await sb
  .from('importer_chapter_mappings')
  .select('chapter_number, chapter_sort_key, status, is_gap, last_error, updated_at, source')
  .eq('work_id', workId)
  .in('chapter_sort_key', [101, 102, 103])
  .order('chapter_sort_key');
console.log("Blocker chapters:");
blockers?.forEach(c => console.log(JSON.stringify(c, null, 2)));

// Check how many jobs are in queue for ch102 of this work
const { data: queueJobs } = await sb
  .from('importer_queue')
  .select('id, status, source, chapter_sort_key, next_run_at, attempts, max_attempts')
  .eq('task_type', 'IMPORT_CHAPTER')
  .eq('chapter_sort_key', 102)
  .filter('payload->>workId', 'eq', workId);
console.log("\nQueue jobs for ch102:", JSON.stringify(queueJobs, null, 2));

// What's the canPublish result from the DB?
const { data: staged103 } = await sb
  .from('importer_chapter_mappings')
  .select('chapter_number, chapter_sort_key, status, is_gap')
  .eq('work_id', workId)
  .eq('chapter_sort_key', 103)
  .single();
console.log("\nCh103 mapping:", staged103);

// How many works CAN actually publish now?
const { data: canPublish } = await sb
  .from('importer_chapter_mappings')
  .select('work_id', { count: 'exact', head: false })
  .eq('status', 'STAGED')
  .eq('is_gap', false)
  .limit(5);
console.log("\nWorks with STAGED chapters:", canPublish?.length);
