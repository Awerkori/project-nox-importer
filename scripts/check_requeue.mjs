import { createClient } from '@supabase/supabase-js';
import { readFileSync } from 'fs';
const env = Object.fromEntries(readFileSync('.env', 'utf-8').split('\n').filter(l => l.includes('=') && !l.startsWith('#')).map(l => { const i = l.indexOf('='); return [l.slice(0,i), l.slice(i+1)]; }));
const sb = createClient(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY);

// Check the re-enqueued jobs
const { data: requeued } = await sb
  .from('importer_queue')
  .select('id, status, source, chapter_sort_key, payload, next_run_at, attempts')
  .in('status', ['QUEUED', 'RETRY', 'IMPORTING'])
  .eq('task_type', 'IMPORT_CHAPTER')
  .like('dedupe_key', 'requeue:%')
  .order('chapter_sort_key');
console.log("Re-enqueued blocker jobs:");
requeued?.forEach(j => console.log(`  status=${j.status} source=${j.source} sortKey=${j.chapter_sort_key} workId=${j.payload?.workId?.slice(0,8)}`));

// Check DIScloud importer-specific admission
// Is safety barrier still OPEN?
const { data: barrier } = await sb.from('settings').select('value').eq('key', 'publication_safety_barrier').single();
console.log("\nBarrier:", barrier?.value);

// How many global IMPORTING right now?
const { data: allImporting } = await sb
  .from('importer_queue')
  .select('id, task_type, source, chapter_sort_key, payload, locked_at')
  .eq('status', 'IMPORTING');
console.log(`\nAll IMPORTING jobs: ${allImporting?.length || 0}`);
allImporting?.forEach(j => {
  const age = j.locked_at ? Math.round((Date.now() - new Date(j.locked_at)) / 1000) : '?';
  console.log(`  ${j.task_type} ${j.source} sortKey=${j.chapter_sort_key} age=${age}s`);
});
