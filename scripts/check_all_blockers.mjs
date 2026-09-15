import { createClient } from '@supabase/supabase-js';
import { readFileSync } from 'fs';
const env = Object.fromEntries(readFileSync('.env', 'utf-8').split('\n').filter(l => l.includes('=') && !l.startsWith('#')).map(l => { const i = l.indexOf('='); return [l.slice(0,i), l.slice(i+1)]; }));
const sb = createClient(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY);

// Get top 6 works by STAGED count
const { data: stagedAll } = await sb
  .from('importer_chapter_mappings')
  .select('work_id, chapter_sort_key')
  .eq('status', 'STAGED');

const byWork = new Map();
for (const r of stagedAll || []) {
  if (!byWork.has(r.work_id)) byWork.set(r.work_id, { count: 0, minKey: Infinity });
  const w = byWork.get(r.work_id);
  w.count++;
  if (r.chapter_sort_key < w.minKey) w.minKey = r.chapter_sort_key;
}

const sorted = [...byWork.entries()].sort((a, b) => b[1].count - a[1].count).slice(0, 6);

for (const [workId, { count, minKey }] of sorted) {
  // Find the chapter just before the lowest STAGED
  const { data: blocker } = await sb
    .from('importer_chapter_mappings')
    .select('chapter_number, chapter_sort_key, status, is_gap, last_error')
    .eq('work_id', workId)
    .lt('chapter_sort_key', minKey)
    .order('chapter_sort_key', { ascending: false })
    .limit(2);

  const { data: wData } = await sb.from('works').select('title').eq('id', workId).single();
  console.log(`\n[${wData?.title || workId.slice(0,8)}] STAGED=${count}, lowestStaged=ch${minKey}`);
  blocker?.forEach(b => console.log(`  blocker ch${b.chapter_number}: status=${b.status} is_gap=${b.is_gap} err=${b.last_error?.slice(0,60) || '-'}`));

  // Check if there's an active queue job for the blocker
  if (blocker && blocker[0] && blocker[0].status === 'PENDING') {
    const blockerKey = blocker[0].chapter_sort_key;
    const { data: qjobs } = await sb
      .from('importer_queue')
      .select('status, source, attempts, max_attempts')
      .eq('task_type', 'IMPORT_CHAPTER')
      .eq('chapter_sort_key', blockerKey)
      .filter('payload->>workId', 'eq', workId);
    console.log(`  Queue jobs for blocker ch${blockerKey}:`, qjobs?.map(j => `${j.status} (${j.attempts}/${j.max_attempts})`).join(', ') || 'NONE');
  }
}
