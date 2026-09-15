import { createClient } from '@supabase/supabase-js';
import { readFileSync } from 'fs';
const env = Object.fromEntries(readFileSync('.env', 'utf-8').split('\n').filter(l => l.includes('=') && !l.startsWith('#')).map(l => { const i = l.indexOf('='); return [l.slice(0,i), l.slice(i+1)]; }));
const sb = createClient(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY);

const now = new Date();
const t1 = new Date(now - 10 * 60000).toISOString(); // 10 min ago
const t5 = new Date(now - 5 * 60000).toISOString();  // 5 min ago

// 1. Publishes in last 10 minutes
const { data: recentPubs } = await sb
  .from('chapters')
  .select('id, published_at, work_id')
  .not('published_at', 'is', null)
  .gte('published_at', t1)
  .order('published_at', { ascending: false });

console.log(`=== Publications in last 10 min ===`);
console.log(`Total: ${recentPubs?.length || 0}`);
console.log(`Rate: ${((recentPubs?.length || 0) / 10).toFixed(1)} chapters/min`);
const byWork = new Map();
for (const c of recentPubs || []) {
  byWork.set(c.work_id, (byWork.get(c.work_id)||0)+1);
}
console.log(`Distinct works: ${byWork.size}`);
if (recentPubs?.length > 0) {
  console.log(`Newest: ${recentPubs[0].published_at}`);
  console.log(`Oldest: ${recentPubs.at(-1).published_at}`);
}

// 2. STAGED queue
const { data: staged } = await sb
  .from('importer_chapter_mappings')
  .select('work_id, chapter_sort_key, updated_at')
  .eq('status', 'STAGED');
console.log(`\n=== STAGED backlog ===`);
console.log(`Total STAGED: ${staged?.length || 0}`);
const stagedByWork = new Map();
for (const s of staged || []) stagedByWork.set(s.work_id, (stagedByWork.get(s.work_id)||0)+1);
console.log(`Distinct works with STAGED: ${stagedByWork.size}`);
[...stagedByWork.entries()].sort((a,b)=>b[1]-a[1]).slice(0,5).forEach(([w,c]) => console.log(`  ${w.slice(0,12)}: ${c} staged`));

// 3. Active imports
const { data: importing } = await sb
  .from('importer_queue')
  .select('id, source, chapter_sort_key, payload')
  .eq('status', 'IMPORTING')
  .eq('task_type', 'IMPORT_CHAPTER');
console.log(`\n=== Active IMPORTING jobs ===`);
console.log(`Total: ${importing?.length || 0}`);
const importByWork = new Map();
for (const j of importing || []) {
  const wid = j.payload?.workId || 'unknown';
  importByWork.set(wid, (importByWork.get(wid)||0)+1);
}
console.log(`Distinct works importing: ${importByWork.size}`);

// 4. Global queue health
const { data: queueHealth } = await sb
  .from('importer_queue')
  .select('status, task_type')
  .in('status', ['QUEUED', 'RETRY', 'IMPORTING'])
  .eq('task_type', 'IMPORT_CHAPTER');
const byStatus = { QUEUED: 0, RETRY: 0, IMPORTING: 0 };
for (const j of queueHealth || []) byStatus[j.status] = (byStatus[j.status]||0)+1;
console.log(`\n=== Queue state ===`);
console.log(`QUEUED: ${byStatus.QUEUED}`);
console.log(`RETRY: ${byStatus.RETRY}`);
console.log(`IMPORTING: ${byStatus.IMPORTING}`);
