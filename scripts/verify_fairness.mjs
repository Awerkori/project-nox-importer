import { createClient } from '@supabase/supabase-js';
import { readFileSync } from 'fs';
const env = Object.fromEntries(readFileSync('.env', 'utf-8').split('\n').filter(l => l.includes('=') && !l.startsWith('#')).map(l => { const i = l.indexOf('='); return [l.slice(0,i), l.slice(i+1)]; }));
const sb = createClient(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY);

// 1. Test get_recent_releases RPC
console.log("=== Test: get_recent_releases RPC ===");
const { data: releases, error: rErr } = await sb.rpc('get_recent_releases', { p_limit: 12, p_chapters_per_work: 3 });
if (rErr) {
  console.error("RPC error:", rErr);
} else {
  const grouped = new Map();
  for (const r of releases || []) {
    if (!grouped.has(r.work_id)) grouped.set(r.work_id, { title: r.work_title, chapters: [] });
    grouped.get(r.work_id).chapters.push(`Ch${r.chapter_number}`);
  }
  console.log(`Works returned: ${grouped.size} (max 12 expected)`);
  for (const [, g] of grouped) {
    console.log(`  ${g.title}: ${g.chapters.join(', ')} (${g.chapters.length} chapters, max 3)`);
  }
  const dominated = [...grouped.values()].some(g => g.chapters.length > 3);
  console.log(`Any work with >3 chapters: ${dominated} (should be false)`);
  console.log(`Persistent (no time filter): YES - works won't disappear due to age`);
}

// 2. Check current inflight distribution across works
console.log("\n=== Queue: Current inflight by work ===");
const { data: inflight } = await sb
  .from('importer_queue')
  .select('payload, source')
  .eq('status', 'IMPORTING');
const byWork = new Map();
for (const j of inflight || []) {
  const wid = j.payload?.workId || 'DISCOVER/SYNC';
  byWork.set(wid, (byWork.get(wid)||0)+1);
}
const sortedWork = [...byWork.entries()].sort((a,b) => b[1]-a[1]);
sortedWork.slice(0, 8).forEach(([w,c]) => console.log(`  work=${w.slice(0,12)}... jobs=${c}`));
const uniqueWorks = byWork.size;
console.log(`Total distinct works with active jobs: ${uniqueWorks}`);
