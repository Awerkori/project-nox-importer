import { createClient } from '@supabase/supabase-js';
import { readFileSync } from 'fs';
const env = Object.fromEntries(readFileSync('.env', 'utf-8').split('\n').filter(l => l.includes('=') && !l.startsWith('#')).map(l => { const i = l.indexOf('='); return [l.slice(0,i), l.slice(i+1)]; }));
const sb = createClient(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY);

// Check STAGED chapters for e74aa68d - why is barrier blocking?
const workId = 'e74aa68d-149f-400f-b65f-c2535e04845b';

// What's the lowest STAGED key?
const { data: stagedChaps } = await sb
  .from('importer_chapter_mappings')
  .select('chapter_number, chapter_sort_key, status, is_gap, last_error')
  .eq('work_id', workId)
  .eq('status', 'STAGED')
  .order('chapter_sort_key', { ascending: true })
  .limit(5);
console.log("Lowest STAGED chapters:", stagedChaps);

// What's blocking them? Check chapters below the lowest STAGED
const lowestStaged = stagedChaps?.[0]?.chapter_sort_key;
if (lowestStaged) {
  const { data: blockers } = await sb
    .from('importer_chapter_mappings')
    .select('chapter_number, chapter_sort_key, status, is_gap, last_error')
    .eq('work_id', workId)
    .lt('chapter_sort_key', lowestStaged)
    .order('chapter_sort_key', { ascending: false })
    .limit(10);
  console.log("Preceding chapters (potential blockers):", blockers);
}

// Also check the publication_safety_barrier setting
const { data: barrier } = await sb.from('settings').select('value').eq('key', 'publication_safety_barrier').single();
console.log("Publication safety barrier state:", barrier?.value);

// Check recent publications
const t10 = new Date(Date.now() - 10*60000).toISOString();
const { data: recentPubs } = await sb
  .from('chapters')
  .select('number, published_at, work_id')
  .not('published_at', 'is', null)
  .gte('published_at', t10)
  .order('published_at', { ascending: false });
console.log(`\nPublications in last 10 min: ${recentPubs?.length || 0}`);
const byWork = new Map();
for (const c of recentPubs || []) byWork.set(c.work_id, (byWork.get(c.work_id)||0)+1);
console.log(`Distinct works: ${byWork.size}`);
