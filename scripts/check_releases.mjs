import { createClient } from '@supabase/supabase-js';
import { readFileSync } from 'fs';
const env = Object.fromEntries(readFileSync('.env', 'utf-8').split('\n').filter(l => l.includes('=') && !l.startsWith('#')).map(l => { const i = l.indexOf('='); return [l.slice(0,i), l.slice(i+1)]; }));
const sb = createClient(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY);

const { data: chapters } = await sb
  .from('chapters')
  .select('id,number,title,published_at,work_id,works!inner(id,slug,title,cover_id,kind,published)')
  .not('published_at', 'is', null)
  .eq('works.published', true)
  .order('published_at', { ascending: false })
  .limit(48);

const grouped = new Map();
for (const row of chapters || []) {
  const w = row.works;
  if (!w) continue;
  if (!grouped.has(w.id)) grouped.set(w.id, { title: w.title, chapters: [] });
  const grp = grouped.get(w.id);
  if (grp.chapters.length < 3) grp.chapters.push({ number: row.number, published_at: row.published_at });
}

console.log("Works in lançamentos (from 48-chapter query):");
let i = 0;
for (const [id, g] of grouped) {
  console.log(`  ${++i}. ${g.title}: ${g.chapters.map(c => `Ch${c.number} (${c.published_at})`).join(', ')}`);
}
console.log(`Total distinct works: ${grouped.size}`);

const countByWork = new Map();
for (const row of chapters || []) {
  const w = row.works;
  if (!w) continue;
  countByWork.set(w.title, (countByWork.get(w.title)||0)+1);
}
const sorted = [...countByWork.entries()].sort((a,b) => b[1]-a[1]);
console.log("\nTop works by chapter count in 48-row window:");
sorted.slice(0, 5).forEach(([t,c]) => console.log(`  ${t}: ${c} chapters`));
console.log('\nOldest published_at in window:', chapters?.at(-1)?.published_at);
console.log('Newest published_at in window:', chapters?.at(0)?.published_at);
