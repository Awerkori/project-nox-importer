import { createClient } from '@supabase/supabase-js';
import { readFileSync } from 'fs';
const env = Object.fromEntries(readFileSync('.env', 'utf-8').split('\n').filter(l => l.includes('=') && !l.startsWith('#')).map(l => { const i = l.indexOf('='); return [l.slice(0,i), l.slice(i+1)]; }));
const sb = createClient(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY);

const t30 = new Date(Date.now() - 30 * 60000).toISOString();

// Check what locked_at looks like in completed jobs
const { data: sample } = await sb
  .from('importer_queue')
  .select('id, status, locked_at, updated_at, created_at')
  .eq('task_type', 'IMPORT_CHAPTER')
  .eq('status', 'COMPLETED')
  .gte('updated_at', t30)
  .order('updated_at', { ascending: false })
  .limit(3);
console.log("Sample completed jobs:", JSON.stringify(sample, null, 2));

// 1.4 jobs/min with 6 slots → avg duration ~60/1.4/6 = 7s per job
// Actually: rate = slots / duration → duration = slots / rate
const rate = 1.4;
const slots = 6;
const impliedDuration = slots / rate * 60;
console.log(`\nImplied avg job duration (from rate+slots): ${impliedDuration.toFixed(0)}s`);
console.log(`(${slots} slots / ${rate} jobs/min = ${impliedDuration.toFixed(0)}s per job)`);

// Check sweeper published vs acquired - are more jobs completing than being published?
const { data: recentPubs } = await sb.from('chapters').select('id').not('published_at','is',null).gte('published_at', t30);
const { data: recentCompleted } = await sb.from('importer_queue').select('id').eq('status','COMPLETED').eq('task_type','IMPORT_CHAPTER').gte('updated_at', t30);
console.log(`\nCompleted jobs: ${recentCompleted?.length || 0} in 30min = ${((recentCompleted?.length||0)/30).toFixed(2)}/min`);
console.log(`Published chapters: ${recentPubs?.length || 0} in 30min = ${((recentPubs?.length||0)/30).toFixed(2)}/min`);
console.log(`\nConclusion: ${(recentCompleted?.length||0) > (recentPubs?.length||0) ? 'MORE completed than published → STAGED backlog growing → publication sweeper is the bottleneck OR barrier blocking' : 'Completed ≈ Published → download/upload is the bottleneck'}`);

// Active importings right now
const { data: currentImporting } = await sb.from('importer_queue').select('id,source,chapter_sort_key,payload,locked_at').eq('status','IMPORTING').eq('task_type','IMPORT_CHAPTER');
console.log(`\nCurrent IMPORTING jobs: ${currentImporting?.length || 0}`);
currentImporting?.forEach(j => {
  const age = Math.round((Date.now() - new Date(j.locked_at)) / 1000);
  console.log(`  ${j.source} sortKey=${j.chapter_sort_key} workId=${j.payload?.workId?.slice(0,8)} age=${age}s`);
});
