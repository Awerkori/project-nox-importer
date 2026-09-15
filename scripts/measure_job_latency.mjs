import { createClient } from '@supabase/supabase-js';
import { readFileSync } from 'fs';
const env = Object.fromEntries(readFileSync('.env', 'utf-8').split('\n').filter(l => l.includes('=') && !l.startsWith('#')).map(l => { const i = l.indexOf('='); return [l.slice(0,i), l.slice(i+1)]; }));
const sb = createClient(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY);

// Measure recent job completion rate from importer_queue history (completed in last 30 min)
const t30 = new Date(Date.now() - 30 * 60000).toISOString();
const { data: completedJobs } = await sb
  .from('importer_queue')
  .select('id, task_type, source, updated_at, created_at, locked_at, attempts')
  .eq('status', 'COMPLETED')
  .eq('task_type', 'IMPORT_CHAPTER')
  .gte('updated_at', t30)
  .order('updated_at', { ascending: false })
  .limit(100);

console.log(`Completed IMPORT_CHAPTER jobs in last 30 min: ${completedJobs?.length || 0}`);
const rate = ((completedJobs?.length || 0) / 30).toFixed(1);
console.log(`Rate: ${rate} jobs/min`);

if (completedJobs && completedJobs.length > 0) {
  const durations = completedJobs
    .filter(j => j.locked_at)
    .map(j => (new Date(j.updated_at) - new Date(j.locked_at)) / 1000);
  durations.sort((a, b) => a - b);
  const p50 = durations[Math.floor(durations.length * 0.5)];
  const p95 = durations[Math.floor(durations.length * 0.95)];
  const avg = durations.reduce((a, b) => a + b, 0) / durations.length;
  console.log(`Avg job duration: ${avg?.toFixed(1)}s`);
  console.log(`p50 job duration: ${p50?.toFixed(1)}s`);
  console.log(`p95 job duration: ${p95?.toFixed(1)}s`);
  // At 6 concurrent, theoretical max = 6 / avg_duration * 60
  const theoreticalMax = avg ? (6 / avg * 60).toFixed(1) : '?';
  console.log(`\nWith 6 concurrent slots and avg=${avg?.toFixed(1)}s/job:`);
  console.log(`Theoretical max: ${theoreticalMax} jobs/min`);
}

// Also check STAGED → published latency from mappings
const { data: recentPubs } = await sb
  .from('chapters')
  .select('id, published_at, work_id')
  .not('published_at', 'is', null)
  .gte('published_at', t30)
  .order('published_at', { ascending: false });
console.log(`\nPublished chapters in last 30 min: ${recentPubs?.length || 0}`);
console.log(`Publication rate: ${((recentPubs?.length || 0) / 30).toFixed(2)} chapters/min`);
