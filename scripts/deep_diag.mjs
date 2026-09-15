import { createClient } from '@supabase/supabase-js';
import { readFileSync } from 'fs';
const env = Object.fromEntries(readFileSync('.env', 'utf-8').split('\n').filter(l => l.includes('=') && !l.startsWith('#')).map(l => { const i = l.indexOf('='); return [l.slice(0,i), l.slice(i+1)]; }));
const sb = createClient(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY);

// 1. Barrier state
const { data: barrier } = await sb.from('settings').select('value').eq('key', 'publication_safety_barrier').single();
console.log("Barrier:", barrier?.value);

// 2. How many QUEUED are actually eligible (next_run_at <= now)?
const now = new Date().toISOString();
const { count: eligibleCount } = await sb
  .from('importer_queue')
  .select('*', { count: 'exact', head: true })
  .in('status', ['QUEUED', 'RETRY'])
  .eq('task_type', 'IMPORT_CHAPTER')
  .lte('next_run_at', now);
console.log("Eligible IMPORT_CHAPTER jobs (next_run_at <= now):", eligibleCount);

// 3. How many are future-scheduled?
const { count: futureCount } = await sb
  .from('importer_queue')
  .select('*', { count: 'exact', head: true })
  .in('status', ['QUEUED', 'RETRY'])
  .eq('task_type', 'IMPORT_CHAPTER')
  .gt('next_run_at', now);
console.log("Future-scheduled IMPORT_CHAPTER jobs:", futureCount);

// 4. Source-level enabled check - any sources disabled?
const { data: disabledSources } = await sb
  .from('importer_sources')
  .select('id, enabled, status, cooldown_until')
  .or('enabled.eq.false,status.in.(PAUSED,DISABLED,UPSTREAM_BLOCKED)')
  .limit(10);
console.log("Disabled/paused sources:", disabledSources?.length || 0);
if (disabledSources?.length) disabledSources.slice(0,5).forEach(s => console.log(`  ${s.id}: enabled=${s.enabled} status=${s.status}`));

// 5. Eligible jobs NOT blocked by source
const { data: sampleEligible } = await sb
  .from('importer_queue')
  .select('id, source, chapter_sort_key, next_run_at, payload')
  .in('status', ['QUEUED', 'RETRY'])
  .eq('task_type', 'IMPORT_CHAPTER')
  .lte('next_run_at', now)
  .order('chapter_sort_key', { ascending: true })
  .limit(5);
console.log("\nSample eligible IMPORT_CHAPTER jobs:");
sampleEligible?.forEach(j => console.log(`  source=${j.source} sortKey=${j.chapter_sort_key} workId=${j.payload?.workId?.slice(0,8)}`));

// 6. Sources with cooldown active
const { data: cooledSources } = await sb
  .from('importer_sources')
  .select('id, cooldown_until')
  .not('cooldown_until', 'is', null)
  .gt('cooldown_until', now)
  .limit(10);
console.log("\nSources in cooldown:", cooledSources?.length || 0);
cooledSources?.slice(0,5).forEach(s => {
  const mins = Math.round((new Date(s.cooldown_until) - Date.now()) / 60000);
  console.log(`  ${s.id}: cooldown ends in ${mins}m`);
});

// 7. Admission check - is importer_admission_available blocking?
const { data: admData } = await sb.rpc('importer_admission_available').single();
console.log("\nimporter_admission_available:", admData);
