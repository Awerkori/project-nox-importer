import { createClient } from '@supabase/supabase-js';
import { readFileSync } from 'fs';
const envRaw = readFileSync('/home/awerkori/.Projects/project-nox-importer/.env', 'utf8');
const env = {};
for (const line of envRaw.split('\n')) { const m = line.match(/^([A-Z_]+)=(.*)$/); if (m) env[m[1]] = m[2].trim(); }
const supabase = createClient(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY);

// Get all jobs with errors (last_error not null, attempts > 0)
const { data: jobs } = await supabase
  .from('importer_queue')
  .select('id, source, status, last_error, attempts, max_attempts, next_run_at, updated_at')
  .not('last_error', 'is', null)
  .gt('attempts', 0)
  .not('status', 'in', '("DONE","COMPLETED","SUPERSEDED","CANCELLED_BY_STAFF")')
  .order('updated_at', { ascending: false })
  .limit(50);

console.log('=== JOBS WITH ERRORS ===');
const bySource = {};
for (const j of (jobs || [])) {
  if (!bySource[j.source]) bySource[j.source] = [];
  bySource[j.source].push(j);
}

for (const [src, list] of Object.entries(bySource)) {
  console.log(`\n${src} (${list.length} jobs):`);
  for (const j of list.slice(0, 3)) {
    console.log(`  [${j.status}] attempts=${j.attempts}/${j.max_attempts} | ${j.last_error?.slice(0,100)}`);
  }
}

// Check importer_telemetry for recent errors
const { data: telemetry } = await supabase
  .from('importer_telemetry')
  .select('*')
  .order('created_at', { ascending: false })
  .limit(3);

if (telemetry?.length) {
  console.log('\n=== RECENT TELEMETRY ===');
  for (const t of telemetry) {
    console.log(`${t.created_at}: active=${t.active_count || 0} retry=${t.retry_count || 0} held=${t.held_count || 0}`);
    if (t.source_health) console.log('  health:', JSON.stringify(t.source_health).slice(0,200));
  }
}

// Count active RETRY jobs
const { count } = await supabase
  .from('importer_queue')
  .select('*', { count: 'exact', head: true })
  .eq('status', 'RETRY');
console.log(`\nTotal RETRY jobs: ${count}`);

const { count: c2 } = await supabase
  .from('importer_queue')
  .select('*', { count: 'exact', head: true })
  .eq('status', 'HELD');
console.log(`Total HELD jobs: ${c2}`);

const { count: c3 } = await supabase
  .from('importer_queue')
  .select('*', { count: 'exact', head: true })
  .eq('status', 'BLOCKED_BY_UPSTREAM');
console.log(`Total BLOCKED_BY_UPSTREAM jobs: ${c3}`);
