import { createClient } from '@supabase/supabase-js';
import { readFileSync } from 'fs';

const envRaw = readFileSync('/home/awerkori/.Projects/project-nox-importer/.env', 'utf8');
const env = {};
for (const line of envRaw.split('\n')) {
  const m = line.match(/^([A-Z_]+)=(.*)$/);
  if (m) env[m[1]] = m[2].trim();
}

const supabase = createClient(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY);

const { data: sources, error } = await supabase
  .from('importer_sources')
  .select('id, status, enabled, consecutive_failures, error_message')
  .order('status').order('id');

if (error) { console.error('sources error:', error); }

console.log('\n=== SOURCES STATE ===');
const grouped = {};
for (const s of (sources || [])) {
  grouped[s.status || 'null'] = grouped[s.status || 'null'] || [];
  grouped[s.status || 'null'].push(s);
}

for (const [status, list] of Object.entries(grouped)) {
  console.log(`\n--- ${status} (${list.length}) ---`);
  for (const s of list) console.log(`  ${s.id} | en=${s.enabled} | fails=${s.consecutive_failures} | ${s.error_message?.slice(0,70) || ''}`);
}

// Check job stats  
const { data: jobs, error: je } = await supabase
  .from('importer_queue')
  .select('status, source')
  .not('status', 'eq', 'DONE')
  .not('status', 'eq', 'FAILED')
  .not('status', 'eq', 'CANCELLED');

if (je) console.error('jobs error:', je);

const jobStats = {};
const sourceCounts = {};
for (const j of (jobs || [])) {
  jobStats[j.status] = (jobStats[j.status] || 0) + 1;
  if (j.status !== 'QUEUED' && j.status !== 'DONE') {
    sourceCounts[j.source + ':' + j.status] = (sourceCounts[j.source + ':' + j.status] || 0) + 1;
  }
}

console.log('\n=== JOB STATUS COUNTS ===');
for (const [s, c] of Object.entries(jobStats)) console.log(`  ${s}: ${c}`);

const interesting = Object.entries(sourceCounts).sort((a, b) => b[1] - a[1]).slice(0, 30);
if (interesting.length) {
  console.log('\n=== ACTIVE NON-QUEUED JOBS BY SOURCE:STATUS ===');
  for (const [k, c] of interesting) console.log(`  ${k}: ${c}`);
}
