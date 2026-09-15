import { createClient } from '@supabase/supabase-js';
import * as dotenv from 'dotenv';
import { readFileSync } from 'fs';

const env = dotenv.parse(readFileSync('/home/awerkori/.Projects/project-nox-importer/.env'));
const supabase = createClient(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY);

const { data: sources } = await supabase
  .from('importer_sources')
  .select('id, status, enabled, consecutive_failures, error_message')
  .order('status').order('id');

const grouped = {};
for (const s of (sources || [])) {
  grouped[s.status] = grouped[s.status] || [];
  grouped[s.status].push(s);
}

console.log('\n=== SOURCES STATE ===');
for (const [status, list] of Object.entries(grouped)) {
  console.log(`\n--- ${status} (${list.length}) ---`);
  for (const s of list) console.log(`  ${s.id} | en=${s.enabled} | fails=${s.consecutive_failures} | ${s.error_message?.slice(0,70) || ''}`);
}

const { data: jobs } = await supabase
  .from('importer_queue')
  .select('status, source')
  .in('status', ['RETRY', 'HELD', 'BLOCKED_BY_UPSTREAM', 'IMPORTING', 'QUEUED', 'PARKED']);

const jobStats = {};
const retrySources = {};
for (const j of (jobs || [])) {
  jobStats[j.status] = (jobStats[j.status] || 0) + 1;
  if (j.status === 'RETRY') retrySources[j.source] = (retrySources[j.source] || 0) + 1;
}

console.log('\n=== JOB STATUS COUNTS ===');
for (const [s, c] of Object.entries(jobStats)) console.log(`  ${s}: ${c}`);

const topRetry = Object.entries(retrySources).sort((a, b) => b[1] - a[1]);
if (topRetry.length) {
  console.log('\n=== RETRY BY SOURCE ===');
  for (const [s, c] of topRetry) console.log(`  ${s}: ${c}`);
}
