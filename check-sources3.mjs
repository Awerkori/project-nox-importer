import { createClient } from '@supabase/supabase-js';
import { readFileSync } from 'fs';

const envRaw = readFileSync('/home/awerkori/.Projects/project-nox-importer/.env', 'utf8');
const env = {};
for (const line of envRaw.split('\n')) {
  const m = line.match(/^([A-Z_]+)=(.*)$/);
  if (m) env[m[1]] = m[2].trim();
}

const supabase = createClient(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY);

// Get actual schema
const { data: sources, error } = await supabase
  .from('importer_sources')
  .select('*')
  .limit(1);
  
if (sources && sources.length > 0) {
  console.log('=== IMPORTER_SOURCES COLUMNS ===');
  console.log(Object.keys(sources[0]).join(', '));
}

const { data: allSources } = await supabase
  .from('importer_sources')
  .select('*')
  .order('id');

console.log('\n=== ALL SOURCES ===');
for (const s of (allSources || [])) {
  const status = s.status || s.provider_status || 'N/A';
  const enabled = s.enabled ?? s.is_enabled ?? 'N/A';
  const err = s.error_message || s.last_error || '';
  console.log(`  ${s.id} | status=${status} | en=${enabled} | ${err.slice(0,60)}`);
}
console.log(`\nTotal: ${(allSources || []).length} sources`);

// Jobs
const { data: jobs } = await supabase.from('importer_queue').select('*').limit(1);
if (jobs && jobs[0]) {
  console.log('\n=== IMPORTER_QUEUE COLUMNS ===');
  console.log(Object.keys(jobs[0]).join(', '));
}

const { data: allJobs } = await supabase.from('importer_queue').select('status, source').not('status', 'in', '("DONE","FAILED","CANCELLED","COMPLETED","SUPERSEDED")');
const jobStats = {};
for (const j of (allJobs || [])) jobStats[j.status] = (jobStats[j.status] || 0) + 1;
console.log('\n=== ACTIVE JOB COUNTS ===');
for (const [s, c] of Object.entries(jobStats)) console.log(`  ${s}: ${c}`);
