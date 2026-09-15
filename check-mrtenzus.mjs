import { createClient } from '@supabase/supabase-js';
import { readFileSync } from 'fs';
const envRaw = readFileSync('/home/awerkori/.Projects/project-nox-importer/.env', 'utf8');
const env = {};
for (const line of envRaw.split('\n')) { const m = line.match(/^([A-Z_]+)=(.*)$/); if (m) env[m[1]] = m[2].trim(); }
const supabase = createClient(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY);

const { data: jobs } = await supabase
  .from('importer_queue')
  .select('id, source, status, last_error, payload, attempts, next_run_at')
  .eq('source', 'mrtenzus')
  .neq('status', 'DONE')
  .order('updated_at', { ascending: false })
  .limit(20);

console.log('=== MRTENZUS JOBS ===');
for (const j of (jobs || [])) {
  const payload = typeof j.payload === 'string' ? JSON.parse(j.payload) : j.payload;
  console.log(`  ${j.status} | attempts=${j.attempts} | task=${payload?.task_type || j.id.slice(0,8)}`);
  if (j.last_error) console.log(`    ERROR: ${j.last_error.slice(0,120)}`);
  if (payload?.chapter_url || payload?.sourceChapterId) console.log(`    URL: ${payload.chapter_url || payload.sourceChapterId}`);
}

// Also check taimumangas
const { data: tJobs } = await supabase
  .from('importer_queue')
  .select('id, source, status, last_error, payload, attempts')
  .eq('source', 'taimumangas')
  .neq('status', 'DONE')
  .order('updated_at', { ascending: false })
  .limit(10);

console.log('\n=== TAIMUMANGAS JOBS ===');
for (const j of (tJobs || [])) {
  const payload = typeof j.payload === 'string' ? JSON.parse(j.payload) : j.payload;
  console.log(`  ${j.status} | attempts=${j.attempts}`);
  if (j.last_error) console.log(`    ERROR: ${j.last_error.slice(0,120)}`);
}
