import { createClient } from '@supabase/supabase-js';
import { readFileSync } from 'fs';
const envRaw = readFileSync('/home/awerkori/.Projects/project-nox-importer/.env', 'utf8');
const env = {};
for (const line of envRaw.split('\n')) { const m = line.match(/^([A-Z_]+)=(.*)$/); if (m) env[m[1]] = m[2].trim(); }
const supabase = createClient(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY);

const now = new Date().toISOString();

// 1. Kuro: BLOCKED_BY_UPSTREAM job - reset to QUEUED (bridge session renewed, source reactivated)
{
  const { data, error } = await supabase.from('importer_queue')
    .update({ status: 'QUEUED', last_error: null, next_run_at: now, updated_at: now })
    .eq('source', 'kuro').eq('status', 'BLOCKED_BY_UPSTREAM').select('id');
  console.log(`kuro BLOCKED→QUEUED: ${data?.length || 0} jobs | ${error?.message || 'ok'}`);
}

// 2. Taimumangas: storage 404 - transient, reset to QUEUED
{
  const { data, error } = await supabase.from('importer_queue')
    .update({ status: 'QUEUED', attempts: 0, last_error: null, next_run_at: now, updated_at: now })
    .eq('source', 'taimumangas').eq('status', 'RETRY').select('id');
  console.log(`taimumangas RETRY→QUEUED: ${data?.length || 0} jobs | ${error?.message || 'ok'}`);
}

// 3. Osakascan: adapter fixed (Blogger JSON API) - reset RETRY jobs to QUEUED
{
  const { data, error } = await supabase.from('importer_queue')
    .update({ status: 'QUEUED', attempts: 0, last_error: null, next_run_at: now, updated_at: now })
    .eq('source', 'osakascan').eq('status', 'RETRY').select('id');
  console.log(`osakascan RETRY→QUEUED: ${data?.length || 0} jobs | ${error?.message || 'ok'}`);
}

// 4. Covenscan: adapter fixed (baseUrl updated to /bruxonas/) - reset RETRY jobs
{
  const { data, error } = await supabase.from('importer_queue')
    .update({ status: 'QUEUED', attempts: 0, last_error: null, next_run_at: now, updated_at: now })
    .eq('source', 'covenscan').eq('status', 'RETRY').select('id');
  console.log(`covenscan RETRY→QUEUED: ${data?.length || 0} jobs | ${error?.message || 'ok'}`);
}

// 5. Hipercool: slug not found - mark those specific jobs CANCELLED (stale slug, can't recover without knowing new slug)
{
  const { data, error } = await supabase.from('importer_queue')
    .update({ 
      status: 'CANCELLED_BY_STAFF', 
      cancel_reason: 'Stale slug: isekai-anthology-hospitalized-life-in-another-world not found on source. Discovery will re-import if chapter returns.',
      cancelled_at: now, updated_at: now 
    })
    .eq('source', 'hipercool').eq('status', 'RETRY')
    .like('last_error', '%isekai-anthology%')
    .select('id');
  console.log(`hipercool stale slug→CANCELLED: ${data?.length || 0} jobs | ${error?.message || 'ok'}`);
}

// 6. Maidscan FAILED (PERMANENT_404): these are definitively dead chapters - keep as FAILED (already terminal)
// Count them for reporting
{
  const { count } = await supabase.from('importer_queue')
    .select('*', { count: 'exact', head: true })
    .eq('source', 'maidscan').eq('status', 'FAILED');
  console.log(`maidscan FAILED (permanent 404): ${count} jobs - keeping as FAILED (terminal)`);
}

// 7. Ninjascan FAILED: same - permanent 404 images, keep as FAILED
{
  const { count } = await supabase.from('importer_queue')
    .select('*', { count: 'exact', head: true })
    .eq('source', 'ninjascan').eq('status', 'FAILED');
  console.log(`ninjascan FAILED (permanent 404): ${count} jobs - keeping as FAILED (terminal)`);
}

// 8. Nocturnesummer: Cloudflare blocks image downloads - check if RETRY should be reset
// These are RETRY with attempts=1 (new failures), give them one more chance
{
  const { data, error } = await supabase.from('importer_queue')
    .update({ status: 'QUEUED', attempts: 0, last_error: null, next_run_at: new Date(Date.now() + 3600000).toISOString(), updated_at: now })
    .eq('source', 'nocturnesummer').eq('status', 'RETRY')
    .lt('attempts', 3)
    .select('id');
  console.log(`nocturnesummer low-attempt RETRY→QUEUED: ${data?.length || 0} jobs | ${error?.message || 'ok'}`);
}

// 9. Mrtenzus and littletyrant: low attempt RETRY - reset for retry
{
  for (const src of ['mrtenzus', 'littletyrant']) {
    const { data, error } = await supabase.from('importer_queue')
      .update({ status: 'QUEUED', attempts: 0, last_error: null, next_run_at: now, updated_at: now })
      .eq('source', src).eq('status', 'RETRY').lt('attempts', 3)
      .select('id');
    console.log(`${src} RETRY→QUEUED: ${data?.length || 0} jobs | ${error?.message || 'ok'}`);
  }
}

// Final summary
const { data: stats } = await supabase.from('importer_queue')
  .select('status').not('status', 'in', '("DONE","COMPLETED","SUPERSEDED","CANCELLED_BY_STAFF","FAILED")');
const counts = {};
for (const j of (stats || [])) counts[j.status] = (counts[j.status] || 0) + 1;
console.log('\n=== FINAL ACTIVE JOB STATE ===');
for (const [s, c] of Object.entries(counts)) console.log(`  ${s}: ${c}`);
