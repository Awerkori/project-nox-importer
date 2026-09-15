import { createClient } from '@supabase/supabase-js';
import { readFileSync } from 'fs';
const envRaw = readFileSync('/home/awerkori/.Projects/project-nox-importer/.env', 'utf8');
const env = {};
for (const line of envRaw.split('\n')) { const m = line.match(/^([A-Z_]+)=(.*)$/); if (m) env[m[1]] = m[2].trim(); }
const supabase = createClient(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY);

const now = new Date().toISOString();

// Block maidscan: API requires VIP auth for all chapters (paywall)
const { error: e1 } = await supabase.from('importer_sources').update({
  status: 'UPSTREAM_BLOCKED',
  enabled: false,
  blocked_reason: 'PAYWALL: All chapter pages now require VIP authentication (api.verdinha.wtf 403 for all /capitulos endpoints)',
  blocked_details: 'Verified 2026-09-14: GET /capitulos/{any_id} returns HTTP 403 + "É necessário estar autenticado e ter VIP ativo". No legitimate bypass.',
  cooldown_until: new Date('2030-01-01').toISOString(),
  updated_at: now
}).eq('id', 'maidscan');
console.log('maidscan blocked:', e1?.message || 'ok');

// Cancel all pending QUEUED maidscan jobs (they will all fail with 403)
const { data: cancelled, error: e2 } = await supabase.from('importer_queue')
  .update({
    status: 'CANCELLED_BY_STAFF',
    cancel_reason: 'Source maidscan blocked: chapter API now requires VIP payment (verified 2026-09-14)',
    cancelled_at: now, updated_at: now
  })
  .eq('source', 'maidscan')
  .in('status', ['QUEUED', 'RETRY'])
  .select('id');
console.log(`maidscan QUEUED/RETRY→CANCELLED: ${cancelled?.length || 0} | ${e2?.message || 'ok'}`);

// Summary
const { count: queuedCount } = await supabase.from('importer_queue')
  .select('*', { count: 'exact', head: true }).eq('status', 'RETRY');
console.log('Remaining RETRY jobs total:', queuedCount);
