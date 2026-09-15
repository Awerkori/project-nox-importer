import { createClient } from '@supabase/supabase-js';
import { readFileSync } from 'fs';
const envRaw = readFileSync('/home/awerkori/.Projects/project-nox-importer/.env', 'utf8');
const env = {};
for (const line of envRaw.split('\n')) { const m = line.match(/^([A-Z_]+)=(.*)$/); if (m) env[m[1]] = m[2].trim(); }
const supabase = createClient(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY);

// Try to update mangaonline with each possible status until one works
const statuses = [
  'ACTIVE', 'PAUSED', 'COOLDOWN', 'DISABLED', 'UPSTREAM_BLOCKED',
  'EXCLUDED_BY_POLICY', 'DEGRADED', 'RECOVERING', 'TEMPORARILY_UNAVAILABLE'
];

for (const status of statuses) {
  const { error } = await supabase
    .from('importer_sources')
    .update({ status })
    .eq('id', 'mangaonline')
    .eq('id', 'mangaonline__NEVER_MATCH'); // Won't actually change, just tests constraint
  
  if (!error) {
    console.log(`  VALID (no error): ${status}`);
  } else if (error.message.includes('check constraint')) {
    console.log(`  INVALID (constraint): ${status}`);
  } else if (error.message.includes('0 rows')) {
    console.log(`  VALID (no rows matched): ${status}`);
  } else {
    console.log(`  OTHER: ${status} - ${error.message.slice(0,50)}`);
  }
}

// Now try the REAL update for mangaonline to UPSTREAM_BLOCKED (it already is that)
// and set enabled=false + cooldown
const { error: e2, data } = await supabase
  .from('importer_sources')
  .update({
    status: 'UPSTREAM_BLOCKED',
    enabled: false,
    blocked_reason: 'Domain migrated to mangaonline.love, both unreachable from datacenter',
    blocked_details: 'mangaonline.red redirects to mangaonline.love which times out (verified 2026-09-14)',
    cooldown_until: new Date(Date.now() + 24 * 60 * 60 * 1000).toISOString()
  })
  .eq('id', 'mangaonline')
  .select();

console.log('\nMangaonline update:', e2 ? 'ERROR: ' + e2.message : 'OK', data?.[0]?.status, data?.[0]?.enabled);
