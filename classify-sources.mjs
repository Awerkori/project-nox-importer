import { createClient } from '@supabase/supabase-js';
import { readFileSync } from 'fs';

const envRaw = readFileSync('/home/awerkori/.Projects/project-nox-importer/.env', 'utf8');
const env = {};
for (const line of envRaw.split('\n')) { const m = line.match(/^([A-Z_]+)=(.*)$/); if (m) env[m[1]] = m[2].trim(); }
const supabase = createClient(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY);

// Apply correct classifications
const updates = [
  // mangaonline: domain changed, new domain also down — TEMPORARILY_UNAVAILABLE
  {
    id: 'mangaonline',
    status: 'TEMPORARILY_UNAVAILABLE',
    blocked_reason: 'Domain migrated to mangaonline.love, currently unreachable',
    blocked_details: 'mangaonline.red redirects to mangaonline.love but connection times out (tested 2026-09-14)'
  },
  // kuro: UPSTREAM_BLOCKED confirmed - site needs CF clearance, bridge exists but session expired
  // Keep as-is: UPSTREAM_BLOCKED + enabled=false
];

let success = 0, fail = 0;
for (const u of updates) {
  const { error } = await supabase
    .from('importer_sources')
    .update({
      status: u.status,
      blocked_reason: u.blocked_reason,
      blocked_details: u.blocked_details,
      updated_at: new Date().toISOString()
    })
    .eq('id', u.id);
  
  if (error) {
    console.log(`FAIL updating ${u.id}:`, error.message);
    fail++;
  } else {
    console.log(`OK: ${u.id} → ${u.status}`);
    success++;
  }
}

// Cancel stale SUPERSEDED jobs for taimumangas to clean up
const { data: superseded } = await supabase
  .from('importer_queue')
  .select('id')
  .eq('status', 'SUPERSEDED');

if (superseded?.length) {
  console.log(`\nFound ${superseded.length} SUPERSEDED jobs`);
  // These are already terminal, no cleanup needed
}

console.log(`\nDone: ${success} updated, ${fail} failed`);

// Final state summary
const { data: sources } = await supabase
  .from('importer_sources')
  .select('id, status, enabled')
  .order('status').order('id');

const grouped = {};
for (const s of (sources || [])) {
  grouped[s.status] = (grouped[s.status] || 0) + 1;
}
console.log('\n=== FINAL STATUS COUNTS ===');
for (const [s, c] of Object.entries(grouped)) console.log(`  ${s}: ${c}`);
