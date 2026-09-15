import { createClient } from '@supabase/supabase-js';
import { readFileSync } from 'fs';
const envRaw = readFileSync('/home/awerkori/.Projects/project-nox-importer/.env', 'utf8');
const env = {};
for (const line of envRaw.split('\n')) { const m = line.match(/^([A-Z_]+)=(.*)$/); if (m) env[m[1]] = m[2].trim(); }
const supabase = createClient(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY);

// Sources confirmed: local 200, DIScloud Cloudflare WAF 403
// No legitimate bypass available (no public API, no alternative domain, no auth mechanism)
// Policy: keep UPSTREAM_BLOCKED + disable + cancel pending jobs
const toRemove = [
  { id: 'acervohentai', reason: 'Cloudflare WAF blocks DIScloud ASN 16276. No public API or alternative endpoint available.' },
  { id: 'amuy', reason: 'Cloudflare WAF blocks DIScloud ASN. No public API available.' },
  { id: 'arthurscan', reason: 'Cloudflare WAF blocks DIScloud ASN. No public API available.' },
  { id: 'inkapk', reason: 'Cloudflare WAF blocks DIScloud ASN. No public API available.' },
  { id: 'tiamanhwa', reason: 'Cloudflare WAF blocks DIScloud ASN. No public API available.' },
  { id: 'yaoifanclub', reason: 'Cloudflare WAF blocks DIScloud ASN. No public API available.' },
  { id: 'yuriverso', reason: 'Cloudflare WAF blocks DIScloud ASN. No public API available.' },
];

for (const s of toRemove) {
  // Mark as UPSTREAM_BLOCKED + disabled
  const { error: e1 } = await supabase
    .from('importer_sources')
    .update({
      status: 'UPSTREAM_BLOCKED',
      enabled: false,
      blocked_reason: 'REMOVED: ' + s.reason,
      blocked_details: 'Permanently disabled 2026-09-14. Published content preserved.',
      cooldown_until: new Date('2030-01-01').toISOString(), // Far future = no reprobe
      updated_at: new Date().toISOString()
    })
    .eq('id', s.id);

  if (e1) { console.log(`FAIL disabling ${s.id}:`, e1.message); continue; }

  // Cancel all pending jobs for this source
  const { data: cancelled, error: e2 } = await supabase
    .from('importer_queue')
    .update({
      status: 'CANCELLED_BY_STAFF',
      cancel_reason: 'Source permanently removed: upstream blocked by Cloudflare WAF on datacenter ASN',
      cancelled_at: new Date().toISOString()
    })
    .eq('source', s.id)
    .in('status', ['QUEUED', 'RETRY', 'HELD', 'PARKED', 'BLOCKED_BY_UPSTREAM'])
    .select('id');

  console.log(`OK: ${s.id} disabled | ${cancelled?.length || 0} jobs cancelled`);
}

// Final summary
const { data: sources } = await supabase
  .from('importer_sources')
  .select('id, status, enabled')
  .order('status').order('id');

const grouped = {};
for (const s of (sources || [])) grouped[s.status] = (grouped[s.status] || 0) + 1;
console.log('\n=== FINAL SOURCE COUNTS ===');
for (const [s, c] of Object.entries(grouped)) console.log(`  ${s}: ${c}`);

const disabled = (sources || []).filter(s => !s.enabled);
console.log('\nDisabled sources:', disabled.map(s => s.id).join(', '));
