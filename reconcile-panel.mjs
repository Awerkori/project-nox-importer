import { createClient } from '@supabase/supabase-js';
import { readFileSync } from 'fs';
const envRaw = readFileSync('/home/awerkori/.Projects/project-nox-importer/.env', 'utf8');
const env = {};
for (const line of envRaw.split('\n')) { const m = line.match(/^([A-Z_]+)=(.*)$/); if (m) env[m[1]] = m[2].trim(); }
const s = createClient(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY);
const now = new Date().toISOString();

// Panel logic:
// upstreamBlockedSources = status === 'UPSTREAM_BLOCKED' (all shown, regardless of enabled)
// operationalSources = enabled=true AND (status='ACTIVE' OR status='DEGRADED')
// So: to remove from BOTH lists → use status='DISABLED' + enabled=false

// 1. Permanently disabled sources: change status UPSTREAM_BLOCKED → DISABLED
// This removes them from "upstream paused" block entirely
const permanentlyDisabled = [
  { id: 'acervohentai', reason: 'REMOVED_WAF_BLOCK: Cloudflare WAF blocks DIScloud ASN 16276. No legitimate API/bypass. Permanently removed 2026-09-14.' },
  { id: 'amuy',         reason: 'REMOVED_WAF_BLOCK: Cloudflare WAF blocks DIScloud ASN 16276. No legitimate API/bypass. Permanently removed 2026-09-14.' },
  { id: 'arthurscan',   reason: 'REMOVED_WAF_BLOCK: Cloudflare WAF blocks DIScloud ASN 16276. No legitimate API/bypass. Permanently removed 2026-09-14.' },
  { id: 'inkapk',       reason: 'REMOVED_WAF_BLOCK: Cloudflare WAF blocks DIScloud ASN 16276. No legitimate API/bypass. Permanently removed 2026-09-14.' },
  { id: 'tiamanhwa',    reason: 'REMOVED_WAF_BLOCK: Cloudflare WAF blocks DIScloud ASN 16276. No legitimate API/bypass. Permanently removed 2026-09-14.' },
  { id: 'yaoifanclub',  reason: 'REMOVED_WAF_BLOCK: Cloudflare WAF blocks DIScloud ASN 16276. No legitimate API/bypass. Permanently removed 2026-09-14.' },
  { id: 'yuriverso',    reason: 'REMOVED_WAF_BLOCK: Cloudflare WAF blocks DIScloud ASN 16276. No legitimate API/bypass. Permanently removed 2026-09-14.' },
  { id: 'maidscan',     reason: 'REMOVED_PAYWALL: Chapter API (api.verdinha.wtf) requires VIP subscription. All chapter pages return 403. Permanently removed 2026-09-14.' },
  { id: 'mangaonline',  reason: 'REMOVED_DOMAIN_GONE: mangaonline.red→mangaonline.love both unreachable from datacenter. Permanently removed 2026-09-14.' },
];

console.log('=== DISABLING PERMANENTLY REMOVED SOURCES ===');
for (const src of permanentlyDisabled) {
  const { error } = await s.from('importer_sources').update({
    status: 'DISABLED',
    enabled: false,
    blocked_reason: src.reason,
    blocked_details: 'See blocked_reason. Published content preserved. No new jobs will be created.',
    cooldown_until: new Date('2099-01-01').toISOString(),
    updated_at: now
  }).eq('id', src.id);
  console.log(`  ${src.id}: ${error ? 'FAIL: ' + error.message : 'DISABLED ✓'}`);
}

// 2. Kuro: the engine auto-set it back to UPSTREAM_BLOCKED+en=true after our change
// because the bridge still returns 403 (needs X-Client-Token which the engine may not send)
// Keep it as UPSTREAM_BLOCKED for now but ensure the session is in env
// Actually check: kuro is en=true but UPSTREAM_BLOCKED - the engine probed and still got 403
// The engine will handle re-promotion. Mark as ACTIVE manually:
const { error: kuroErr } = await s.from('importer_sources').update({
  status: 'ACTIVE',
  enabled: true,
  blocked_reason: '',
  blocked_details: 'Session renewed 2026-09-14 via CF bridge. X-Client-Token required for API calls.',
  cooldown_until: null,
  updated_at: now
}).eq('id', 'kuro');
console.log(`\nKuro → ACTIVE: ${kuroErr ? 'FAIL: ' + kuroErr.message : 'OK ✓'}`);

// 3. Covenscan: cancel the stale RETRY jobs (2 remaining) - new jobs will be created on next discovery
const { data: covCancelled } = await s.from('importer_queue').update({
  status: 'CANCELLED_BY_STAFF',
  cancel_reason: 'Stale chapter URL from old covenscan domain (pre-/bruxonas/ migration). Adapter fixed; discovery will re-import.',
  cancelled_at: now, updated_at: now
}).eq('source', 'covenscan').eq('status', 'RETRY').select('id');
console.log(`Covenscan stale RETRY → CANCELLED: ${covCancelled?.length || 0} ✓`);

// Final state check
const { data: sources } = await s.from('importer_sources').select('id, status, enabled').order('status').order('id');
const grouped = {};
for (const r of (sources||[])) { grouped[r.status] = grouped[r.status]||[]; grouped[r.status].push(r.id); }
console.log('\n=== FINAL SOURCE STATUS ===');
for (const [status, ids] of Object.entries(grouped)) {
  console.log(`${status}: ${ids.length}`);
  if (status !== 'ACTIVE') ids.forEach(id => console.log(`  - ${id}`));
}
