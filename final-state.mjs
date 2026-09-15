import { createClient } from '@supabase/supabase-js';
import { readFileSync } from 'fs';
const envRaw = readFileSync('/home/awerkori/.Projects/project-nox-importer/.env', 'utf8');
const env = {};
for (const line of envRaw.split('\n')) { const m = line.match(/^([A-Z_]+)=(.*)$/); if (m) env[m[1]] = m[2].trim(); }
const s = createClient(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY);

const { data: src } = await s.from('importer_sources').select('id, status, enabled').order('status').order('id');

const op = src.filter(r => r.enabled && (r.status === 'ACTIVE' || r.status === 'DEGRADED'));
const upstream = src.filter(r => r.status === 'UPSTREAM_BLOCKED');
const excl = src.filter(r => r.status === 'EXCLUDED_BY_POLICY');
const disabled = src.filter(r => r.status === 'DISABLED');

console.log('=== PANEL WILL SHOW ===');
console.log(`Operational (ACTIVE+enabled): ${op.length}`);
console.log(`Upstream Blocked block: ${upstream.length} → ${upstream.map(r=>r.id).join(', ')||'(none)'}`);
console.log(`Excluded by Policy: ${excl.length} → ${excl.map(r=>r.id).join(', ')}`);
console.log(`DISABLED (hidden from panel): ${disabled.length} → ${disabled.map(r=>r.id).join(', ')}`);

// Kuro specific
const kuro = src.find(r => r.id === 'kuro');
console.log(`\nKuro: status=${kuro?.status} enabled=${kuro?.enabled}`);

// Job counts
const { data: qc } = await s.rpc('admin_importer_queue_counts').catch(() => ({ data: null }));
if (qc) {
  console.log(`\nQueue: queued=${qc.queued} retry=${qc.retry} importing=${qc.importing} failed=${qc.failed}`);
} else {
  // Fallback manual count
  const statuses = ['QUEUED','RETRY','IMPORTING','FAILED','BLOCKED_BY_UPSTREAM'];
  for (const st of statuses) {
    const { count } = await s.from('importer_queue').select('*', {count:'exact',head:true}).eq('status', st);
    if (count) console.log(`  ${st}: ${count}`);
  }
}

// Covenscan RETRY check
const { count: covRetry } = await s.from('importer_queue').select('*',{count:'exact',head:true}).eq('source','covenscan').eq('status','RETRY');
const { count: covQueued } = await s.from('importer_queue').select('*',{count:'exact',head:true}).eq('source','covenscan').eq('status','QUEUED');
console.log(`\nCovenscan: ${covQueued} QUEUED, ${covRetry} RETRY`);
