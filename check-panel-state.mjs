import { createClient } from '@supabase/supabase-js';
import { readFileSync } from 'fs';
const envRaw = readFileSync('/home/awerkori/.Projects/project-nox-importer/.env', 'utf8');
const env = {};
for (const line of envRaw.split('\n')) { const m = line.match(/^([A-Z_]+)=(.*)$/); if (m) env[m[1]] = m[2].trim(); }
const s = createClient(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY);

const { data } = await s.from('importer_sources').select('id, name, status, enabled, blocked_reason').order('status').order('id');

console.log('=== ALL SOURCES: id | status | enabled | blocked_reason[:50] ===');
for (const r of (data || [])) {
  console.log(`${r.id.padEnd(20)} | ${(r.status||'').padEnd(22)} | en=${r.enabled} | ${(r.blocked_reason||'').slice(0,50)}`);
}

// Jobs for affected sources
const targets = ['kuro','maidscan','mangaonline','acervohentai','amuy','arthurscan','inkapk','tiamanhwa','yaoifanclub','yuriverso','covenscan'];
const { data: jobs } = await s.from('importer_queue')
  .select('source, status')
  .in('source', targets)
  .not('status','in','("DONE","COMPLETED","SUPERSEDED","CANCELLED_BY_STAFF","FAILED")');
const jc = {};
for (const j of (jobs||[])) { jc[j.source] = jc[j.source]||{}; jc[j.source][j.status] = (jc[j.source][j.status]||0)+1; }
console.log('\n=== ACTIVE JOBS FOR AFFECTED SOURCES ===');
for (const [src, cnts] of Object.entries(jc)) console.log(`  ${src}: ${JSON.stringify(cnts)}`);
if (!Object.keys(jc).length) console.log('  (none)');
