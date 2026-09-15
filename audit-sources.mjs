// Comprehensive source audit script
import { createClient } from '@supabase/supabase-js';
import { readFileSync } from 'fs';

const envRaw = readFileSync('/home/awerkori/.Projects/project-nox-importer/.env', 'utf8');
const env = {};
for (const line of envRaw.split('\n')) { const m = line.match(/^([A-Z_]+)=(.*)$/); if (m) env[m[1]] = m[2].trim(); }
const supabase = createClient(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY);

// Test URL reachability with timeout
async function testUrl(url, headers = {}) {
  try {
    const ctrl = new AbortController();
    const t = setTimeout(() => ctrl.abort(), 8000);
    const res = await fetch(url, {
      method: 'HEAD',
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        ...headers
      },
      signal: ctrl.signal,
      redirect: 'follow'
    });
    clearTimeout(t);
    const hasCF = res.headers.get('server')?.includes('cloudflare') || false;
    return { status: res.status, cloudflare: hasCF };
  } catch(e) {
    return { status: 0, error: e.message.slice(0,50) };
  }
}

// Source test definitions
const sources = [
  // UPSTREAM_BLOCKED candidates (local 200 / DIScloud 403)
  { id: 'acervohentai', url: 'https://acervohentai.com' },
  { id: 'amuy', url: 'https://www.apenasmaisumyaoi.com' },
  { id: 'arthurscan', url: 'https://arthurscan.xyz' },
  { id: 'inkapk', url: 'https://inkapk.net' },
  { id: 'mangaonline', url: 'https://mangaonline.red' },
  { id: 'tiamanhwa', url: 'https://tiamanhwa.com' },
  { id: 'yaoifanclub', url: 'https://yaoifanclub.com' },
  { id: 'yuriverso', url: 'https://yurionair.top' },
  { id: 'kuro', url: 'https://kuromangas.com' },
  // Other problem sources
  { id: 'nocturnesummer', url: 'https://nocfsb.com' },
  { id: 'osakascan', url: 'https://www.osakascan.com' },
  { id: 'maidscan', url: 'https://empreguetes.wtf' },
  { id: 'mangalivreto', url: 'https://mangalivre.to' },
  { id: 'ninjascan', url: 'https://ninjacomics.xyz' },
  { id: 'borutoexplorer', url: 'https://leitor.borutoexplorer.com.br' },
  { id: 'fleurblanche', url: 'https://fbsquadx.com' },
  { id: 'littletyrant', url: 'https://tiraninha.world' },
  { id: 'pointzerotoons', url: 'https://kitsuneyako.com' },
  { id: 'megahentai', url: 'https://megahentai.biz' },
  { id: 'mrtenzus', url: 'https://mrtenzus.com' },
  { id: 'taimumangas', url: 'https://apiv2.taimumangas.com/api/v1/reader/updates?page=1&per_page=3&adult_mode=true' },
];

console.log('Testing all sources...\n');
const results = [];
for (const s of sources) {
  const r = await testUrl(s.url);
  results.push({ id: s.id, url: s.url, ...r });
  console.log(`${s.id}: status=${r.status} cf=${r.cloudflare} ${r.error || ''}`);
}

// Get current DB job counts per source for RETRY/HELD
const { data: jobs } = await supabase
  .from('importer_queue')
  .select('source, status')
  .in('status', ['RETRY', 'HELD', 'BLOCKED_BY_UPSTREAM', 'IMPORTING', 'QUEUED', 'PARKED']);

const counts = {};
for (const j of (jobs || [])) {
  if (!counts[j.source]) counts[j.source] = {};
  counts[j.source][j.status] = (counts[j.source][j.status] || 0) + 1;
}
console.log('\n=== JOB COUNTS BY SOURCE ===');
for (const [src, stats] of Object.entries(counts)) {
  console.log(`  ${src}: ${JSON.stringify(stats)}`);
}
