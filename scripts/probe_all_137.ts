import fs from 'fs';
import path from 'path';

const ptDir = '/home/awerkori/.Projects/Project-Nox/fonte-extensoes/src/pt';
const dirs = fs.readdirSync(ptDir).filter(d => fs.statSync(path.join(ptDir, d)).isDirectory()).sort();

export interface ExtMeta {
  id: string;
  name: string;
  theme: string;
  baseUrl: string;
  isNsfw: boolean;
  statusCode?: number;
  probeResult?: string;
  detail?: string;
  title?: string;
}

const allMeta: ExtMeta[] = [];

for (const d of dirs) {
  const gradlePath = path.join(ptDir, d, 'build.gradle.kts');
  let name = d;
  let theme = 'custom';
  let baseUrl = '';
  let isNsfw = false;

  if (fs.existsSync(gradlePath)) {
    const c = fs.readFileSync(gradlePath, 'utf8');
    const mName = c.match(/name\s*=\s*\"([^\"]+)\"/);
    if (mName) name = mName[1];
    const mTheme = c.match(/theme\s*=\s*\"([^\"]+)\"/);
    if (mTheme) theme = mTheme[1];
    const mUrl = c.match(/baseUrl\s*=\s*\"([^\"]+)\"/) || c.match(/custom\(\"([^\"]+)\"\)/);
    if (mUrl) baseUrl = mUrl[1];
    if (c.includes('NSFW')) isNsfw = true;
  }

  allMeta.push({ id: d, name, theme, baseUrl, isNsfw });
}

async function probeOne(meta: ExtMeta): Promise<ExtMeta> {
  if (!meta.baseUrl) {
    meta.probeResult = 'NO_URL';
    meta.detail = 'Nenhuma URL definida';
    return meta;
  }
  try {
    const res = await fetch(meta.baseUrl, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
        'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7'
      },
      signal: AbortSignal.timeout(6000),
      redirect: 'follow'
    });
    meta.statusCode = res.status;
    const body = await res.text().catch(() => '');
    const titleMatch = body.match(/<title>(.*?)<\/title>/i);
    const title = titleMatch ? titleMatch[1].trim() : '';
    meta.title = title;

    if (res.status === 200) {
      if (title.includes('Just a moment...') || title.includes('Attention Required!') || (body.toLowerCase().includes('cloudflare') && body.toLowerCase().includes('challenge'))) {
        meta.probeResult = 'CF_CHALLENGE';
        meta.detail = 'Cloudflare challenge (' + title + ')';
      } else {
        meta.probeResult = '200_OK';
        meta.detail = 'OK (' + title.slice(0, 30) + ')';
      }
    } else if (res.status === 403) {
      meta.probeResult = 'HTTP_403_BLOCKED';
      meta.detail = 'Cloudflare/WAF 403 Forbidden';
    } else if (res.status === 404) {
      meta.probeResult = 'HTTP_404_NOT_FOUND';
      meta.detail = 'HTTP 404 Not Found';
    } else if (res.status >= 500) {
      meta.probeResult = 'HTTP_' + res.status + '_SERVER_ERROR';
      meta.detail = 'Server Error ' + res.status;
    } else {
      meta.probeResult = 'HTTP_' + res.status;
      meta.detail = 'HTTP ' + res.status;
    }
  } catch (err: any) {
    const msg = String(err.message || err);
    if (msg.includes('aborted') || msg.includes('timeout')) {
      meta.probeResult = 'TIMEOUT';
      meta.detail = 'Timeout (>6s)';
    } else if (msg.includes('ENOTFOUND') || msg.includes('getaddrinfo')) {
      meta.probeResult = 'DNS_NXDOMAIN';
      meta.detail = 'DNS NXDOMAIN (domínio extinto)';
    } else if (msg.includes('ECONNREFUSED')) {
      meta.probeResult = 'CONN_REFUSED';
      meta.detail = 'Connection refused';
    } else if (msg.includes('certificate') || msg.includes('SSL') || msg.includes('ERR_TLS_CERT')) {
      meta.probeResult = 'SSL_ERROR';
      meta.detail = 'SSL Error: ' + msg.slice(0, 25);
    } else {
      meta.probeResult = 'NET_ERROR';
      meta.detail = msg.slice(0, 30);
    }
  }
  return meta;
}

async function probeAll(items: ExtMeta[], concurrency = 20): Promise<ExtMeta[]> {
  const results: ExtMeta[] = [];
  let idx = 0;
  async function worker() {
    while (idx < items.length) {
      const current = items[idx++];
      results.push(await probeOne(current));
    }
  }
  await Promise.all(Array.from({ length: concurrency }, () => worker()));
  return results;
}

async function main() {
  console.log(`Probing ${allMeta.length} candidates concurrently with native fetch...`);
  const start = Date.now();
  const probed = await probeAll(allMeta, 25);
  console.log(`Completed in ${Date.now() - start}ms`);

  fs.writeFileSync('scripts/probe_137_precise.json', JSON.stringify(probed, null, 2));

  const summary: Record<string, number> = {};
  for (const p of probed) {
    const r = p.probeResult || 'UNKNOWN';
    summary[r] = (summary[r] || 0) + 1;
  }

  console.log('\nResults summary:');
  for (const [k, v] of Object.entries(summary).sort((a, b) => b[1] - a[1])) {
    console.log(`  ${k}: ${v}`);
  }
}

main().catch(console.error);
