import crypto from 'crypto';

const BASE_URL = 'https://manga.project-nox-awerkori.workers.dev';
const TOKEN = 'FIzfNCLxC58nSriPHiXfo8PRYRz9zzKq';
const SECRET = 'prod-secret-9876543210-abcdef';

const sig = crypto.createHmac('sha256', SECRET).update(TOKEN).digest('base64');
const signedCookie = `${TOKEN}.${sig}`;
const cookieHeader = `better-auth.session_token=${signedCookie}; __Secure-better-auth.session_token=${signedCookie}`;

const routes = [
  { name: 'Admin Prioridades (/admin/importer/prioridades)', path: '/admin/importer/prioridades' },
  { name: 'Admin Erros (/admin/importer/erros)', path: '/admin/importer/erros' },
  { name: 'Admin Importer Home (/admin/importer)', path: '/admin/importer' },
  { name: 'User Profile (/me)', path: '/me' }
];

async function measure() {
  console.log('=== AUTHENTICATED ROUTE LATENCY MEASUREMENTS ===\n');

  for (const r of routes) {
    const times = [];
    let finalStatus = 0;
    let redirectedTo = null;

    // Run 5 requests per route to measure P50 and observe cache behavior
    for (let i = 0; i < 5; i++) {
      const start = performance.now();
      const res = await fetch(`${BASE_URL}${r.path}`, {
        headers: {
          'Cookie': cookieHeader,
          'User-Agent': 'NoxAuditor/1.0',
          'Accept': 'text/html'
        },
        redirect: 'manual'
      });
      const elapsed = performance.now() - start;
      times.push(elapsed);
      finalStatus = res.status;
      if (res.status === 303 || res.status === 302 || res.status === 301) {
        redirectedTo = res.headers.get('location');
      }
      await new Promise(res => setTimeout(res, 100));
    }

    times.sort((a, b) => a - b);
    const p50 = times[Math.floor(times.length / 2)].toFixed(1);
    const p95 = times[times.length - 1].toFixed(1);
    const min = times[0].toFixed(1);

    const redirectInfo = redirectedTo ? ` (Redirected to: ${redirectedTo})` : '';
    console.log(`${r.name.padEnd(50)} -> HTTP ${finalStatus}${redirectInfo} | P50: ${p50}ms | Min: ${min}ms | Max: ${p95}ms`);
  }
}

measure().catch(console.error);
