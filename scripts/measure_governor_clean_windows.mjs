import pg from 'pg';
import https from 'https';
import http from 'http';
import { performance } from 'perf_hooks';

const EMBEDDED_YUGABYTE_CA = `-----BEGIN CERTIFICATE-----
MIIGxDCCBKygAwIBAgITF8MQH/VpOvqeGxybP3vBoEPj0TANBgkqhkiG9w0BAQsF
ADCBhDELMAkGA1UEBhMCVVMxCzAJBgNVBAgTAkNBMRIwEAYDVQQHEwlTdW5ueXZh
bGUxFTATBgNVBAoTDFl1Z2FieXRlIEluYzEXMBUGA1UECxMOWXVnYWJ5dGUgQ2xv
dWQxJDAiBgNVBAMTG1l1Z2FieXRlIENsb3VkIFJvb3QgQ0EgcHJvZDAeFw0yNTA5
MTgxNjIzMjBaFw0zMDA5MTcxNjIzMTlaMIGWMQswCQYDVQQGEwJVUzELMAkGA1UE
CBMCQ0ExEjAQBgNVBAcTCVN1bm55dmFsZTEXMBUGA1UEChMOWXVnYWJ5dGVEQiBJ
bmMxIDAeBgNVBAsTF1l1Z2FieXRlREIgQWVvbiBNYW5hZ2VkMSswKQYDVQQDEyJZ
dWdhYnl0ZSBDbG91ZCBTdWJvcmRpbmF0ZSBDQSBwcm9kMIIBojANBgkqhkiG9w0B
AQEFAAOCAY8AMIIBigKCAYEAl1CO4UpzZYYDqVuzLUyhNEah0a0VMNzgYQkCxXGN
QOqA0sp60bfJiAaUe1fO1A/VYSur3kpuxWk3/CxBrR6RTsZKpFy1XgeK6McecWUV
ACOtINMwrmHYg0hb032bPSvwDweTddkG9wzuD4Md9+5FK6ebjDhq/kPwSQHhG1lS
qygAcwRJgHcb3Ad6ZrtG5hYNG2Ikt86ipIV/6zgwHYoaDjI4u5Nl5Gqblkea2yn6
M+Z0AM0u+k6FsMkqu9IMXYC466ZTNXHB4pMLVgRkiUvLnUnA/nROGTHmi1ivLGDO
TvKPlEkVvbdZYbutG9isGUg61Fhqb1hRISPdC1uxD4RBoooIBHoOP27gWcdVa+qZ
Q/N6oYy6Yw3R58d0JJrjE5BwppAK9q4EMlRIj9JEbQIyBhtbkqPn+PwwvUHRFavY
s3BKmx2BSuYWaDCSmbDSOLxuf2bzaHIwrhlBgDhWwtVl4F5W1tzDV7FPl19vkl77
UPQo87d3C55rofMJ9BRsdqHbAgMBAAGjggGZMIIBlTAOBgNVHQ8BAf8EBAMCAQYw
HQYDVR0lBBYwFAYIKwYBBQUHAwEGCCsGAQUFBwMCMA8GA1UdEwEB/wQFMAMBAf8w
HQYDVR0OBBYEFLnilBT2qOKcpt+IED4inSpiiLCnMB8GA1UdIwQYMBaAFGCPfEqV
jRfctFj3jSsv+QkB/hI7MIGNBggrBgEFBQcBAQSBgDB+MHwGCCsGAQUFBzAChnBo
dHRwOi8vcHJpdmF0ZWNhLWNvbnRlbnQtNjExZTIyZjYtMDAwMC0yMGM5LWE2NTAt
ZDRmNTQ3ZjdlZTFjLnN0b3JhZ2UuZ29vZ2xlYXBpcy5jb20vYWExNDA4ODNkNGE3
YTJjMDBhMDkvY2EuY3J0MIGCBgNVHR8EezB5MHegdaBzhnFodHRwOi8vcHJpdmF0
ZWNhLWNvbnRlbnQtNjExZTIyZjYtMDAwMC0yMGM5LWE2NTAtZDRmNTQ3ZjdlZTFj
LnN0b3JhZ2UuZ29vZ2xlYXBpcy5jb20vYWExNDA4ODNkNGE3YTJjMDBhMDkvY3Js
LmNybDANBgkqhkiG9w0BAQsFAAOCAgEAt9HGIX1Btl0Fb70GpySsxv94632Kvs6G
1zTL21lpAnLmngezaGOuOzR/swwCIfHj3rf+wFKY6HQ371pLgEUvfe3Ve5aVKIs/
krvGzUIJybaMIZ2rtO88C+X+a+LInRJLJMhJ366X+jTNURLV2S2twCQYDnodyYWA
6z+Fq5fPs+O5khj6qCork6tyEgV9F1bLJNRatNRIYBOhif6GeR3bFOBoSb++d1KB
OrqB5fMIXnqcwV7Zjz8goaWQgeWf1VTyAeavUfKLmZl/SzmKfOGUeux5ezZk77KA
beXE/wMCCARVeV4LPf4nOIcJIciF94A2kZuFpyJmM0VVKbcGuSQe58HxVS3iwmN6
GHGegOqDy8yWknbrjADDyYzJvhUJciKN/Nu0XarXvCSR84BnNfKLB0zBNIUAlAUq
iT3NnyYZYlhD/lHZOtFRphyOQXU6VVkRrRjXer7LF9b7BWYaly1f3FzQf9gwoPyO
sprdOhIsaGIbAzx/hoOgUg28HNyfwih6pI80vGqRg2Zgri8Qb8cOSXVkb6Y0Voic
Y9KaDmilwQAALxGW/H/ScXNExKvWy5o7+H4v3Id/Dr026n1wPE3qOlERoaFmBj0U
+E5UevW1SOwyBjS3XR29cOz89FDIq418DGVpSex2q26ALmNX9E/A0XCPP2bk5lFX
hDzC5OY3o1A=
-----END CERTIFICATE-----
-----BEGIN CERTIFICATE-----
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
-----END CERTIFICATE-----`;

const pool = new pg.Pool({
  host: process.env.YUGABYTE_HOST || 'us-east-1.1cfa7350-48e0-47a3-83eb-6ebec7bbef80.aws.ybdb.io',
  port: parseInt(process.env.YUGABYTE_PORT || '5433', 10),
  database: process.env.YUGABYTE_DATABASE || 'yugabyte',
  user: process.env.YUGABYTE_USER || 'admin',
  password: process.env.YUGABYTE_PASSWORD || 'O-u4b8A*lV1i3f4A#qU5',
  ssl: {
    rejectUnauthorized: true,
    ca: EMBEDDED_YUGABYTE_CA,
  },
  max: 3,
});

async function probeUrl(url) {
  return new Promise((resolve) => {
    const t0 = performance.now();
    const req = https.get(url, { headers: { 'User-Agent': 'Project-Nox-Auditor/1.0' }, timeout: 10000 }, (res) => {
      let data = '';
      res.on('data', chunk => { data += chunk; });
      res.on('end', () => {
        const ttfb = Math.round(performance.now() - t0);
        resolve({ statusCode: res.statusCode, ttfb });
      });
    });
    req.on('error', err => resolve({ statusCode: 0, ttfb: 9999, error: err.message }));
    req.on('timeout', () => { req.destroy(); resolve({ statusCode: 0, ttfb: 10000, error: 'timeout' }); });
  });
}

function percentile(arr, p) {
  if (arr.length === 0) return 0;
  const sorted = [...arr].sort((a, b) => a - b);
  const idx = Math.min(sorted.length - 1, Math.floor(sorted.length * p));
  return sorted[idx];
}

async function run() {
  const now = new Date();
  console.log(`Auditing live production metrics at ${now.toISOString()}...`);

  // 1. Fresh publications in clean windows
  const cleanWindowsRes = await pool.query(`
    SELECT
      count(*) FILTER (WHERE published_at >= NOW() - INTERVAL '1 minute') as fresh_1m,
      count(*) FILTER (WHERE published_at >= NOW() - INTERVAL '3 minute') as fresh_3m,
      count(*) FILTER (WHERE published_at >= NOW() - INTERVAL '5 minute') as fresh_5m,
      count(*) FILTER (WHERE published_at >= NOW() - INTERVAL '10 minute') as fresh_10m,
      count(*) FILTER (WHERE published_at >= NOW() - INTERVAL '15 minute') as fresh_15m,
      count(*) FILTER (WHERE published_at >= NOW() - INTERVAL '30 minute') as fresh_30m,
      MAX(published_at) as last_published_at
    FROM chapters
    WHERE is_fresh_release = true
  `);

  const cw = cleanWindowsRes.rows[0];

  // 2. Settings table heartbeat
  const hbRes = await pool.query(`SELECT value FROM settings WHERE key = 'importer_health_panel'`);
  let hb = {};
  if (hbRes.rows.length > 0 && hbRes.rows[0].value) {
    try {
      hb = typeof hbRes.rows[0].value === 'string' ? JSON.parse(hbRes.rows[0].value) : hbRes.rows[0].value;
    } catch {}
  }

  // 3. Auto emergency pause setting
  const pauseRes = await pool.query(`SELECT value FROM settings WHERE key = 'importer_auto_emergency_pause'`);
  let pauseState = {};
  if (pauseRes.rows.length > 0 && pauseRes.rows[0].value) {
    try {
      pauseState = typeof pauseRes.rows[0].value === 'string' ? JSON.parse(pauseRes.rows[0].value) : pauseRes.rows[0].value;
    } catch {}
  }

  // 4. Probes for Site Home and Reader
  const homeProbes = [];
  const readerProbes = [];

  // Probe home 5 times
  for (let i = 0; i < 5; i++) {
    const res = await probeUrl('https://manga.nox.com.br/');
    homeProbes.push(res.ttfb);
    await new Promise(r => setTimeout(r, 200));
  }

  // Get a recent chapter for reader probe
  const chRes = await pool.query(`SELECT id FROM chapters WHERE published_at IS NOT NULL ORDER BY published_at DESC LIMIT 1`);
  const chapterId = chRes.rows[0]?.id;

  if (chapterId) {
    for (let i = 0; i < 5; i++) {
      const res = await probeUrl(`https://manga.nox.com.br/ler/${chapterId}`);
      readerProbes.push(res.ttfb);
      await new Promise(r => setTimeout(r, 200));
    }
  }

  const homeP95 = percentile(homeProbes, 0.95);
  const readerP95 = percentile(readerProbes, 0.95);

  console.log(JSON.stringify({
    snapshotAt: now.toISOString(),
    cleanWindows: {
      fresh_1m: parseInt(cw.fresh_1m, 10),
      fresh_3m: parseInt(cw.fresh_3m, 10),
      fresh_5m: parseInt(cw.fresh_5m, 10),
      fresh_10m: parseInt(cw.fresh_10m, 10),
      fresh_15m: parseInt(cw.fresh_15m, 10),
      fresh_30m: parseInt(cw.fresh_30m, 10),
      last_published_at: cw.last_published_at,
    },
    heartbeat: {
      status: hb.status,
      capacity: hb.capacity,
      autoEmergencyPause: hb.autoEmergencyPause || pauseState,
      throughput: hb.throughput,
    },
    siteProbes: {
      homeProbes,
      homeP95,
      readerProbes,
      readerP95,
    }
  }, null, 2));

  await pool.end();
}

run().catch(err => {
  console.error(err);
  process.exit(1);
});
