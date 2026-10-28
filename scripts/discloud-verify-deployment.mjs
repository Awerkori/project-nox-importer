import { setTimeout as sleep } from 'node:timers/promises';

const token = process.env.DISCLOUD_TOKEN;
const appId = process.env.DISCLOUD_APP_ID || process.argv[2];
const isTeam = (process.env.DISCLOUD_IS_TEAM || process.argv[3]) === 'true';

if (!token || !token.trim()) {
  console.error('::error::DISCLOUD_TOKEN is missing or empty.');
  process.exit(1);
}

if (!appId || !appId.trim()) {
  console.error('::error::Target Discloud App ID is missing.');
  process.exit(1);
}

const headers = {
  'api-token': token.trim(),
  'User-Agent': 'project-nox-importer-ci-verify',
};

const baseUrl = isTeam
  ? `https://api.discloud.app/v2/team/${appId}`
  : `https://api.discloud.app/v2/app/${appId}`;

console.log(`[Discloud Verify] Verifying deployment for App ID: ${appId} (Team Mode: ${isTeam}) ...`);

async function fetchJson(url) {
  try {
    const res = await fetch(url, { headers });
    const text = await res.text();
    try {
      return { status: res.status, ok: res.ok, data: JSON.parse(text) };
    } catch {
      return { status: res.status, ok: false, error: text };
    }
  } catch (err) {
    return { status: 0, ok: false, error: err.message };
  }
}

async function verify() {
  // Step 1: Poll status until the container reports Online (up to 75 seconds)
  let isOnline = false;
  let lastStatus = 'unknown';
  const maxAttempts = 15;

  console.log('[Discloud Verify] Step 1: Polling application status on Discloud API ...');
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    const res = await fetchJson(`${baseUrl}/status`);
    if (res.ok && res.data) {
      const app = res.data.apps || res.data;
      lastStatus = app.status || (app.online ? 'Online' : 'Offline');
      console.log(`  [Attempt ${attempt}/${maxAttempts}] Status: "${lastStatus}", Online: ${app.online}, RAM: ${app.ram || 'unknown'}`);
      if (app.online === true || lastStatus.toLowerCase() === 'online') {
        isOnline = true;
        break;
      }
    } else {
      console.log(`  [Attempt ${attempt}/${maxAttempts}] HTTP ${res.status}:`, JSON.stringify(res.data || res.error));
    }
    await sleep(5000);
  }

  if (!isOnline) {
    console.warn(`[Discloud Verify] Warning: Container did not report Online within timeout (last status: ${lastStatus}).`);
  } else {
    console.log(`[Discloud Verify] Application confirmed Online on Discloud.`);
  }

  // Step 2: Fetch and verify recent container logs
  console.log('\n[Discloud Verify] Step 2: Checking application terminal logs ...');
  const logsRes = await fetchJson(`${baseUrl}/logs`);
  if (logsRes.ok && logsRes.data) {
    const terminal = logsRes.data.apps?.terminal || logsRes.data.terminal || {};
    const logContent = terminal.big || terminal.small || '';
    if (logContent) {
      const lines = logContent.split('\n').filter(l => l.trim().length > 0);
      console.log(`[Discloud Verify] Recent terminal output (${Math.min(lines.length, 12)} lines):`);
      for (const line of lines.slice(-12)) {
        console.log(`  | ${line}`);
      }
      // Check for immediate crash indicators
      const tailText = lines.slice(-15).join('\n');
      if (tailText.includes('FATAL') || tailText.includes('UnhandledPromiseRejection') || tailText.includes('ELIFECYCLE')) {
        console.error('::error::Discloud terminal logs indicate a startup error.');
        process.exit(1);
      }
    }
  } else {
    console.log(`  Could not fetch logs (HTTP ${logsRes.status})`);
  }

  // Step 3: Verify Public Site Health
  console.log('\n[Discloud Verify] Step 3: Probing Project Nox public site ...');
  const routes = [
    '/',
    '/lancamentos',
    '/catalogo',
    '/api/releases?limit=1'
  ];
  let siteSuccess = true;
  for (const r of routes) {
    const url = `https://manga.project-nox-awerkori.workers.dev${r}`;
    const t0 = performance.now();
    try {
      const res = await fetch(url, {
        headers: { 'User-Agent': 'ProjectNoxCI/1.0' },
        signal: AbortSignal.timeout(10000),
      });
      const ttfb = Math.round(performance.now() - t0);
      const text = await res.text();
      const hasErrorMarkup = /data-error="true"|Internal Server Error/.test(text);
      console.log(`  Route ${r}: HTTP ${res.status}, TTFB ${ttfb}ms, bytes: ${Buffer.byteLength(text)}, errorMarkup: ${hasErrorMarkup}`);
      if (!res.ok || hasErrorMarkup) {
        siteSuccess = false;
      }
    } catch (err) {
      console.error(`  Route ${r}: FAILED (${err.message})`);
      siteSuccess = false;
    }
  }

  if (!siteSuccess) {
    console.error('::error::Public site health checks failed after deploy.');
    process.exit(1);
  }

  console.log('\n======================================================');
  console.log('[Discloud Verify] POST-DEPLOY HEALTH VALIDATION PASSED.');
  console.log('======================================================\n');
}

verify().catch(err => {
  console.error('[Discloud Verify] Fatal verification error:', err);
  process.exit(1);
});
