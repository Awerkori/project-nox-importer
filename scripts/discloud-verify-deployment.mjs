import fs from 'node:fs';
import path from 'node:path';
import { setTimeout as sleep } from 'node:timers/promises';
import {
  extractGitSha,
  shaMatches,
  parseTerminalErrors,
} from './discloud-deploy-tracker.mjs';

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

// Determine expected commit from environment or local build/BUILD_ID
let expectedCommit = process.env.EXPECTED_COMMIT ? process.env.EXPECTED_COMMIT.trim() : null;
if (!expectedCommit) {
  try {
    const buildIdPath = path.resolve('build/BUILD_ID');
    if (fs.existsSync(buildIdPath)) {
      const parsed = JSON.parse(fs.readFileSync(buildIdPath, 'utf8'));
      if (parsed.gitSha && !parsed.gitSha.startsWith('artifact:') && parsed.gitSha !== 'unknown') {
        expectedCommit = parsed.gitSha.trim();
      }
    }
  } catch {
    // Non-fatal fallback
  }
}

const headers = {
  'api-token': token.trim(),
  'User-Agent': 'project-nox-importer-ci-verify',
};

const baseUrl = isTeam
  ? `https://api.discloud.app/v2/team/${appId}`
  : `https://api.discloud.app/v2/app/${appId}`;

console.log(`[Discloud Verify] Verifying deployment for App ID: ${appId} (Team Mode: ${isTeam}) ...`);
if (expectedCommit) {
  console.log(`[Discloud Verify] Expected commit: ${expectedCommit} (${expectedCommit.slice(0, 7)})`);
} else {
  console.log(`[Discloud Verify] Expected commit: not specified (verifying general container health)`);
}

async function fetchJson(url, options = {}) {
  try {
    const res = await fetch(url, { headers, ...options });
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
  const maxAttempts = 24; // 24 * 5s = 120s timeout
  let confirmedCommit = false;
  let isOnline = false;
  let lastStatus = 'unknown';
  let restartTriggered = false;
  let runningSha = null;

  console.log('[Discloud Verify] Step 1: Polling container status and verifying deployed commit ...');

  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    // 1. Fetch container status
    const statusRes = await fetchJson(`${baseUrl}/status`);
    if (statusRes.ok && statusRes.data) {
      const app = statusRes.data.apps || statusRes.data;
      isOnline = Boolean(
        app.online === true ||
        (typeof app.container === 'string' && app.container.toLowerCase() === 'online') ||
        (typeof app.status === 'string' && app.status.toLowerCase() === 'online')
      );
      lastStatus = app.container || app.status || (app.online ? 'Online' : 'Offline');
    }

    // 2. Fetch terminal logs to inspect running commit & health
    const logsRes = await fetchJson(`${baseUrl}/logs`);
    let terminalText = '';
    if (logsRes.ok && logsRes.data) {
      const terminal = logsRes.data.apps?.terminal || logsRes.data.terminal || {};
      terminalText = terminal.big || terminal.small || (typeof terminal === 'string' ? terminal : '');
    }

    // 3. Inspect errors
    const errorCheck = parseTerminalErrors(terminalText);
    if (errorCheck.hasFatalError) {
      console.error('::error::Discloud terminal logs indicate a critical crash/error:', errorCheck.fatalErrors);
      process.exit(1);
    }

    // 4. Extract commit SHA from logs
    runningSha = extractGitSha(terminalText);

    const commitMatched = expectedCommit ? shaMatches(runningSha, expectedCommit) : true;
    console.log(
      `  [Attempt ${attempt}/${maxAttempts}] Container: "${lastStatus}", Running SHA: ${runningSha ? runningSha.slice(0, 7) : 'pending'}, Match: ${commitMatched}`
    );

    if (isOnline && commitMatched) {
      confirmedCommit = true;
      console.log(`[Discloud Verify] Target commit ${expectedCommit ? expectedCommit.slice(0, 7) : ''} confirmed running and container Online.`);
      break;
    }

    // If after 30 seconds (6 attempts) the container is still reporting an old commit,
    // explicitly request a container restart in case DIScloud auto-restart hung
    if (attempt >= 6 && !restartTriggered && expectedCommit && !commitMatched) {
      console.log(`[Discloud Verify] Container has not booted new commit after 30s. Triggering explicit restart via DIScloud API...`);
      restartTriggered = true;
      const restartRes = await fetchJson(`${baseUrl}/restart`, { method: 'PUT' });
      console.log(`[Discloud Verify] Restart request result: HTTP ${restartRes.status}`, restartRes.data || restartRes.error);
    }

    await sleep(5000);
  }

  if (!confirmedCommit) {
    console.error(
      `::error::Discloud deployment verification timed out after 120s. Container: "${lastStatus}", Running SHA: ${runningSha || 'none'}, Expected: ${expectedCommit || 'any'}.`
    );
    process.exit(1);
  }

  // Step 2: Print terminal tail
  console.log('\n[Discloud Verify] Step 2: Recent terminal output:');
  const logsRes = await fetchJson(`${baseUrl}/logs`);
  if (logsRes.ok && logsRes.data) {
    const terminal = logsRes.data.apps?.terminal || logsRes.data.terminal || {};
    const logContent = terminal.big || terminal.small || '';
    const lines = logContent.split('\n').filter(l => l.trim().length > 0);
    for (const line of lines.slice(-10)) {
      console.log(`  | ${line}`);
    }
  }

  // Step 3: Verify Public Site Health
  console.log('\n[Discloud Verify] Step 3: Probing Project Nox public site ...');
  const routes = [
    '/',
    '/lancamentos',
    '/catalogo',
    '/api/releases?limit=1',
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
  console.log('[Discloud Verify] POST-DEPLOY HEALTH & COMMIT VALIDATION PASSED.');
  console.log(`[Discloud Verify] Confirmed commit: ${runningSha ? runningSha.slice(0, 7) : (expectedCommit ? expectedCommit.slice(0, 7) : 'OK')}`);
  console.log('======================================================\n');
}

verify().catch(err => {
  console.error('[Discloud Verify] Fatal verification error:', err);
  process.exit(1);
});
