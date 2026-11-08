import fs from 'node:fs';

const token = process.env.DISCLOUD_TOKEN;
if (!token || !token.trim()) {
  console.error('::error::DISCLOUD_TOKEN is missing or empty.');
  process.exit(1);
}

const headers = {
  'api-token': token.trim(),
  'User-Agent': 'project-nox-bot-live-audit',
};

async function apiRequest(endpoint) {
  const clean = endpoint.startsWith('/') ? endpoint : `/${endpoint}`;
  const url = `https://api.discloud.app/v2${clean}`;
  try {
    const res = await fetch(url, { headers, signal: AbortSignal.timeout(30_000) });
    const text = await res.text();
    try {
      return { ok: res.ok, status: res.status, body: JSON.parse(text) };
    } catch {
      return { ok: res.ok, status: res.status, body: { raw: text } };
    }
  } catch (err) {
    return { ok: false, status: 0, error: err.message };
  }
}

async function main() {
  console.log('================================================================');
  console.log('  PROJECT NOX — LIVE DISCLOUD AUDIT & HEALTH VERIFICATION');
  console.log('================================================================\n');

  // 1. User info & RAM
  const userRes = await apiRequest('/user');
  if (!userRes.ok) {
    throw new Error(`Failed to query /v2/user: HTTP ${userRes.status}`);
  }
  const user = userRes.body?.user || {};
  console.log(`Discloud User: ${user.userID}`);
  console.log(`Plan:          ${user.plan}`);
  console.log(`RAM Allocated: ${user.ramUsedMb} / ${user.totalRamMb} MB`);
  console.log(`Active Apps:   ${JSON.stringify(user.apps)}\n`);

  // 2. Importer Safety Check
  const IMPORTER_ID = '1788873398156';
  console.log('>>> [1/5] PROTECTED IMPORTER STATUS <<<');
  const impRes = await apiRequest(`/app/${IMPORTER_ID}/status`);
  if (!impRes.ok) {
    console.error(`❌ CRITICAL: Failed to query Importer status: HTTP ${impRes.status}`);
  } else {
    const impApp = impRes.body?.apps || {};
    console.log(`  ID:          ${IMPORTER_ID}`);
    console.log(`  Container:   ${impApp.container || 'unknown'}`);
    console.log(`  Online:      ${impApp.online}`);
    console.log(`  CPU:         ${impApp.cpu || 'N/A'}`);
    console.log(`  Memory:      ${impApp.memory || 'N/A'}`);
    console.log(`  LastRestart: ${impApp.last_restart || 'N/A'}`);
  }
  console.log('');

  // 3. Four Bots Audit (New Production IDs)
  const bots = [
    { name: 'Project Nox Mangá', oldId: '1794078662407', id: '1794120431436' },
    { name: 'Keiyoushi Monitor', oldId: '1794078615622', id: '1794120475569' },
    { name: 'Yuzono Monitor', oldId: '1794078707942', id: '1794120537671' },
    { name: 'Project Nox Anime', oldId: '1794078752240', id: '1794120637293' },
  ];

  for (let i = 0; i < bots.length; i++) {
    const b = bots[i];
    console.log(`>>> [${i + 2}/5] ${b.name.toUpperCase()} (ID: ${b.id} | Old: ${b.oldId}) <<<`);
    const statusRes = await apiRequest(`/app/${b.id}/status`);
    if (!statusRes.ok) {
      console.error(`  ❌ Status query failed: HTTP ${statusRes.status}`);
    } else {
      const appData = statusRes.body?.apps || {};
      console.log(`  Container:   ${appData.container || 'unknown'}`);
      console.log(`  Online:      ${appData.online}`);
      console.log(`  CPU:         ${appData.cpu || 'N/A'}`);
      console.log(`  Memory:      ${appData.memory || 'N/A'}`);
      console.log(`  SSD:         ${appData.ssd || 'N/A'}`);
      console.log(`  StartedAt:   ${appData.startedAt || 'N/A'}`);
    }

    const logsRes = await apiRequest(`/app/${b.id}/logs`);
    if (logsRes.ok && logsRes.body?.apps?.terminal?.small) {
      const lines = logsRes.body.apps.terminal.small.split('\n');
      console.log('  --- RECENT TERMINAL LOGS (last 15 lines) ---');
      console.log(lines.slice(-15).map((l) => `  | ${l}`).join('\n'));
      console.log('  --------------------------------------------');
    } else {
      console.log('  --- NO LOGS RETURNED ---');
    }
    console.log('');
  }

  console.log('================================================================');
  console.log('  LIVE DISCLOUD AUDIT COMPLETED');
  console.log('================================================================');
}

main().catch((err) => {
  console.error('Fatal audit error:', err);
  process.exit(1);
});
