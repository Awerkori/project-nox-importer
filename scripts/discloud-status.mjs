import { discloudStatus } from './discloud-status-classify.mjs';

const token = process.env.DISCLOUD_TOKEN;
const appId = process.env.DISCLOUD_APP_ID || '1788873398156';

if (!token) {
  console.error('DISCLOUD_TOKEN is missing');
  process.exit(1);
}

const headers = {
  'api-token': token.trim(),
  'User-Agent': 'project-nox-status-check',
};

async function check() {
  console.log(`Checking Discloud App ${appId}...`);
  const statusRes = await fetch(`https://api.discloud.app/v2/app/${appId}/status`, { headers });
  const statusData = await statusRes.json();
  const initialStatus = discloudStatus(statusRes.ok, statusData);
  if (initialStatus === 'UNKNOWN') throw new Error('DIScloud status unavailable; no restart attempted');
  console.log('App Status:\n', JSON.stringify(statusData, null, 2));

  const logsRes = await fetch(`https://api.discloud.app/v2/app/${appId}/logs`, { headers });
  const logsData = await logsRes.json();
  const terminal = logsData.apps?.terminal?.big || logsData.apps?.terminal?.small || logsData;
  console.log('Terminal Output (last 40 lines):');
  if (typeof terminal === 'string') {
    terminal.split('\n').slice(-40).forEach(l => console.log('  |', l));
  } else {
    console.log(JSON.stringify(terminal, null, 2));
  }

  const force = process.env.FORCE_RESTART === 'true';
  if (initialStatus === 'OFFLINE' || force) {
    if (!force) {
      await new Promise(resolve => setTimeout(resolve, 5000));
      const confirmation = await fetch(`https://api.discloud.app/v2/app/${appId}/status`, { headers, signal: AbortSignal.timeout(10000) });
      const confirmed = discloudStatus(confirmation.ok, await confirmation.json());
      if (confirmed !== 'OFFLINE') throw new Error(`DIScloud offline state not confirmed (${confirmed}); no restart attempted`);
    }
    console.log(`Confirmed offline or explicitly forced; requesting restart.`);
    const restartRes = await fetch(`https://api.discloud.app/v2/app/${appId}/restart`, { method: 'PUT', headers, signal: AbortSignal.timeout(10000) });
    if (!restartRes.ok) throw new Error(`DIScloud restart failed: HTTP ${restartRes.status}`);
    console.log('Restart response:', await restartRes.text());
  }
}

check().catch(error => { console.error(error.message); process.exitCode = 1; });
