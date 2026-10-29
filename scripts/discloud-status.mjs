import { fileURLToPath } from 'url';

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

  // If app is offline, trigger restart
  const app = statusData.apps || statusData;
  const isOnline = app.online === true || (app.status && app.status.toLowerCase() === 'online');
  if (!isOnline || process.env.FORCE_RESTART === 'true') {
    console.log('\nApp is offline or FORCE_RESTART=true. Triggering restart via Discloud API...');
    const restartRes = await fetch(`https://api.discloud.app/v2/app/${appId}/restart`, {
      method: 'PUT',
      headers,
    });
    console.log('Restart response:', await restartRes.text());
  }
}

check().catch(console.error);
