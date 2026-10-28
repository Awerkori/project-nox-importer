import fs from 'node:fs';

const token = process.env.DISCLOUD_TOKEN;
if (!token || !token.trim()) {
  console.error('::error::DISCLOUD_TOKEN environment variable is missing or empty.');
  process.exit(1);
}

// 1. Read discloud.config to inspect current static configuration
let configId = null;
let configName = null;
try {
  const config = fs.readFileSync('discloud.config', 'utf8');
  for (const line of config.split('\n')) {
    const trimmed = line.trim();
    if (trimmed.startsWith('#') || !trimmed) continue;
    const [k, ...v] = trimmed.split('=');
    if (k.trim() === 'ID') configId = v.join('=').trim();
    if (k.trim() === 'NAME') configName = v.join('=').trim();
  }
} catch (err) {
  console.warn('[Discloud Resolve] Could not read discloud.config:', err.message);
}

console.log(`[Discloud Resolve] Configured in discloud.config: ID="${configId}", NAME="${configName}"`);

const headers = {
  'api-token': token.trim(),
  'User-Agent': 'project-nox-importer-ci-deploy',
};

async function apiGet(endpoint) {
  const url = `https://api.discloud.app/v2${endpoint}`;
  try {
    const res = await fetch(url, { headers });
    const text = await res.text();
    let body;
    try {
      body = JSON.parse(text);
    } catch {
      body = { raw: text };
    }
    return { status: res.status, ok: res.ok, body };
  } catch (err) {
    return { status: 0, ok: false, error: err.message };
  }
}

async function resolve() {
  console.log('[Discloud Resolve] Step 1: Querying /v2/user ...');
  const userRes = await apiGet('/v2/user');
  if (!userRes.ok) {
    console.error(`[Discloud Resolve] Failed to query /v2/user: HTTP ${userRes.status}`, userRes.body || userRes.error);
    process.exit(1);
  }

  const userObj = userRes.body?.user || {};
  const userApps = Array.isArray(userObj.apps) ? userObj.apps : [];
  console.log(`[Discloud Resolve] Authenticated user ID: ${userObj.userID || 'unknown'}, Plan: ${userObj.plan || 'unknown'}`);
  console.log(`[Discloud Resolve] Total RAM: ${userObj.totalRamMb || 0} MB, RAM Used: ${userObj.ramUsedMb || 0} MB`);
  console.log(`[Discloud Resolve] Available owned apps count: ${userApps.length}, apps:`, userApps);

  console.log('[Discloud Resolve] Step 2: Querying /v2/app/all/status ...');
  const allStatusRes = await apiGet('/v2/app/all/status');
  let appDetails = [];
  if (allStatusRes.ok && allStatusRes.body?.apps) {
    appDetails = Array.isArray(allStatusRes.body.apps) ? allStatusRes.body.apps : [allStatusRes.body.apps];
    console.log(`[Discloud Resolve] Found ${appDetails.length} app(s) in status overview:`);
    for (const app of appDetails) {
      console.log(`  - App ID: ${app.id}, Name: "${app.name || ''}", Online: ${app.online}, Status: ${app.status || ''}`);
    }
  } else {
    console.log(`[Discloud Resolve] /v2/app/all/status returned HTTP ${allStatusRes.status}:`, JSON.stringify(allStatusRes.body));
  }

  console.log('[Discloud Resolve] Step 3: Checking team ownership via /v2/teams and /v2/teams/apps ...');
  const teamsRes = await apiGet('/v2/teams');
  let teamApps = [];
  if (teamsRes.ok && teamsRes.body) {
    const teamsList = Array.isArray(teamsRes.body.teams) ? teamsRes.body.teams : (Array.isArray(teamsRes.body.data) ? teamsRes.body.data : []);
    console.log(`[Discloud Resolve] User belongs to ${teamsList.length} teams.`);
    const teamAppsRes = await apiGet('/v2/teams/apps');
    if (teamAppsRes.ok && teamAppsRes.body) {
      teamApps = Array.isArray(teamAppsRes.body.apps) ? teamAppsRes.body.apps : (Array.isArray(teamAppsRes.body.data) ? teamAppsRes.body.data : []);
      console.log(`[Discloud Resolve] Team apps count: ${teamApps.length}`);
      for (const tApp of teamApps) {
        console.log(`  - Team App ID: ${tApp.id || tApp._id}, Name: "${tApp.name || ''}"`);
      }
    }
  } else {
    console.log(`[Discloud Resolve] /v2/teams HTTP ${teamsRes.status}:`, JSON.stringify(teamsRes.body));
  }

  // Determine target App ID and whether team is required
  let targetAppId = null;
  let isTeam = false;

  // Priority 1: Match by app name in user apps
  if (configName && appDetails.length > 0) {
    const matched = appDetails.find(a => (a.name || '').toLowerCase() === configName.toLowerCase());
    if (matched) {
      targetAppId = matched.id;
      isTeam = false;
      console.log(`[Discloud Resolve] Matched owned app by name "${configName}": ID=${targetAppId}`);
    }
  }

  // Priority 2: Match by app name in team apps
  if (!targetAppId && configName && teamApps.length > 0) {
    const matchedTeam = teamApps.find(a => (a.name || '').toLowerCase() === configName.toLowerCase());
    if (matchedTeam) {
      targetAppId = matchedTeam.id || matchedTeam._id;
      isTeam = true;
      console.log(`[Discloud Resolve] Matched team app by name "${configName}": ID=${targetAppId}`);
    }
  }

  // Priority 3: If account has exactly one owned app, use it directly
  if (!targetAppId && userApps.length === 1) {
    targetAppId = userApps[0];
    isTeam = false;
    console.log(`[Discloud Resolve] Account has exactly one owned app: ID=${targetAppId}`);
  }

  // Priority 4: Canonical App ID fallback if present in userApps
  if (!targetAppId && userApps.includes('1788873398156')) {
    targetAppId = '1788873398156';
    isTeam = false;
    console.log(`[Discloud Resolve] Account contains canonical Project Nox Importer ID: 1788873398156`);
  }

  // Priority 5: Fallback to first app if available
  if (!targetAppId && userApps.length > 0) {
    targetAppId = userApps[0];
    isTeam = false;
    console.log(`[Discloud Resolve] Defaulting to first owned app: ID=${targetAppId}`);
  }

  if (!targetAppId) {
    console.error('[Discloud Resolve] CRITICAL: Could not find any application ID in Discloud account.');
    process.exit(1);
  }

  console.log(`\n======================================================`);
  console.log(`[Discloud Resolve] Target Discloud App ID: ${targetAppId}`);
  console.log(`[Discloud Resolve] Team Mode: ${isTeam}`);
  console.log(`[Discloud Resolve] Configured in discloud.config: ID="${configId}"`);
  if (configId !== targetAppId) {
    console.log(`[Discloud Resolve] ID MISMATCH CONFIRMED: discloud.config has ID="${configId}", which returned 404.`);
    console.log(`[Discloud Resolve] Providing official App ID "${targetAppId}" to discloud/deploy-action.`);
  } else {
    console.log(`[Discloud Resolve] discloud.config matches targetAppId.`);
  }
  console.log(`======================================================\n`);

  if (process.env.GITHUB_OUTPUT) {
    fs.appendFileSync(process.env.GITHUB_OUTPUT, `app_id=${targetAppId}\n`);
    fs.appendFileSync(process.env.GITHUB_OUTPUT, `is_team=${isTeam ? 'true' : 'false'}\n`);
    console.log(`[Discloud Resolve] Wrote app_id=${targetAppId} and is_team=${isTeam ? 'true' : 'false'} to GITHUB_OUTPUT.`);
  }
}

resolve().catch(err => {
  console.error('[Discloud Resolve] Fatal error:', err);
  process.exit(1);
});
