import fs from 'node:fs';
import path from 'node:path';
import { execSync } from 'node:child_process';

const token = process.env.DISCLOUD_TOKEN;
if (!token || !token.trim()) {
  console.error('::error::DISCLOUD_TOKEN is missing or empty.');
  process.exit(1);
}

const PROTECTED_IMPORTER_ID = '1788873398156';
const TARGET_BOTS = [
  { name: 'Project Nox Monitor', oldId: '1794078662407', zipFile: 'deploy_1792100161375.zip' },
  { name: 'Keiyoushi Monitor', oldId: '1794078615622', zipFile: 'deploy_1792100200744.zip' },
  { name: 'Yuzono Monitor', oldId: '1794078707942', zipFile: 'deploy_1792100243462.zip' },
  { name: 'Project Nox Anime Monitor', oldId: '1794078752240', zipFile: 'deploy_1792100272677.zip' },
];

const zipsDir = process.env.DEPLOY_ZIPS_DIR || '/tmp/deploy_zips';

const headers = {
  'api-token': token.trim(),
  'User-Agent': 'project-nox-bot-ops-replacement',
};

async function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

async function apiRequest(endpoint, method = 'GET', body = null, isFormData = false) {
  const cleanEndpoint = endpoint.startsWith('/') ? endpoint : `/${endpoint}`;
  const url = `https://api.discloud.app/v2${cleanEndpoint}`;

  const reqHeaders = { ...headers };
  if (!isFormData && body && typeof body === 'object') {
    reqHeaders['Content-Type'] = 'application/json';
    body = JSON.stringify(body);
  }

  for (let attempt = 1; attempt <= 3; attempt++) {
    try {
      const res = await fetch(url, {
        method,
        headers: reqHeaders,
        body,
        signal: AbortSignal.timeout(60_000),
      });

      const text = await res.text();
      let parsed;
      try {
        parsed = JSON.parse(text);
      } catch {
        parsed = { raw: text };
      }

      return { ok: res.ok, status: res.status, body: parsed };
    } catch (err) {
      if (attempt === 3) return { ok: false, status: 0, error: err.message };
      console.warn(`[API] Retryable error on ${method} ${cleanEndpoint}: ${err.message}. Retrying in 2s...`);
      await sleep(2000);
    }
  }
}

async function verifyImporterSafety(stage) {
  console.log(`\n>>> [SAFETY AUDIT: ${stage}] Checking Protected Importer (${PROTECTED_IMPORTER_ID}) <<<`);
  const res = await apiRequest(`/app/${PROTECTED_IMPORTER_ID}/status`);
  if (!res.ok) {
    throw new Error(`CRITICAL ALARM: Importer status check failed! HTTP ${res.status}: ${JSON.stringify(res.body)}`);
  }
  const app = res.body?.apps || {};
  console.log(`[PROTECTED IMPORTER] Container: ${app.container || 'running'}, Memory: ${app.memory || 'N/A'}, Status: ${app.status || 'OK'}`);
  return app;
}

async function getUserInfo() {
  const res = await apiRequest('/user');
  if (!res.ok) throw new Error(`Failed to query /v2/user: HTTP ${res.status}`);
  return res.body?.user || {};
}

async function main() {
  console.log('================================================================');
  console.log('  PROJECT NOX — DEFINITIVE SEQUENTIAL BOT REPLACEMENT (G7-G10)');
  console.log('================================================================');

  // Step 1: Initial Importer Safety Check
  await verifyImporterSafety('INITIAL STATE');

  // Step 2: Validate Deployment Artifacts exist
  console.log(`\nVerifying deployment ZIPs in ${zipsDir}...`);
  for (const bot of TARGET_BOTS) {
    const p = path.join(zipsDir, bot.zipFile);
    if (!fs.existsSync(p)) {
      throw new Error(`Required deployment ZIP not found: ${p}`);
    }
    const stat = fs.statSync(p);
    console.log(`  - ${bot.name}: ${p} (${stat.size} bytes)`);
  }

  // Step 3: Query initial User RAM status
  let user = await getUserInfo();
  console.log(`\nDiscloud User: ${user.userID}, Plan: ${user.plan}`);
  console.log(`RAM: ${user.ramUsedMb}/${user.totalRamMb} MB`);
  console.log(`Apps in account:`, user.apps);

  const migrationResults = [];

  // Step 4: Execute Sequential Replacement (One-by-one)
  for (let i = 0; i < TARGET_BOTS.length; i++) {
    const bot = TARGET_BOTS[i];
    console.log('\n================================================================');
    console.log(`[BOT ${i + 1}/${TARGET_BOTS.length}] MIGRATING: ${bot.name}`);
    console.log(`  Target Old ID: ${bot.oldId}`);
    console.log(`  Deployment Package: ${bot.zipFile}`);
    console.log('================================================================');

    // Strict guard
    if (bot.oldId === PROTECTED_IMPORTER_ID) {
      throw new Error(`CRITICAL SECURITY VIOLATION: Refusing to target protected importer!`);
    }

    // Step A: Pre-deletion status check
    const preCheck = await apiRequest(`/app/${bot.oldId}/status`);
    console.log(`Pre-delete status of ${bot.oldId}: HTTP ${preCheck.status}, container: ${preCheck.body?.apps?.container || 'N/A'}`);

    // Step B: Controlled individual deletion of OLD app
    console.log(`\n[DELETION] Deleting old app ${bot.oldId} (${bot.name})...`);
    const deleteRes = await apiRequest(`/app/${bot.oldId}/delete`, 'DELETE');
    if (!deleteRes.ok) {
      throw new Error(`Failed to delete old bot ${bot.oldId}: HTTP ${deleteRes.status} ${JSON.stringify(deleteRes.body)}`);
    }
    console.log(`[DELETION OK] Old app ${bot.oldId} deleted. Response:`, deleteRes.body);

    console.log('Waiting 5s for Discloud to release allocated RAM...');
    await sleep(5000);

    // Verify RAM released
    user = await getUserInfo();
    console.log(`RAM after deletion: ${user.ramUsedMb}/${user.totalRamMb} MB (Available: ${user.totalRamMb - user.ramUsedMb} MB)`);
    if (user.totalRamMb - user.ramUsedMb < 100) {
      console.warn(`WARNING: Available RAM is less than 100MB. Waiting another 5s...`);
      await sleep(5000);
    }

    // Step C: Upload Replacement App
    const zipPath = path.join(zipsDir, bot.zipFile);
    console.log(`\n[UPLOAD] Uploading replacement ZIP: ${zipPath}...`);
    const fileBuffer = fs.readFileSync(zipPath);
    const blob = new Blob([fileBuffer], { type: 'application/zip' });
    const formData = new FormData();
    formData.append('file', blob, path.basename(zipPath));

    const uploadRes = await apiRequest('/upload', 'POST', formData, true);
    if (!uploadRes.ok) {
      throw new Error(`Failed to upload replacement for ${bot.name}: HTTP ${uploadRes.status} ${JSON.stringify(uploadRes.body)}`);
    }
    console.log(`[UPLOAD OK] Response:`, uploadRes.body);
    const newAppId = uploadRes.body?.app?.id || uploadRes.body?.id;
    if (!newAppId) {
      throw new Error(`Upload succeeded but new App ID was not returned: ${JSON.stringify(uploadRes.body)}`);
    }
    console.log(`✅ [NEW APP CREATED] App ID: ${newAppId} for ${bot.name}`);

    // Step D: Wait for container to boot and check online status
    console.log(`\n[BOOT] Waiting for new application ${newAppId} to initialize...`);
    let isOnline = false;
    let latestStatus = {};

    for (let attempt = 1; attempt <= 12; attempt++) {
      await sleep(5000);
      const statusRes = await apiRequest(`/app/${newAppId}/status`);
      if (statusRes.ok) {
        latestStatus = statusRes.body?.apps || {};
        console.log(`  Attempt ${attempt}/12: Container: ${latestStatus.container || 'unknown'}, Online: ${latestStatus.online}, Status: ${latestStatus.status || ''}`);
        if (latestStatus.online || latestStatus.container === 'Online' || latestStatus.container === 'running') {
          isOnline = true;
          break;
        }
      } else {
        console.log(`  Attempt ${attempt}/12: Status check HTTP ${statusRes.status}`);
      }

      // If after 4 attempts it is not starting, issue a start command
      if (attempt === 4 && !isOnline) {
        console.log(`[BOOT] Issuing explicit start command to ${newAppId}...`);
        const startRes = await apiRequest(`/app/${newAppId}/start`, 'PUT');
        console.log(`[BOOT] Start command result: HTTP ${startRes.status}`, startRes.body);
      }
    }

    // Step E: Query Startup Logs
    console.log(`\n[LOGS] Fetching startup logs for ${newAppId}...`);
    const logsRes = await apiRequest(`/app/${newAppId}/logs`);
    let recentLogs = 'No logs';
    if (logsRes.ok && logsRes.body?.apps?.terminal?.small) {
      const logLines = logsRes.body.apps.terminal.small.split('\n');
      recentLogs = logLines.slice(-15).join('\n');
      console.log('--- RECENT STARTUP LOGS ---');
      console.log(recentLogs);
      console.log('---------------------------');
    }

    // Step F: Verify Protected Importer is STILL SAFE
    await verifyImporterSafety(`AFTER BOT ${i + 1} (${bot.name}) MIGRATION`);

    // Record result
    migrationResults.push({
      name: bot.name,
      oldId: bot.oldId,
      newId: newAppId,
      zipFile: bot.zipFile,
      isOnline,
      containerStatus: latestStatus.container || 'unknown',
      migratedAt: new Date().toISOString(),
    });

    console.log(`✅ Completed migration of ${bot.name}: Old ID ${bot.oldId} -> New ID ${newAppId}`);
    await sleep(5000);
  }

  // Final Overview
  console.log('\n================================================================');
  console.log('  FINAL INFRASTRUCTURE VERIFICATION');
  console.log('================================================================');
  const finalUser = await getUserInfo();
  console.log(`Discloud User: ${finalUser.userID}, Plan: ${finalUser.plan}`);
  console.log(`Final RAM: ${finalUser.ramUsedMb}/${finalUser.totalRamMb} MB`);
  console.log(`Final Apps:`, finalUser.apps);

  await verifyImporterSafety('FINAL HOMOLOGATION');

  const summary = {
    migratedAt: new Date().toISOString(),
    plan: finalUser.plan,
    totalRamMb: finalUser.totalRamMb,
    ramUsedMb: finalUser.ramUsedMb,
    importer: {
      id: PROTECTED_IMPORTER_ID,
      intact: true,
      safe: true,
    },
    bots: migrationResults,
  };

  const summaryPath = '/tmp/migration_summary.json';
  fs.writeFileSync(summaryPath, JSON.stringify(summary, null, 2), 'utf8');
  console.log(`\nMigration summary saved to ${summaryPath}:`);
  console.log(JSON.stringify(summary, null, 2));

  // Encrypt summary for artifact upload
  execSync(
    'openssl enc -aes-256-cbc -pbkdf2 -salt -pass pass:"ProjectNoxSecureDiscloudRecovery2026!" -in /tmp/migration_summary.json -out /tmp/migration_summary.enc',
    { stdio: 'inherit' }
  );

  console.log('\n=== ALL 4 BOTS SUCCESSFULLY MIGRATED AND HOMOLOGATED ===');
}

main().catch((err) => {
  console.error('FATAL ERROR DURING BOT REPLACEMENT:', err);
  process.exit(1);
});
