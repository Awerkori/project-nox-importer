import fs from 'node:fs';
import { execSync } from 'node:child_process';
import { discloudStatus } from './discloud-status-classify.mjs';
import { extractGitSha, evaluateDiscloudDeployState } from './discloud-deploy-tracker.mjs';

const token = process.env.DISCLOUD_TOKEN;
const appId = process.env.DISCLOUD_APP_ID || '1788873398156';
const readerBaseUrl = (process.env.NOX_MANGA_URL || 'https://manga.project-nox-awerkori.workers.dev').replace(/\/$/, '');

if (!token) {
  console.error('DISCLOUD_TOKEN is missing');
  process.exit(1);
}

const headers = {
  'api-token': token.trim(),
  'User-Agent': 'project-nox-status-check',
};

function parseRamUsage(memoryField) {
  if (typeof memoryField !== 'string') return null;
  const match = memoryField.match(/([\d.]+)\s*(MB|GB)\s*\/\s*([\d.]+)\s*(MB|GB)/i);
  if (!match) return null;
  const toMb = (val, unit) => Number(val) * (unit.toUpperCase() === 'GB' ? 1024 : 1);
  const usedMb = toMb(match[1], match[2]);
  const totalMb = toMb(match[3], match[4]);
  if (!Number.isFinite(usedMb) || !Number.isFinite(totalMb) || totalMb <= 0) return null;
  return { usedMb, totalMb, percent: Math.round((usedMb / totalMb) * 1000) / 10 };
}

async function probeReader() {
  const startedAt = Date.now();
  try {
    const res = await fetch(`${readerBaseUrl}/api/releases?limit=5`, {
      headers: { Accept: 'application/json', 'User-Agent': 'project-nox-cloud-sentinel' },
      signal: AbortSignal.timeout(10_000),
    });
    const latencyMs = Date.now() - startedAt;
    if (!res.ok) return { ok: false, status: res.status, latencyMs, latestPublishedAt: null };
    const body = await res.json().catch(() => null);
    const items = Array.isArray(body) ? body : (body?.releases || body?.items || body?.data || []);
    const latestPublishedAt = items?.[0]?.published_at || items?.[0]?.publishedAt || items?.[0]?.created_at || null;
    return { ok: true, status: res.status, latencyMs, latestPublishedAt };
  } catch (error) {
    return { ok: false, status: null, latencyMs: Date.now() - startedAt, error: error.message, latestPublishedAt: null };
  }
}

async function fetchGithub(url, method = 'GET', body = null) {
  const ghToken = process.env.GITHUB_TOKEN;
  if (!ghToken) return null;
  const opts = {
    method,
    headers: {
      Authorization: `Bearer ${ghToken}`,
      Accept: 'application/vnd.github+json',
      'User-Agent': 'project-nox-discloud-status',
    },
    signal: AbortSignal.timeout(10000),
  };
  if (body) {
    opts.headers['Content-Type'] = 'application/json';
    opts.body = JSON.stringify(body);
  }
  try {
    const res = await fetch(url, opts);
    if (!res.ok) return { ok: false, status: res.status, body: await res.text() };
    if (res.status === 204) return { ok: true, status: 204, data: null };
    return { ok: true, status: res.status, data: await res.json() };
  } catch (err) {
    return { ok: false, status: 0, error: err.message };
  }
}

function appendStepSummary(lines) {
  const summaryPath = process.env.GITHUB_STEP_SUMMARY;
  if (!summaryPath) return;
  try {
    fs.appendFileSync(summaryPath, `${lines.join('\n')}\n`, 'utf8');
  } catch {
    // Non-fatal outside GitHub Actions
  }
}

async function check() {
  console.log(`Checking Discloud App ${appId}...`);
  const statusRes = await fetch(`https://api.discloud.app/v2/app/${appId}/status`, {
    headers,
    signal: AbortSignal.timeout(12_000),
  });
  const statusData = await statusRes.json();
  const initialStatus = discloudStatus(statusRes.ok, statusData);
  if (initialStatus === 'UNKNOWN') throw new Error('DIScloud status unavailable; no restart attempted');
  console.log('App Status:\n', JSON.stringify(statusData, null, 2));

  const appInfo = statusData?.apps || statusData || {};
  const ram = parseRamUsage(appInfo.memory);
  const cpu = typeof appInfo.cpu === 'string' ? appInfo.cpu : 'N/A';

  const logsRes = await fetch(`https://api.discloud.app/v2/app/${appId}/logs`, {
    headers,
    signal: AbortSignal.timeout(12_000),
  });
  const logsData = await logsRes.json();
  const terminal = logsData.apps?.terminal?.big || logsData.apps?.terminal?.small || logsData;
  const terminalLines = typeof terminal === 'string' ? terminal.split('\n') : [];
  console.log('Terminal Output (last 40 lines):');
  if (terminalLines.length > 0) {
    terminalLines.slice(-40).forEach(l => console.log('  |', l));
  } else {
    console.log(JSON.stringify(terminal, null, 2));
  }

  const reader = await probeReader();
  console.log('Reader Probe:', JSON.stringify(reader));

  const recentFatalOom = terminalLines.slice(-60).some(l => /JavaScript heap out of memory|FATAL ERROR:/i.test(l));
  const force = process.env.FORCE_RESTART === 'true';
  let restarted = false;

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
    restarted = true;
  }

  // --- AUTOMATED PENDING DEPLOY DETECTION & RECOVERY ---
  const terminalText = typeof terminal === 'string' ? terminal : terminalLines.join('\n');
  const runningSha = extractGitSha(terminalText);
  let targetSha = null;
  let deployState = { status: 'UNKNOWN', shouldTrigger: false, reason: 'Target commit not determined' };
  let deployTriggered = false;

  const repo = process.env.GITHUB_REPOSITORY || 'Awerkori/project-nox-importer';
  const ghToken = process.env.GITHUB_TOKEN;

  if (ghToken) {
    const commitRes = await fetchGithub(`https://api.github.com/repos/${repo}/commits/main`);
    if (commitRes?.ok && commitRes.data?.sha) {
      targetSha = commitRes.data.sha;
    }
    const runsRes = await fetchGithub(`https://api.github.com/repos/${repo}/actions/workflows/deploy-discloud.yml/runs?per_page=10`);
    const workflowRuns = runsRes?.ok && Array.isArray(runsRes.data?.workflow_runs)
      ? runsRes.data.workflow_runs
      : [];

    deployState = evaluateDiscloudDeployState({
      runningSha,
      targetSha,
      workflowRuns,
      maxRetries: 3,
    });
    console.log(`[Discloud Deploy Evaluation] Status: ${deployState.status} | ${deployState.reason}`);

    if (deployState.shouldTrigger) {
      console.log(`[Auto-Recovery] Triggering deploy-discloud.yml for target commit ${targetSha.slice(0, 7)}...`);
      const dispatchRes = await fetchGithub(
        `https://api.github.com/repos/${repo}/actions/workflows/deploy-discloud.yml/dispatches`,
        'POST',
        { ref: 'main' }
      );
      if (dispatchRes?.ok || dispatchRes?.status === 204) {
        console.log('[Auto-Recovery] Deployment workflow dispatch requested successfully.');
        deployTriggered = true;
      } else {
        console.error(`[Auto-Recovery] Workflow dispatch failed: HTTP ${dispatchRes?.status}`, dispatchRes?.body || dispatchRes?.error);
      }
    }
  } else {
    // Local CLI execution fallback: query git & gh CLI
    try {
      const gitOut = execSync('git rev-parse HEAD', { encoding: 'utf8', stdio: ['ignore', 'pipe', 'ignore'] }).trim();
      if (gitOut && gitOut.length === 40) targetSha = gitOut;

      const ghRunsOut = execSync('gh run list --workflow=deploy-discloud.yml --limit 10 --json databaseId,status,conclusion,headSha', {
        encoding: 'utf8',
        stdio: ['ignore', 'pipe', 'ignore'],
      });
      const parsedRuns = JSON.parse(ghRunsOut || '[]');
      deployState = evaluateDiscloudDeployState({
        runningSha,
        targetSha,
        workflowRuns: parsedRuns,
        maxRetries: 3,
      });
      console.log(`[Discloud Deploy Evaluation (CLI)] Status: ${deployState.status} | ${deployState.reason}`);

      if (deployState.shouldTrigger) {
        console.log(`[Auto-Recovery (CLI)] Triggering deploy-discloud.yml via gh...`);
        execSync('gh workflow run deploy-discloud.yml --ref main', { stdio: 'inherit' });
        deployTriggered = true;
      }
    } catch (err) {
      console.log('[Discloud Deploy Evaluation] Local CLI fallback notice:', err.message);
    }
  }

  const alerts = [];
  if (!reader.ok) alerts.push(`Reader /api/releases failed (status=${reader.status ?? 'ERR'}, latency=${reader.latencyMs}ms)`);
  else if (reader.latencyMs > 4000) alerts.push(`Reader /api/releases slow (${reader.latencyMs}ms)`);
  if (ram && ram.percent >= 95) alerts.push(`DIScloud RAM critical: ${ram.usedMb}MB/${ram.totalMb}MB (${ram.percent}%)`);
  if (recentFatalOom) alerts.push('Recent OOM / FATAL ERROR detected in DIScloud terminal tail');

  if (deployState.status === 'RETRIES_EXCEEDED') {
    alerts.push(`DIScloud deploy failed repeatedly for commit ${targetSha ? targetSha.slice(0, 7) : 'target'}: ${deployState.reason}`);
  } else if (deployState.status === 'PENDING_RECOVERY') {
    alerts.push(`DIScloud running outdated commit (${runningSha ? runningSha.slice(0, 7) : 'none'} vs ${targetSha ? targetSha.slice(0, 7) : 'target'})${deployTriggered ? ' [Auto-Recovery Triggered]' : ''}`);
  }

  appendStepSummary([
    '## Project Nox 24/7 Cloud Sentinel',
    `- **Observed At:** \`${new Date().toISOString()}\``,
    `- **DIScloud Container:** \`${initialStatus}\`${restarted ? ' (auto-restart triggered)' : ''}`,
    `- **CPU / RAM:** \`${cpu}\` / \`${ram ? `${ram.usedMb}MB / ${ram.totalMb}MB (${ram.percent}%)` : (appInfo.memory || 'N/A')}\``,
    `- **Deployed Commit:** \`${runningSha ? runningSha.slice(0, 7) : 'UNKNOWN'}\``,
    `- **Target \`main\` Commit:** \`${targetSha ? targetSha.slice(0, 7) : 'N/A'}\``,
    `- **Deploy Sync Status:** \`${deployState.status}\` (${deployState.reason})${deployTriggered ? ' 🚀 **[AUTO-RECOVERY DISPATCHED]**' : ''}`,
    `- **Reader (/api/releases):** \`${reader.ok ? `HTTP ${reader.status} (${reader.latencyMs}ms)` : `FAIL (${reader.status ?? reader.error})`}\``,
    `- **Latest Published Chapter At:** \`${reader.latestPublishedAt || 'N/A'}\``,
    `- **Alerts:** ${alerts.length ? alerts.map(a => `\`${a}\``).join(', ') : 'None (HEALTHY)'}`,
  ]);

  if (!reader.ok || (ram && ram.percent >= 97)) {
    throw new Error(`Cloud Sentinel alert: ${alerts.join('; ')}`);
  }
}

check().catch(error => { console.error(error.message); process.exitCode = 1; });
