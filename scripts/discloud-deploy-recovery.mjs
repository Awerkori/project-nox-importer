#!/usr/bin/env node
/**
 * Project Nox — Autonomous DIScloud Deployment Recovery & Drift Watchdog
 *
 * Continuously verifies that DIScloud is running the latest reviewed commit of main.
 * If auto-deploy fails, hangs, or desynchronizes:
 * - Prevents duplicate runs by checking for active/queued deploy workflows.
 * - Enforces bounded retries (max 3 per commit SHA).
 * - Dispatches official GitHub Actions deployment workflow with discloud-production credentials.
 * - Logs structured failures and state to scratch/discloud_deploy_recovery_status.json.
 *
 * Usage:
 *   node scripts/discloud-deploy-recovery.mjs           # One-shot check
 *   node scripts/discloud-deploy-recovery.mjs --watch   # Continuous monitoring loop (every 60s)
 */

import fs from 'node:fs';
import path from 'node:path';
import { execSync } from 'node:child_process';
import { setTimeout as sleep } from 'node:timers/promises';
import { getYugabytePool } from '../build/db/yugabyte-direct.js';
import { telemetryCollector } from '../build/core/telemetry-collector.js';
import {
  extractGitSha,
  shaMatches,
  evaluateDiscloudDeployState,
} from './discloud-deploy-tracker.mjs';

// Disable telemetryCollector in watchdog process to prevent overwriting production diagnostic telemetry
if (telemetryCollector?.stop) {
  telemetryCollector.stop();
}

const SCRATCH_DIR = path.resolve('scratch');
if (!fs.existsSync(SCRATCH_DIR)) {
  fs.mkdirSync(SCRATCH_DIR, { recursive: true });
}

const STATUS_FILE = path.join(SCRATCH_DIR, 'discloud_deploy_recovery_status.json');
const LOG_FILE = path.join(SCRATCH_DIR, 'discloud_deploy_recovery.log');
const FAILURES_FILE = path.join(SCRATCH_DIR, 'discloud_deploy_failures.json');

const APP_ID = process.env.DISCLOUD_APP_ID || '1788873398156';
const MAX_RETRIES = 3;

function log(msg) {
  const line = `[${new Date().toISOString()}] [DeployWatchdog] ${msg}\n`;
  try {
    fs.appendFileSync(LOG_FILE, line, 'utf8');
  } catch {}
  process.stdout.write(line);
}

/**
 * Resolves the target commit SHA from origin/main or local HEAD.
 */
function getTargetCommitSha() {
  try {
    const remote = execSync('git rev-parse origin/main', { encoding: 'utf8', stdio: ['ignore', 'pipe', 'ignore'] }).trim();
    if (remote && remote.length === 40) return remote;
  } catch {}

  try {
    const head = execSync('git rev-parse HEAD', { encoding: 'utf8', stdio: ['ignore', 'pipe', 'ignore'] }).trim();
    if (head && head.length === 40) return head;
  } catch {}

  return null;
}

/**
 * Resolves the commit SHA currently executing on DIScloud.
 * Priority:
 * 1. DIScloud API logs (if DISCLOUD_TOKEN provided)
 * 2. YugabyteDB diagnostic telemetry (if gitSha present)
 * 3. Latest successfully verified GitHub Actions deployment run
 */
async function getLiveDiscloudSha(pool, recentRuns = []) {
  let apiSha = null;
  const token = process.env.DISCLOUD_TOKEN;
  if (token) {
    try {
      const res = await fetch(`https://api.discloud.app/v2/app/${APP_ID}/logs`, {
        headers: { 'api-token': token.trim(), 'User-Agent': 'project-nox-deploy-recovery' },
        signal: AbortSignal.timeout(8000),
      });
      if (res.ok) {
        const body = await res.json();
        const terminal = body?.apps?.terminal?.big || body?.apps?.terminal?.small || body?.apps?.terminal || '';
        apiSha = extractGitSha(typeof terminal === 'string' ? terminal : JSON.stringify(terminal));
      }
    } catch {}
  }

  let dbSha = null;
  let dbBootedAt = null;
  let dbReportedAt = null;

  if (pool) {
    let client;
    try {
      client = await pool.connect();
      const res = await client.query(`
        SELECT data->'runtimeFingerprint' as fp, created_at
        FROM importer_diagnostic_telemetry
        WHERE id = 'session-runtime' AND (data->'runtimeFingerprint'->>'gitSha') IS NOT NULL
        LIMIT 1;
      `);
      if (res.rows.length > 0) {
        const fp = res.rows[0].fp;
        dbSha = fp?.gitSha || null;
        dbBootedAt = fp?.bootedAt || null;
        dbReportedAt = res.rows[0].created_at || null;
      }
    } catch (err) {
      log(`DB telemetry query notice: ${err.message}`);
    } finally {
      if (client) client.release();
    }
  }

  // Fallback to latest successfully verified deploy run
  let ciSha = null;
  const latestSuccessfulDeploy = recentRuns.find(run => {
    return String(run.status || '').toLowerCase() === 'completed' &&
           String(run.conclusion || '').toLowerCase() === 'success' &&
           run.headSha;
  });
  if (latestSuccessfulDeploy) {
    ciSha = latestSuccessfulDeploy.headSha;
  }

  const resolvedSha = apiSha || dbSha || ciSha;
  return {
    sha: resolvedSha,
    source: apiSha ? 'discloud-api' : (dbSha ? 'yugabyte-db' : (ciSha ? 'verified-ci-deploy' : 'unknown')),
    dbSha,
    apiSha,
    ciSha,
    bootedAt: dbBootedAt,
    lastReportedAt: dbReportedAt,
  };
}

/**
 * Queries recent workflow runs for deploy-discloud.yml via gh CLI.
 */
function getRecentDeployRuns() {
  try {
    const stdout = execSync(
      'gh run list --workflow=deploy-discloud.yml --limit 10 --json databaseId,status,conclusion,headSha,url,createdAt',
      { encoding: 'utf8', stdio: ['ignore', 'pipe', 'ignore'] }
    );
    return JSON.parse(stdout || '[]');
  } catch (err) {
    log(`gh run list warning: ${err.message}`);
    return [];
  }
}

/**
 * Triggers GitHub Actions deploy-discloud.yml workflow via gh CLI.
 */
function triggerDeployWorkflow(targetSha) {
  try {
    log(`Triggering GitHub Actions deploy workflow for commit ${targetSha.slice(0, 7)}...`);
    const stdout = execSync('gh workflow run deploy-discloud.yml --ref main', {
      encoding: 'utf8',
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    log(`gh workflow run result: ${stdout.trim()}`);
    return true;
  } catch (err) {
    log(`Failed to trigger deploy workflow via gh: ${err.message}`);
    return false;
  }
}

/**
 * Records failure event to scratch/discloud_deploy_failures.json
 */
function recordFailure(targetSha, reason, failedAttempts) {
  try {
    let failures = [];
    if (fs.existsSync(FAILURES_FILE)) {
      failures = JSON.parse(fs.readFileSync(FAILURES_FILE, 'utf8') || '[]');
    }
    failures.push({
      timestamp: new Date().toISOString(),
      targetSha,
      failedAttempts,
      reason,
    });
    fs.writeFileSync(FAILURES_FILE, JSON.stringify(failures.slice(-50), null, 2), 'utf8');
  } catch {}
}

export async function checkOnce(pool) {
  const targetSha = getTargetCommitSha();
  const workflowRuns = getRecentDeployRuns();
  const live = await getLiveDiscloudSha(pool, workflowRuns);

  const evaluation = evaluateDiscloudDeployState({
    runningSha: live.sha,
    targetSha,
    workflowRuns,
    maxRetries: MAX_RETRIES,
  });

  let triggered = false;
  if (evaluation.shouldTrigger) {
    log(`DRIFT CONFIRMED: ${evaluation.reason}`);
    triggered = triggerDeployWorkflow(targetSha);
    if (!triggered) {
      recordFailure(targetSha, 'Trigger invocation failed', evaluation.failedAttempts);
    }
  } else if (evaluation.status === 'RETRIES_EXCEEDED') {
    log(`CRITICAL: ${evaluation.reason}`);
    recordFailure(targetSha, evaluation.reason, evaluation.failedAttempts);
  } else {
    log(`[State: ${evaluation.status}] ${evaluation.reason}`);
  }

  const statusReport = {
    timestamp: new Date().toISOString(),
    status: evaluation.status,
    reason: evaluation.reason,
    targetCommit: targetSha,
    runningCommit: live.sha,
    commitSource: live.source,
    inSync: evaluation.status === 'IN_SYNC',
    liveDetails: {
      bootedAt: live.bootedAt,
      lastReportedAt: live.lastReportedAt,
      dbSha: live.dbSha,
      apiSha: live.apiSha,
      ciSha: live.ciSha,
    },
    workflowRunsCount: workflowRuns.length,
    activeRun: evaluation.activeRun ? {
      id: evaluation.activeRun.databaseId || evaluation.activeRun.id,
      status: evaluation.activeRun.status,
      url: evaluation.activeRun.url,
    } : null,
    failedAttempts: evaluation.failedAttempts,
    deployTriggered: triggered,
  };

  fs.writeFileSync(STATUS_FILE, JSON.stringify(statusReport, null, 2), 'utf8');
  return statusReport;
}

async function main() {
  const isWatch = process.argv.includes('--watch') || process.argv.includes('--daemon');
  const pool = getYugabytePool();
  if (telemetryCollector?.stop) telemetryCollector.stop();

  log('===========================================================');
  log('Project Nox — DIScloud Deployment Recovery & Drift Watchdog');
  log(`Mode: ${isWatch ? 'CONTINUOUS WATCH (every 60s)' : 'ONE-SHOT EVALUATION'}`);
  log(`Target App ID: ${APP_ID}`);
  log('===========================================================');

  try {
    const report = await checkOnce(pool);
    if (!isWatch) {
      await pool.end();
      process.exit(report.status === 'RETRIES_EXCEEDED' ? 1 : 0);
    }

    // Continuous watch loop
    while (true) {
      await sleep(60000);
      try {
        await checkOnce(pool);
      } catch (err) {
        log(`Watch loop iteration error: ${err.message}`);
      }
    }
  } catch (err) {
    log(`Fatal watchdog error: ${err.message}\n${err.stack}`);
    await pool.end();
    process.exit(1);
  }
}

if (process.argv[1] && path.resolve(process.argv[1]) === path.resolve(new URL(import.meta.url).pathname)) {
  main();
}
