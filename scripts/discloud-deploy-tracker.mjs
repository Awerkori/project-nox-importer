/**
 * Project Nox — DIScloud Deployment Tracking and Recovery Logic
 * Pure functions for detecting deploy drift, preventing duplicate runs,
 * and managing bounded recovery retries.
 */

/**
 * Extracts the latest git commit SHA from DIScloud terminal logs or telemetry output.
 * @param {string} logsText
 * @returns {string | null}
 */
export function extractGitSha(logsText) {
  if (!logsText || typeof logsText !== 'string') return null;

  // Match gitSha inside JSON structures (runtimeFingerprint, BUILD_ID, etc.)
  const matches = [...logsText.matchAll(/"gitSha"\s*:\s*"([a-f0-9]{7,40})"/gi)];
  if (matches.length > 0) {
    return matches[matches.length - 1][1].toLowerCase();
  }

  // Fallback: match plain build log line: "Build: {"gitSha":"..."}" or "Build: 1e5b7b4..."
  const buildMatches = [...logsText.matchAll(/Build:\s*([a-f0-9]{7,40})/gi)];
  if (buildMatches.length > 0) {
    return buildMatches[buildMatches.length - 1][1].toLowerCase();
  }

  return null;
}

/**
 * Compares two commit SHAs, accommodating 7-char short SHAs and full 40-char SHAs.
 * @param {string | null | undefined} actual
 * @param {string | null | undefined} expected
 * @returns {boolean}
 */
export function shaMatches(actual, expected) {
  if (!actual || !expected) return false;
  const a = String(actual).toLowerCase().trim();
  const e = String(expected).toLowerCase().trim();
  if (!a || !e) return false;
  if (a.length < 7 || e.length < 7) return false;
  return a === e || a.startsWith(e) || e.startsWith(a);
}

/**
 * Inspects recent terminal log lines for critical startup or crash errors.
 * @param {string} logsText
 * @returns {{ hasFatalError: boolean, fatalErrors: string[] }}
 */
export function parseTerminalErrors(logsText) {
  if (!logsText || typeof logsText !== 'string') {
    return { hasFatalError: false, fatalErrors: [] };
  }

  const fatalPatterns = [
    /FATAL ERROR:/i,
    /JavaScript heap out of memory/i,
    /UnhandledPromiseRejection/i,
    /ELIFECYCLE/i,
    /ERR_UNHANDLED_REJECTION/i,
  ];

  const lines = logsText.split('\n');
  const tailLines = lines.slice(-50);
  const detected = [];

  for (const line of tailLines) {
    for (const pattern of fatalPatterns) {
      if (pattern.test(line)) {
        detected.push(line.trim());
        break;
      }
    }
  }

  return {
    hasFatalError: detected.length > 0,
    fatalErrors: detected,
  };
}

/**
 * Evaluates whether DIScloud is in sync with the desired target commit,
 * checking for active workflow runs (duplicate prevention) and failed attempts (bounded retries).
 *
 * @param {Object} params
 * @param {string | null} params.runningSha - SHA currently running on DIScloud
 * @param {string | null} params.targetSha - Target commit SHA on main
 * @param {Array<Object>} [params.workflowRuns] - Recent workflow runs for deploy-discloud.yml
 * @param {number} [params.maxRetries=3] - Maximum retry attempts before giving up
 * @returns {{
 *   status: 'IN_SYNC' | 'DEPLOYING' | 'PENDING_RECOVERY' | 'RETRIES_EXCEEDED' | 'UNKNOWN',
 *   shouldTrigger: boolean,
 *   reason: string,
 *   failedAttempts: number,
 *   activeRun: Object | null
 * }}
 */
export function evaluateDiscloudDeployState({
  runningSha,
  targetSha,
  workflowRuns = [],
  maxRetries = 3,
}) {
  if (!targetSha) {
    return {
      status: 'UNKNOWN',
      shouldTrigger: false,
      reason: 'Target commit SHA could not be determined.',
      failedAttempts: 0,
      activeRun: null,
    };
  }

  if (runningSha && shaMatches(runningSha, targetSha)) {
    return {
      status: 'IN_SYNC',
      shouldTrigger: false,
      reason: `DIScloud is running expected commit (${runningSha.slice(0, 7)}).`,
      failedAttempts: 0,
      activeRun: null,
    };
  }

  // Check if a deployment is currently in progress or queued
  const activeRun = workflowRuns.find(run => {
    const s = String(run.status || '').toLowerCase();
    return s === 'in_progress' || s === 'queued' || s === 'waiting' || s === 'requested';
  }) || null;

  if (activeRun) {
    return {
      status: 'DEPLOYING',
      shouldTrigger: false,
      reason: `Deployment workflow run #${activeRun.id || activeRun.databaseId || 'active'} is currently in progress (${activeRun.status}).`,
      failedAttempts: 0,
      activeRun,
    };
  }

  // Count failed runs for this specific target commit
  const failedRuns = workflowRuns.filter(run => {
    const isTargetCommit = shaMatches(run.head_sha || run.headSha, targetSha);
    const conclusion = String(run.conclusion || '').toLowerCase();
    return isTargetCommit && (conclusion === 'failure' || conclusion === 'timed_out');
  });

  const failedAttempts = failedRuns.length;

  if (failedAttempts >= maxRetries) {
    return {
      status: 'RETRIES_EXCEEDED',
      shouldTrigger: false,
      reason: `Target commit ${targetSha.slice(0, 7)} has already failed ${failedAttempts} deployment attempts (max allowed: ${maxRetries}). Bounded limit reached.`,
      failedAttempts,
      activeRun: null,
    };
  }

  return {
    status: 'PENDING_RECOVERY',
    shouldTrigger: true,
    reason: `Desynchronization detected: DIScloud running ${runningSha ? runningSha.slice(0, 7) : 'unknown'}, target is ${targetSha.slice(0, 7)}. Attempt ${failedAttempts + 1}/${maxRetries}.`,
    failedAttempts,
    activeRun: null,
  };
}
