import { describe, it, expect } from 'vitest';
import {
  extractGitSha,
  shaMatches,
  parseTerminalErrors,
  evaluateDiscloudDeployState,
} from '../scripts/discloud-deploy-tracker.mjs';

describe('DIScloud Deploy Tracker', () => {
  describe('extractGitSha', () => {
    it('extracts gitSha from runtimeFingerprint JSON', () => {
      const logs = '{"timestamp":"2026-11-02T05:00:21.348Z","meta":{"runtimeFingerprint":{"gitSha":"1e5b7b494e237c24561d53058761d3c3c15c7f38"}}}';
      expect(extractGitSha(logs)).toBe('1e5b7b494e237c24561d53058761d3c3c15c7f38');
    });

    it('extracts the most recent gitSha when multiple exist', () => {
      const logs = [
        '{"meta":{"runtimeFingerprint":{"gitSha":"a38a4e5014dfe4840ce99f58f4288f5cc8355a42"}}}',
        'Starting daemon...',
        '{"meta":{"runtimeFingerprint":{"gitSha":"1e5b7b494e237c24561d53058761d3c3c15c7f38"}}}',
      ].join('\n');
      expect(extractGitSha(logs)).toBe('1e5b7b494e237c24561d53058761d3c3c15c7f38');
    });

    it('extracts gitSha from Build line fallback', () => {
      const logs = 'Starting Project Nox Importer daemon... | Build: 1e5b7b494e237c24561d53058761d3c3c15c7f38';
      expect(extractGitSha(logs)).toBe('1e5b7b494e237c24561d53058761d3c3c15c7f38');
    });

    it('returns null on empty or non-matching logs', () => {
      expect(extractGitSha('')).toBeNull();
      expect(extractGitSha(null)).toBeNull();
      expect(extractGitSha('Just a regular log line without commit sha')).toBeNull();
    });
  });

  describe('shaMatches', () => {
    const fullSha = '1e5b7b494e237c24561d53058761d3c3c15c7f38';
    const shortSha = '1e5b7b4';

    it('matches identical full SHAs', () => {
      expect(shaMatches(fullSha, fullSha)).toBe(true);
      expect(shaMatches(fullSha.toUpperCase(), fullSha)).toBe(true);
    });

    it('matches short SHA with full SHA in either order', () => {
      expect(shaMatches(fullSha, shortSha)).toBe(true);
      expect(shaMatches(shortSha, fullSha)).toBe(true);
    });

    it('returns false for mismatched SHAs', () => {
      expect(shaMatches(fullSha, 'a38a4e5014dfe4840ce99f58f4288f5cc8355a42')).toBe(false);
      expect(shaMatches(shortSha, 'a38a4e5')).toBe(false);
    });

    it('returns false for degenerate inputs', () => {
      expect(shaMatches(null, fullSha)).toBe(false);
      expect(shaMatches(fullSha, '')).toBe(false);
      expect(shaMatches('1e5b', fullSha)).toBe(false); // < 7 chars
    });
  });

  describe('parseTerminalErrors', () => {
    it('detects heap out of memory and fatal crashes', () => {
      const logs = 'Some normal logs\n<--- Last few GCs --->\nFATAL ERROR: Ineffective mark-compacts near heap limit Allocation failed - JavaScript heap out of memory';
      const result = parseTerminalErrors(logs);
      expect(result.hasFatalError).toBe(true);
      expect(result.fatalErrors.length).toBeGreaterThan(0);
    });

    it('detects unhandled promise rejections and ELIFECYCLE', () => {
      const logs = 'npm ERR! code ELIFECYCLE\nnpm ERR! errno 1\nnpm ERR! project-nox-importer@1.0.0 start: `node build/index.js`';
      const result = parseTerminalErrors(logs);
      expect(result.hasFatalError).toBe(true);
    });

    it('reports clean on healthy logs', () => {
      const logs = 'INFO: ImporterLite: Lite liveness: OK\nAll 43 sources healthy\nPublications: 650 cap/h';
      const result = parseTerminalErrors(logs);
      expect(result.hasFatalError).toBe(false);
      expect(result.fatalErrors).toHaveLength(0);
    });
  });

  describe('evaluateDiscloudDeployState', () => {
    const targetSha = '1e5b7b494e237c24561d53058761d3c3c15c7f38';
    const oldSha = 'a38a4e5014dfe4840ce99f58f4288f5cc8355a42';

    it('returns IN_SYNC when running commit matches target', () => {
      const result = evaluateDiscloudDeployState({
        runningSha: targetSha,
        targetSha,
        workflowRuns: [],
      });
      expect(result.status).toBe('IN_SYNC');
      expect(result.shouldTrigger).toBe(false);
    });

    it('returns DEPLOYING when an active workflow run is in progress', () => {
      const result = evaluateDiscloudDeployState({
        runningSha: oldSha,
        targetSha,
        workflowRuns: [
          { id: 12345, status: 'in_progress', head_sha: targetSha },
        ],
      });
      expect(result.status).toBe('DEPLOYING');
      expect(result.shouldTrigger).toBe(false);
      expect(result.activeRun).toBeDefined();
    });

    it('returns DEPLOYING when a run is queued', () => {
      const result = evaluateDiscloudDeployState({
        runningSha: oldSha,
        targetSha,
        workflowRuns: [
          { id: 12346, status: 'queued', head_sha: targetSha },
        ],
      });
      expect(result.status).toBe('DEPLOYING');
      expect(result.shouldTrigger).toBe(false);
    });

    it('returns PENDING_RECOVERY when desynchronized and under retry limit', () => {
      const result = evaluateDiscloudDeployState({
        runningSha: oldSha,
        targetSha,
        workflowRuns: [
          { id: 12340, status: 'completed', conclusion: 'failure', head_sha: targetSha },
        ],
        maxRetries: 3,
      });
      expect(result.status).toBe('PENDING_RECOVERY');
      expect(result.shouldTrigger).toBe(true);
      expect(result.failedAttempts).toBe(1);
    });

    it('returns RETRIES_EXCEEDED when failed attempts reach maxRetries', () => {
      const result = evaluateDiscloudDeployState({
        runningSha: oldSha,
        targetSha,
        workflowRuns: [
          { id: 1, status: 'completed', conclusion: 'failure', head_sha: targetSha },
          { id: 2, status: 'completed', conclusion: 'failure', head_sha: targetSha },
          { id: 3, status: 'completed', conclusion: 'timed_out', head_sha: targetSha },
        ],
        maxRetries: 3,
      });
      expect(result.status).toBe('RETRIES_EXCEEDED');
      expect(result.shouldTrigger).toBe(false);
      expect(result.failedAttempts).toBe(3);
    });

    it('ignores failed runs of older commits when evaluating targetSha retries', () => {
      const result = evaluateDiscloudDeployState({
        runningSha: oldSha,
        targetSha,
        workflowRuns: [
          { id: 1, status: 'completed', conclusion: 'failure', head_sha: '0000000000000000000000000000000000000000' },
          { id: 2, status: 'completed', conclusion: 'failure', head_sha: '1111111111111111111111111111111111111111' },
          { id: 3, status: 'completed', conclusion: 'failure', head_sha: '2222222222222222222222222222222222222222' },
        ],
        maxRetries: 3,
      });
      expect(result.status).toBe('PENDING_RECOVERY');
      expect(result.shouldTrigger).toBe(true);
      expect(result.failedAttempts).toBe(0);
    });

    it('returns UNKNOWN when targetSha is missing', () => {
      const result = evaluateDiscloudDeployState({
        runningSha: oldSha,
        targetSha: null,
      });
      expect(result.status).toBe('UNKNOWN');
      expect(result.shouldTrigger).toBe(false);
    });
  });
});
