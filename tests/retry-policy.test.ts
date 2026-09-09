import { describe, it, expect } from 'vitest';
import { RetryPolicy, ProviderDownloadError } from '../src/core/retry-policy.js';
import { NoxWorkerStorageError } from '../src/storage/worker.js';

describe('RetryPolicy - Classification & Adaptive Backoff', () => {
  describe('Classification', () => {
    it('classifies NoxWorkerStorageError with 502 as QUEUE_RETRY_STORAGE_502', () => {
      const err = new NoxWorkerStorageError('http', 502, 'Bad Gateway');
      const c = RetryPolicy.classify(err);

      expect(c.retryClass).toBe('QUEUE_RETRY_STORAGE_502');
      expect(c.sourceStage).toBe('storage');
      expect(c.isTransient).toBe(true);
      expect(c.isPermanent).toBe(false);
    });

    it('classifies NoxWorkerStorageError with 503 as QUEUE_RETRY_STORAGE_503', () => {
      const err = new NoxWorkerStorageError('http', 503, 'Service Unavailable');
      const c = RetryPolicy.classify(err);

      expect(c.retryClass).toBe('QUEUE_RETRY_STORAGE_503');
      expect(c.sourceStage).toBe('storage');
      expect(c.isTransient).toBe(true);
    });

    it('classifies NoxWorkerStorageError with 429 and Retry-After', () => {
      const err = new NoxWorkerStorageError('http', 429, 'Rate limited. Retry-After: 45s');
      (err as any).retryAfter = 45;
      const c = RetryPolicy.classify(err);

      expect(c.retryClass).toBe('QUEUE_RETRY_429');
      expect(c.sourceStage).toBe('storage');
      expect(c.retryAfterSeconds).toBe(45);
      expect(c.isTransient).toBe(true);
    });

    it('classifies ProviderDownloadError with 502 as QUEUE_RETRY_PROVIDER (transient)', () => {
      const err = new ProviderDownloadError(502, 'https://nexus.com/page1.png', 'nexus');
      const c = RetryPolicy.classify(err);

      expect(c.retryClass).toBe('QUEUE_RETRY_PROVIDER');
      expect(c.sourceStage).toBe('provider');
      expect(c.isTransient).toBe(true);
      expect(c.isPermanent).toBe(false);
    });

    it('classifies ProviderDownloadError with 404 as QUEUE_RETRY_PROVIDER (permanent)', () => {
      const err = new ProviderDownloadError(404, 'https://nexus.com/missing.png', 'nexus');
      const c = RetryPolicy.classify(err);

      expect(c.retryClass).toBe('QUEUE_RETRY_PROVIDER');
      expect(c.sourceStage).toBe('provider');
      expect(c.isTransient).toBe(false);
      expect(c.isPermanent).toBe(true);
    });

    it('differentiates Provider 429 from Storage 429', () => {
      const providerErr: any = new Error('HTTP 429 Too Many Requests from manga site');
      providerErr.status = 429;
      providerErr.retryAfter = 90;
      const c1 = RetryPolicy.classify(providerErr);
      expect(c1.retryClass).toBe('QUEUE_RETRY_429');
      expect(c1.sourceStage).toBe('provider');
      expect(c1.retryAfterSeconds).toBe(90);

      const storageErr: any = new Error('Storage bridge upload returned 429');
      storageErr.status = 429;
      storageErr.retryAfter = 30;
      const c2 = RetryPolicy.classify(storageErr);
      expect(c2.retryClass).toBe('QUEUE_RETRY_429');
      expect(c2.sourceStage).toBe('storage');
      expect(c2.retryAfterSeconds).toBe(30);
    });

    it('classifies permanent auth/client errors as QUEUE_RETRY_PERMANENT', () => {
      const err401 = new NoxWorkerStorageError('auth', 401, 'Unauthorized');
      const c1 = RetryPolicy.classify(err401);
      expect(c1.retryClass).toBe('QUEUE_RETRY_PERMANENT');
      expect(c1.isPermanent).toBe(true);

      const err404 = new Error('Work not found: HTTP 404');
      (err404 as any).status = 404;
      const c2 = RetryPolicy.classify(err404);
      expect(c2.isPermanent).toBe(true);
    });

    it('classifies network timeouts and resets as QUEUE_RETRY_TIMEOUT', () => {
      const timeoutErr = new Error('connect ETIMEDOUT 104.21.32.1:443');
      const c = RetryPolicy.classify(timeoutErr);
      expect(c.retryClass).toBe('QUEUE_RETRY_TIMEOUT');
      expect(c.isTransient).toBe(true);
    });
  });

  describe('Decision Logic (3-Level Model & Clamp)', () => {
    it('returns quick retry for attempt 1 (~30s delay with small jitter)', () => {
      const classification = RetryPolicy.classify(new NoxWorkerStorageError('http', 502, 'Storage 502'));
      const decision = RetryPolicy.decide(classification, 1, 6);

      expect(decision.status).toBe('RETRY');
      expect(decision.delaySeconds).toBeGreaterThanOrEqual(30);
      expect(decision.delaySeconds).toBeLessThanOrEqual(35);
    });

    it('returns ~60s delay for attempt 2 and ~120s for attempt 3', () => {
      const classification = RetryPolicy.classify(new NoxWorkerStorageError('http', 502, 'Storage 502'));

      const d2 = RetryPolicy.decide(classification, 2, 6);
      expect(d2.status).toBe('RETRY');
      expect(d2.delaySeconds).toBeGreaterThanOrEqual(60);
      expect(d2.delaySeconds).toBeLessThanOrEqual(70);

      const d3 = RetryPolicy.decide(classification, 3, 6);
      expect(d3.status).toBe('RETRY');
      expect(d3.delaySeconds).toBeGreaterThanOrEqual(120);
      expect(d3.delaySeconds).toBeLessThanOrEqual(140);
    });

    it('clamps queue retry delay at 300s (5 minutes) instead of unbounded exponential backoff', () => {
      const classification = RetryPolicy.classify(new NoxWorkerStorageError('http', 502, 'Storage 502'));

      const d4 = RetryPolicy.decide(classification, 4, 6);
      expect(d4.delaySeconds).toBeGreaterThanOrEqual(240);
      expect(d4.delaySeconds).toBeLessThanOrEqual(280);

      // Attempt 5 would be 480s without clamp, but must be clamped to 300s!
      const d5 = RetryPolicy.decide(classification, 5, 6);
      expect(d5.delaySeconds).toBe(300);
    });

    it('uses exact Retry-After delay for 429 errors plus micro-jitter', () => {
      const classification = RetryPolicy.classify({
        status: 429,
        retryAfter: 15,
        message: 'Storage upload rate limited',
      });
      const decision = RetryPolicy.decide(classification, 1, 6);

      expect(decision.status).toBe('RETRY');
      expect(decision.delaySeconds).toBeGreaterThanOrEqual(16);
      expect(decision.delaySeconds).toBeLessThanOrEqual(19);
    });

    it('enters persistent retry for provider 404/auth errors with clamped delay instead of terminating', () => {
      const classification = RetryPolicy.classify({
        status: 404,
        message: 'Manga chapter not found (HTTP 404)',
      });
      const decision = RetryPolicy.decide(classification, 1, 6);

      expect(decision.status).toBe('RETRY');
      expect(decision.delaySeconds).toBeGreaterThanOrEqual(45);
      expect(decision.delaySeconds).toBeLessThanOrEqual(300);
    });

    it('NEVER marks technical errors as FAILED when max_attempts is reached, keeping them in RETRY', () => {
      const classification = RetryPolicy.classify(new NoxWorkerStorageError('http', 502, 'Storage 502'));
      const decision = RetryPolicy.decide(classification, 6, 6);

      expect(decision.status).toBe('RETRY');
      expect(decision.delaySeconds).toBe(300);
    });

    it('marks fatal unrecoverable data corruption as FAILED with 0 delay', () => {
      const fatalClassification = {
        retryClass: 'FAILED' as const,
        isTransient: false,
        isPermanent: true,
        message: 'Fatal payload schema corruption',
        sourceStage: 'system' as const,
      };
      const decision = RetryPolicy.decide(fatalClassification, 1, 6);

      expect(decision.status).toBe('FAILED');
      expect(decision.delaySeconds).toBe(0);
    });
  });
});
