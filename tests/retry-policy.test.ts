import { describe, it, expect } from 'vitest';
import { RetryPolicy, ProviderDownloadError, InvalidMediaError } from '../src/core/retry-policy.js';
import { NoxWorkerStorageError } from '../src/storage/worker.js';

describe('RetryPolicy - Classification & Adaptive Backoff', () => {
  describe('Classification & Error Taxonomy', () => {
    it('classifies NoxWorkerStorageError with 502 as TELEGRAM_TRANSIENT / QUEUE_RETRY_STORAGE_502', () => {
      const err = new NoxWorkerStorageError('http', 502, 'Bad Gateway');
      const c = RetryPolicy.classify(err);

      expect(c.taxonomyCode).toBe('TELEGRAM_TRANSIENT');
      expect(c.retryClass).toBe('QUEUE_RETRY_STORAGE_502');
      expect(c.sourceStage).toBe('storage');
      expect(c.isTransient).toBe(true);
      expect(c.isPermanent).toBe(false);
    });

    it('classifies NoxWorkerStorageError with 503 as TELEGRAM_TRANSIENT / QUEUE_RETRY_STORAGE_503', () => {
      const err = new NoxWorkerStorageError('http', 503, 'Service Unavailable');
      const c = RetryPolicy.classify(err);

      expect(c.taxonomyCode).toBe('TELEGRAM_TRANSIENT');
      expect(c.retryClass).toBe('QUEUE_RETRY_STORAGE_503');
      expect(c.sourceStage).toBe('storage');
      expect(c.isTransient).toBe(true);
    });

    it('classifies NoxWorkerStorageError with 429 and Retry-After as TELEGRAM_RATE_LIMIT', () => {
      const err = new NoxWorkerStorageError('http', 429, 'Rate limited. Retry-After: 45s');
      (err as any).retryAfter = 45;
      const c = RetryPolicy.classify(err);

      expect(c.taxonomyCode).toBe('TELEGRAM_RATE_LIMIT');
      expect(c.retryClass).toBe('QUEUE_RETRY_429');
      expect(c.sourceStage).toBe('storage');
      expect(c.retryAfterSeconds).toBe(45);
      expect(c.isTransient).toBe(true);
    });

    it('classifies ProviderDownloadError with 502 as SOURCE_5XX / QUEUE_RETRY_PROVIDER (transient)', () => {
      const err = new ProviderDownloadError(502, 'https://nexus.com/page1.png', 'nexus');
      const c = RetryPolicy.classify(err);

      expect(c.taxonomyCode).toBe('SOURCE_5XX');
      expect(c.retryClass).toBe('QUEUE_RETRY_PROVIDER');
      expect(c.sourceStage).toBe('provider');
      expect(c.isTransient).toBe(true);
      expect(c.isPermanent).toBe(false);
    });

    it('classifies ProviderDownloadError with 404 as IMAGE_404 / QUEUE_RETRY_PROVIDER (permanent)', () => {
      const err = new ProviderDownloadError(404, 'https://nexus.com/missing.png', 'nexus');
      const c = RetryPolicy.classify(err);

      expect(c.taxonomyCode).toBe('IMAGE_404');
      expect(c.retryClass).toBe('QUEUE_RETRY_PROVIDER');
      expect(c.sourceStage).toBe('provider');
      expect(c.isTransient).toBe(false);
      expect(c.isPermanent).toBe(true);
      expect(c.needsRevalidation).toBe(true);
    });

    it('classifies Formato não permitido as INVALID_MEDIA', () => {
      const err = new Error('Formato não permitido. Use PNG, JPEG, WebP, GIF ou AVIF.');
      const c = RetryPolicy.classify(err);

      expect(c.taxonomyCode).toBe('INVALID_MEDIA');
      expect(c.retryClass).toBe('QUEUE_RETRY_PROVIDER');
      expect(c.sourceStage).toBe('provider');
      expect(c.needsRevalidation).toBe(true);
      expect(c.retryBudgetMax).toBe(3);
    });

    it('classifies 0 pages returned as EMPTY_PAGES', () => {
      const err = new Error('Source mrtenzus failed to return pages for chapter 0 (https://mrtenzus.com/manga/...)');
      const c = RetryPolicy.classify(err);

      expect(c.taxonomyCode).toBe('EMPTY_PAGES');
      expect(c.retryClass).toBe('QUEUE_RETRY_PROVIDER');
      expect(c.sourceStage).toBe('provider');
      expect(c.retryBudgetMax).toBe(2);
    });

    it('classifies Lease expired as LEASE_EXPIRED', () => {
      const err = new Error('Lease expired / worker unresponsive');
      const c = RetryPolicy.classify(err);

      expect(c.taxonomyCode).toBe('LEASE_EXPIRED');
      expect(c.sourceStage).toBe('system');
      expect(c.retryBudgetMax).toBe(3);
    });

    it('classifies JobExecutionTimeout as WORKER_STALL', () => {
      const err = new Error('JobExecutionTimeout: Job exceeded safety limit of 5 minutes');
      const c = RetryPolicy.classify(err);

      expect(c.taxonomyCode).toBe('WORKER_STALL');
      expect(c.sourceStage).toBe('system');
      expect(c.retryBudgetMax).toBe(3);
    });

    it('differentiates Provider 429 from Storage 429', () => {
      const providerErr: any = new Error('HTTP 429 Too Many Requests from manga site');
      providerErr.status = 429;
      providerErr.retryAfter = 90;
      const c1 = RetryPolicy.classify(providerErr);
      expect(c1.taxonomyCode).toBe('SOURCE_429');
      expect(c1.retryClass).toBe('QUEUE_RETRY_429');
      expect(c1.sourceStage).toBe('provider');
      expect(c1.retryAfterSeconds).toBe(90);

      const storageErr: any = new Error('Storage bridge upload returned 429');
      storageErr.status = 429;
      storageErr.retryAfter = 30;
      const c2 = RetryPolicy.classify(storageErr);
      expect(c2.taxonomyCode).toBe('TELEGRAM_RATE_LIMIT');
      expect(c2.retryClass).toBe('QUEUE_RETRY_429');
      expect(c2.sourceStage).toBe('storage');
      expect(c2.retryAfterSeconds).toBe(30);
    });

    it('classifies permanent auth/client errors as PERMANENT_NOT_FOUND', () => {
      const err401 = new NoxWorkerStorageError('auth', 401, 'Unauthorized');
      const c1 = RetryPolicy.classify(err401);
      expect(c1.taxonomyCode).toBe('PERMANENT_NOT_FOUND');
      expect(c1.isPermanent).toBe(true);
    });

    it('classifies network timeouts and resets as IMAGE_TIMEOUT or SOURCE_TIMEOUT', () => {
      const timeoutErr = new Error('connect ETIMEDOUT 104.21.32.1:443');
      const c = RetryPolicy.classify(timeoutErr);
      expect(c.taxonomyCode).toBe('TRANSIENT_NETWORK');
      expect(c.isTransient).toBe(true);

      const imgTimeout = new Error('Image download ETIMEDOUT from cdn.manga.com');
      const cImg = RetryPolicy.classify(imgTimeout);
      expect(cImg.taxonomyCode).toBe('IMAGE_TIMEOUT');
    });
  });

  describe('Decision Logic (Strict Retry Budget & Adaptive Backoff)', () => {
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

    it('terminates EMPTY_PAGES after budget is exhausted (2 attempts)', () => {
      const classification = RetryPolicy.classify(new Error('Source failed to return pages for chapter 0'));
      const d1 = RetryPolicy.decide(classification, 1, 5);
      expect(d1.status).toBe('RETRY');

      const d2 = RetryPolicy.decide(classification, 2, 5);
      expect(d2.status).toBe('FAILED');
      expect(d2.reason).toContain('RETRY_BUDGET_EXHAUSTED');
    });

    it('terminates INVALID_MEDIA after budget is exhausted (3 attempts)', () => {
      const classification = RetryPolicy.classify(new Error('Formato não permitido. Use PNG, JPEG, WebP, GIF ou AVIF.'));
      const d1 = RetryPolicy.decide(classification, 1, 5);
      expect(d1.status).toBe('RETRY');

      const d2 = RetryPolicy.decide(classification, 2, 5);
      expect(d2.status).toBe('RETRY');

      const d3 = RetryPolicy.decide(classification, 3, 5);
      expect(d3.status).toBe('FAILED');
      expect(d3.reason).toContain('RETRY_BUDGET_EXHAUSTED');
    });

    it('terminates jobs at global ceiling of 7 attempts (NO INFINITE RETRIES)', () => {
      const classification = RetryPolicy.classify(new NoxWorkerStorageError('http', 502, 'Storage 502'));
      const decision = RetryPolicy.decide(classification, 7, 7);

      expect(decision.status).toBe('FAILED');
      expect(decision.reason).toContain('RETRY_BUDGET_EXHAUSTED');
      expect(decision.delaySeconds).toBe(0);
    });

    it('marks fatal unrecoverable data corruption as FAILED with 0 delay', () => {
      const fatalClassification = {
        taxonomyCode: 'PERMANENT_NOT_FOUND' as const,
        retryClass: 'FAILED' as const,
        isTransient: false,
        isPermanent: true,
        retryBudgetMax: 1,
        needsRevalidation: false,
        message: 'Fatal payload schema corruption',
        structuredMessage: '[PERMANENT_NOT_FOUND] Fatal payload schema corruption',
        sourceStage: 'system' as const,
      };
      const decision = RetryPolicy.decide(fatalClassification, 1, 6);

      expect(decision.status).toBe('FAILED');
      expect(decision.delaySeconds).toBe(0);
    });
  });
});
