import { describe, it, expect, vi, beforeEach } from 'vitest';
import { NoxWorkerStorageProvider, NoxWorkerStorageError } from '../src/storage/worker.js';
import { GlobalStorageRateLimiter } from '../src/core/rate-limiter.js';

describe('Storage Resilience & Rate Limiter Differentiation', () => {
  const TEST_BRIDGE_TOKEN = 'test-storage-bridge-token-12345';
  const TEST_WORKER_URL = 'https://manga.test.workers.dev';
  const DUMMY_BYTES = new Uint8Array([1, 2, 3, 4, 5, 6, 7, 8]);

  it('handles HTTP 429 by honoring Retry-After, pausing the global limiter, and retrying after cooldown', async () => {
    const rateLimiter = new GlobalStorageRateLimiter({ maxRequestsPerMinute: 60, minIntervalMs: 10 });
    const recordRateLimitSpy = vi.spyOn(rateLimiter, 'recordRateLimit');

    let callCount = 0;
    const mockTransport = vi.fn(async () => {
      callCount++;
      if (callCount === 1) {
        // Return 429 with Retry-After: 1 (1 second for quick test)
        return new Response(JSON.stringify({ error: 'Too Many Requests', retryAfter: 1 }), {
          status: 429,
          headers: { 'Retry-After': '1', 'Content-Type': 'application/json' },
        });
      }
      // Second attempt succeeds
      return new Response(JSON.stringify({ providerKey: 'tg-file-key-recovered' }), {
        status: 200,
        headers: { 'Content-Type': 'application/json' },
      });
    });

    const provider = new NoxWorkerStorageProvider(TEST_WORKER_URL, TEST_BRIDGE_TOKEN, mockTransport as any, rateLimiter);

    const providerKey = await provider.upload(DUMMY_BYTES, 'image/png', 'test-page-1');
    expect(providerKey).toBe('tg-file-key-recovered');
    expect(callCount).toBe(2);
    expect(recordRateLimitSpy).toHaveBeenCalledWith(1);
    expect(rateLimiter.isBlocked()).toBe(false); // After 1s wait, block should expire
  });

  it('applies local retry with progressive backoff on isolated HTTP 502 without blocking global limiter', async () => {
    const rateLimiter = new GlobalStorageRateLimiter({ maxRequestsPerMinute: 60, minIntervalMs: 10 });

    let callCount = 0;
    const timestamps: number[] = [];
    const mockTransport = vi.fn(async () => {
      callCount++;
      timestamps.push(Date.now());
      if (callCount === 1) {
        // Isolated 502 Bad Gateway
        return new Response('Bad Gateway upstream', { status: 502 });
      }
      return new Response(JSON.stringify({ providerKey: 'tg-file-success-after-502' }), {
        status: 200,
        headers: { 'Content-Type': 'application/json' },
      });
    });

    // Mock setTimeout to accelerate the test while verifying delay range
    const originalSetTimeout = globalThis.setTimeout;
    const delays: number[] = [];
    vi.spyOn(globalThis, 'setTimeout').mockImplementation((cb: any, ms?: number) => {
      if (ms) delays.push(ms);
      return originalSetTimeout(cb, 5); // run almost immediately in test
    });

    const provider = new NoxWorkerStorageProvider(TEST_WORKER_URL, TEST_BRIDGE_TOKEN, mockTransport as any, rateLimiter);
    const providerKey = await provider.upload(DUMMY_BYTES, 'image/png', 'test-page-502');

    expect(providerKey).toBe('tg-file-success-after-502');
    expect(callCount).toBe(2);

    // Verify first retry delay was ~1000ms + jitter (between 1000 and 1500)
    const backoffDelay = delays.find((d) => d >= 1000 && d <= 1500);
    expect(backoffDelay).toBeDefined();

    // Isolated 502 (1 failure) should NOT have blocked the global rate limiter!
    expect(rateLimiter.isBlocked()).toBe(false);
    expect(rateLimiter.getRecentTransientErrorCount()).toBe(1);

    vi.restoreAllMocks();
  });

  it('does NOT block the global rate limiter on 502/503 transient errors (reserves isBlocked for 429)', async () => {
    const rateLimiter = new GlobalStorageRateLimiter({ maxRequestsPerMinute: 60, minIntervalMs: 10 });

    expect(rateLimiter.isBlocked()).toBe(false);

    // Transient errors adjust rate but do NOT block the global limiter
    for (let i = 1; i <= 10; i++) {
      rateLimiter.recordTransientError();
      expect(rateLimiter.isBlocked()).toBe(false);
      expect(rateLimiter.getRecentTransientErrorCount()).toBe(i);
    }
  });
});
