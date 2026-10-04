import { describe, expect, it } from 'vitest';
import { GlobalStorageRateLimiter, HostRateLimiter } from '../src/core/rate-limiter.js';

describe('rate limiter cancellation', () => {
  it('aborts a host wait before a stalled job can retain its slot', async () => {
    const limiter = new HostRateLimiter(0.01);
    const controller = new AbortController();
    controller.abort(new Error('job deadline'));

    await expect(limiter.acquire('example.test', controller.signal)).rejects.toThrow('job deadline');
  });

  it('aborts a storage pacing wait during job cancellation', async () => {
    const limiter = new GlobalStorageRateLimiter({
      maxRequestsPerMinute: 1,
      minRequestsPerMinute: 1,
      minIntervalMs: 60_000,
    });
    await limiter.acquire();

    const controller = new AbortController();
    const pending = limiter.acquire(controller.signal);
    controller.abort(new Error('lease lost'));

    await expect(pending).rejects.toThrow('lease lost');
  });
});
