import { describe, it, expect, beforeEach } from 'vitest';
import { GlobalStorageRateLimiter } from '../src/core/rate-limiter.js';

describe('GlobalStorageRateLimiter', () => {
  let limiter: GlobalStorageRateLimiter;

  beforeEach(() => {
    limiter = new GlobalStorageRateLimiter({
      maxRequestsPerMinute: 105,
      minIntervalMs: 50, // Small interval for fast testing
    });
  });

  it('initializes with target rate (105 req/min) and zero initial uploads', () => {
    expect(limiter.getCurrentRatePerMinute()).toBe(105);
    expect(limiter.getRecentUploadCount()).toBe(0);
    expect(limiter.isBlocked()).toBe(false);
  });

  it('allows token acquisition within limits and increments recent upload count', async () => {
    await limiter.acquire();
    expect(limiter.getRecentUploadCount()).toBe(1);

    await limiter.acquire();
    expect(limiter.getRecentUploadCount()).toBe(2);
  });

  it('triggers cooldown and steps down rate upon receiving HTTP 429', () => {
    limiter.recordRateLimit(5); // 5s cooldown
    expect(limiter.isBlocked()).toBe(true);
    expect(limiter.getBlockedRemainingMs()).toBeGreaterThan(3000);
    // 105 * 0.8 = 84
    expect(limiter.getCurrentRatePerMinute()).toBe(84);
  });

  it('gradually restores throttled rate back to base rate upon calling restoreRate()', () => {
    limiter.recordRateLimit(1);
    expect(limiter.getCurrentRatePerMinute()).toBe(84);

    limiter.restoreRate();
    expect(limiter.getCurrentRatePerMinute()).toBe(89);

    limiter.restoreRate();
    expect(limiter.getCurrentRatePerMinute()).toBe(94);
  });
});
