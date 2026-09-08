import { describe, it, expect, beforeEach } from 'vitest';
import { HostRateLimiter } from '../src/core/rate-limiter.js';

describe('HostRateLimiter', () => {
  let limiter: HostRateLimiter;

  beforeEach(() => {
    limiter = new HostRateLimiter(10.0);
  });

  it('allows token acquisition within rate bounds', async () => {
    const start = Date.now();
    await limiter.acquire('api.test.com');
    await limiter.acquire('api.test.com');
    const duration = Date.now() - start;
    // Both acquisitions should complete quickly since initial capacity is available
    expect(duration).toBeLessThan(500);
  });

  it('handles 429 Retry-After header in seconds', () => {
    const wait = limiter.handle429('api.test.com', '12', 1);
    expect(wait).toBe(12);
  });

  it('handles 429 Retry-After header with HTTP Date', () => {
    const future = new Date(Date.now() + 15_000).toUTCString();
    const wait = limiter.handle429('api.test.com', future, 1);
    expect(wait).toBeGreaterThanOrEqual(14);
    expect(wait).toBeLessThanOrEqual(16);
  });

  it('falls back to exponential backoff when Retry-After is absent', () => {
    const wait1 = limiter.handle429('api.test.com', null, 1);
    expect(wait1).toBeGreaterThanOrEqual(2);
    expect(wait1).toBeLessThanOrEqual(6);

    const wait3 = limiter.handle429('api.test.com', null, 3);
    expect(wait3).toBeGreaterThanOrEqual(10);
    expect(wait3).toBeLessThanOrEqual(22);
  });
});
