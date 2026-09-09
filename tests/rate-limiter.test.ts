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

  it('implements AIMD scale-up on consecutive successes', () => {
    limiter.setHostRate('cdn.test.com', 8.0, 16, 16.0);
    expect(limiter.getHostRate('cdn.test.com')).toBe(8.0);

    // 8 consecutive successes triggers +0.5 req/s increase
    for (let i = 0; i < 8; i++) {
      limiter.recordSuccess('cdn.test.com');
    }
    expect(limiter.getHostRate('cdn.test.com')).toBe(8.5);

    // another 8 successes triggers another +0.5 req/s
    for (let i = 0; i < 8; i++) {
      limiter.recordSuccess('cdn.test.com');
    }
    expect(limiter.getHostRate('cdn.test.com')).toBe(9.0);
  });

  it('implements AIMD multiplicative decrease on 429 response', () => {
    limiter.setHostRate('cdn.test.com', 10.0, 20, 20.0, 2.0);
    expect(limiter.getHostRate('cdn.test.com')).toBe(10.0);

    limiter.handle429('cdn.test.com', '5', 1);
    // 10.0 * 0.7 = 7.0
    expect(limiter.getHostRate('cdn.test.com')).toBeCloseTo(7.0, 1);
  });

  it('enables turbo mode boosting host capacity for priority works', () => {
    limiter.setHostRate('cdn.turbo.com', 8.0, 16, 16.0);
    expect(limiter.getHostRate('cdn.turbo.com')).toBe(8.0);

    limiter.setTurboMode(true);
    expect(limiter.isTurboMode()).toBe(true);
    // 8.0 * 1.5 = 12.0
    expect(limiter.getHostRate('cdn.turbo.com')).toBe(12.0);

    limiter.setTurboMode(false);
    expect(limiter.isTurboMode()).toBe(false);
    expect(limiter.getHostRate('cdn.turbo.com')).toBe(8.0);
  });
});
