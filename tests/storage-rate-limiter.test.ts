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

  it('adapts rate upwards on healthy throughput window (AIMD additive increase)', () => {
    // Simulate window of 30s with 25 successful uploads
    const now = Date.now();
    (limiter as any).windowStartTime = now - 31_000;
    (limiter as any).consecutiveSuccessfulUploads = 25;
    (limiter as any).windowUploadCount = 25;
    (limiter as any).windowDurationSumMs = 25 * 800; // 800ms avg latency (healthy)

    const initialRate = limiter.getCurrentRatePerMinute(); // 105
    limiter.recordSuccess(1024 * 500, 800);

    // Initial probe increases rate by +2
    expect(limiter.getCurrentRatePerMinute()).toBe(initialRate + 2);
  });

  it('detects throughput plateau with hysteresis and maintains rate without ratcheting', () => {
    const now = Date.now();
    (limiter as any).windowStartTime = now - 31_000;
    (limiter as any).consecutiveSuccessfulUploads = 25;
    (limiter as any).windowUploadCount = 50; // 50 * 2 = 100 pages/min
    (limiter as any).previousWindowRate = 100; // 0% delta (plateau)
    (limiter as any).windowDurationSumMs = 50 * 1200;

    const rateBefore = limiter.getCurrentRatePerMinute();
    limiter.recordSuccess(1024 * 300, 1200);

    // Rate should remain unchanged at plateau
    expect(limiter.getCurrentRatePerMinute()).toBe(rateBefore);
  });

  it('respects safety ceiling and never exceeds it', () => {
    const safetyCeiling = (limiter as any).safetyCeilingRate;
    (limiter as any).currentRatePerMinute = safetyCeiling;

    const now = Date.now();
    (limiter as any).windowStartTime = now - 31_000;
    (limiter as any).consecutiveSuccessfulUploads = 25;
    (limiter as any).windowUploadCount = 60;
    (limiter as any).previousWindowRate = 50; // High gain
    (limiter as any).windowDurationSumMs = 60 * 1000;

    limiter.recordSuccess(1024 * 300, 1000);
    expect(limiter.getCurrentRatePerMinute()).toBe(safetyCeiling);
  });

  it('provides detailed metrics summary', () => {
    limiter.recordSuccess(1024 * 1024, 1500);
    limiter.recordTransientError();

    const summary = limiter.getMetricsSummary();
    expect(summary.currentRate).toBeDefined();
    expect(summary.peakTestedRate).toBeDefined();
    expect(summary.recent502Count).toBe(1);
    expect(summary.isBlocked).toBe(false);
  });
});
