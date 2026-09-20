import { describe, it, expect, vi, beforeEach } from 'vitest';
import { AsyncSemaphore, AdaptiveAutotuner } from '../src/core/concurrency.js';
import { diagnostics } from '../src/core/diagnostics.js';

describe('Concurrency & Autotuner', () => {
  describe('AsyncSemaphore', () => {
    it('does not grant newly added capacity twice when callers are waiting', async () => {
      const sem = new AsyncSemaphore(1);
      await sem.acquire();
      const waiting = sem.acquire();
      sem.setCapacity(2);
      await waiting;
      expect(sem.active).toBe(2);
      expect(sem.available).toBe(0);
      let admitted = false;
      const third = sem.acquire().then(() => { admitted = true; });
      await Promise.resolve();
      expect(admitted).toBe(false);
      sem.release();
      await third;
      expect(sem.active).toBe(2);
      sem.release();
      sem.release();
    });

    it('drains existing holders before admitting callers after a downscale', async () => {
      const sem = new AsyncSemaphore(3);
      await Promise.all([sem.acquire(), sem.acquire(), sem.acquire()]);
      let admitted = false;
      const waiting = sem.acquire().then(() => { admitted = true; });
      sem.setCapacity(1);
      expect(sem.active).toBe(3);
      sem.release();
      sem.release();
      await Promise.resolve();
      expect(admitted).toBe(false);
      expect(sem.active).toBe(1);
      sem.release();
      await waiting;
      expect(sem.active).toBe(1);
      sem.release();
      expect(sem.available).toBe(1);
    });

    it('limits concurrent executions to capacity', async () => {
      const sem = new AsyncSemaphore(2);
      let running = 0;
      let maxRunning = 0;

      const task = async () => {
        await sem.acquire();
        running++;
        maxRunning = Math.max(maxRunning, running);
        await new Promise((r) => setTimeout(r, 10));
        running--;
        sem.release();
      };

      await Promise.all([task(), task(), task(), task(), task()]);
      expect(maxRunning).toBe(2);
      expect(sem.available).toBe(2);
      expect(sem.active).toBe(0);
    });

    it('dynamically adjusts capacity', () => {
      const sem = new AsyncSemaphore(2);
      expect(sem.capacity).toBe(2);
      sem.setCapacity(4);
      expect(sem.capacity).toBe(4);
      expect(sem.available).toBe(4);
      sem.setCapacity(1);
      expect(sem.capacity).toBe(1);
    });
  });

  describe('AdaptiveAutotuner', () => {
    it('requires multiple stable cycles before scaling up (no single-cycle scale-up)', () => {
      const autotuner = new AdaptiveAutotuner({
        initialConcurrency: 3,
        maxConcurrency: 6,
        requiredStableCycles: 4,
        cooldownPeriodMs: 60_000,
      });

      // Stub diagnostics memory snapshot to safe values
      vi.spyOn(diagnostics, 'getMemorySnapshot').mockReturnValue({
        rssMb: 150,
        heapUsedMb: 80,
        heapTotalMb: 120,
        externalMb: 10,
        arrayBuffersMb: 5,
      });
      vi.spyOn((diagnostics as any).lagMonitor, 'getMetrics').mockReturnValue({
        avgLagMs: 5,
        maxLagMs: 15,
        recentLagMs: 5,
      });

      // Cycle 1: stable, should NOT scale up yet
      let res = autotuner.evaluateCycle();
      expect(res.action).toBe('STABLE');
      expect(res.concurrency).toBe(3);

      // Cycle 2: stable, should NOT scale up yet
      res = autotuner.evaluateCycle();
      expect(res.action).toBe('STABLE');
      expect(res.concurrency).toBe(3);

      // Cycle 3: stable, should NOT scale up yet
      res = autotuner.evaluateCycle();
      expect(res.action).toBe('STABLE');
      expect(res.concurrency).toBe(3);

      // Cycle 4: 4th stable cycle reached -> NOW scales up 3 -> 4
      res = autotuner.evaluateCycle();
      expect(res.action).toBe('SCALED_UP');
      expect(res.concurrency).toBe(4);
    });

    it('immediately scales down on stress and applies cooldown', () => {
      const autotuner = new AdaptiveAutotuner({
        initialConcurrency: 4,
        minConcurrency: 1,
        maxConcurrency: 6,
        requiredStableCycles: 4,
        cooldownPeriodMs: 60_000,
        maxRssMb: 360,
      });

      vi.spyOn(diagnostics, 'getMemorySnapshot').mockReturnValue({
        rssMb: 390, // Exceeds 360MB limit!
        heapUsedMb: 100,
        heapTotalMb: 150,
        externalMb: 10,
        arrayBuffersMb: 5,
      });
      vi.spyOn((diagnostics as any).lagMonitor, 'getMetrics').mockReturnValue({
        avgLagMs: 10,
        maxLagMs: 20,
        recentLagMs: 10,
      });

      const res = autotuner.evaluateCycle();
      expect(res.action).toBe('SCALED_DOWN');
      expect(res.concurrency).toBe(3); // Reduced by 1 level: 4 -> 3
      expect(res.reason).toContain('High RSS');

      // Subsequent cycle even if memory recovers should be in COOLDOWN
      vi.spyOn(diagnostics, 'getMemorySnapshot').mockReturnValue({
        rssMb: 150,
        heapUsedMb: 80,
        heapTotalMb: 120,
        externalMb: 10,
        arrayBuffersMb: 5,
      });
      const coolRes = autotuner.evaluateCycle();
      expect(coolRes.action).toBe('COOLDOWN');
    });

    it('immediately scales down when 429 rate limit or errors are recorded', () => {
      const autotuner = new AdaptiveAutotuner({
        initialConcurrency: 3,
        minConcurrency: 1,
        maxConcurrency: 6,
        requiredStableCycles: 4,
        cooldownPeriodMs: 60_000,
      });

      vi.spyOn(diagnostics, 'getMemorySnapshot').mockReturnValue({
        rssMb: 120,
        heapUsedMb: 50,
        heapTotalMb: 80,
        externalMb: 5,
        arrayBuffersMb: 2,
      });
      vi.spyOn((diagnostics as any).lagMonitor, 'getMetrics').mockReturnValue({
        avgLagMs: 5,
        maxLagMs: 10,
        recentLagMs: 5,
      });

      autotuner.recordError('ratelimit');

      const res = autotuner.evaluateCycle();
      expect(res.action).toBe('SCALED_DOWN');
      expect(res.reason).toContain('429');
    });
  });
});
