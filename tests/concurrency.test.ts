import { describe, it, expect, vi, beforeEach } from 'vitest';
import { AsyncSemaphore, AdaptiveAutotuner } from '../src/core/concurrency.js';
import { diagnostics } from '../src/core/diagnostics.js';

describe('Concurrency & Autotuner', () => {
  describe('AsyncSemaphore', () => {
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
