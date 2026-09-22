import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { AdaptiveAutotuner } from '../src/core/concurrency.js';
import { diagnostics } from '../src/core/diagnostics.js';

describe('Memory Pressure State Machine Transition Flow', () => {
  beforeEach(() => {
    vi.spyOn((diagnostics as any).lagMonitor, 'getMetrics').mockReturnValue({
      avgLagMs: 5,
      maxLagMs: 15,
      recentLagMs: 5,
    });
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it('cycles through HEALTHY -> SOFT -> HARD -> EMERGENCY -> RECOVERING -> HEALTHY', async () => {
    const autotuner = new AdaptiveAutotuner({
      initialConcurrency: 5,
      minConcurrency: 1,
      maxConcurrency: 8,
      rssSoftLimitMb: 330,
      rssHardLimitMb: 380,
      rssEmergencyLimitMb: 410,
      requiredStableCycles: 2,
      cooldownPeriodMs: 200, // Short cooldown for deterministic test
    });

    // 1. STATE: HEALTHY (RSS = 200MB)
    vi.spyOn(diagnostics, 'getMemorySnapshot').mockReturnValue({
      rssMb: 200,
      heapUsedMb: 80,
      heapTotalMb: 120,
      externalMb: 10,
      arrayBuffersMb: 5,
    });

    expect(autotuner.canAdmitReservation(2 * 1024 * 1024)).toBe(true);
    let evalRes = autotuner.evaluateCycle();
    expect(evalRes.action).toBe('STABLE');
    expect(evalRes.concurrency).toBe(5);

    // 2. STATE: SOFT LIMIT REACHED (RSS = 335MB >= 330MB)
    // Add active buffer so progress exception does not bypass soft limit
    autotuner.trackBufferedBytes(1 * 1024 * 1024);
    vi.spyOn(diagnostics, 'getMemorySnapshot').mockReturnValue({
      rssMb: 335,
      heapUsedMb: 150,
      heapTotalMb: 200,
      externalMb: 10,
      arrayBuffersMb: 5,
    });

    // Backpressure: admission must be denied
    expect(autotuner.canAdmitReservation(2 * 1024 * 1024)).toBe(false);

    // Evaluator: scale down concurrency by 1 (5 -> 4) and enter cooldown
    evalRes = autotuner.evaluateCycle();
    expect(evalRes.action).toBe('SCALED_DOWN');
    expect(evalRes.concurrency).toBe(4);
    expect(evalRes.reason).toContain('High RSS');

    // 3. STATE: HARD LIMIT REACHED (RSS = 385MB >= 380MB)
    vi.spyOn(diagnostics, 'getMemorySnapshot').mockReturnValue({
      rssMb: 385,
      heapUsedMb: 180,
      heapTotalMb: 250,
      externalMb: 20,
      arrayBuffersMb: 10,
    });

    // Backpressure: admission denied
    expect(autotuner.canAdmitReservation(2 * 1024 * 1024)).toBe(false);

    // Evaluator: severe downscale by 3 (4 -> 1)
    evalRes = autotuner.evaluateCycle();
    expect(evalRes.action).toBe('SCALED_DOWN');
    expect(evalRes.concurrency).toBe(1);
    expect(evalRes.reason).toContain('Hard RSS');

    // 4. STATE: EMERGENCY LIMIT REACHED (RSS = 415MB >= 410MB)
    vi.spyOn(diagnostics, 'getMemorySnapshot').mockReturnValue({
      rssMb: 415,
      heapUsedMb: 220,
      heapTotalMb: 290,
      externalMb: 30,
      arrayBuffersMb: 15,
    });

    // Evaluator: downscales to minConcurrency (1) and flags emergency
    evalRes = autotuner.evaluateCycle();
    expect(evalRes.action).toBe('STRESS_DETECTED'); // already at minConcurrency 1
    expect(evalRes.concurrency).toBe(1);
    expect(evalRes.reason).toContain('Emergency RSS');

    // 5. STATE: RECOVERING (RSS drops back to 220MB, buffers released)
    autotuner.releaseActiveBufferedBytes(1 * 1024 * 1024);
    vi.spyOn(diagnostics, 'getMemorySnapshot').mockReturnValue({
      rssMb: 220,
      heapUsedMb: 90,
      heapTotalMb: 130,
      externalMb: 10,
      arrayBuffersMb: 5,
    });

    // Headroom is restored: new reservations can be admitted again
    expect(autotuner.canAdmitReservation(2 * 1024 * 1024)).toBe(true);

    // During cooldown period, concurrency remains pegged at 1
    evalRes = autotuner.evaluateCycle();
    expect(evalRes.action).toBe('COOLDOWN');
    expect(evalRes.concurrency).toBe(1);

    // Wait for cooldown period (200ms) to elapse
    await new Promise((r) => setTimeout(r, 220));

    // 6. STATE: HEALTHY RECOVERY (Stable cycles required before scale up)
    // Cycle 1 after cooldown: stable (1/2)
    evalRes = autotuner.evaluateCycle();
    expect(evalRes.action).toBe('STABLE');
    expect(evalRes.concurrency).toBe(1);

    // Cycle 2 after cooldown: 2nd consecutive stable cycle -> scales up 1 -> 2
    evalRes = autotuner.evaluateCycle();
    expect(evalRes.action).toBe('SCALED_UP');
    expect(evalRes.concurrency).toBe(2);
  });
});
