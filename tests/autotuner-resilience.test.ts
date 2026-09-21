import { describe, it, expect, vi, beforeEach } from 'vitest';
import { AdaptiveAutotuner } from '../src/core/concurrency.js';
import { diagnostics } from '../src/core/diagnostics.js';

describe('Autotuner Resilience & Penalty Elimination', () => {
  beforeEach(() => {
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
  });

  it('does NOT scale down or enter cooldown on an isolated error (cycleErrors === 1)', () => {
    const autotuner = new AdaptiveAutotuner({
      initialConcurrency: 3,
      minConcurrency: 1,
      maxConcurrency: 5,
      requiredStableCycles: 2,
      cooldownPeriodMs: 35_000,
    });

    expect(autotuner.getCurrentConcurrency()).toBe(3);

    // Record an isolated error in cycle
    autotuner.recordError('error');

    const result = autotuner.evaluateCycle();
    // Concurrency preserved at 3!
    expect(result.action).toBe('STABLE');
    expect(result.concurrency).toBe(3);
    expect(autotuner.getCurrentConcurrency()).toBe(3);

    // Next cycle (stable): should remain at 3 (first stable cycle of 2 needed to scale up)
    const nextResult = autotuner.evaluateCycle();
    expect(nextResult.action).toBe('STABLE');
    expect(nextResult.concurrency).toBe(3);

    // Second consecutive stable cycle: scales up 3 -> 4!
    const scaledResult = autotuner.evaluateCycle();
    expect(scaledResult.action).toBe('SCALED_UP');
    expect(scaledResult.concurrency).toBe(4);
  });

  it('does NOT scale down or enter cooldown on an isolated timeout (cycleTimeouts === 1)', () => {
    const autotuner = new AdaptiveAutotuner({
      initialConcurrency: 2,
      minConcurrency: 1,
      maxConcurrency: 5,
      requiredStableCycles: 2,
      cooldownPeriodMs: 35_000,
    });

    autotuner.recordError('timeout');

    const result = autotuner.evaluateCycle();
    expect(result.action).toBe('STABLE');
    expect(result.concurrency).toBe(2);
  });

  it('detects stress and enters cooldown without downscaling when error pattern (cycleErrors >= 2) occurs', () => {
    const autotuner = new AdaptiveAutotuner({
      initialConcurrency: 3,
      minConcurrency: 1,
      maxConcurrency: 5,
      requiredStableCycles: 2,
      cooldownPeriodMs: 35_000,
    });

    autotuner.recordError('error');
    autotuner.recordError('error');

    const result = autotuner.evaluateCycle();
    expect(result.action).toBe('STRESS_DETECTED');
    expect(result.concurrency).toBe(3);
    expect(result.reason).toContain('Detected error pattern');

    // Immediate next evaluation while in cooldown
    const cooldownResult = autotuner.evaluateCycle();
    expect(cooldownResult.action).toBe('COOLDOWN');
  });

  it('accelerates scale-up ramp with 2 stable cycles instead of 4', () => {
    const autotuner = new AdaptiveAutotuner({
      initialConcurrency: 1,
      minConcurrency: 1,
      maxConcurrency: 3,
      requiredStableCycles: 2,
      cooldownPeriodMs: 35_000,
    });

    // Cycle 1: stable
    let res = autotuner.evaluateCycle();
    expect(res.action).toBe('STABLE');
    expect(res.concurrency).toBe(1);

    // Cycle 2: 2nd consecutive stable cycle -> scale up 1 -> 2!
    res = autotuner.evaluateCycle();
    expect(res.action).toBe('SCALED_UP');
    expect(res.concurrency).toBe(2);

    // Cycle 3: stable
    res = autotuner.evaluateCycle();
    expect(res.action).toBe('STABLE');
    expect(res.concurrency).toBe(2);

    // Cycle 4: 2nd consecutive stable cycle -> scale up 2 -> 3!
    res = autotuner.evaluateCycle();
    expect(res.action).toBe('SCALED_UP');
    expect(res.concurrency).toBe(3);
  });
});
