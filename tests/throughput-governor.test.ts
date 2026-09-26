import { describe, it, expect, beforeEach, vi } from 'vitest';
import { AdaptiveAutotuner, AutotunerConfig } from '../src/core/concurrency.js';
import { ProtectiveSentinel } from '../src/core/protective-sentinel.js';
import { ImporterEngine } from '../src/core/engine.js';
import { diagnostics } from '../src/core/diagnostics.js';

describe('Definitive Throughput Governor & Auto-Emergency Pause Policy', () => {
  let autotuner: AdaptiveAutotuner;

  beforeEach(() => {
    autotuner = new AdaptiveAutotuner({
      minConcurrency: 1,
      maxConcurrency: 18,
      initialConcurrency: 4,
      requiredStableCycles: 2,
      cooldownPeriodMs: 5000,
      scaleUpDwellTimeMs: 0,
      maxRssMb: 350,
      maxHeapMb: 200,
      rssSoftLimitMb: 330,
      rssHardLimitMb: 380,
      rssEmergencyLimitMb: 410,
      maxEventLoopLagMs: 300,
      desiredFloorFreshPerMin: 5,
      optimalFreshPerMinLow: 7,
      optimalFreshPerMinHigh: 9,
      preferredFreshPerMin: 10,
      maxFreshPerMin: 12,
    });
  });

  it('Requirement: Floor, Optimal, Preferred, and Ceiling targets are tracked', () => {
    const telem = autotuner.getThroughputTelemetry();
    expect(telem.targetFloor).toBe(5);
    expect(telem.optimalLow).toBe(7);
    expect(telem.optimalHigh).toBe(9);
    expect(telem.preferredHigh).toBe(10);
    expect(telem.ceiling).toBe(12);
  });

  it('Requirement: Throughput Ceiling (>= 12 cap/min) freezes scale-up and sets CEILING_REACHED', () => {
    // Record 13 fresh publications in the last minute
    for (let i = 0; i < 13; i++) {
      autotuner.recordFreshChapterPublished();
    }

    const telem = autotuner.getThroughputTelemetry();
    expect(telem.status).toBe('CEILING_REACHED');

    const result = autotuner.evaluateCycle({
      timestamp: Date.now(),
      siteHealth: 'GREEN',
      pressureScore: 0,
      pressureBreakdown: { sitePressure: 0, dbPressure: 0, memoryPressure: 0, eventLoopPressure: 0, storagePressure: 0, sourcePressure: 0, publicationPressure: 0 },
      pressureReason: 'Healthy',
      memoryMb: 150,
      heapMb: 80,
      eventLoopLagMs: 20,
    });

    expect(result.state).toBe('CEILING_REACHED');
    expect(result.action).toBe('STABLE');
    expect(result.concurrency).toBe(4); // Did not scale up!
  });

  it('Requirement: Throughput below floor (< 5 cap/min) with constraint reports THROUGHPUT_CONSTRAINED and limiting factor', () => {
    // Zero fresh chapters, staged backlog waiting predecessors
    const result = autotuner.evaluateCycle(
      {
        timestamp: Date.now(),
        siteHealth: 'GREEN',
        pressureScore: 0,
        pressureBreakdown: { sitePressure: 0, dbPressure: 0, memoryPressure: 0, eventLoopPressure: 0, storagePressure: 0, sourcePressure: 0, publicationPressure: 0 },
        pressureReason: 'Healthy',
        memoryMb: 150,
        heapMb: 80,
        eventLoopLagMs: 20,
      },
      { stagedDebt: 45 }
    );

    expect(result.state).toBe('THROUGHPUT_CONSTRAINED');
    expect(result.reason).toContain('WAITING_PREDECESSORS_STAGED');
    expect(result.concurrency).toBe(4);
  });

  it('Requirement: Catastrophic Emergency Auto-Pause sets state AUTO_EMERGENCY_PAUSE with capacity strictly >= 1', () => {
    const result = autotuner.evaluateCycle(
      {
        timestamp: Date.now(),
        siteHealth: 'RED',
        pressureScore: 85,
        pressureBreakdown: { sitePressure: 80, dbPressure: 0, memoryPressure: 0, eventLoopPressure: 0, storagePressure: 0, sourcePressure: 0, publicationPressure: 0 },
        pressureReason: 'Catastrophic degradation',
        memoryMb: 150,
        heapMb: 80,
        eventLoopLagMs: 20,
      },
      { emergencyPauseActive: true, emergencyPauseReason: 'P95 >= 10,000ms breach' }
    );

    expect(result.state).toBe('AUTO_EMERGENCY_PAUSE');
    expect(result.concurrency).toBe(1); // STRICT INVARIANT: >= 1, NEVER 0!
    expect(result.action).toBe('HOLD');
  });

  it('Requirement: Event loop lag calibrated tiers for 0.50 vCPU', () => {
    // 1. Normal lag (<300ms): evaluates healthy, scales up when stable
    autotuner.evaluateCycle();
    const normalResult = autotuner.evaluateCycle();
    expect(normalResult.concurrency).toBeGreaterThanOrEqual(4);

    // 2. Elevated transient lag (300-450ms): does NOT downscale to 1! Holds or mild downscale
    const elevatedAutotuner = new AdaptiveAutotuner({
      minConcurrency: 1,
      maxConcurrency: 18,
      initialConcurrency: 6,
      requiredStableCycles: 2,
      rssSoftLimitMb: 330,
      rssHardLimitMb: 380,
      rssEmergencyLimitMb: 410,
    });

    vi.spyOn((diagnostics as any).lagMonitor, 'getMetrics').mockReturnValue({ avgLagMs: 380 });
    const elevatedResult = elevatedAutotuner.evaluateCycle();
    expect(elevatedResult.concurrency).toBeGreaterThan(1); // STRICT: NEVER drops to 1 on elevated lag!

    // 3. High sustained lag (450-700ms): downscales gently (-1 worker), NOT to 1
    vi.spyOn((diagnostics as any).lagMonitor, 'getMetrics').mockReturnValue({ avgLagMs: 550 });
    const highResult = elevatedAutotuner.evaluateCycle();
    expect(highResult.concurrency).toBe(4); // 5 -> 4 (-1 worker)

    // 4. Critical lag (>=700ms): downscales to 1 permit (Survival)
    vi.spyOn((diagnostics as any).lagMonitor, 'getMetrics').mockReturnValue({ avgLagMs: 780 });
    const criticalResult = elevatedAutotuner.evaluateCycle();
    expect(criticalResult.concurrency).toBe(1);
    expect(criticalResult.state).toBe('SURVIVAL');

    vi.restoreAllMocks();
  });

  it('Requirement: ProtectiveSentinel catastrophic detection and auto-resume window', async () => {
    const mockSupabase: any = {
      from: vi.fn().mockReturnValue({
        select: vi.fn().mockReturnValue({
          eq: vi.fn().mockReturnValue({
            maybeSingle: vi.fn().mockResolvedValue({ data: null, error: null }),
          }),
        }),
      }),
    };

    const sentinel = new ProtectiveSentinel(mockSupabase, undefined, undefined);

    // Feed catastrophic latencies (P95 >= 10,000ms)
    for (let cycle = 1; cycle <= 3; cycle++) {
      sentinel.recordProbeResult('home', 11000, 200);
      sentinel.recordProbeResult('reader', 11000, 200);
      await sentinel.evaluatePreSlaGuardRails();
    }

    expect(sentinel.isEmergencyPaused()).toBe(true);
    expect(sentinel.getEmergencyPauseState().active).toBe(true);

    // Now feed healthy latencies (< 1500ms) for 30 cycles to clear the 20-sample window and accumulate >= 8 consecutive healthy cycles
    for (let cycle = 1; cycle <= 30; cycle++) {
      sentinel.recordProbeResult('home', 400, 200);
      sentinel.recordProbeResult('reader', 300, 200);
      await sentinel.evaluatePreSlaGuardRails();
    }

    expect(sentinel.isEmergencyPaused()).toBe(false);
    expect(sentinel.getEmergencyPauseState().resumedAt).not.toBeNull();
  });

  it('Requirement 1: AUTO_EMERGENCY_PAUSE survives process restart, keeps claims gated, and auto-resumes upon site recovery', async () => {
    const persistedState = {
      active: true,
      pausedAt: '2026-09-26T17:00:00.000Z',
      reason: 'Catastrophic site latency breach sustained for 3 cycles (Home p95: 11500ms)',
      siteP95: 11500,
      consecutiveCatastrophicCycles: 3,
      nextRecheckAt: '2026-09-26T17:00:15.000Z',
      resumedAt: null,
      healthyCyclesCount: 0,
    };

    const mockSupabase: any = {
      from: vi.fn().mockReturnValue({
        select: vi.fn().mockReturnValue({
          eq: vi.fn().mockReturnValue({
            maybeSingle: vi.fn().mockResolvedValue({
              data: { value: JSON.stringify(persistedState) },
              error: null,
            }),
          }),
        }),
      }),
    };

    // 1. Process starts up fresh: sentinel initializes with active=false in memory
    const restartedSentinel = new ProtectiveSentinel(mockSupabase, undefined, undefined, null);
    expect(restartedSentinel.isEmergencyPaused()).toBe(false);

    // 2. Hydration runs on boot: state is restored from settings table
    await restartedSentinel.hydrateAutoEmergencyPauseOnStartup();
    expect(restartedSentinel.isEmergencyPaused()).toBe(true);
    expect(restartedSentinel.getEmergencyPauseState().active).toBe(true);
    expect(restartedSentinel.getEmergencyPauseState().reason).toContain('Catastrophic site latency breach');

    // 3. Claims remain strictly gated in autotuner
    const gatedResult = autotuner.evaluateCycle(restartedSentinel.getPressureSnapshot(), {
      emergencyPauseActive: restartedSentinel.isEmergencyPaused(),
      emergencyPauseReason: restartedSentinel.getEmergencyPauseState().reason || undefined,
    });
    expect(gatedResult.state).toBe('AUTO_EMERGENCY_PAUSE');
    expect(gatedResult.concurrency).toBe(1); // Never 0, strictly minimum 1

    // 4. Site recovers: feed healthy latencies (< 1500ms) for 8 cycles
    for (let i = 1; i <= 8; i++) {
      restartedSentinel.recordProbeResult('home', 320, 200);
      restartedSentinel.recordProbeResult('reader', 280, 200);
      await restartedSentinel.evaluatePreSlaGuardRails();
    }

    // 5. Sentinel auto-resumes after stabilization window
    expect(restartedSentinel.isEmergencyPaused()).toBe(false);
    expect(restartedSentinel.getEmergencyPauseState().active).toBe(false);
    expect(restartedSentinel.getEmergencyPauseState().resumedAt).not.toBeNull();
  });

  it('Requirement 2: STRESS_DETECTED clears automatically when latency rolling window recovers without staying stale', async () => {
    const mockSupabase: any = {
      from: vi.fn().mockReturnValue({
        select: vi.fn().mockReturnValue({
          eq: vi.fn().mockReturnValue({
            maybeSingle: vi.fn().mockResolvedValue({ data: null, error: null }),
          }),
        }),
      }),
    };

    const mockPool: any = {
      query: vi.fn().mockResolvedValue({ rows: [{ total_connections: 3, active_connections: 1 }] }),
    };

    const sentinel = new ProtectiveSentinel(mockSupabase, mockPool, undefined);
    const now = Date.now();

    // Simulate transient latency spike that occurred 80 seconds ago (> 75s window)
    sentinel.recordProbeResult('home', 1202, 200, now - 80_000);

    // Subsequent recent healthy probes within the last 30 seconds
    sentinel.recordProbeResult('home', 250, 200, now - 20_000);
    sentinel.recordProbeResult('home', 280, 200, now - 10_000);
    sentinel.recordProbeResult('reader', 310, 200, now - 10_000);

    await sentinel.evaluatePreSlaGuardRails();

    const snapshot = sentinel.getPressureSnapshot();

    // Stale 1202ms spike was pruned from the 75s window!
    expect(snapshot.homeP95).toBeLessThanOrEqual(280);
    expect(snapshot.siteHealth).toBe('GREEN');

    // Autotuner evaluates cycle: transitions cleanly without phantom stress
    const result = autotuner.evaluateCycle(snapshot);
    expect(result.action).not.toBe('STRESS_DETECTED');
    expect(result.state).not.toBe('RUNNING_THROTTLED');
  });

  it('Requirement 3: Source cooldowns are strictly local and do not throttle global capacity or set global limiting factor when other sources are healthy', () => {
    const initialGlobalCapacity = autotuner.getCurrentConcurrency();
    expect(initialGlobalCapacity).toBe(4);

    // Record failures for a specific source: mangalivreto (default capacity 3)
    const fail1 = autotuner.recordSourceFailure('mangalivreto');
    const fail2 = autotuner.recordSourceFailure('mangalivreto');

    // Local source capacity was throttled (3 -> 2)
    expect(fail2.throttled).toBe(true);
    expect(fail2.newCapacity).toBe(2);

    // BUT global concurrency and global semaphore capacity remain COMPLETELY UNCHANGED
    expect(autotuner.getCurrentConcurrency()).toBe(initialGlobalCapacity);
    expect(autotuner.getGlobalChapterSemaphore().capacity).toBe(initialGlobalCapacity);

    // When some sources are in cooldown but others are healthy (allSourcesBlocked: false):
    const telem = autotuner.getThroughputTelemetry({
      allSourcesBlocked: false,
      eligibleJobs: 28912,
    });
    expect(telem.limitingFactor).not.toBe('ALL_SOURCES_IN_COOLDOWN');

    // ONLY when all viable sources are blocked (allSourcesBlocked: true):
    const blockedTelem = autotuner.getThroughputTelemetry({
      allSourcesBlocked: true,
      eligibleJobs: 28912,
    });
    expect(blockedTelem.limitingFactor).toBe('ALL_SOURCES_IN_COOLDOWN');

    const blockedCycle = autotuner.evaluateCycle(undefined, { allSourcesBlocked: true });
    expect(blockedCycle.state).toBe('WAITING_SOURCES');
  });
});
