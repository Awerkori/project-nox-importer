import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { AdaptiveAutotuner } from '../src/core/concurrency.js';
import { diagnostics } from '../src/core/diagnostics.js';

describe('AdaptiveAutotuner Memory Backpressure', () => {
  beforeEach(() => {
    vi.restoreAllMocks();
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it('allows immediate proceed when memory and buffer are below thresholds', async () => {
    const autotuner = new AdaptiveAutotuner({
      maxBufferedBytes: 64 * 1024 * 1024,
      rssSoftLimitMb: 330,
    });

    vi.spyOn(diagnostics, 'getMemorySnapshot').mockReturnValue({
      rssMb: 150,
      heapUsedMb: 80,
      heapTotalMb: 120,
      externalMb: 10,
      arrayBuffersMb: 5,
    });

    // Should resolve immediately without blocking
    await expect(autotuner.waitForMemoryHeadroom(2 * 1024 * 1024)).resolves.toBeUndefined();
  });

  it('tracks buffered bytes correctly on track and release', () => {
    const autotuner = new AdaptiveAutotuner();
    expect(autotuner.getBufferedBytes()).toBe(0);

    autotuner.trackBufferedBytes(10 * 1024 * 1024);
    expect(autotuner.getBufferedBytes()).toBe(10 * 1024 * 1024);

    autotuner.trackBufferedBytes(5 * 1024 * 1024);
    expect(autotuner.getBufferedBytes()).toBe(15 * 1024 * 1024);

    autotuner.releaseBufferedBytes(8 * 1024 * 1024);
    expect(autotuner.getBufferedBytes()).toBe(7 * 1024 * 1024);

    autotuner.releaseBufferedBytes(20 * 1024 * 1024); // Clamped at 0
    expect(autotuner.getBufferedBytes()).toBe(0);
  });

  it('blocks in waitForMemoryHeadroom when buffer is full and unblocks when released', async () => {
    const autotuner = new AdaptiveAutotuner({
      maxBufferedBytes: 10 * 1024 * 1024, // 10 MB ceiling
      rssSoftLimitMb: 330,
    });

    vi.spyOn(diagnostics, 'getMemorySnapshot').mockReturnValue({
      rssMb: 200,
      heapUsedMb: 80,
      heapTotalMb: 120,
      externalMb: 10,
      arrayBuffersMb: 5,
    });

    // Fill buffer to 9MB
    autotuner.trackBufferedBytes(9 * 1024 * 1024);

    // Requesting 2MB will exceed 10MB ceiling -> should wait
    let unblocked = false;
    const waitPromise = autotuner.waitForMemoryHeadroom(2 * 1024 * 1024).then(() => {
      unblocked = true;
    });

    // Yield macro-task
    await new Promise((r) => setTimeout(r, 50));
    expect(unblocked).toBe(false);

    // Release 6MB of buffers -> buffer drops to 3MB (< 7.5MB)
    autotuner.releaseBufferedBytes(6 * 1024 * 1024);

    await waitPromise;
    expect(unblocked).toBe(true);
  });

  it('downscales concurrency under emergency memory stress and triggers gc', () => {
    const autotuner = new AdaptiveAutotuner({
      initialConcurrency: 8,
      minConcurrency: 2,
      rssEmergencyLimitMb: 410,
    });

    let gcCalled = false;
    (global as any).gc = () => { gcCalled = true; };

    vi.spyOn(diagnostics, 'getMemorySnapshot').mockReturnValue({
      rssMb: 430, // Exceeds emergency 410MB
      heapUsedMb: 250,
      heapTotalMb: 300,
      externalMb: 150,
      arrayBuffersMb: 100,
    });
    vi.spyOn((diagnostics as any).lagMonitor, 'getMetrics').mockReturnValue({
      avgLagMs: 5,
      maxLagMs: 10,
      recentLagMs: 5,
    });

    const res = autotuner.evaluateCycle();
    expect(res.action).toBe('SCALED_DOWN');
    expect(res.concurrency).toBe(2); // Dropped to minConcurrency
    expect(gcCalled).toBe(true);
    delete (global as any).gc;
  });
});
