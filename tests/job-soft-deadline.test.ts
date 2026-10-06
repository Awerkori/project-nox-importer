import { afterEach, describe, expect, it, vi } from 'vitest';
import { getJobAbortError, ImporterEngine } from '../src/core/engine.js';
import { RetryPolicy } from '../src/core/retry-policy.js';

describe('ImporterEngine job soft deadline', () => {
  afterEach(() => vi.useRealTimers());

  it('does not release the heartbeat-bearing execution before an over-deadline operation settles', async () => {
    vi.useFakeTimers();
    let resolveExecution!: () => void;
    const execution = new Promise<void>((resolve) => { resolveExecution = resolve; });
    const stop = vi.fn();
    const logger = { warn: vi.fn() };
    const engine: any = Object.create(ImporterEngine.prototype);
    engine.config = { QUEUE_HEARTBEAT_INTERVAL_SECONDS: 20 };
    engine.queue = { startHeartbeat: vi.fn(() => ({ stop })) };
    engine.logger = logger;
    engine.isDiscoveryAllowed = vi.fn().mockResolvedValue(true);
    engine.processJob = vi.fn(() => execution);

    const job = {
      id: 'job-1',
      task_type: 'SYNC_WORK',
      source: 'slow-source',
      payload: { pageCount: 1 },
      progress_total: 1,
    };
    let settled = false;
    const running = engine.executeJobDirectly(job).then(() => { settled = true; });

    await vi.advanceTimersByTimeAsync(180_000);
    expect(logger.warn).toHaveBeenCalledWith(
      expect.stringContaining('[JOB_SOFT_DEADLINE]'),
      expect.objectContaining({ jobId: 'job-1', source: 'slow-source' })
    );
    expect(settled).toBe(false);
    expect(stop).not.toHaveBeenCalled();

    resolveExecution();
    await running;
    expect(stop).toHaveBeenCalledTimes(1);
  });

  it('preserves a controlled deadline as a retryable abort instead of a downstream page-gap error', async () => {
    const controller = new AbortController();
    controller.abort(new Error('Job job-2 exceeded its bounded execution deadline'));

    const abortError = getJobAbortError(controller.signal)!;
    expect(abortError.message).toContain('bounded execution deadline');
    expect(RetryPolicy.classify(abortError)).toMatchObject({
      retryClass: 'LOCAL_RETRY',
      isTransient: true,
      isPermanent: false,
    });

    const engine: any = Object.create(ImporterEngine.prototype);
    engine.config = { QUEUE_HEARTBEAT_INTERVAL_SECONDS: 20 };
    engine.queue = { startHeartbeat: vi.fn(() => ({ stop: vi.fn() })) };
    engine.logger = { warn: vi.fn() };
    engine.isDiscoveryAllowed = vi.fn().mockResolvedValue(true);
    engine.processJob = vi.fn((_job: any, _cancelled: any, _timing: any, signal: AbortSignal) =>
      new Promise<void>((_resolve, reject) => {
        signal.addEventListener('abort', () => reject(signal.reason), { once: true });
      }),
    );

    vi.useFakeTimers();
    const running = engine.executeJobDirectly({
      id: 'job-2',
      task_type: 'SYNC_WORK',
      source: 'slow-source',
      payload: { pageCount: 1 },
      progress_total: 1,
    });
    const rejected = expect(running).rejects.toThrow('bounded execution deadline');

    await vi.advanceTimersByTimeAsync(180_000);
    await rejected;
  });
});
