import { afterEach, describe, expect, it, vi } from 'vitest';
import { ImporterEngine } from '../src/core/engine.js';

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
});
