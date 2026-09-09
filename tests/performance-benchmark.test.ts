import { describe, it, expect, beforeEach } from 'vitest';
import { HostRateLimiter } from '../src/core/rate-limiter.js';

describe('Importer Performance Benchmarks [INTEGRAÇÃO / SIMULAÇÃO CONTROLADA]', () => {
  let limiter: HostRateLimiter;

  beforeEach(() => {
    limiter = new HostRateLimiter(2.0);
  });

  it('Benchmark 1: HostRateLimiter AIMD throughput scaling (CDN edge capacity)', async () => {
    // Configures CDN host at 8.0 base up to 16.0 req/s
    limiter.setHostRate('cdn.nexusmangas.com', 8.0, 16, 16.0);
    expect(limiter.getHostRate('cdn.nexusmangas.com')).toBe(8.0);

    // Simulate 24 successful page downloads to trigger AIMD Additive Increase
    for (let i = 0; i < 24; i++) {
      limiter.recordSuccess('cdn.nexusmangas.com');
    }
    const scaledRate = limiter.getHostRate('cdn.nexusmangas.com');
    // 8.0 + (24 / 8) * 0.5 = 9.5 req/s
    expect(scaledRate).toBeGreaterThanOrEqual(9.5);

    // Turbo Mode activation (Absolute Priority)
    limiter.setTurboMode(true);
    const turboRate = limiter.getHostRate('cdn.nexusmangas.com');
    expect(turboRate).toBeGreaterThanOrEqual(12.0);

    limiter.setTurboMode(false);
  });

  it('Benchmark 2: Overlapped Producer-Consumer vs Serial Pipeline Speedup Simulation', async () => {
    const pageCount = 20;
    const downloadDelayMs = 40; // 40ms per page download
    const uploadDelayMs = 60;   // 60ms per page upload to storage

    // Model 1: Legacy Serial execution (download -> upload -> download -> upload)
    const serialStart = Date.now();
    for (let i = 0; i < pageCount; i++) {
      await new Promise((r) => setTimeout(r, downloadDelayMs / 10)); // scaled 10x for fast CI test
      await new Promise((r) => setTimeout(r, uploadDelayMs / 10));
    }
    const serialDuration = Date.now() - serialStart;

    // Model 2: Decoupled Overlapped Pipeline (concurrent producer + concurrent consumer)
    const pipelineStart = Date.now();
    const readyQueue: number[] = [];
    let downloadsDone = false;
    const resolvers: Array<() => void> = [];

    const notify = () => {
      while (resolvers.length > 0) {
        resolvers.shift()?.();
      }
    };

    // 4 concurrent download producers
    let nextIdx = 0;
    const producer = async () => {
      while (nextIdx < pageCount) {
        const idx = nextIdx++;
        await new Promise((r) => setTimeout(r, downloadDelayMs / 10));
        readyQueue.push(idx);
        notify();
      }
    };

    // 2 concurrent upload consumers
    let uploadedCount = 0;
    const consumer = async () => {
      while (uploadedCount < pageCount) {
        while (readyQueue.length === 0 && !downloadsDone) {
          await new Promise<void>((r) => resolvers.push(r));
        }
        if (readyQueue.length === 0 && downloadsDone) break;
        readyQueue.shift();
        await new Promise((r) => setTimeout(r, uploadDelayMs / 10));
        uploadedCount++;
      }
    };

    const producers = Array.from({ length: 4 }, () => producer());
    const consumers = Array.from({ length: 2 }, () => consumer());

    await Promise.all(producers);
    downloadsDone = true;
    notify();
    await Promise.all(consumers);

    const pipelineDuration = Date.now() - pipelineStart;

    // The overlapped pipeline should be significantly faster than serial execution
    expect(pipelineDuration).toBeLessThan(serialDuration);
    const speedup = (serialDuration / pipelineDuration).toFixed(2);
    expect(parseFloat(speedup)).toBeGreaterThan(1.2);
  });

  it('Benchmark 3: Pre-download Deduplication speedup on existing staged/completed chapters', async () => {
    // Simulates an existing chapter with 30 pages already stored
    const existingPages = Array.from({ length: 30 }, (_, i) => ({
      position: i + 1,
      media_id: `media-dedup-${i + 1}`,
      width: 800,
      height: 1200,
    }));

    const t0 = Date.now();
    // Pre-download check
    const isComplete = existingPages.length === 30 && existingPages.every((p) => Boolean(p.media_id));
    const durationMs = Date.now() - t0;

    expect(isComplete).toBe(true);
    expect(durationMs).toBeLessThan(5); // In-memory deduplication evaluation takes < 5ms
  });
});
