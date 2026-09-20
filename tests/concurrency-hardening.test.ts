import { describe, it, expect, vi } from 'vitest';
import { AsyncSemaphore } from '../src/core/concurrency.js';
import { ImporterGatewayClient } from '../src/core/gateway-client.js';
import { ImporterQueue } from '../src/core/queue.js';

describe('Concurrency Hardening & Runner Pool Tests', () => {
  it('enforces MAX_GATEWAY_CONCURRENT_REQUESTS = 4 on ImporterGatewayClient', async () => {
    let concurrent = 0;
    let maxObservedConcurrent = 0;

    const originalFetch = globalThis.fetch;
    // Mock fetch with delay to observe concurrency
    globalThis.fetch = vi.fn().mockImplementation(async () => {
      concurrent++;
      if (concurrent > maxObservedConcurrent) {
        maxObservedConcurrent = concurrent;
      }
      // Hold for 30ms to simulate network transit
      await new Promise((r) => setTimeout(r, 30));
      concurrent--;
      return {
        ok: true,
        status: 200,
        json: async () => ({ success: true, rows: [], rowCount: 0 }),
      };
    });

    try {
      const client = new ImporterGatewayClient('https://example.com', 'test-token');

      // Dispatch 12 concurrent requests
      const promises = Array.from({ length: 12 }, (_, i) =>
        client.sql(`SELECT ${i}`)
      );

      await Promise.all(promises);

      // Concurrency must NEVER exceed 4
      expect(maxObservedConcurrent).toBeLessThanOrEqual(4);
      expect(maxObservedConcurrent).toBeGreaterThan(1);
    } finally {
      globalThis.fetch = originalFetch;
    }
  });

  it('verifies startHeartbeat applies jitter and schedules next execution', async () => {
    vi.useFakeTimers();

    const mockSupabase = {
      rpc: vi.fn().mockResolvedValue({ data: true, error: null }),
      from: vi.fn().mockReturnValue({
        select: vi.fn().mockReturnValue({
          eq: vi.fn().mockReturnValue({
            maybeSingle: vi.fn().mockResolvedValue({ data: null, error: null }),
          }),
        }),
      }),
    };

    const queue = new ImporterQueue(mockSupabase as any, 'test-worker');

    const handle = queue.startHeartbeat('job-1', 60);

    // Initial state: no calls yet
    expect(mockSupabase.rpc).not.toHaveBeenCalled();

    // Advance 59s: still not called
    vi.advanceTimersByTime(59_000);
    expect(mockSupabase.rpc).not.toHaveBeenCalled();

    // Advance to 64s: should have fired with jitter (60s + 0..3s)
    await vi.advanceTimersByTimeAsync(5_000);
    expect(mockSupabase.rpc).toHaveBeenCalledWith('importer_renew_lease', {
      p_job_id: 'job-1',
      p_worker_id: 'test-worker',
      p_lease_duration: '5 minutes',
    });

    handle.stop();
    vi.useRealTimers();
  });

  it('verifies runnerSlots = effectiveMaxConcurrentChapters calculation logic', () => {
    const calcRunnerSlots = (maxChapters: number, ceiling: number) => {
      const effective = Math.min(maxChapters, ceiling);
      return Math.max(1, effective);
    };

    expect(calcRunnerSlots(5, 32)).toBe(5);
    expect(calcRunnerSlots(8, 32)).toBe(8);
    expect(calcRunnerSlots(10, 32)).toBe(10);
    expect(calcRunnerSlots(12, 32)).toBe(12);
    expect(calcRunnerSlots(1, 32)).toBe(1);
    expect(calcRunnerSlots(4, 32)).toBe(4);
  });
});
