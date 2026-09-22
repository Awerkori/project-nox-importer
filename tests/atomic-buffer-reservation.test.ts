import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { AdaptiveAutotuner, BufferReservation } from '../src/core/concurrency.js';
import { diagnostics } from '../src/core/diagnostics.js';
import { InvalidMediaError } from '../src/core/retry-policy.js';

describe('Atomic Buffer Reservation & Backpressure', () => {
  beforeEach(() => {
    vi.spyOn(diagnostics, 'getMemorySnapshot').mockReturnValue({
      rssMb: 150,
      heapUsedMb: 80,
      heapTotalMb: 120,
      externalMb: 10,
      arrayBuffersMb: 5,
    });
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it('prevents TOCTOU: 8 concurrent 2MB requests on 10MB budget (2MB active) admit exactly 4 and queue 4', async () => {
    const maxBudget = 10 * 1024 * 1024; // 10 MB
    const autotuner = new AdaptiveAutotuner({
      maxBufferedBytes: maxBudget,
      rssSoftLimitMb: 330,
      rssHardLimitMb: 380,
    });

    // 2 MB active already in use
    const initialActive = 2 * 1024 * 1024;
    autotuner.trackBufferedBytes(initialActive);

    expect(autotuner.getBufferedBytes()).toBe(initialActive);
    expect(autotuner.getReservedBytes()).toBe(0);
    expect(autotuner.getCommittedBytes()).toBe(initialActive);

    // 8 concurrent requests for 2MB each
    const reqSize = 2 * 1024 * 1024;
    const results: Array<{ id: number; resolved: boolean; reservation?: BufferReservation }> = [];
    const promises: Promise<void>[] = [];

    for (let i = 0; i < 8; i++) {
      const entry = { id: i, resolved: false, reservation: undefined as BufferReservation | undefined };
      results.push(entry);
      const p = autotuner.reserveBufferBudget(reqSize).then((res) => {
        entry.resolved = true;
        entry.reservation = res;
      });
      promises.push(p);
    }

    // Allow microtasks to run
    await new Promise((r) => setTimeout(r, 10));

    // Exactly 4 requests should be admitted (4 * 2MB = 8MB reserved + 2MB active = 10MB max)
    // The remaining 4 must be queued in FIFO order
    const admitted = results.filter((r) => r.resolved);
    const queued = results.filter((r) => !r.resolved);

    expect(admitted.length).toBe(4);
    expect(queued.length).toBe(4);
    expect(admitted.map((r) => r.id)).toEqual([0, 1, 2, 3]);
    expect(queued.map((r) => r.id)).toEqual([4, 5, 6, 7]);

    expect(autotuner.getReservedBytes()).toBe(8 * 1024 * 1024);
    expect(autotuner.getCommittedBytes()).toBe(10 * 1024 * 1024);

    // Releasing the initial 2MB active buffer frees up room for exactly 1 queued waiter
    autotuner.releaseActiveBufferedBytes(initialActive);
    await new Promise((r) => setTimeout(r, 10));

    expect(results[4].resolved).toBe(true);
    expect(results.filter((r) => r.resolved).length).toBe(5);
    expect(results.filter((r) => !r.resolved).length).toBe(3);

    // Release one admitted reservation without committing (e.g. download failed/cancelled)
    results[0].reservation!.release();
    await new Promise((r) => setTimeout(r, 10));

    expect(results[5].resolved).toBe(true);
    expect(results.filter((r) => r.resolved).length).toBe(6);

    // Clean up remaining reservations
    for (const r of results) {
      if (r.reservation && !r.reservation.isReleased && !r.reservation.isCommitted) {
        r.reservation.release();
      }
    }
  });

  it('oversized page commit accounts for actual bytes and stalls new entries until drained', async () => {
    const maxBudget = 10 * 1024 * 1024; // 10 MB
    const autotuner = new AdaptiveAutotuner({
      maxBufferedBytes: maxBudget,
      rssSoftLimitMb: 330,
    });

    // Initial reservation of 2MB
    const res1 = await autotuner.reserveBufferBudget(2 * 1024 * 1024);
    expect(autotuner.getReservedBytes()).toBe(2 * 1024 * 1024);
    expect(autotuner.getBufferedBytes()).toBe(0);

    // Commit actual 3.5MB (> 2MB reserved)
    res1.commit(3.5 * 1024 * 1024);
    expect(res1.isCommitted).toBe(true);
    expect(autotuner.getReservedBytes()).toBe(0);
    expect(autotuner.getBufferedBytes()).toBe(3.5 * 1024 * 1024);
    expect(autotuner.getCommittedBytes()).toBe(3.5 * 1024 * 1024);

    // Reserve 6MB more -> total committed = 9.5MB (fits under 10MB)
    const res2 = await autotuner.reserveBufferBudget(6 * 1024 * 1024);
    expect(autotuner.getCommittedBytes()).toBe(9.5 * 1024 * 1024);

    // Now try reserving 2MB -> exceeds 10MB, must be queued
    let queuedResolved = false;
    const queuedPromise = autotuner.reserveBufferBudget(2 * 1024 * 1024).then((res) => {
      queuedResolved = true;
      res.release();
    });

    await new Promise((r) => setTimeout(r, 10));
    expect(queuedResolved).toBe(false);

    // Release res2 -> frees 6MB -> queued request unblocks
    res2.release();
    await queuedPromise;
    expect(queuedResolved).toBe(true);

    // Cleanup res1 active bytes
    autotuner.releaseActiveBufferedBytes(3.5 * 1024 * 1024);
    expect(autotuner.getBufferedBytes()).toBe(0);
  });

  it('reservation upgrade checks Content-Length and upgrades atomically', async () => {
    const maxBudget = 10 * 1024 * 1024; // 10 MB
    const autotuner = new AdaptiveAutotuner({
      maxBufferedBytes: maxBudget,
      rssSoftLimitMb: 330,
    });

    const res = await autotuner.reserveBufferBudget(2 * 1024 * 1024);
    expect(res.reservedBytes).toBe(2 * 1024 * 1024);
    expect(autotuner.getReservedBytes()).toBe(2 * 1024 * 1024);

    // Upgrade to 5MB (additional 3MB)
    await res.upgrade(5 * 1024 * 1024);
    expect(res.reservedBytes).toBe(5 * 1024 * 1024);
    expect(autotuner.getReservedBytes()).toBe(5 * 1024 * 1024);

    // Try upgrading to 12MB -> exceeds 10MB budget, must queue
    let upgradeFinished = false;
    const upgradeP = res.upgrade(12 * 1024 * 1024).then(() => {
      upgradeFinished = true;
    });

    await new Promise((r) => setTimeout(r, 10));
    expect(upgradeFinished).toBe(false);

    // Cancel / abort the waiter
    res.release();
  });

  it('safe backpressure: high RSS stops reservation admission until memory recovers (no blind bypass)', async () => {
    const autotuner = new AdaptiveAutotuner({
      maxBufferedBytes: 64 * 1024 * 1024,
      rssSoftLimitMb: 330,
      rssHardLimitMb: 380,
    });

    // Simulate active buffer so progress exception does not trigger
    autotuner.trackBufferedBytes(1 * 1024 * 1024);

    // Simulate high RSS >= 330MB
    vi.spyOn(diagnostics, 'getMemorySnapshot').mockReturnValue({
      rssMb: 345, // >= 330
      heapUsedMb: 150,
      heapTotalMb: 200,
      externalMb: 10,
      arrayBuffersMb: 5,
    });

    let admitted = false;
    let reservation: BufferReservation | null = null;
    const reqP = autotuner.reserveBufferBudget(2 * 1024 * 1024).then((r) => {
      admitted = true;
      reservation = r;
    });

    // Should NOT be admitted immediately
    await new Promise((r) => setTimeout(r, 50));
    expect(admitted).toBe(false);

    // Memory recovers below soft limit
    vi.spyOn(diagnostics, 'getMemorySnapshot').mockReturnValue({
      rssMb: 280, // < 330
      heapUsedMb: 100,
      heapTotalMb: 150,
      externalMb: 10,
      arrayBuffersMb: 5,
    });

    // Wait for the periodic interval timer to drain waiters
    await new Promise((r) => setTimeout(r, 300));
    expect(admitted).toBe(true);

    reservation?.release();
    autotuner.releaseActiveBufferedBytes(1 * 1024 * 1024);
  });
});
