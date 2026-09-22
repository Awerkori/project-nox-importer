import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { AdaptiveAutotuner, BufferReservation } from '../src/core/concurrency.js';
import { readImageBody } from '../src/core/bounded-body.js';
import { diagnostics } from '../src/core/diagnostics.js';
import { InvalidMediaError } from '../src/core/retry-policy.js';

function createChunkedStream(chunkSize: number, totalChunks: number, delayMs = 0): { stream: ReadableStream<Uint8Array>; isCancelled: () => boolean } {
  let chunksSent = 0;
  let cancelled = false;
  const stream = new ReadableStream<Uint8Array>({
    async pull(controller) {
      if (cancelled) return;
      if (chunksSent >= totalChunks) {
        controller.close();
        return;
      }
      if (delayMs > 0) {
        await new Promise((r) => setTimeout(r, delayMs));
      }
      if (cancelled) return;
      controller.enqueue(new Uint8Array(chunkSize));
      chunksSent++;
    },
    cancel() {
      cancelled = true;
    },
  });
  return { stream, isCancelled: () => cancelled };
}

describe('Atomic Buffer Reservation & Streaming Backpressure', () => {
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

    const initialActive = 2 * 1024 * 1024;
    autotuner.trackBufferedBytes(initialActive);

    expect(autotuner.getBufferedBytes()).toBe(initialActive);
    expect(autotuner.getReservedBytes()).toBe(0);
    expect(autotuner.getCommittedBytes()).toBe(initialActive);

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

    await new Promise((r) => setTimeout(r, 10));

    const admitted = results.filter((r) => r.resolved);
    const queued = results.filter((r) => !r.resolved);

    expect(admitted.length).toBe(4);
    expect(queued.length).toBe(4);
    expect(admitted.map((r) => r.id)).toEqual([0, 1, 2, 3]);
    expect(queued.map((r) => r.id)).toEqual([4, 5, 6, 7]);

    expect(autotuner.getReservedBytes()).toBe(8 * 1024 * 1024);
    expect(autotuner.getCommittedBytes()).toBe(10 * 1024 * 1024);

    autotuner.releaseActiveBufferedBytes(initialActive);
    await new Promise((r) => setTimeout(r, 10));

    expect(results[4].resolved).toBe(true);
    expect(results.filter((r) => r.resolved).length).toBe(5);

    results[0].reservation!.release();
    await new Promise((r) => setTimeout(r, 10));

    expect(results[5].resolved).toBe(true);
    expect(results.filter((r) => r.resolved).length).toBe(6);

    for (const r of results) {
      if (r.reservation && !r.reservation.isReleased && !r.reservation.isCommitted) {
        r.reservation.release();
      }
    }
  });

  // A) NO CONTENT-LENGTH: MAX=10MB, ACTIVE=4MB, RESERVATION=2MB, body=5MB. Dynamic upgrades prevent committed > 10MB.
  it('A) NO CONTENT-LENGTH: dynamic upgrades prevent committed > 10MB during 5MB chunked read', async () => {
    const maxBudget = 10 * 1024 * 1024; // 10 MB
    const autotuner = new AdaptiveAutotuner({
      maxBufferedBytes: maxBudget,
      rssSoftLimitMb: 330,
    });

    // 4 MB active already in use
    const initialActive = 4 * 1024 * 1024;
    autotuner.trackBufferedBytes(initialActive);

    // Initial reservation of 2 MB -> Committed = 6 MB
    const reservation = await autotuner.reserveBufferBudget(2 * 1024 * 1024);
    expect(reservation.reservedBytes).toBe(2 * 1024 * 1024);
    expect(autotuner.getCommittedBytes()).toBe(6 * 1024 * 1024);

    // 5 MB body sent in 5 x 1 MB chunks without Content-Length
    const { stream } = createChunkedStream(1 * 1024 * 1024, 5);
    const response = new Response(stream); // Response has no content-length header
    expect(response.headers.get('content-length')).toBeNull();

    const bodyPromise = readImageBody(response, { reservation });
    const body = await bodyPromise;

    expect(body.byteLength).toBe(5 * 1024 * 1024);
    // Reservation was dynamically upgraded from 2 MB to 5 MB
    expect(reservation.reservedBytes).toBe(5 * 1024 * 1024);
    expect(autotuner.getReservedBytes()).toBe(5 * 1024 * 1024);
    // Total committed: 4 MB active + 5 MB reserved = 9 MB <= 10 MB maxBudget
    expect(autotuner.getCommittedBytes()).toBe(9 * 1024 * 1024);
    expect(autotuner.getMaxCommittedBytesObserved()).toBeLessThanOrEqual(maxBudget);

    // Commit actual bytes: reserved drops to 0, active rises by 5MB (total 9MB committed)
    reservation.commit(body.byteLength);
    expect(reservation.isCommitted).toBe(true);
    expect(autotuner.getReservedBytes()).toBe(0);
    expect(autotuner.getBufferedBytes()).toBe(9 * 1024 * 1024);
    expect(autotuner.getCommittedBytes()).toBe(9 * 1024 * 1024);
    expect(autotuner.getMaxCommittedBytesObserved()).toBeLessThanOrEqual(maxBudget);

    // Cleanup
    autotuner.releaseActiveBufferedBytes(body.byteLength);
    autotuner.releaseActiveBufferedBytes(initialActive);
  });

  // B) UNDERREPORTED CONTENT-LENGTH: header declared 2MB, body 6MB. Dynamic incremental upgrade.
  it('B) UNDERREPORTED CONTENT-LENGTH: detects growth past 2MB header and reserves budget incrementally', async () => {
    const maxBudget = 10 * 1024 * 1024; // 10 MB
    const autotuner = new AdaptiveAutotuner({
      maxBufferedBytes: maxBudget,
      rssSoftLimitMb: 330,
    });

    const reservation = await autotuner.reserveBufferBudget(2 * 1024 * 1024);

    // Response header falsely claims 2 MB, but body actually delivers 6 MB
    const { stream } = createChunkedStream(1 * 1024 * 1024, 6);
    const response = new Response(stream, {
      headers: { 'content-length': String(2 * 1024 * 1024) },
    });

    const body = await readImageBody(response, { reservation });
    expect(body.byteLength).toBe(6 * 1024 * 1024);
    expect(reservation.reservedBytes).toBe(6 * 1024 * 1024);
    expect(autotuner.getCommittedBytes()).toBe(6 * 1024 * 1024);
    expect(autotuner.getMaxCommittedBytesObserved()).toBeLessThanOrEqual(maxBudget);

    // Commit actual bytes succeeds with invariant check
    reservation.commit(body.byteLength);
    expect(autotuner.getBufferedBytes()).toBe(6 * 1024 * 1024);
    autotuner.releaseActiveBufferedBytes(body.byteLength);
  });

  // C) MULTIPLE CHUNKED STREAMS: concurrent chunked streams never exceed MAX_BUFFERED_BYTES.
  it('C) MULTIPLE CHUNKED STREAMS: concurrent chunked streams apply backpressure and never exceed max budget', async () => {
    const maxBudget = 6 * 1024 * 1024; // 6 MB limit
    const autotuner = new AdaptiveAutotuner({
      maxBufferedBytes: maxBudget,
      rssSoftLimitMb: 330,
    });

    // 3 concurrent chunked streams without Content-Length:
    // Stream 1 = 1 MB (2 x 512KB)
    // Stream 2 = 2 MB (4 x 512KB)
    // Stream 3 = 4.5 MB (9 x 512KB)
    // Total = 7.5 MB (exceeds 6 MB maxBudget)
    const stream1 = createChunkedStream(512 * 1024, 2, 2);
    const stream2 = createChunkedStream(512 * 1024, 4, 3);
    const stream3 = createChunkedStream(512 * 1024, 9, 2);

    let maxCommittedObserved = 0;
    const interval = setInterval(() => {
      const c = autotuner.getCommittedBytes();
      if (c > maxCommittedObserved) maxCommittedObserved = c;
    }, 1);

    const runStream = async (s: { stream: ReadableStream<Uint8Array> }, initialReservation: number) => {
      const reservation = await autotuner.reserveBufferBudget(initialReservation);
      const res = new Response(s.stream);
      const body = await readImageBody(res, { reservation });
      reservation.commit(body.byteLength);
      // Simulate post-commit pipeline processing before release
      await new Promise((r) => setTimeout(r, 10));
      autotuner.releaseActiveBufferedBytes(body.byteLength);
      return body.byteLength;
    };

    // Run Stream 1 and 2, and start Stream 3 concurrently
    const p1 = runStream(stream1, 1 * 1024 * 1024);
    const p2 = runStream(stream2, 1 * 1024 * 1024);
    const p3 = runStream(stream3, 1 * 1024 * 1024);

    const results = await Promise.all([p1, p2, p3]);
    clearInterval(interval);

    expect(results).toEqual([1 * 1024 * 1024, 2 * 1024 * 1024, 4.5 * 1024 * 1024]);
    expect(maxCommittedObserved).toBeLessThanOrEqual(maxBudget);
    expect(autotuner.getMaxCommittedBytesObserved()).toBeLessThanOrEqual(maxBudget);
    expect(autotuner.getCommittedBytes()).toBe(0);
  });

  // D) BODY > 20 MB: stream canceled, reservation released, zero buffer leak.
  it('D) BODY > 20 MB: stream canceled, reservation released, zero buffer leak', async () => {
    const maxBudget = 30 * 1024 * 1024; // 30 MB
    const autotuner = new AdaptiveAutotuner({
      maxBufferedBytes: maxBudget,
      rssSoftLimitMb: 330,
    });

    const initialReserved = 2 * 1024 * 1024;
    const reservation = await autotuner.reserveBufferBudget(initialReserved);
    expect(autotuner.getReservedBytes()).toBe(initialReserved);

    // Stream delivers 22 MB in 1 MB chunks (exceeds default 20 MB limit)
    const { stream, isCancelled } = createChunkedStream(1 * 1024 * 1024, 22);
    const response = new Response(stream);

    await expect(readImageBody(response, { reservation })).rejects.toThrow(InvalidMediaError);

    // 1. Stream was cancelled
    expect(isCancelled()).toBe(true);
    // 2. Reservation was released
    expect(reservation.isReleased).toBe(true);
    // 3. Zero buffer leak
    expect(autotuner.getReservedBytes()).toBe(0);
    expect(autotuner.getBufferedBytes()).toBe(0);
    expect(autotuner.getCommittedBytes()).toBe(0);
  });

  // E) COMMIT INVARIANT: reservation 2MB, direct commit 8MB -> rejected.
  it('E) COMMIT INVARIANT: direct commit exceeding reservation budget is strictly rejected', async () => {
    const maxBudget = 10 * 1024 * 1024; // 10 MB
    const autotuner = new AdaptiveAutotuner({
      maxBufferedBytes: maxBudget,
      rssSoftLimitMb: 330,
    });

    const reservation = await autotuner.reserveBufferBudget(2 * 1024 * 1024);
    expect(reservation.reservedBytes).toBe(2 * 1024 * 1024);

    // Attempting direct commit of 8MB without upgrade must throw invariant violation
    expect(() => reservation.commit(8 * 1024 * 1024)).toThrow(
      /BufferReservation invariant violation: cannot commit 8388608 bytes exceeding reserved budget 2097152 bytes/
    );

    // Lower-level autotuner method must also throw invariant violation
    expect(() => autotuner.commitReservation(2 * 1024 * 1024, 8 * 1024 * 1024)).toThrow(
      /commitReservation invariant violation: actualBytes \(8388608\) exceeds reservedBytes \(2097152\)/
    );

    // Reservation state was preserved and can be cleanly released
    expect(reservation.isCommitted).toBe(false);
    reservation.release();
    expect(autotuner.getCommittedBytes()).toBe(0);
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

    res.release();
  });

  it('safe backpressure: high RSS stops reservation admission until memory recovers (no blind bypass)', async () => {
    const autotuner = new AdaptiveAutotuner({
      maxBufferedBytes: 64 * 1024 * 1024,
      rssSoftLimitMb: 330,
      rssHardLimitMb: 380,
    });

    autotuner.trackBufferedBytes(1 * 1024 * 1024);

    vi.spyOn(diagnostics, 'getMemorySnapshot').mockReturnValue({
      rssMb: 345,
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

    await new Promise((r) => setTimeout(r, 50));
    expect(admitted).toBe(false);

    vi.spyOn(diagnostics, 'getMemorySnapshot').mockReturnValue({
      rssMb: 280,
      heapUsedMb: 100,
      heapTotalMb: 150,
      externalMb: 10,
      arrayBuffersMb: 5,
    });

    await new Promise((r) => setTimeout(r, 300));
    expect(admitted).toBe(true);

    reservation?.release();
    autotuner.releaseActiveBufferedBytes(1 * 1024 * 1024);
  });
});
