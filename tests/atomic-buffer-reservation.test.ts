import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { acquirePageBufferAdmission, AdaptiveAutotuner, AsyncSemaphore, BufferReservation } from '../src/core/concurrency.js';
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

  it('does not reserve bytes while waiting for a buffered-page permit', async () => {
    const twoMb = 2 * 1024 * 1024;
    const autotuner = new AdaptiveAutotuner({
      maxBufferedBytes: 4 * 1024 * 1024,
      rssSoftLimitMb: 330,
    });
    const pageSemaphore = new AsyncSemaphore(1, 'test_buffered_page_permit');

    const first = await acquirePageBufferAdmission(pageSemaphore, autotuner, twoMb);
    let second: Awaited<ReturnType<typeof acquirePageBufferAdmission>> | undefined;
    const secondPromise = acquirePageBufferAdmission(pageSemaphore, autotuner, twoMb)
      .then((admission) => { second = admission; });

    await new Promise((resolve) => setTimeout(resolve, 10));
    expect(pageSemaphore.active).toBe(1);
    expect(autotuner.getReservedBytes()).toBe(twoMb);

    first.reservation.release();
    first.releasePagePermit();
    await secondPromise;

    expect(second).toBeDefined();
    expect(autotuner.getReservedBytes()).toBe(twoMb);
    second!.reservation.release();
    second!.releasePagePermit();
    expect(autotuner.getCommittedBytes()).toBe(0);
  });

  it('returns a page permit when byte admission is aborted', async () => {
    const twoMb = 2 * 1024 * 1024;
    const autotuner = new AdaptiveAutotuner({
      maxBufferedBytes: twoMb,
      rssSoftLimitMb: 330,
    });
    const pageSemaphore = new AsyncSemaphore(1, 'test_page_permit_abort');
    const heldReservation = await autotuner.reserveBufferBudget(twoMb);
    const controller = new AbortController();
    const pending = acquirePageBufferAdmission(pageSemaphore, autotuner, twoMb, controller.signal);

    await new Promise((resolve) => setTimeout(resolve, 10));
    expect(pageSemaphore.active).toBe(1);
    controller.abort(new Error('test abort'));
    await expect(pending).rejects.toThrow('test abort');

    expect(pageSemaphore.active).toBe(0);
    expect(autotuner.getReservedBytes()).toBe(twoMb);
    heldReservation.release();
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

  it('completion-aware admission drains four unknown 10MB bodies within a 40MB budget', async () => {
    const mib = 1024 * 1024;
    const maxBudget = 40 * mib;
    const initialBytes = 2 * mib;
    const autotuner = new AdaptiveAutotuner({ maxBufferedBytes: maxBudget, rssSoftLimitMb: 330 });
    const pageSemaphore = new AsyncSemaphore(12, 'completion_aware_page_permit');
    const admissions = await Promise.all(Array.from({ length: 4 }, () => acquirePageBufferAdmission(
      pageSemaphore,
      autotuner,
      initialBytes,
      undefined,
      { completionHeadroomBytes: 20 * mib - initialBytes },
    )));

    const runBody = async (admission: Awaited<ReturnType<typeof acquirePageBufferAdmission>>) => {
      const { stream } = createChunkedStream(mib, 10);
      const body = await readImageBody(new Response(stream), { reservation: admission.reservation });
      admission.reservation.commit(body.byteLength);
      autotuner.releaseActiveBufferedBytes(body.byteLength);
      admission.releasePagePermit();
      return body.byteLength;
    };

    await expect(Promise.all(admissions.map(runBody))).resolves.toEqual([10 * mib, 10 * mib, 10 * mib, 10 * mib]);
    expect(autotuner.getMaxCommittedBytesObserved()).toBeLessThanOrEqual(maxBudget);
    expect(autotuner.getCommittedBytes()).toBe(0);
  });

  it('does not let a twelfth speculative admission block a completion promotion', async () => {
    const mib = 1024 * 1024;
    const maxBudget = 40 * mib;
    const initialBytes = 2 * mib;
    const autotuner = new AdaptiveAutotuner({ maxBufferedBytes: maxBudget, rssSoftLimitMb: 330 });
    const pageSemaphore = new AsyncSemaphore(12, 'completion_headroom_page_permit');
    const options = { completionHeadroomBytes: 20 * mib - initialBytes };
    const admissions = await Promise.all(Array.from({ length: 11 }, () => acquirePageBufferAdmission(
      pageSemaphore, autotuner, initialBytes, undefined, options,
    )));
    let twelfthResolved = false;
    const twelfth = acquirePageBufferAdmission(pageSemaphore, autotuner, initialBytes, undefined, options)
      .then((admission) => {
        twelfthResolved = true;
        return admission;
      });

    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(twelfthResolved).toBe(false);

    const { stream } = createChunkedStream(mib, 10);
    const body = await readImageBody(new Response(stream), { reservation: admissions[0].reservation });
    admissions[0].reservation.commit(body.byteLength);
    autotuner.releaseActiveBufferedBytes(body.byteLength);
    admissions[0].releasePagePermit();

    const twelfthAdmission = await twelfth;
    expect(twelfthResolved).toBe(true);
    expect(autotuner.getMaxCommittedBytesObserved()).toBeLessThanOrEqual(maxBudget);

    for (const admission of admissions.slice(1)) {
      admission.reservation.release();
      admission.releasePagePermit();
    }
    twelfthAdmission.reservation.release();
    twelfthAdmission.releasePagePermit();
    expect(autotuner.getCommittedBytes()).toBe(0);
  });

  it('preserves FIFO fairness by admitting an initial waiter when its headroom fits before the next completion promotion', async () => {
    const mib = 1024 * 1024;
    const autotuner = new AdaptiveAutotuner({ maxBufferedBytes: 40 * mib, rssSoftLimitMb: 330 });
    const pageSemaphore = new AsyncSemaphore(12, 'completion_fairness_page_permit');
    const options = { completionHeadroomBytes: 18 * mib };
    const admissions = await Promise.all(Array.from({ length: 11 }, () => acquirePageBufferAdmission(
      pageSemaphore, autotuner, 2 * mib, undefined, options,
    )));
    const grantOrder: string[] = [];
    const twelfth = acquirePageBufferAdmission(pageSemaphore, autotuner, 2 * mib, undefined, options)
      .then((admission) => {
        grantOrder.push('initial');
        return admission;
      });

    await admissions[0].reservation.upgrade(20 * mib, undefined, { intent: 'COMPLETION' });
    const nextCompletion = admissions[1].reservation.upgrade(20 * mib, undefined, { intent: 'COMPLETION' })
      .then(() => grantOrder.push('completion'));

    admissions[0].reservation.commit(10 * mib);
    autotuner.releaseActiveBufferedBytes(10 * mib);
    const twelfthAdmission = await twelfth;
    await nextCompletion;

    expect(grantOrder).toEqual(['initial', 'completion']);
    expect(autotuner.getMaxCommittedBytesObserved()).toBeLessThanOrEqual(40 * mib);

    for (const admission of admissions) {
      if (!admission.reservation.isReleased && !admission.reservation.isCommitted) admission.reservation.release();
      admission.releasePagePermit();
    }
    twelfthAdmission.reservation.release();
    twelfthAdmission.releasePagePermit();
    expect(autotuner.getCommittedBytes()).toBe(0);
  });

  it('keeps valid Content-Length reservations exact and promotes underreported bodies to completion capacity', async () => {
    const mib = 1024 * 1024;
    const autotuner = new AdaptiveAutotuner({ maxBufferedBytes: 40 * mib, rssSoftLimitMb: 330 });
    const pageSemaphore = new AsyncSemaphore(12, 'declared_length_page_permit');
    const options = { completionHeadroomBytes: 18 * mib };

    const declared = await acquirePageBufferAdmission(pageSemaphore, autotuner, 2 * mib, undefined, options);
    const declaredBody = await readImageBody(new Response(createChunkedStream(mib, 5).stream, {
      headers: { 'content-length': String(5 * mib) },
    }), { reservation: declared.reservation });
    expect(declared.reservation.reservedBytes).toBe(5 * mib);
    declared.reservation.commit(declaredBody.byteLength);
    autotuner.releaseActiveBufferedBytes(declaredBody.byteLength);
    declared.releasePagePermit();

    const underreported = await acquirePageBufferAdmission(pageSemaphore, autotuner, 2 * mib, undefined, options);
    const underreportedBody = await readImageBody(new Response(createChunkedStream(mib, 6).stream, {
      headers: { 'content-length': String(2 * mib) },
    }), { reservation: underreported.reservation });
    expect(underreported.reservation.reservedBytes).toBe(20 * mib);
    underreported.reservation.commit(underreportedBody.byteLength);
    autotuner.releaseActiveBufferedBytes(underreportedBody.byteLength);
    underreported.releasePagePermit();

    expect(autotuner.getMaxCommittedBytesObserved()).toBeLessThanOrEqual(40 * mib);
    expect(autotuner.getCommittedBytes()).toBe(0);
  });

  it('aborts an unknown body while waiting for completion promotion and releases its reservation', async () => {
    const mib = 1024 * 1024;
    const autotuner = new AdaptiveAutotuner({ maxBufferedBytes: 40 * mib, rssSoftLimitMb: 330 });
    const pageSemaphore = new AsyncSemaphore(12, 'completion_abort_page_permit');
    const options = { completionHeadroomBytes: 18 * mib };
    const admissions = await Promise.all(Array.from({ length: 11 }, () => acquirePageBufferAdmission(
      pageSemaphore, autotuner, 2 * mib, undefined, options,
    )));

    await admissions[0].reservation.upgrade(20 * mib, undefined, { intent: 'COMPLETION' });
    const abort = new AbortController();
    let cancelled = false;
    const pending = readImageBody(new Response(new ReadableStream<Uint8Array>({
      cancel() { cancelled = true; },
    })), { reservation: admissions[1].reservation, signal: abort.signal });

    await new Promise((resolve) => setTimeout(resolve, 20));
    abort.abort(new Error('test completion abort'));
    await expect(pending).rejects.toThrow('test completion abort');
    expect(cancelled).toBe(true);
    expect(admissions[1].reservation.isReleased).toBe(true);

    for (const admission of admissions) {
      if (!admission.reservation.isReleased && !admission.reservation.isCommitted) admission.reservation.release();
      admission.releasePagePermit();
    }
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
