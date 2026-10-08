import { describe, expect, it } from 'vitest';
import { readImageBody } from '../src/core/bounded-body';
import { AsyncSemaphore } from '../src/core/concurrency';

describe('bounded image pipeline', () => {
  it('cancels oversized chunked responses before collecting the entire body', async () => {
    let cancelled = false;
    const body = new ReadableStream({ pull(c) { c.enqueue(new Uint8Array(8)); }, cancel() { cancelled = true; } });
    await expect(readImageBody(new Response(body), 10)).rejects.toThrow('byte limit');
    expect(cancelled).toBe(true);
  });
  it('preserves bytes across chunk boundaries', async () => {
    const body = new ReadableStream({ start(c) { c.enqueue(new Uint8Array([1,2])); c.enqueue(new Uint8Array([3])); c.close(); } });
    expect(await readImageBody(new Response(body), 10)).toEqual(new Uint8Array([1,2,3]));
  });
  it('reports first body chunk and completion counters without retaining media', async () => {
    const events: Array<{ type: string; bytes: number; chunks?: number }> = [];
    const body = new ReadableStream({ start(c) { c.enqueue(new Uint8Array([1, 2])); c.enqueue(new Uint8Array([3])); c.close(); } });

    await expect(readImageBody(new Response(body), {
      trace: {
        onFirstChunk: ({ bytes }) => events.push({ type: 'first', bytes }),
        onComplete: ({ bytes, chunks }) => events.push({ type: 'complete', bytes, chunks }),
      },
    })).resolves.toEqual(new Uint8Array([1, 2, 3]));

    expect(events).toEqual([
      { type: 'first', bytes: 2 },
      { type: 'complete', bytes: 3, chunks: 2 },
    ]);
  });
  it('cancels a response body that stalls after its headers', async () => {
    let cancelled = false;
    const body = new ReadableStream<Uint8Array>({
      pull() {}, // Deliberately never enqueue or close.
      cancel() { cancelled = true; },
    });
    const abort = new AbortController();
    const pending = readImageBody(new Response(body), { signal: abort.signal });
    setTimeout(() => abort.abort(new DOMException('body deadline', 'AbortError')), 10);

    await expect(pending).rejects.toMatchObject({ name: 'AbortError' });
    expect(cancelled).toBe(true);
  });
  it('removes aborted buffer waiters without leaking a permit', async () => {
    const sem = new AsyncSemaphore(1), abort = new AbortController();
    await sem.acquire();
    const waiting = sem.acquire(abort.signal);
    abort.abort();
    await expect(waiting).rejects.toBeDefined();
    expect(sem.queued).toBe(0);
    sem.release();
    expect(sem.available).toBe(1);
  });
  it('aborts runExclusive before entering the critical section', async () => {
    const sem = new AsyncSemaphore(1, 'test-run-exclusive');
    const abort = new AbortController();
    await sem.acquire();
    let entered = false;
    const waiting = sem.runExclusive(async () => {
      entered = true;
    }, abort.signal);
    abort.abort(new Error('job deadline'));
    await expect(waiting).rejects.toThrow('job deadline');
    expect(entered).toBe(false);
    expect(sem.queued).toBe(0);
    sem.release();
    expect(sem.available).toBe(1);
  });
  it('cancels a body that stops producing while a read is pending', async () => {
    let cancelled = false;
    let sentFirstChunk = false;
    const body = new ReadableStream<Uint8Array>({
      pull(controller) {
        if (!sentFirstChunk) {
          sentFirstChunk = true;
          controller.enqueue(new Uint8Array([1]));
          return;
        }
        // Simulate a connected HTTP response whose body never advances.
        return;
      },
      cancel() {
        cancelled = true;
      },
    });
    const abort = new AbortController();
    const reading = readImageBody(new Response(body), { signal: abort.signal });
    setTimeout(() => abort.abort(new Error('job deadline')), 10);
    await expect(reading).rejects.toThrow('job deadline');
    expect(cancelled).toBe(true);
  });
  it('reports partial counters when an in-progress body is aborted', async () => {
    let sentFirstChunk = false;
    const failures: Array<{ bytes: number; chunks: number; error: unknown }> = [];
    const body = new ReadableStream<Uint8Array>({
      pull(controller) {
        if (!sentFirstChunk) {
          sentFirstChunk = true;
          controller.enqueue(new Uint8Array([1, 2]));
        }
      },
    });
    const abort = new AbortController();
    const reading = readImageBody(new Response(body), {
      signal: abort.signal,
      trace: { onError: (metrics) => failures.push(metrics) },
    });
    setTimeout(() => abort.abort(new Error('job deadline')), 10);

    await expect(reading).rejects.toThrow('job deadline');
    expect(failures).toHaveLength(1);
    expect(failures[0]).toMatchObject({ bytes: 2, chunks: 1 });
  });
});
