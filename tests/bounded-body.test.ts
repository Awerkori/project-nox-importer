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
});
