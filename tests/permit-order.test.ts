import { expect, it } from 'vitest';
import { AsyncSemaphore, withSourceChapterPermits } from '../src/core/concurrency';
it('lets a source worker progress while the general runner waits for the same source', async () => {
  const source = new AsyncSemaphore(1), global = new AsyncSemaphore(1);
  await source.acquire(); // Source runner acquired its source first.
  await global.acquire(); // General runner temporarily holds claim admission.
  const order: string[] = [];
  const sourceRunner = (async () => {
    await global.acquire();
    order.push('source');
    global.release(); source.release();
  })();
  global.release(); // General runner releases admission before waiting for source.
  const generalRunner = withSourceChapterPermits(source, global, async () => { order.push('general'); });
  await Promise.all([sourceRunner, generalRunner]);
  expect(order).toEqual(['source', 'general']);
  expect(source.active).toBe(0); expect(global.active).toBe(0);
}, 1000);
it('releases source capacity if waiting for global capacity is cancelled', async () => {
  const source = new AsyncSemaphore(1), global = new AsyncSemaphore(1), abort = new AbortController();
  await global.acquire();
  const pending = withSourceChapterPermits(source, global, async () => {}, abort.signal);
  await Promise.resolve(); abort.abort();
  await expect(pending).rejects.toBeDefined();
  expect(source.active).toBe(0); expect(global.active).toBe(1);
  global.release();
});
