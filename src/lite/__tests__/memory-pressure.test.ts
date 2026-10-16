import { afterEach, describe, expect, it, vi } from 'vitest';
import { AsyncSemaphore } from '../../core/concurrency.js';
import { LITE_PAGE_BUFFER_BUDGET_BYTES, PageByteBudget } from '../page-byte-budget.js';
import { LiteMemoryPressureGuard } from '../memory-pressure.js';

const mib = 1024 * 1024;
const memory = (rssMiB: number) => ({
  rss: rssMiB * mib,
  heapUsed: 24 * mib,
  heapTotal: 32 * mib,
  external: 12 * mib,
  arrayBuffers: 8 * mib,
}) as NodeJS.MemoryUsage;

afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
});

describe('LiteMemoryPressureGuard', () => {
  it('cuts future page admission and yields metadata before the container limit', () => {
    const pages = new AsyncSemaphore(20, 'test_lite_memory_pages');
    const bytes = new PageByteBudget(LITE_PAGE_BUFFER_BUDGET_BYTES);
    const guard = new LiteMemoryPressureGuard(pages, bytes, 20);

    expect(guard.sample(memory(345))).toMatchObject({ state: 'NORMAL', pageBufferCapacity: 20, pageByteCapacity: 80 * mib, pauseLowPriorityMetadata: false });
    expect(guard.sample(memory(355))).toMatchObject({ state: 'REDUCED', pageBufferCapacity: 20, pageByteCapacity: 48 * mib, pauseLowPriorityMetadata: false });
    expect(guard.sample(memory(405))).toMatchObject({ state: 'CRITICAL', pageBufferCapacity: 1, pageByteCapacity: 20 * mib, pauseLowPriorityMetadata: true });
  });

  it('uses hysteresis so retained native RSS does not oscillate admissions', () => {
    const pages = new AsyncSemaphore(20, 'test_lite_memory_hysteresis');
    const bytes = new PageByteBudget(LITE_PAGE_BUFFER_BUDGET_BYTES);
    const guard = new LiteMemoryPressureGuard(pages, bytes, 20);

    expect(guard.sample(memory(405)).state).toBe('CRITICAL');
    expect(guard.sample(memory(380)).state).toBe('CRITICAL');
    expect(guard.sample(memory(360)).state).toBe('REDUCED');
    expect(guard.sample(memory(340)).state).toBe('REDUCED');
    expect(guard.sample(memory(320)).state).toBe('NORMAL');
  });

  it('collects once more before retained RSS escalates REDUCED into CRITICAL', () => {
    const pages = new AsyncSemaphore(20, 'test_lite_memory_second_gc');
    const bytes = new PageByteBudget(LITE_PAGE_BUFFER_BUDGET_BYTES);
    const guard = new LiteMemoryPressureGuard(pages, bytes, 20);
    // Explicit samples keep the first transition deterministic and avoid the
    // test process's RSS.  The next production-style sample crosses the
    // critical fence, then the bounded GC reveals that its ArrayBuffers were
    // already dead; admission remains REDUCED rather than collapsing to one.
    expect(guard.sample(memory(355)).state).toBe('REDUCED');
    const processMemory = vi.spyOn(process, 'memoryUsage');
    processMemory.mockReturnValueOnce(memory(405)).mockReturnValueOnce(memory(360));
    const gc = vi.fn();
    vi.stubGlobal('gc', gc);

    expect(guard.sample()).toMatchObject({ state: 'REDUCED', pageBufferCapacity: 20, pageByteCapacity: 48 * mib });
    expect(gc).toHaveBeenCalledTimes(1);
  });
});
