import { afterEach, describe, expect, it, vi } from 'vitest';
import { AsyncSemaphore } from '../../core/concurrency.js';
import { LITE_PAGE_BUFFER_BUDGET_BYTES, PageByteBudget } from '../page-byte-budget.js';
import { LiteMemoryPressureGuard } from '../memory-pressure.js';

const mib = 1024 * 1024;
const memory = (rssMiB: number, externalMiB = 12, arrayBuffersMiB = 8) => ({
  rss: rssMiB * mib,
  heapUsed: 24 * mib,
  heapTotal: 32 * mib,
  external: externalMiB * mib,
  arrayBuffers: arrayBuffersMiB * mib,
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

    expect(guard.sample(memory(365))).toMatchObject({ state: 'NORMAL', pageBufferCapacity: 20, pageByteCapacity: 96 * mib, pauseLowPriorityMetadata: false });
    expect(guard.sample(memory(375))).toMatchObject({ state: 'REDUCED', pageBufferCapacity: 20, pageByteCapacity: 48 * mib, pauseLowPriorityMetadata: false });
    expect(guard.sample(memory(415))).toMatchObject({ state: 'CRITICAL', pageBufferCapacity: 1, pageByteCapacity: 20 * mib, pauseLowPriorityMetadata: true });
  });

  it('uses hysteresis so retained native RSS does not oscillate admissions', () => {
    const pages = new AsyncSemaphore(20, 'test_lite_memory_hysteresis');
    const bytes = new PageByteBudget(LITE_PAGE_BUFFER_BUDGET_BYTES);
    const guard = new LiteMemoryPressureGuard(pages, bytes, 20);

    expect(guard.sample(memory(415)).state).toBe('CRITICAL');
    expect(guard.sample(memory(390)).state).toBe('CRITICAL');
    expect(guard.sample(memory(375)).state).toBe('REDUCED');
    expect(guard.sample(memory(350)).state).toBe('REDUCED');
    expect(guard.sample(memory(335)).state).toBe('NORMAL');
  });

  it('uses an operator-sized normal profile without weakening reduced or critical backpressure', () => {
    const pages = new AsyncSemaphore(48, 'test_lite_memory_operator_profile');
    const bytes = new PageByteBudget(LITE_PAGE_BUFFER_BUDGET_BYTES);
    const guard = new LiteMemoryPressureGuard(pages, bytes, 48);

    guard.setNormalMediaProfile({ pageBufferCapacity: 24, pageByteCapacity: 48 * mib });
    expect(guard.getNormalMediaProfile()).toEqual({ pageBufferCapacity: 24, pageByteCapacity: 48 * mib });
    expect(guard.sample(memory(320))).toMatchObject({ state: 'NORMAL', pageBufferCapacity: 24, pageByteCapacity: 48 * mib });
    expect(guard.sample(memory(415))).toMatchObject({ state: 'CRITICAL', pageBufferCapacity: 1, pageByteCapacity: 20 * mib });

    guard.setNormalMediaProfile({ pageBufferCapacity: 48, pageByteCapacity: 80 * mib });
    // The requested wider profile is remembered, but it cannot bypass a
    // current critical brake. It applies only after normal-state recovery.
    expect(guard.sample(memory(415))).toMatchObject({ state: 'CRITICAL', pageBufferCapacity: 1, pageByteCapacity: 20 * mib });
    expect(guard.sample(memory(320))).toMatchObject({ state: 'NORMAL', pageBufferCapacity: 48, pageByteCapacity: 80 * mib });
  });

  it('makes a compact cap-aware profile materially smaller when RSS enters REDUCED', () => {
    const pages = new AsyncSemaphore(48, 'test_lite_memory_compact_reduced_profile');
    const bytes = new PageByteBudget(LITE_PAGE_BUFFER_BUDGET_BYTES);
    const guard = new LiteMemoryPressureGuard(pages, bytes, 48);

    guard.setNormalMediaProfile({ pageBufferCapacity: 16, pageByteCapacity: 48 * mib });
    expect(guard.sample(memory(320))).toMatchObject({ state: 'NORMAL', pageBufferCapacity: 16, pageByteCapacity: 48 * mib });
    expect(guard.sample(memory(375))).toMatchObject({ state: 'REDUCED', pageBufferCapacity: 8, pageByteCapacity: 32 * mib });
    expect(guard.sample(memory(320))).toMatchObject({ state: 'NORMAL', pageBufferCapacity: 16, pageByteCapacity: 48 * mib });
  });

  it('collects once more before retained RSS escalates REDUCED into CRITICAL', () => {
    const pages = new AsyncSemaphore(20, 'test_lite_memory_second_gc');
    const bytes = new PageByteBudget(LITE_PAGE_BUFFER_BUDGET_BYTES);
    const guard = new LiteMemoryPressureGuard(pages, bytes, 20);
    // Explicit samples keep the first transition deterministic and avoid the
    // test process's RSS.  The next production-style sample crosses the
    // critical fence, then the bounded GC reveals that its ArrayBuffers were
    // already dead; admission remains REDUCED rather than collapsing to one.
    expect(guard.sample(memory(375)).state).toBe('REDUCED');
    const processMemory = vi.spyOn(process, 'memoryUsage');
    processMemory.mockReturnValueOnce(memory(415)).mockReturnValueOnce(memory(385));
    const gc = vi.fn();
    vi.stubGlobal('gc', gc);

    expect(guard.sample()).toMatchObject({ state: 'REDUCED', pageBufferCapacity: 20, pageByteCapacity: 48 * mib });
    expect(gc).toHaveBeenCalledTimes(1);
  });

  it('collects a drained reduced media wave before native RSS reaches CRITICAL', () => {
    const pages = new AsyncSemaphore(20, 'test_lite_memory_drained_reduced_gc');
    const bytes = new PageByteBudget(LITE_PAGE_BUFFER_BUDGET_BYTES);
    const guard = new LiteMemoryPressureGuard(pages, bytes, 20);
    expect(guard.sample(memory(375)).state).toBe('REDUCED');

    // Explicit setup above cannot invoke GC. In the real sampler all page
    // reservations and ArrayBuffers have drained, while allocator RSS remains
    // high; collect before allowing that retained plateau to become CRITICAL.
    const processMemory = vi.spyOn(process, 'memoryUsage');
    processMemory.mockReturnValueOnce(memory(395, 8, 0)).mockReturnValueOnce(memory(385, 8, 0));
    const gc = vi.fn();
    vi.stubGlobal('gc', gc);

    expect(guard.sample()).toMatchObject({ state: 'REDUCED', pageBufferCapacity: 20, pageByteCapacity: 48 * mib });
    expect(gc).toHaveBeenCalledTimes(1);
  });

  it('releases only the single metadata lane after a stable, low-live-native RSS plateau', () => {
    const pages = new AsyncSemaphore(20, 'test_lite_retained_rss_resume');
    const bytes = new PageByteBudget(LITE_PAGE_BUFFER_BUDGET_BYTES);
    const guard = new LiteMemoryPressureGuard(pages, bytes, 20);
    const clock = vi.spyOn(Date, 'now');

    clock.mockReturnValue(1_000);
    expect(guard.sample(memory(415))).toMatchObject({
      state: 'CRITICAL', pageBufferCapacity: 1, pauseLowPriorityMetadata: true,
      retainedRssMetadataResume: false,
    });
    clock.mockReturnValue(60_999);
    expect(guard.sample(memory(416))).toMatchObject({ pauseLowPriorityMetadata: true });
    clock.mockReturnValue(61_000);
    expect(guard.sample(memory(416))).toMatchObject({
      state: 'CRITICAL', pageBufferCapacity: 1, pageByteCapacity: 20 * mib,
      pauseLowPriorityMetadata: false, retainedRssMetadataResume: true,
    });
  });

  it('keeps metadata paused when critical RSS is actively growing or live memory is high', () => {
    const pages = new AsyncSemaphore(20, 'test_lite_retained_rss_circuit');
    const bytes = new PageByteBudget(LITE_PAGE_BUFFER_BUDGET_BYTES);
    const guard = new LiteMemoryPressureGuard(pages, bytes, 20);
    const clock = vi.spyOn(Date, 'now');

    clock.mockReturnValue(1_000);
    expect(guard.sample(memory(415))).toMatchObject({ pauseLowPriorityMetadata: true });
    clock.mockReturnValue(61_000);
    expect(guard.sample(memory(422))).toMatchObject({ pauseLowPriorityMetadata: true, retainedRssMetadataResume: false });
    clock.mockReturnValue(122_000);
    expect(guard.sample(memory(422, 40, 20))).toMatchObject({ pauseLowPriorityMetadata: true, retainedRssMetadataResume: false });
  });
});
