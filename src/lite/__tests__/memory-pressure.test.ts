import { describe, expect, it } from 'vitest';
import { AsyncSemaphore } from '../../core/concurrency.js';
import { PageByteBudget } from '../page-byte-budget.js';
import { LiteMemoryPressureGuard } from '../memory-pressure.js';

const mib = 1024 * 1024;
const memory = (rssMiB: number) => ({
  rss: rssMiB * mib,
  heapUsed: 24 * mib,
  heapTotal: 32 * mib,
  external: 12 * mib,
  arrayBuffers: 8 * mib,
}) as NodeJS.MemoryUsage;

describe('LiteMemoryPressureGuard', () => {
  it('cuts future page admission and yields metadata before the container limit', () => {
    const pages = new AsyncSemaphore(20, 'test_lite_memory_pages');
    const bytes = new PageByteBudget(64 * mib);
    const guard = new LiteMemoryPressureGuard(pages, bytes, 20);

    expect(guard.sample(memory(280))).toMatchObject({ state: 'NORMAL', pageBufferCapacity: 20, pageByteCapacity: 64 * mib, pauseLowPriorityMetadata: false });
    expect(guard.sample(memory(310))).toMatchObject({ state: 'REDUCED', pageBufferCapacity: 16, pageByteCapacity: 48 * mib, pauseLowPriorityMetadata: false });
    expect(guard.sample(memory(365))).toMatchObject({ state: 'CRITICAL', pageBufferCapacity: 1, pageByteCapacity: 20 * mib, pauseLowPriorityMetadata: true });
  });

  it('uses hysteresis so retained native RSS does not oscillate admissions', () => {
    const pages = new AsyncSemaphore(20, 'test_lite_memory_hysteresis');
    const bytes = new PageByteBudget(64 * mib);
    const guard = new LiteMemoryPressureGuard(pages, bytes, 20);

    expect(guard.sample(memory(365)).state).toBe('CRITICAL');
    expect(guard.sample(memory(330)).state).toBe('CRITICAL');
    expect(guard.sample(memory(310)).state).toBe('REDUCED');
    expect(guard.sample(memory(270)).state).toBe('REDUCED');
    expect(guard.sample(memory(250)).state).toBe('NORMAL');
  });
});
