import { AsyncSemaphore } from '../core/concurrency.js';
import { PageByteBudget } from './page-byte-budget.js';

const MIB = 1024 * 1024;
const NORMAL_RSS_MIB = 300;
const CRITICAL_RSS_MIB = 360;
const REDUCED_RECOVERY_RSS_MIB = 260;
const CRITICAL_RECOVERY_RSS_MIB = 320;

export type LiteMemoryPressureState = 'NORMAL' | 'REDUCED' | 'CRITICAL';

export type LiteMemoryPressureSnapshot = {
  state: LiteMemoryPressureState;
  rssMiB: number;
  heapUsedMiB: number;
  heapTotalMiB: number;
  externalMiB: number;
  arrayBuffersMiB: number;
  pageBufferCapacity: number;
  pageByteCapacity: number;
  pauseLowPriorityMetadata: boolean;
};

type MemoryUsageLike = Pick<NodeJS.MemoryUsage, 'rss' | 'heapUsed' | 'heapTotal' | 'external' | 'arrayBuffers'>;

/**
 * Small runtime-local memory brake for the Lite media path. It controls only
 * future page admission: an in-flight image/upload keeps its permits and can
 * finish its durable receipt safely. The hysteresis matters because Node may
 * retain freed native allocations in RSS for later reuse.
 */
export class LiteMemoryPressureGuard {
  private state: LiteMemoryPressureState = 'NORMAL';
  private snapshotValue: LiteMemoryPressureSnapshot;

  constructor(
    private readonly pageBuffers: AsyncSemaphore,
    private readonly pageBytes: PageByteBudget,
    private readonly normalPageBufferCapacity: number,
  ) {
    this.snapshotValue = this.buildSnapshot(process.memoryUsage());
  }

  sample(memory?: MemoryUsageLike): LiteMemoryPressureSnapshot {
    let observed = memory || process.memoryUsage();
    // V8's heap can remain small while completed image ArrayBuffers are ready
    // for collection. Before cutting page admission on RSS alone, give that
    // exact transition one opportunity to release dead backing stores. This
    // runs only in production (tests pass an explicit snapshot) and never
    // relaxes REDUCED/CRITICAL protection for genuinely live buffers.
    if (!memory && this.state === 'NORMAL' && observed.rss / MIB >= NORMAL_RSS_MIB) {
      const gc = (globalThis as typeof globalThis & { gc?: () => void }).gc;
      if (typeof gc === 'function') {
        gc();
        observed = process.memoryUsage();
      }
    }
    const rssMiB = observed.rss / MIB;
    const next = this.nextState(rssMiB);
    if (next !== this.state) {
      this.state = next;
      const profile = this.profile(next);
      this.pageBuffers.setCapacity(profile.pageBufferCapacity);
      this.pageBytes.setCapacityBytes(profile.pageByteCapacity);
    }
    this.snapshotValue = this.buildSnapshot(observed);
    return this.snapshotValue;
  }

  snapshot(): LiteMemoryPressureSnapshot {
    return this.snapshotValue;
  }

  private nextState(rssMiB: number): LiteMemoryPressureState {
    if (this.state === 'CRITICAL') {
      if (rssMiB >= CRITICAL_RECOVERY_RSS_MIB) return 'CRITICAL';
      return rssMiB >= NORMAL_RSS_MIB ? 'REDUCED' : 'NORMAL';
    }
    if (this.state === 'REDUCED') {
      if (rssMiB >= CRITICAL_RSS_MIB) return 'CRITICAL';
      return rssMiB >= REDUCED_RECOVERY_RSS_MIB ? 'REDUCED' : 'NORMAL';
    }
    if (rssMiB >= CRITICAL_RSS_MIB) return 'CRITICAL';
    if (rssMiB >= NORMAL_RSS_MIB) return 'REDUCED';
    return 'NORMAL';
  }

  private profile(state: LiteMemoryPressureState): { pageBufferCapacity: number; pageByteCapacity: number } {
    if (state === 'CRITICAL') {
      // One 20 MiB maximum image can still complete, but no new wave starts.
      return { pageBufferCapacity: 1, pageByteCapacity: 20 * MIB };
    }
    if (state === 'REDUCED') {
      return {
        // RSS can retain allocator arenas after a completed media wave even
        // when the live page payload is well below the reduced byte fence.
        // Keeping only eight *count* permits in that state turned the
        // allocator high-water mark into a permanent throughput collapse:
        // small pages queued behind the count semaphore while the 32 MiB
        // authoritative payload budget was mostly unused.  Raise only the
        // count fence here.  Page bodies are still bounded by the same 32 MiB
        // budget, and CRITICAL remains the one-page emergency profile.
        pageBufferCapacity: Math.min(this.normalPageBufferCapacity, 16),
        pageByteCapacity: 32 * MIB,
      };
    }
    return {
      pageBufferCapacity: this.normalPageBufferCapacity,
      pageByteCapacity: 64 * MIB,
    };
  }

  private buildSnapshot(memory: MemoryUsageLike): LiteMemoryPressureSnapshot {
    return {
      state: this.state,
      rssMiB: Math.round(memory.rss / MIB),
      heapUsedMiB: Math.round((memory.heapUsed || 0) / MIB),
      heapTotalMiB: Math.round((memory.heapTotal || 0) / MIB),
      externalMiB: Math.round((memory.external || 0) / MIB),
      arrayBuffersMiB: Math.round((memory.arrayBuffers || 0) / MIB),
      pageBufferCapacity: this.pageBuffers.capacity,
      pageByteCapacity: this.pageBytes.capacityBytes,
      // Reduced mode limits source metadata to one lane in the runtime. It
      // only pauses it completely at CRITICAL so allocator-retained RSS cannot
      // turn a short media burst into a permanent metadata freeze.
      pauseLowPriorityMetadata: this.state === 'CRITICAL',
    };
  }
}
