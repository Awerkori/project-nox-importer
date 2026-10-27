import { AsyncSemaphore } from '../core/concurrency.js';
import { LITE_PAGE_BUFFER_BUDGET_BYTES, PageByteBudget } from './page-byte-budget.js';

const MIB = 1024 * 1024;
// A 512 MiB container exists to absorb a bounded media wave.  Production has
// already shown a stable native allocator plateau above 330 MiB; reducing
// admission at 350 and collapsing to a one-page wave at 400 therefore turned
// healthy available memory into a permanent throughput cap.  Keep NORMAL
// through 420 MiB, use REDUCED only as a measured warning band, and retain a
// 62 MiB margin for the hard one-page brake at 450 MiB.
const NORMAL_RSS_MIB = 420;
const CRITICAL_RSS_MIB = 450;
const REDUCED_RECOVERY_RSS_MIB = 400;
const CRITICAL_RECOVERY_RSS_MIB = 425;
// A process can retain libc/undici arenas after a media wave even after the
// JS heap, ArrayBuffers and the authoritative page-byte budget have drained.
// Keeping optional JSON-only metadata paused forever in that condition turns
// a protective page brake into a permanent onboarding/backfill starvation.
// This is deliberately not a relaxation of the page brake: it only allows the
// already one-seat metadata path back after a quiet, low-live-memory window,
// and immediately closes again if RSS grows.
const RETAINED_RSS_METADATA_RESUME_AFTER_MS = 60_000;
// At the higher hard-media brake, a flat allocator plateau near 470 MiB must
// still be able to run the already-bounded one-seat metadata lane.  It does
// not relax the page brake or admit media; live external/ArrayBuffer checks
// and the growth check below remain mandatory.
const RETAINED_RSS_METADATA_MAX_MIB = 480;
const RETAINED_RSS_METADATA_MAX_GROWTH_MIB = 4;
const RETAINED_RSS_METADATA_MAX_EXTERNAL_MIB = 32;
const RETAINED_RSS_METADATA_MAX_ARRAY_BUFFER_MIB = 16;
const RETAINED_RSS_METADATA_MAX_RESERVED_PAGE_BYTES = 4 * MIB;
// Node is started with --expose-gc.  A bounded second collection is useful
// when allocator-retained page backing stores lift RSS through the CRITICAL
// fence after the first NORMAL->REDUCED collection.  Do not turn this into a
// continuous stop-the-world loop under genuine live-media pressure.
const FORCED_GC_COOLDOWN_MS = 10_000;

export type LiteMemoryPressureState = 'NORMAL' | 'REDUCED' | 'CRITICAL';

export type LiteMemoryPressureSnapshot = {
  state: LiteMemoryPressureState;
  rssMiB: number;
  heapUsedMiB: number;
  heapTotalMiB: number;
  externalMiB: number;
  arrayBuffersMiB: number;
  /** Approximate RSS not accounted for by V8 heap/external memory. */
  unattributedNativeMiB: number;
  pageBufferCapacity: number;
  pageByteCapacity: number;
  /** A high but stable native-RSS plateau may admit one metadata seat only. */
  retainedRssMetadataResume: boolean;
  pauseLowPriorityMetadata: boolean;
};

export type LiteNormalMediaProfile = {
  pageBufferCapacity: number;
  pageByteCapacity: number;
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
  private normalPageBufferCapacity: number;
  private normalPageByteCapacity: number;
  private lastForcedGcAt = 0;
  private retainedCriticalSince: number | null = null;
  private retainedCriticalBaselineMiB = 0;
  private retainedRssMetadataResume = false;

  constructor(
    private readonly pageBuffers: AsyncSemaphore,
    private readonly pageBytes: PageByteBudget,
    normalPageBufferCapacity: number,
    normalPageByteCapacity = LITE_PAGE_BUFFER_BUDGET_BYTES,
  ) {
    this.normalPageBufferCapacity = normalPageBufferCapacity;
    this.normalPageByteCapacity = normalPageByteCapacity;
    this.snapshotValue = this.buildSnapshot(process.memoryUsage());
  }

  /**
   * The durable chapter-admission cap bounds how many page waves can exist at
   * once. Keep the normal media profile no wider than that cap needs, while
   * retaining the established 48/80 profile for a wider measured ramp.
   *
   * A pressure state remains authoritative: changing the operator cap never
   * widens an already reduced or critical process mid-wave.
   */
  setNormalMediaProfile(profile: LiteNormalMediaProfile): void {
    if (!Number.isSafeInteger(profile.pageBufferCapacity) || profile.pageBufferCapacity < 1 ||
        !Number.isSafeInteger(profile.pageByteCapacity) || profile.pageByteCapacity < 1) {
      throw new Error('LITE_NORMAL_MEDIA_PROFILE_INVALID');
    }
    this.normalPageBufferCapacity = profile.pageBufferCapacity;
    this.normalPageByteCapacity = profile.pageByteCapacity;
    if (this.state === 'NORMAL') {
      this.pageBuffers.setCapacity(profile.pageBufferCapacity);
      this.pageBytes.setCapacityBytes(profile.pageByteCapacity);
      this.snapshotValue = this.buildSnapshot(process.memoryUsage());
    }
  }

  getNormalMediaProfile(): LiteNormalMediaProfile {
    return {
      pageBufferCapacity: this.normalPageBufferCapacity,
      pageByteCapacity: this.normalPageByteCapacity,
    };
  }

  sample(memory?: MemoryUsageLike): LiteMemoryPressureSnapshot {
    let observed = memory || process.memoryUsage();
    const rssMiBBeforeGc = observed.rss / MIB;
    // V8's heap can remain small while completed image ArrayBuffers are ready
    // for collection. A first collection happens before NORMAL yields. Once
    // REDUCED has drained its bounded page wave, collect again on the same
    // cooldown instead of waiting for CRITICAL: production retained 286 MiB
    // of native RSS at 364 MiB after all tasks, page bytes and ArrayBuffers
    // were already idle. This cannot run during a live page wave.
    const pageBytes = this.pageBytes.snapshot().reservedBytes;
    const drainedPageWave = pageBytes <= RETAINED_RSS_METADATA_MAX_RESERVED_PAGE_BYTES &&
      (observed.external || 0) / MIB <= RETAINED_RSS_METADATA_MAX_EXTERNAL_MIB &&
      (observed.arrayBuffers || 0) / MIB <= RETAINED_RSS_METADATA_MAX_ARRAY_BUFFER_MIB;
    const shouldCollect = this.state === 'NORMAL'
      ? rssMiBBeforeGc >= NORMAL_RSS_MIB
      : this.state === 'REDUCED'
        ? rssMiBBeforeGc >= CRITICAL_RSS_MIB || drainedPageWave
        : false;
    if (!memory && shouldCollect && Date.now() - this.lastForcedGcAt >= FORCED_GC_COOLDOWN_MS) {
      const gc = (globalThis as typeof globalThis & { gc?: () => void }).gc;
      if (typeof gc === 'function') {
        this.lastForcedGcAt = Date.now();
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
    this.retainedRssMetadataResume = this.updateRetainedRssMetadataResume(observed);
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
      // An operator profile at or below the former reduced 48 MiB ceiling
      // must still have a smaller REDUCED mode.  Otherwise cap 1--3 profiles
      // (8/48, 16/48 and 24/48) only changed the state label while admitting
      // exactly the same media load.  Production demonstrated that failure:
      // cap 2 reached 359 MiB RSS with 50 MiB live external memory, yet its
      // 16/48 normal profile was identical to REDUCED.  Halve both bounded
      // gates in that compact operating mode; NORMAL recovery restores the
      // operator-selected envelope through the existing hysteresis.
      if (this.normalPageByteCapacity <= 48 * MIB) {
        return {
          pageBufferCapacity: Math.max(1, Math.floor(this.normalPageBufferCapacity / 2)),
          pageByteCapacity: Math.min(this.normalPageByteCapacity, 32 * MIB),
        };
      }
      return {
        // RSS can retain allocator arenas after a completed media wave even
        // when the live page payload is well below the reduced byte fence.
        // This permit now owns only active HTTP body/validation (it is
        // released before the Telegram upload), so cutting it below the
        // measured 24-request fence made REDUCED a permanent source-download
        // bottleneck without reducing retained media. Keep the same 48 MiB
        // authoritative byte ceiling; CRITICAL remains the one-page brake.
        pageBufferCapacity: Math.min(this.normalPageBufferCapacity, 24),
        // Production showed the 32 MiB reduced budget itself saturated with
        // many small pages while RSS was stable around 310 MiB.  It left
        // capacity unused in the 512 MiB container and turned REDUCED into a
        // permanent 3--6 chapter/minute state.  48 MiB is still below the
        // normal 80 MiB fence and leaves the CRITICAL 20 MiB brake intact.
        pageByteCapacity: 48 * MIB,
      };
    }
    return {
      pageBufferCapacity: this.normalPageBufferCapacity,
      pageByteCapacity: this.normalPageByteCapacity,
    };
  }

  /**
   * Distinguish active media pressure from allocator-retained RSS. The latter
   * is observable when V8/external memory and the page budget are all low for
   * a full minute, while RSS is flat and still comfortably below the
   * container ceiling. Keep CRITICAL page admission intact in both cases;
   * this merely unblocks the existing single-seat metadata/onboarding lane.
   */
  private updateRetainedRssMetadataResume(memory: MemoryUsageLike): boolean {
    if (this.state !== 'CRITICAL') {
      this.resetRetainedCriticalWindow();
      return false;
    }
    const rssMiB = memory.rss / MIB;
    const externalMiB = (memory.external || 0) / MIB;
    const arrayBuffersMiB = (memory.arrayBuffers || 0) / MIB;
    const pageBytes = this.pageBytes.snapshot().reservedBytes;
    const lowLiveMemory = externalMiB <= RETAINED_RSS_METADATA_MAX_EXTERNAL_MIB
      && arrayBuffersMiB <= RETAINED_RSS_METADATA_MAX_ARRAY_BUFFER_MIB
      && pageBytes <= RETAINED_RSS_METADATA_MAX_RESERVED_PAGE_BYTES;

    if (!lowLiveMemory || rssMiB >= RETAINED_RSS_METADATA_MAX_MIB) {
      this.resetRetainedCriticalWindow();
      return false;
    }

    const now = Date.now();
    if (this.retainedCriticalSince === null ||
        rssMiB - this.retainedCriticalBaselineMiB > RETAINED_RSS_METADATA_MAX_GROWTH_MIB) {
      this.retainedCriticalSince = now;
      this.retainedCriticalBaselineMiB = rssMiB;
      return false;
    }
    return now - this.retainedCriticalSince >= RETAINED_RSS_METADATA_RESUME_AFTER_MS;
  }

  private resetRetainedCriticalWindow(): void {
    this.retainedCriticalSince = null;
    this.retainedCriticalBaselineMiB = 0;
  }

  private buildSnapshot(memory: MemoryUsageLike): LiteMemoryPressureSnapshot {
    const heapTotalMiB = Math.round((memory.heapTotal || 0) / MIB);
    const externalMiB = Math.round((memory.external || 0) / MIB);
    const rssMiB = Math.round(memory.rss / MIB);
    return {
      state: this.state,
      rssMiB,
      heapUsedMiB: Math.round((memory.heapUsed || 0) / MIB),
      heapTotalMiB,
      externalMiB,
      arrayBuffersMiB: Math.round((memory.arrayBuffers || 0) / MIB),
      unattributedNativeMiB: Math.max(0, rssMiB - heapTotalMiB - externalMiB),
      pageBufferCapacity: this.pageBuffers.capacity,
      pageByteCapacity: this.pageBytes.capacityBytes,
      // Reduced mode limits source metadata to one lane in the runtime. It
      // only pauses it completely at CRITICAL so allocator-retained RSS cannot
      // turn a short media burst into a permanent metadata freeze.
      retainedRssMetadataResume: this.retainedRssMetadataResume,
      pauseLowPriorityMetadata: this.state === 'CRITICAL' && !this.retainedRssMetadataResume,
    };
  }
}
