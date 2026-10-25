import { LITE_PAGE_BUFFER_BUDGET_BYTES } from './page-byte-budget.js';
const MIB = 1024 * 1024;
// With the bounded normal page budget, production establishes an allocator
// RSS plateau around 330 MiB after a sustained media wave while only roughly
// 30 MiB remains live in ArrayBuffers.  The former 300/360 thresholds treated
// that retained native arena as ongoing growth and held the importer at a
// permanently throttled 16/48 profile.  Leave a full 112 MiB below the 512
// MiB container ceiling for the hard brake, but let the normal bounded media
// pipeline use its measured steady-state headroom first.
const NORMAL_RSS_MIB = 350;
const CRITICAL_RSS_MIB = 400;
const REDUCED_RECOVERY_RSS_MIB = 330;
const CRITICAL_RECOVERY_RSS_MIB = 370;
// A process can retain libc/undici arenas after a media wave even after the
// JS heap, ArrayBuffers and the authoritative page-byte budget have drained.
// Keeping optional JSON-only metadata paused forever in that condition turns
// a protective page brake into a permanent onboarding/backfill starvation.
// This is deliberately not a relaxation of the page brake: it only allows the
// already one-seat metadata path back after a quiet, low-live-memory window,
// and immediately closes again if RSS grows.
const RETAINED_RSS_METADATA_RESUME_AFTER_MS = 60_000;
const RETAINED_RSS_METADATA_MAX_MIB = 440;
const RETAINED_RSS_METADATA_MAX_GROWTH_MIB = 4;
const RETAINED_RSS_METADATA_MAX_EXTERNAL_MIB = 32;
const RETAINED_RSS_METADATA_MAX_ARRAY_BUFFER_MIB = 16;
const RETAINED_RSS_METADATA_MAX_RESERVED_PAGE_BYTES = 4 * MIB;
// Node is started with --expose-gc.  A bounded second collection is useful
// when allocator-retained page backing stores lift RSS through the CRITICAL
// fence after the first NORMAL->REDUCED collection.  Do not turn this into a
// continuous stop-the-world loop under genuine live-media pressure.
const FORCED_GC_COOLDOWN_MS = 10_000;
/**
 * Small runtime-local memory brake for the Lite media path. It controls only
 * future page admission: an in-flight image/upload keeps its permits and can
 * finish its durable receipt safely. The hysteresis matters because Node may
 * retain freed native allocations in RSS for later reuse.
 */
export class LiteMemoryPressureGuard {
    pageBuffers;
    pageBytes;
    state = 'NORMAL';
    snapshotValue;
    normalPageBufferCapacity;
    normalPageByteCapacity;
    lastForcedGcAt = 0;
    retainedCriticalSince = null;
    retainedCriticalBaselineMiB = 0;
    retainedRssMetadataResume = false;
    constructor(pageBuffers, pageBytes, normalPageBufferCapacity, normalPageByteCapacity = LITE_PAGE_BUFFER_BUDGET_BYTES) {
        this.pageBuffers = pageBuffers;
        this.pageBytes = pageBytes;
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
    setNormalMediaProfile(profile) {
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
    getNormalMediaProfile() {
        return {
            pageBufferCapacity: this.normalPageBufferCapacity,
            pageByteCapacity: this.normalPageByteCapacity,
        };
    }
    sample(memory) {
        let observed = memory || process.memoryUsage();
        const rssMiBBeforeGc = observed.rss / MIB;
        // V8's heap can remain small while completed image ArrayBuffers are ready
        // for collection.  A first collection happens before NORMAL yields.  A
        // second, cooldown-bounded collection happens only if retained RSS would
        // otherwise move REDUCED into CRITICAL.  That distinction matters for the
        // lengthless-page pipeline: its durable byte budget can already be empty
        // while the previous wave's dead backing stores still occupy RSS.
        const shouldCollect = this.state === 'NORMAL'
            ? rssMiBBeforeGc >= NORMAL_RSS_MIB
            : this.state === 'REDUCED'
                ? rssMiBBeforeGc >= CRITICAL_RSS_MIB
                : false;
        if (!memory && shouldCollect && Date.now() - this.lastForcedGcAt >= FORCED_GC_COOLDOWN_MS) {
            const gc = globalThis.gc;
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
    snapshot() {
        return this.snapshotValue;
    }
    nextState(rssMiB) {
        if (this.state === 'CRITICAL') {
            if (rssMiB >= CRITICAL_RECOVERY_RSS_MIB)
                return 'CRITICAL';
            return rssMiB >= NORMAL_RSS_MIB ? 'REDUCED' : 'NORMAL';
        }
        if (this.state === 'REDUCED') {
            if (rssMiB >= CRITICAL_RSS_MIB)
                return 'CRITICAL';
            return rssMiB >= REDUCED_RECOVERY_RSS_MIB ? 'REDUCED' : 'NORMAL';
        }
        if (rssMiB >= CRITICAL_RSS_MIB)
            return 'CRITICAL';
        if (rssMiB >= NORMAL_RSS_MIB)
            return 'REDUCED';
        return 'NORMAL';
    }
    profile(state) {
        if (state === 'CRITICAL') {
            // One 20 MiB maximum image can still complete, but no new wave starts.
            return { pageBufferCapacity: 1, pageByteCapacity: 20 * MIB };
        }
        if (state === 'REDUCED') {
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
    updateRetainedRssMetadataResume(memory) {
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
    resetRetainedCriticalWindow() {
        this.retainedCriticalSince = null;
        this.retainedCriticalBaselineMiB = 0;
    }
    buildSnapshot(memory) {
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
