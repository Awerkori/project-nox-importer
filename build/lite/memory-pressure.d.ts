import { AsyncSemaphore } from '../core/concurrency.js';
import { PageByteBudget } from './page-byte-budget.js';
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
export declare class LiteMemoryPressureGuard {
    private readonly pageBuffers;
    private readonly pageBytes;
    private state;
    private snapshotValue;
    private normalPageBufferCapacity;
    private normalPageByteCapacity;
    private lastForcedGcAt;
    private retainedCriticalSince;
    private retainedCriticalBaselineMiB;
    private retainedRssMetadataResume;
    constructor(pageBuffers: AsyncSemaphore, pageBytes: PageByteBudget, normalPageBufferCapacity: number, normalPageByteCapacity?: number);
    /**
     * The durable chapter-admission cap bounds how many page waves can exist at
     * once. Keep the normal media profile no wider than that cap needs, while
     * retaining the established 48/80 profile for a wider measured ramp.
     *
     * A pressure state remains authoritative: changing the operator cap never
     * widens an already reduced or critical process mid-wave.
     */
    setNormalMediaProfile(profile: LiteNormalMediaProfile): void;
    getNormalMediaProfile(): LiteNormalMediaProfile;
    sample(memory?: MemoryUsageLike): LiteMemoryPressureSnapshot;
    snapshot(): LiteMemoryPressureSnapshot;
    private nextState;
    private profile;
    /**
     * Distinguish active media pressure from allocator-retained RSS. The latter
     * is observable when V8/external memory and the page budget are all low for
     * a full minute, while RSS is flat and still comfortably below the
     * container ceiling. Keep CRITICAL page admission intact in both cases;
     * this merely unblocks the existing single-seat metadata/onboarding lane.
     */
    private updateRetainedRssMetadataResume;
    private resetRetainedCriticalWindow;
    private buildSnapshot;
}
export {};
