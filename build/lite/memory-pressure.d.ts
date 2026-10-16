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
    private readonly normalPageBufferCapacity;
    private state;
    private snapshotValue;
    private lastForcedGcAt;
    private retainedCriticalSince;
    private retainedCriticalBaselineMiB;
    private retainedRssMetadataResume;
    constructor(pageBuffers: AsyncSemaphore, pageBytes: PageByteBudget, normalPageBufferCapacity: number);
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
