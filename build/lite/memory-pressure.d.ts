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
export declare class LiteMemoryPressureGuard {
    private readonly pageBuffers;
    private readonly pageBytes;
    private readonly normalPageBufferCapacity;
    private state;
    private snapshotValue;
    constructor(pageBuffers: AsyncSemaphore, pageBytes: PageByteBudget, normalPageBufferCapacity: number);
    sample(memory?: MemoryUsageLike): LiteMemoryPressureSnapshot;
    snapshot(): LiteMemoryPressureSnapshot;
    private nextState;
    private profile;
    private buildSnapshot;
}
export {};
