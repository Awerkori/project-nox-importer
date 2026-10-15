const MIB = 1024 * 1024;
const NORMAL_RSS_MIB = 300;
const CRITICAL_RSS_MIB = 360;
const REDUCED_RECOVERY_RSS_MIB = 260;
const CRITICAL_RECOVERY_RSS_MIB = 320;
/**
 * Small runtime-local memory brake for the Lite media path. It controls only
 * future page admission: an in-flight image/upload keeps its permits and can
 * finish its durable receipt safely. The hysteresis matters because Node may
 * retain freed native allocations in RSS for later reuse.
 */
export class LiteMemoryPressureGuard {
    pageBuffers;
    pageBytes;
    normalPageBufferCapacity;
    state = 'NORMAL';
    snapshotValue;
    constructor(pageBuffers, pageBytes, normalPageBufferCapacity) {
        this.pageBuffers = pageBuffers;
        this.pageBytes = pageBytes;
        this.normalPageBufferCapacity = normalPageBufferCapacity;
        this.snapshotValue = this.buildSnapshot(process.memoryUsage());
    }
    sample(memory = process.memoryUsage()) {
        const rssMiB = memory.rss / MIB;
        const next = this.nextState(rssMiB);
        if (next !== this.state) {
            this.state = next;
            const profile = this.profile(next);
            this.pageBuffers.setCapacity(profile.pageBufferCapacity);
            this.pageBytes.setCapacityBytes(profile.pageByteCapacity);
        }
        this.snapshotValue = this.buildSnapshot(memory);
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
                pageBufferCapacity: Math.min(this.normalPageBufferCapacity, 8),
                pageByteCapacity: 32 * MIB,
            };
        }
        return {
            pageBufferCapacity: this.normalPageBufferCapacity,
            pageByteCapacity: 64 * MIB,
        };
    }
    buildSnapshot(memory) {
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
