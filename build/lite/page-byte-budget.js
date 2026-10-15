/**
 * A small, Lite-local byte budget for page bodies retained between download
 * and durable receipt persistence.  A page-count semaphore alone cannot
 * protect a small container when a few source images are much larger than
 * the usual page.
 *
 * Admission is controlled by bytes actually reserved by active page
 * pipelines, and releases immediately wake blocked producers.  The Lite
 * runtime may lower the effective cap when the container reports memory
 * pressure; that only holds *new* producers back. Completion upgrades keep
 * precedence so an already-started body can always drain rather than forming
 * a backpressure deadlock.
 */
export const LITE_PAGE_BUFFER_INITIAL_BYTES = 1 * 1024 * 1024;
// Production page bodies in the public throughput window were predominantly
// sub-2 MiB. Start a lengthless body at that measured size and reserve only
// admission headroom for one 20 MiB terminal promotion; this lets normal and
// reduced profiles use their bounded page slots without increasing either
// profile's total byte ceiling.
export const LITE_PAGE_BUFFER_UNKNOWN_INITIAL_BYTES = 2 * 1024 * 1024;
// `readImageBody` fills a declared-length response directly into its final
// Uint8Array, so the former streamed-chunks-plus-final-body duplication no
// longer determines the normal high-water mark. Keep a deliberately bounded
// 64 MiB ceiling nevertheless: uploads, HTTP/TLS and malformed responses
// still need room inside the 512 MiB container, and the runtime cuts this
// budget well before memory pressure becomes critical.
export const LITE_PAGE_BUFFER_BUDGET_BYTES = 64 * 1024 * 1024;
export class PageByteReservation {
    budget;
    _reservedBytes;
    _released = false;
    constructor(budget, _reservedBytes) {
        this.budget = budget;
        this._reservedBytes = _reservedBytes;
    }
    get reservedBytes() { return this._reservedBytes; }
    // `readImageBody` only needs these flags to make its error cleanup safe.
    // Lite holds this reservation until the page receipt is durable, so there
    // is no separate committed state to track.
    get isCommitted() { return false; }
    get isReleased() { return this._released; }
    async upgrade(newBytes, signal, _options) {
        if (this._released || newBytes <= this._reservedBytes)
            return;
        await this.budget.grow(this, newBytes - this._reservedBytes, signal);
        this._reservedBytes = newBytes;
    }
    release() {
        if (this._released)
            return;
        this._released = true;
        this.budget.release(this._reservedBytes);
    }
}
export class PageByteBudget {
    reservedBytes = 0;
    waiters = [];
    _capacityBytes;
    constructor(capacityBytes = LITE_PAGE_BUFFER_BUDGET_BYTES) {
        if (!Number.isSafeInteger(capacityBytes) || capacityBytes < 1) {
            throw new Error('LITE_PAGE_BUFFER_BUDGET_INVALID');
        }
        this._capacityBytes = capacityBytes;
    }
    get capacityBytes() {
        return this._capacityBytes;
    }
    /**
     * Lowering capacity never revokes an in-flight reservation. Existing page
     * work drains normally; new body starts wait until the remaining live bytes
     * fit under the new cap. Raising capacity wakes eligible waiters at once.
     */
    setCapacityBytes(capacityBytes) {
        if (!Number.isSafeInteger(capacityBytes) || capacityBytes < 1) {
            throw new Error('LITE_PAGE_BUFFER_BUDGET_INVALID');
        }
        this._capacityBytes = capacityBytes;
        this.drain();
    }
    snapshot() {
        return { capacityBytes: this.capacityBytes, reservedBytes: this.reservedBytes, waiting: this.waiters.length };
    }
    async reserve(bytes = LITE_PAGE_BUFFER_INITIAL_BYTES, signal, options) {
        await this.acquire(bytes, 'INITIAL', signal, Math.max(0, options?.completionHeadroomBytes || 0));
        return new PageByteReservation(this, bytes);
    }
    async grow(_reservation, bytes, signal) {
        await this.acquire(bytes, 'UPGRADE', signal);
    }
    release(bytes) {
        this.reservedBytes = Math.max(0, this.reservedBytes - bytes);
        this.drain();
    }
    async acquire(bytes, kind, signal, completionHeadroomBytes = 0) {
        if (!Number.isSafeInteger(bytes) || bytes <= 0 || bytes > this.capacityBytes ||
            !Number.isSafeInteger(completionHeadroomBytes) || completionHeadroomBytes < 0 ||
            bytes + completionHeadroomBytes > this.capacityBytes) {
            throw new Error('LITE_PAGE_BUFFER_RESERVATION_INVALID');
        }
        signal?.throwIfAborted();
        if (!this.waiters.length && this.canReserve(bytes, completionHeadroomBytes)) {
            this.reservedBytes += bytes;
            return;
        }
        await new Promise((resolve, reject) => {
            const waiter = { bytes, kind, completionHeadroomBytes, resolve: () => {
                    cleanup();
                    resolve();
                }, reject: (error) => {
                    cleanup();
                    reject(error);
                }, signal };
            const onAbort = () => waiter.reject(signal?.reason || new Error('LITE_PAGE_BUFFER_RESERVATION_ABORTED'));
            const cleanup = () => {
                signal?.removeEventListener('abort', onAbort);
                const index = this.waiters.indexOf(waiter);
                if (index >= 0)
                    this.waiters.splice(index, 1);
            };
            signal?.addEventListener('abort', onAbort, { once: true });
            this.waiters.push(waiter);
            this.drain();
        });
    }
    canReserve(bytes, completionHeadroomBytes = 0) {
        return this.reservedBytes + bytes + completionHeadroomBytes <= this.capacityBytes;
    }
    drain() {
        while (this.waiters.length) {
            // Complete in-flight page bodies before admitting a new producer. This
            // is the only priority rule here and prevents byte-budget deadlock.
            const upgradeIndex = this.waiters.findIndex((waiter) => waiter.kind === 'UPGRADE' && this.canReserve(waiter.bytes));
            const initialIndex = this.waiters.findIndex((waiter) => waiter.kind === 'INITIAL' && this.canReserve(waiter.bytes, waiter.completionHeadroomBytes));
            const index = upgradeIndex >= 0 ? upgradeIndex : initialIndex;
            if (index < 0)
                return;
            const [waiter] = this.waiters.splice(index, 1);
            this.reservedBytes += waiter.bytes;
            waiter.resolve();
        }
    }
}
