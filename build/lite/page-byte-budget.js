/**
 * A small, Lite-local byte budget for page bodies retained between download
 * and durable receipt persistence.  A page-count semaphore alone cannot
 * protect a small container when a few source images are much larger than
 * the usual page.
 *
 * This intentionally has no RSS feedback loop or background timer: admission
 * is controlled by bytes actually reserved by active page pipelines, and
 * releases immediately wake blocked producers.  Completion upgrades get
 * precedence over new initial reservations so an already-started body can
 * always drain instead of forming a backpressure deadlock.
 */
export const LITE_PAGE_BUFFER_INITIAL_BYTES = 1 * 1024 * 1024;
// After the 64→80 MiB ramp, snapshots still showed 69–75/80 MiB reserved
// with 2–3 producers waiting; RSS stayed below 160 MiB. One further bounded
// 16 MiB increment tests remaining byte-admission pressure. Other limits stay.
export const LITE_PAGE_BUFFER_BUDGET_BYTES = 96 * 1024 * 1024;
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
    capacityBytes;
    reservedBytes = 0;
    waiters = [];
    constructor(capacityBytes = LITE_PAGE_BUFFER_BUDGET_BYTES) {
        this.capacityBytes = capacityBytes;
        if (!Number.isSafeInteger(capacityBytes) || capacityBytes < 1) {
            throw new Error('LITE_PAGE_BUFFER_BUDGET_INVALID');
        }
    }
    snapshot() {
        return { capacityBytes: this.capacityBytes, reservedBytes: this.reservedBytes, waiting: this.waiters.length };
    }
    async reserve(bytes = LITE_PAGE_BUFFER_INITIAL_BYTES, signal) {
        await this.acquire(bytes, 'INITIAL', signal);
        return new PageByteReservation(this, bytes);
    }
    async grow(_reservation, bytes, signal) {
        await this.acquire(bytes, 'UPGRADE', signal);
    }
    release(bytes) {
        this.reservedBytes = Math.max(0, this.reservedBytes - bytes);
        this.drain();
    }
    async acquire(bytes, kind, signal) {
        if (!Number.isSafeInteger(bytes) || bytes <= 0 || bytes > this.capacityBytes) {
            throw new Error('LITE_PAGE_BUFFER_RESERVATION_INVALID');
        }
        signal?.throwIfAborted();
        if (!this.waiters.length && this.canReserve(bytes)) {
            this.reservedBytes += bytes;
            return;
        }
        await new Promise((resolve, reject) => {
            const waiter = { bytes, kind, resolve: () => {
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
    canReserve(bytes) {
        return this.reservedBytes + bytes <= this.capacityBytes;
    }
    drain() {
        while (this.waiters.length) {
            // Complete in-flight page bodies before admitting a new producer. This
            // is the only priority rule here and prevents byte-budget deadlock.
            const upgradeIndex = this.waiters.findIndex((waiter) => waiter.kind === 'UPGRADE' && this.canReserve(waiter.bytes));
            const initialIndex = this.waiters.findIndex((waiter) => waiter.kind === 'INITIAL' && this.canReserve(waiter.bytes));
            const index = upgradeIndex >= 0 ? upgradeIndex : initialIndex;
            if (index < 0)
                return;
            const [waiter] = this.waiters.splice(index, 1);
            this.reservedBytes += waiter.bytes;
            waiter.resolve();
        }
    }
}
