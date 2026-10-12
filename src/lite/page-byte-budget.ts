import type { BufferReservationRequestOptions } from '../core/concurrency.js';

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
export const LITE_PAGE_BUFFER_BUDGET_BYTES = 48 * 1024 * 1024;

type Waiter = {
  bytes: number;
  kind: 'INITIAL' | 'UPGRADE';
  resolve: () => void;
  reject: (error: unknown) => void;
  signal?: AbortSignal;
};

export type PageByteBudgetSnapshot = {
  capacityBytes: number;
  reservedBytes: number;
  waiting: number;
};

export class PageByteReservation {
  private _released = false;

  constructor(private readonly budget: PageByteBudget, private _reservedBytes: number) {}

  get reservedBytes(): number { return this._reservedBytes; }
  // `readImageBody` only needs these flags to make its error cleanup safe.
  // Lite holds this reservation until the page receipt is durable, so there
  // is no separate committed state to track.
  get isCommitted(): boolean { return false; }
  get isReleased(): boolean { return this._released; }

  async upgrade(
    newBytes: number,
    signal?: AbortSignal,
    _options?: BufferReservationRequestOptions,
  ): Promise<void> {
    if (this._released || newBytes <= this._reservedBytes) return;
    await this.budget.grow(this, newBytes - this._reservedBytes, signal);
    this._reservedBytes = newBytes;
  }

  release(): void {
    if (this._released) return;
    this._released = true;
    this.budget.release(this._reservedBytes);
  }
}

export class PageByteBudget {
  private reservedBytes = 0;
  private readonly waiters: Waiter[] = [];

  constructor(readonly capacityBytes: number = LITE_PAGE_BUFFER_BUDGET_BYTES) {
    if (!Number.isSafeInteger(capacityBytes) || capacityBytes < 1) {
      throw new Error('LITE_PAGE_BUFFER_BUDGET_INVALID');
    }
  }

  snapshot(): PageByteBudgetSnapshot {
    return { capacityBytes: this.capacityBytes, reservedBytes: this.reservedBytes, waiting: this.waiters.length };
  }

  async reserve(bytes = LITE_PAGE_BUFFER_INITIAL_BYTES, signal?: AbortSignal): Promise<PageByteReservation> {
    await this.acquire(bytes, 'INITIAL', signal);
    return new PageByteReservation(this, bytes);
  }

  async grow(_reservation: PageByteReservation, bytes: number, signal?: AbortSignal): Promise<void> {
    await this.acquire(bytes, 'UPGRADE', signal);
  }

  release(bytes: number): void {
    this.reservedBytes = Math.max(0, this.reservedBytes - bytes);
    this.drain();
  }

  private async acquire(bytes: number, kind: Waiter['kind'], signal?: AbortSignal): Promise<void> {
    if (!Number.isSafeInteger(bytes) || bytes <= 0 || bytes > this.capacityBytes) {
      throw new Error('LITE_PAGE_BUFFER_RESERVATION_INVALID');
    }
    signal?.throwIfAborted();
    if (!this.waiters.length && this.canReserve(bytes)) {
      this.reservedBytes += bytes;
      return;
    }
    await new Promise<void>((resolve, reject) => {
      const waiter: Waiter = { bytes, kind, resolve: () => {
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
        if (index >= 0) this.waiters.splice(index, 1);
      };
      signal?.addEventListener('abort', onAbort, { once: true });
      this.waiters.push(waiter);
      this.drain();
    });
  }

  private canReserve(bytes: number): boolean {
    return this.reservedBytes + bytes <= this.capacityBytes;
  }

  private drain(): void {
    while (this.waiters.length) {
      // Complete in-flight page bodies before admitting a new producer. This
      // is the only priority rule here and prevents byte-budget deadlock.
      const upgradeIndex = this.waiters.findIndex((waiter) => waiter.kind === 'UPGRADE' && this.canReserve(waiter.bytes));
      const initialIndex = this.waiters.findIndex((waiter) => waiter.kind === 'INITIAL' && this.canReserve(waiter.bytes));
      const index = upgradeIndex >= 0 ? upgradeIndex : initialIndex;
      if (index < 0) return;
      const [waiter] = this.waiters.splice(index, 1);
      this.reservedBytes += waiter.bytes;
      waiter.resolve();
    }
  }
}
