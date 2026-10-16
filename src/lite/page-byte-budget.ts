import type { BufferReservationRequestOptions } from '../core/concurrency.js';

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
// A source that omits Content-Length cannot receive exact admission before
// its response headers arrive.  Production bodies are usually small, but the
// former 2 MiB placeholder filled the 48 MiB REDUCED profile at 15--16
// in-flight pages even though the global download fence is 24. Start at 1 MiB
// and retain terminal-promotion headroom through the 20 MiB hard bound. Any
// larger stream upgrades before accepting the overflowing chunk, so this is
// an admission estimate only: it never weakens the authoritative byte cap.
export const LITE_PAGE_BUFFER_UNKNOWN_INITIAL_BYTES = 1 * 1024 * 1024;
// `readImageBody` fills a declared-length response directly into its final
// Uint8Array, so the former streamed-chunks-plus-final-body duplication no
// longer determines the normal high-water mark. Keep a deliberately bounded
// 64 MiB ceiling nevertheless: uploads, HTTP/TLS and malformed responses
// still need room inside the 512 MiB container, and the runtime cuts this
// budget well before memory pressure becomes critical.
export const LITE_PAGE_BUFFER_BUDGET_BYTES = 64 * 1024 * 1024;

type Waiter = {
  bytes: number;
  kind: 'INITIAL' | 'UPGRADE';
  // Initial admission can leave bounded room for one already-started,
  // lengthless response to make terminal progress. This is a predicate only:
  // it is never double-counted as retained media.
  completionHeadroomBytes: number;
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
  private _capacityBytes: number;

  constructor(capacityBytes: number = LITE_PAGE_BUFFER_BUDGET_BYTES) {
    if (!Number.isSafeInteger(capacityBytes) || capacityBytes < 1) {
      throw new Error('LITE_PAGE_BUFFER_BUDGET_INVALID');
    }
    this._capacityBytes = capacityBytes;
  }

  get capacityBytes(): number {
    return this._capacityBytes;
  }

  /**
   * Lowering capacity never revokes an in-flight reservation. Existing page
   * work drains normally; new body starts wait until the remaining live bytes
   * fit under the new cap. Raising capacity wakes eligible waiters at once.
   */
  setCapacityBytes(capacityBytes: number): void {
    if (!Number.isSafeInteger(capacityBytes) || capacityBytes < 1) {
      throw new Error('LITE_PAGE_BUFFER_BUDGET_INVALID');
    }
    this._capacityBytes = capacityBytes;
    this.drain();
  }

  snapshot(): PageByteBudgetSnapshot {
    return { capacityBytes: this.capacityBytes, reservedBytes: this.reservedBytes, waiting: this.waiters.length };
  }

  async reserve(
    bytes = LITE_PAGE_BUFFER_INITIAL_BYTES,
    signal?: AbortSignal,
    options?: { completionHeadroomBytes?: number },
  ): Promise<PageByteReservation> {
    await this.acquire(bytes, 'INITIAL', signal, Math.max(0, options?.completionHeadroomBytes || 0));
    return new PageByteReservation(this, bytes);
  }

  async grow(_reservation: PageByteReservation, bytes: number, signal?: AbortSignal): Promise<void> {
    await this.acquire(bytes, 'UPGRADE', signal);
  }

  release(bytes: number): void {
    this.reservedBytes = Math.max(0, this.reservedBytes - bytes);
    this.drain();
  }

  private async acquire(
    bytes: number,
    kind: Waiter['kind'],
    signal?: AbortSignal,
    completionHeadroomBytes = 0,
  ): Promise<void> {
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
    await new Promise<void>((resolve, reject) => {
      const waiter: Waiter = { bytes, kind, completionHeadroomBytes, resolve: () => {
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

  private canReserve(bytes: number, completionHeadroomBytes = 0): boolean {
    return this.reservedBytes + bytes + completionHeadroomBytes <= this.capacityBytes;
  }

  private drain(): void {
    while (this.waiters.length) {
      // Complete in-flight page bodies before admitting a new producer. This
      // is the only priority rule here and prevents byte-budget deadlock.
      const upgradeIndex = this.waiters.findIndex((waiter) => waiter.kind === 'UPGRADE' && this.canReserve(waiter.bytes));
      const initialIndex = this.waiters.findIndex((waiter) => waiter.kind === 'INITIAL' && this.canReserve(waiter.bytes, waiter.completionHeadroomBytes));
      const index = upgradeIndex >= 0 ? upgradeIndex : initialIndex;
      if (index < 0) return;
      const [waiter] = this.waiters.splice(index, 1);
      this.reservedBytes += waiter.bytes;
      waiter.resolve();
    }
  }
}
