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
export const LITE_PAGE_BUFFER_INITIAL_BYTES = 512 * 1024;
// A source that omits Content-Length cannot receive exact admission before
// its response headers arrive.  Production bodies are usually small, but the
// former 2 MiB placeholder filled the 48 MiB REDUCED profile at 15--16
// in-flight pages even though the global download fence is 24. REDUCED starts
// at 1 MiB and retains terminal-promotion headroom through the 20 MiB hard
// bound. Any larger stream upgrades before accepting the overflowing chunk,
// so this is an admission estimate only: it never weakens the authoritative
// byte cap.
export const LITE_PAGE_BUFFER_UNKNOWN_REDUCED_INITIAL_BYTES = 1 * 1024 * 1024;
// Production page p95 is below 2 MiB, p50 is 0.29 MiB, avg is 0.49 MiB.
// Using 512 KiB for the normal profile gives 2x the concurrent admission slots
// while streaming upgrade accommodates larger pages safely without queue stalls.
export const LITE_PAGE_BUFFER_UNKNOWN_NORMAL_INITIAL_BYTES = 512 * 1024;
// `readImageBody` fills a declared-length response directly into its final
// Uint8Array, so the former streamed-chunks-plus-final-body duplication no
// longer determines the normal high-water mark. Production reaches 96 MiB
// with real promoted bodies while RSS stays well below 420 MiB. The runtime still
// cuts new admission to 48 MiB at 420 MiB RSS, safely below the 512 MiB limit.
export const LITE_PAGE_BUFFER_BUDGET_BYTES = 96 * 1024 * 1024;

/**
 * Keep REDUCED able to use the full 24-page request fence, while NORMAL uses
 * the measured 2 MiB p95-sized provisional body to avoid needless terminal
 * promotions. The selected value is an admission estimate; every response is
 * still validated and upgraded before an overflowing chunk is retained.
 */
export function unknownPageInitialReservationBytes(capacityBytes: number): number {
  return capacityBytes >= LITE_PAGE_BUFFER_BUDGET_BYTES
    ? LITE_PAGE_BUFFER_UNKNOWN_NORMAL_INITIAL_BYTES
    : LITE_PAGE_BUFFER_UNKNOWN_REDUCED_INITIAL_BYTES;
}

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
  private completionInProgress = false;

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
    options?: BufferReservationRequestOptions,
  ): Promise<void> {
    if (this._released || newBytes <= this._reservedBytes) return;
    await this.budget.grow(this, newBytes - this._reservedBytes, signal);
    this._reservedBytes = newBytes;
    // Initial admission reserves headroom for exactly one started stream to
    // make terminal progress. Once this body has actually grown, that
    // headroom is represented by its real reservation; keeping it as a
    // second virtual reservation would strand usable bytes behind one large
    // page. Other growth requests remain priority waiters in the same budget.
    if (options?.intent === 'COMPLETION' && !this.completionInProgress) {
      this.completionInProgress = true;
      this.budget.beginCompletion();
    }
  }

  /**
   * Headers can refine the conservative lengthless admission downward before
   * the body is read. Unlike upgrade this is synchronous: it only returns
   * already-reserved budget and immediately wakes another producer.
   */
  shrink(newBytes: number): void {
    if (this._released || !Number.isSafeInteger(newBytes) || newBytes <= 0 || newBytes >= this._reservedBytes) return;
    const releasedBytes = this._reservedBytes - newBytes;
    this._reservedBytes = newBytes;
    this.budget.release(releasedBytes);
  }

  /**
   * A lengthless response temporarily reserves its 20 MiB terminal bound
   * while its chunks are still being read and assembled.  After
   * `readImageBody` returns, that transient state is gone: only the exact
   * returned body remains for upload.  Keep the exact body reservation, but
   * return both the speculative tail and this response's completion token so
   * the next lengthless response still receives its own safe headroom.
   */
  completeBody(actualBytes: number): void {
    if (this._released) return;
    this.shrink(actualBytes);
    if (this.completionInProgress) {
      this.completionInProgress = false;
      this.budget.endCompletion();
    }
  }

  release(): void {
    if (this._released) return;
    this._released = true;
    if (this.completionInProgress) this.budget.endCompletion();
    this.budget.release(this._reservedBytes);
  }
}

export class PageByteBudget {
  private reservedBytes = 0;
  private completionInProgress = 0;
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

  beginCompletion(): void {
    this.completionInProgress++;
    this.drain();
  }

  endCompletion(): void {
    this.completionInProgress = Math.max(0, this.completionInProgress - 1);
    this.drain();
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
    // A real upgrade is the completion headroom made concrete. Retaining the
    // virtual headroom after that point double-counts one bounded body and
    // unnecessarily blocks ordinary small pages. A second terminal body is
    // still governed by the regular UPGRADE priority and byte ceiling.
    const requiredHeadroom = this.completionInProgress > 0 ? 0 : completionHeadroomBytes;
    return this.reservedBytes + bytes + requiredHeadroom <= this.capacityBytes;
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
