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
export declare const LITE_PAGE_BUFFER_INITIAL_BYTES: number;
export declare const LITE_PAGE_BUFFER_UNKNOWN_REDUCED_INITIAL_BYTES: number;
export declare const LITE_PAGE_BUFFER_UNKNOWN_NORMAL_INITIAL_BYTES: number;
export declare const LITE_PAGE_BUFFER_BUDGET_BYTES: number;
/**
 * Keep REDUCED able to use the full 24-page request fence, while NORMAL uses
 * the measured 2 MiB p95-sized provisional body to avoid needless terminal
 * promotions. The selected value is an admission estimate; every response is
 * still validated and upgraded before an overflowing chunk is retained.
 */
export declare function unknownPageInitialReservationBytes(capacityBytes: number): number;
export type PageByteBudgetSnapshot = {
    capacityBytes: number;
    reservedBytes: number;
    waiting: number;
};
export declare class PageByteReservation {
    private readonly budget;
    private _reservedBytes;
    private _released;
    constructor(budget: PageByteBudget, _reservedBytes: number);
    get reservedBytes(): number;
    get isCommitted(): boolean;
    get isReleased(): boolean;
    upgrade(newBytes: number, signal?: AbortSignal, _options?: BufferReservationRequestOptions): Promise<void>;
    /**
     * Headers can refine the conservative lengthless admission downward before
     * the body is read. Unlike upgrade this is synchronous: it only returns
     * already-reserved budget and immediately wakes another producer.
     */
    shrink(newBytes: number): void;
    release(): void;
}
export declare class PageByteBudget {
    private reservedBytes;
    private readonly waiters;
    private _capacityBytes;
    constructor(capacityBytes?: number);
    get capacityBytes(): number;
    /**
     * Lowering capacity never revokes an in-flight reservation. Existing page
     * work drains normally; new body starts wait until the remaining live bytes
     * fit under the new cap. Raising capacity wakes eligible waiters at once.
     */
    setCapacityBytes(capacityBytes: number): void;
    snapshot(): PageByteBudgetSnapshot;
    reserve(bytes?: number, signal?: AbortSignal, options?: {
        completionHeadroomBytes?: number;
    }): Promise<PageByteReservation>;
    grow(_reservation: PageByteReservation, bytes: number, signal?: AbortSignal): Promise<void>;
    release(bytes: number): void;
    private acquire;
    private canReserve;
    private drain;
}
