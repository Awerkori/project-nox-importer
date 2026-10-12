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
export declare const LITE_PAGE_BUFFER_INITIAL_BYTES: number;
export declare const LITE_PAGE_BUFFER_BUDGET_BYTES: number;
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
    release(): void;
}
export declare class PageByteBudget {
    readonly capacityBytes: number;
    private reservedBytes;
    private readonly waiters;
    constructor(capacityBytes?: number);
    snapshot(): PageByteBudgetSnapshot;
    reserve(bytes?: number, signal?: AbortSignal): Promise<PageByteReservation>;
    grow(_reservation: PageByteReservation, bytes: number, signal?: AbortSignal): Promise<void>;
    release(bytes: number): void;
    private acquire;
    private canReserve;
    private drain;
}
