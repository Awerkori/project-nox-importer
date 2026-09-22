import type { BufferReservation } from './concurrency.js';
export interface ReadImageBodyOptions {
    maxBytes?: number;
    reservation?: BufferReservation;
    signal?: AbortSignal;
}
export declare function readImageBody(response: Response, optionsOrMaxBytes?: number | ReadImageBodyOptions): Promise<Uint8Array>;
