import type { BufferReservation } from './concurrency.js';
export interface ReadImageBodyOptions {
    maxBytes?: number;
    reservation?: BufferReservation;
    signal?: AbortSignal;
    /**
     * Optional, opt-in lifecycle hooks used by the bounded request trace. They
     * receive counters only; no image data is retained or copied for tracing.
     */
    trace?: {
        onFirstChunk?: (metrics: {
            bytes: number;
            elapsedMs: number;
        }) => void;
        onComplete?: (metrics: {
            bytes: number;
            chunks: number;
            elapsedMs: number;
        }) => void;
        onReservationUpgrade?: (metrics: {
            phase: 'HEADER' | 'STREAM';
            state: 'STARTED' | 'COMPLETED';
            fromBytes: number;
            toBytes: number;
            elapsedMs: number;
        }) => void;
        onError?: (metrics: {
            bytes: number;
            chunks: number;
            elapsedMs: number;
            waitingOn: 'RESERVATION_UPGRADE' | null;
            error: unknown;
        }) => void;
    };
}
export declare function readImageBody(response: Response, optionsOrMaxBytes?: number | ReadImageBodyOptions): Promise<Uint8Array>;
