import { Logger } from './logger.js';
import { InvalidMediaError } from './retry-policy.js';
import { performance } from 'node:perf_hooks';
function emitTrace(callback) {
    try {
        callback?.();
    }
    catch {
        // Observability must never affect the media pipeline.
    }
}
/**
 * A one-shot, opt-in probe for the transient allocation made while stream
 * chunks are copied into their final Uint8Array. It is deliberately disabled
 * by default and records only sizes and process-memory counters: never media
 * content, URLs, headers, or source identity.
 */
const bodyMemoryProbeEnabled = process.env.NOX_BODY_MEMORY_PROBE === '1';
let bodyMemoryProbeConsumed = false;
const bodyMemoryProbeLogger = new Logger('BodyMemoryProbe');
function captureBodyMemorySnapshot() {
    const memory = process.memoryUsage();
    return {
        rssBytes: memory.rss,
        externalBytes: memory.external,
        arrayBuffersBytes: memory.arrayBuffers || 0,
    };
}
function maybeStartBodyMemoryProbe(contentLengthBytes, chunkCount, chunkBytes) {
    if (!bodyMemoryProbeEnabled || bodyMemoryProbeConsumed)
        return null;
    bodyMemoryProbeConsumed = true;
    return {
        contentLengthBytes,
        chunkCount,
        chunkBytes,
        finalBytes: chunkBytes,
        beforeConcat: captureBodyMemorySnapshot(),
    };
}
function emitBodyMemoryProbe(probe) {
    const snapshots = [
        probe.beforeConcat,
        probe.afterAllocation,
        probe.afterCopy,
        probe.afterChunkRefsReleased,
    ].filter((snapshot) => Boolean(snapshot));
    const peakRssBytes = Math.max(...snapshots.map((snapshot) => snapshot.rssBytes));
    const peakExternalBytes = Math.max(...snapshots.map((snapshot) => snapshot.externalBytes));
    const peakArrayBuffersBytes = Math.max(...snapshots.map((snapshot) => snapshot.arrayBuffersBytes));
    bodyMemoryProbeLogger.info('BODY_MEMORY_PROBE', {
        contentLengthBytes: probe.contentLengthBytes,
        chunkCount: probe.chunkCount,
        chunkBytes: probe.chunkBytes,
        finalBytes: probe.finalBytes,
        concatDurationMs: probe.concatDurationMs,
        beforeConcat: probe.beforeConcat,
        afterAllocation: probe.afterAllocation,
        afterCopy: probe.afterCopy,
        afterChunkRefsReleased: probe.afterChunkRefsReleased,
        peakRssBytes,
        peakExternalBytes,
        peakArrayBuffersBytes,
    });
}
function abortError(signal) {
    const reason = signal.reason;
    if (reason instanceof Error)
        return reason;
    return new DOMException('Image response body read was aborted', 'AbortError');
}
/**
 * A fetch timeout only protects the response headers unless it is also raced
 * against body reads. Some upstream CDNs send headers/content-length and then
 * stop delivering bytes; leaving reader.read() pending would retain both the
 * download permit and its buffer reservation forever.
 */
async function readChunk(reader, signal) {
    if (!signal)
        return reader.read();
    signal.throwIfAborted();
    return new Promise((resolve, reject) => {
        const onAbort = () => {
            // Do not await cancellation here: a broken upstream stream must not be
            // able to keep the importer slot/buffer hostage while cancel settles.
            void reader.cancel(signal.reason).catch(() => { });
            reject(abortError(signal));
        };
        signal.addEventListener('abort', onAbort, { once: true });
        reader.read().then((result) => {
            signal.removeEventListener('abort', onAbort);
            resolve(result);
        }, (error) => {
            signal.removeEventListener('abort', onAbort);
            reject(error);
        });
    });
}
// Match the existing media validator's size limit before buffering the response.
// Supports streaming reservation upgrade to enforce memory backpressure for chunked streams.
export async function readImageBody(response, optionsOrMaxBytes) {
    const options = typeof optionsOrMaxBytes === 'number'
        ? { maxBytes: optionsOrMaxBytes }
        : (optionsOrMaxBytes || {});
    const maxBytes = options.maxBytes ?? 20 * 1024 * 1024;
    const reservation = options.reservation;
    const signal = options.signal;
    const url = response.url || 'unknown';
    const declaredStr = response.headers.get('content-length');
    let contentLengthBytes = null;
    if (declaredStr) {
        const declared = parseInt(declaredStr, 10);
        if (!Number.isNaN(declared) && declared > 0) {
            contentLengthBytes = declared;
            if (declared > maxBytes) {
                await response.body?.cancel().catch(() => { });
                if (reservation && !reservation.isCommitted && !reservation.isReleased) {
                    reservation.release();
                }
                throw new InvalidMediaError(url, 'media', `Image declared content-length (${Math.round(declared / 1024 / 1024)}MB) exceeds maximum safe limit of ${Math.round(maxBytes / 1024 / 1024)}MB`);
            }
            if (reservation && declared > reservation.reservedBytes) {
                await reservation.upgrade(declared, signal);
            }
        }
    }
    if (!response.body)
        throw new Error('Image response has no body');
    const reader = response.body.getReader();
    const chunks = [];
    let length = 0;
    let firstChunkSeen = false;
    const bodyStartedAt = performance.now();
    let waitingOnReservationUpgrade = false;
    let bodyMemoryProbe = null;
    try {
        while (true) {
            signal?.throwIfAborted();
            const { value, done } = await readChunk(reader, signal);
            if (done)
                break;
            if (!firstChunkSeen) {
                firstChunkSeen = true;
                emitTrace(() => options.trace?.onFirstChunk?.({
                    bytes: value.byteLength,
                    elapsedMs: Math.round((performance.now() - bodyStartedAt) * 1000) / 1000,
                }));
            }
            const newLength = length + value.byteLength;
            if (newLength > maxBytes) {
                await reader.cancel().catch(() => { });
                if (reservation && !reservation.isCommitted && !reservation.isReleased) {
                    reservation.release();
                }
                throw new InvalidMediaError(url, 'media', `Image exceeds media byte limit of ${Math.round(maxBytes / 1024 / 1024)}MB`);
            }
            // STREAMING RESERVATION UPGRADE:
            // If accumulated length exceeds currently reserved budget, upgrade BEFORE accepting chunk!
            // This applies TCP backpressure upstream via the async pause.
            if (reservation && newLength > reservation.reservedBytes) {
                const fromBytes = reservation.reservedBytes;
                waitingOnReservationUpgrade = true;
                emitTrace(() => options.trace?.onReservationUpgrade?.({
                    phase: 'STREAM', state: 'STARTED', fromBytes, toBytes: newLength,
                    elapsedMs: Math.round((performance.now() - bodyStartedAt) * 1000) / 1000,
                }));
                await reservation.upgrade(newLength, signal);
                waitingOnReservationUpgrade = false;
                emitTrace(() => options.trace?.onReservationUpgrade?.({
                    phase: 'STREAM', state: 'COMPLETED', fromBytes, toBytes: reservation.reservedBytes,
                    elapsedMs: Math.round((performance.now() - bodyStartedAt) * 1000) / 1000,
                }));
            }
            length = newLength;
            chunks.push(value);
        }
        bodyMemoryProbe = maybeStartBodyMemoryProbe(contentLengthBytes, chunks.length, length);
        const concatStartedAt = bodyMemoryProbe ? performance.now() : 0;
        const result = new Uint8Array(length);
        if (bodyMemoryProbe)
            bodyMemoryProbe.afterAllocation = captureBodyMemorySnapshot();
        let offset = 0;
        for (const chunk of chunks) {
            result.set(chunk, offset);
            offset += chunk.byteLength;
        }
        if (bodyMemoryProbe) {
            bodyMemoryProbe.concatDurationMs = Math.round((performance.now() - concatStartedAt) * 1000) / 1000;
            bodyMemoryProbe.afterCopy = captureBodyMemorySnapshot();
        }
        emitTrace(() => options.trace?.onComplete?.({
            bytes: length,
            chunks: chunks.length,
            elapsedMs: Math.round((performance.now() - bodyStartedAt) * 1000) / 1000,
        }));
        return result;
    }
    catch (error) {
        emitTrace(() => options.trace?.onError?.({
            bytes: length,
            chunks: chunks.length,
            elapsedMs: Math.round((performance.now() - bodyStartedAt) * 1000) / 1000,
            waitingOn: waitingOnReservationUpgrade ? 'RESERVATION_UPGRADE' : null,
            error,
        }));
        // Keep error recovery bounded too. The abort handler above already asked
        // the stream to cancel; a second cancel is best-effort only.
        void reader.cancel().catch(() => { });
        if (reservation && !reservation.isCommitted && !reservation.isReleased) {
            reservation.release();
        }
        throw error;
    }
    finally {
        chunks.length = 0;
        if (bodyMemoryProbe) {
            bodyMemoryProbe.afterChunkRefsReleased = captureBodyMemorySnapshot();
            emitBodyMemoryProbe(bodyMemoryProbe);
        }
        try {
            reader.releaseLock();
        }
        catch {
            // A non-cooperative stream can still have its cancelled read settling.
            // It no longer owns importer permits/reservations at this point.
        }
    }
}
