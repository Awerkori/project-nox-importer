import { Logger } from './logger.js';
import { InvalidMediaError } from './retry-policy.js';
import { performance } from 'node:perf_hooks';
export const MAX_IMAGE_BODY_BYTES = 20 * 1024 * 1024;
// Some image CDNs (including MangaFlix's current CDN) omit Content-Length
// and stream thousands of 4 KiB chunks. Retaining every chunk until the final
// contiguous body is built leaves hundreds of tiny native ArrayBuffers per
// page behind in the allocator. Fixed-size blocks keep the same bounded byte
// accounting and final body contract while making the transient allocation
// count proportional to image size rather than network chunking.
const LENGTHLESS_IMAGE_BLOCK_BYTES = 256 * 1024;
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
    const maxBytes = options.maxBytes ?? MAX_IMAGE_BODY_BYTES;
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
                try {
                    await reservation.upgrade(declared, signal, { intent: 'COMPLETION' });
                }
                catch (error) {
                    await response.body?.cancel().catch(() => { });
                    if (!reservation.isCommitted && !reservation.isReleased)
                        reservation.release();
                    throw error;
                }
            }
            else if (reservation && declared < reservation.reservedBytes) {
                // Lite admits lengthless sources before taking a global request slot.
                // Once a trustworthy header arrives, return the unused conservative
                // prefix immediately; a dishonest short declaration still follows the
                // existing bounded promotion path below.
                reservation.shrink?.(declared);
            }
        }
    }
    if (!response.body)
        throw new Error('Image response has no body');
    const reader = response.body.getReader();
    // Most chapter-image CDNs give us a trustworthy content length.  In that
    // case fill the final body as bytes arrive instead of retaining every stream
    // chunk and allocating a second contiguous body at completion.  The page
    // reservation has already been upgraded to this exact size above, so this
    // does not weaken the memory accounting; it removes the transient second
    // live copy which was the previous RSS high-water source.
    let directBody = contentLengthBytes === null ? null : new Uint8Array(contentLengthBytes);
    // Declared-size responses use `directBody`. Lengthless responses assemble
    // into bounded blocks, then make the one unavoidable contiguous copy that
    // inspection and storage require. Do not preallocate MAX_IMAGE_BODY_BYTES:
    // most real MangaFlix pages are far smaller and the reservation remains the
    // authority for their terminal-size safety.
    const lengthlessBlocks = [];
    let lengthlessBlockOffset = 0;
    let length = 0;
    let chunkCount = 0;
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
            chunkCount++;
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
                const completionPromotion = contentLengthBytes === null || newLength > contentLengthBytes;
                // Lengthless responses (e.g. MangaFlix) omit Content-Length even though 99.6%
                // of real pages are <= 4 MiB. Promote lengthless streams first to a bounded
                // 4 MiB ceiling so two slow lengthless pages never lock 40 MiB of PageByteBudget,
                // escalating to maxBytes (20 MiB) only if a page exceeds 4 MiB or Content-Length lied.
                const lengthlessMediumBytes = Math.min(maxBytes, 4 * 1024 * 1024);
                const targetBytes = completionPromotion
                    ? (contentLengthBytes === null && newLength <= lengthlessMediumBytes ? lengthlessMediumBytes : maxBytes)
                    : newLength;
                const phase = completionPromotion ? 'COMPLETION' : 'STREAM';
                waitingOnReservationUpgrade = true;
                emitTrace(() => options.trace?.onReservationUpgrade?.({
                    phase, state: 'STARTED', fromBytes, toBytes: targetBytes,
                    elapsedMs: Math.round((performance.now() - bodyStartedAt) * 1000) / 1000,
                }));
                await reservation.upgrade(targetBytes, signal, completionPromotion ? { intent: 'COMPLETION' } : undefined);
                waitingOnReservationUpgrade = false;
                emitTrace(() => options.trace?.onReservationUpgrade?.({
                    phase, state: 'COMPLETED', fromBytes, toBytes: reservation.reservedBytes,
                    elapsedMs: Math.round((performance.now() - bodyStartedAt) * 1000) / 1000,
                }));
            }
            if (directBody) {
                // A misreported Content-Length must retain the existing safe behavior:
                // reserve completion headroom first, then continue into a bounded
                // maximum-size body.  This is exceptional; correct declarations keep
                // the one-buffer fast path above.
                if (newLength > directBody.byteLength) {
                    const expanded = new Uint8Array(maxBytes);
                    expanded.set(directBody.subarray(0, length));
                    directBody = expanded;
                }
                directBody.set(value, length);
            }
            else {
                let valueOffset = 0;
                while (valueOffset < value.byteLength) {
                    let block = lengthlessBlocks[lengthlessBlocks.length - 1];
                    if (!block || lengthlessBlockOffset >= block.byteLength) {
                        const remainingBodyCapacity = maxBytes - (length + valueOffset);
                        block = new Uint8Array(Math.min(LENGTHLESS_IMAGE_BLOCK_BYTES, remainingBodyCapacity));
                        lengthlessBlocks.push(block);
                        lengthlessBlockOffset = 0;
                    }
                    const copied = Math.min(block.byteLength - lengthlessBlockOffset, value.byteLength - valueOffset);
                    block.set(value.subarray(valueOffset, valueOffset + copied), lengthlessBlockOffset);
                    lengthlessBlockOffset += copied;
                    valueOffset += copied;
                }
            }
            length = newLength;
        }
        if (directBody) {
            // A short body after a declared length is malformed but historically
            // accepted. Return an exact-size copy only for that exceptional case so
            // the normal path never retains a too-large backing buffer.
            if (length === directBody.byteLength) {
                emitTrace(() => options.trace?.onComplete?.({
                    bytes: length,
                    chunks: chunkCount,
                    elapsedMs: Math.round((performance.now() - bodyStartedAt) * 1000) / 1000,
                }));
                return directBody;
            }
            const result = directBody.slice(0, length);
            emitTrace(() => options.trace?.onComplete?.({
                bytes: length,
                chunks: chunkCount,
                elapsedMs: Math.round((performance.now() - bodyStartedAt) * 1000) / 1000,
            }));
            return result;
        }
        bodyMemoryProbe = maybeStartBodyMemoryProbe(contentLengthBytes, chunkCount, length);
        const concatStartedAt = bodyMemoryProbe ? performance.now() : 0;
        const result = new Uint8Array(length);
        if (bodyMemoryProbe)
            bodyMemoryProbe.afterAllocation = captureBodyMemorySnapshot();
        let offset = 0;
        for (const block of lengthlessBlocks) {
            const bytes = Math.min(block.byteLength, length - offset);
            if (bytes <= 0)
                break;
            result.set(block.subarray(0, bytes), offset);
            offset += bytes;
        }
        if (bodyMemoryProbe) {
            bodyMemoryProbe.concatDurationMs = Math.round((performance.now() - concatStartedAt) * 1000) / 1000;
            bodyMemoryProbe.afterCopy = captureBodyMemorySnapshot();
        }
        emitTrace(() => options.trace?.onComplete?.({
            bytes: length,
            chunks: chunkCount,
            elapsedMs: Math.round((performance.now() - bodyStartedAt) * 1000) / 1000,
        }));
        return result;
    }
    catch (error) {
        emitTrace(() => options.trace?.onError?.({
            bytes: length,
            chunks: chunkCount,
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
        lengthlessBlocks.length = 0;
        lengthlessBlockOffset = 0;
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
