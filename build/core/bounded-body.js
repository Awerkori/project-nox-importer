import { InvalidMediaError } from './retry-policy.js';
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
    if (declaredStr) {
        const declared = parseInt(declaredStr, 10);
        if (!Number.isNaN(declared) && declared > 0) {
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
    try {
        while (true) {
            signal?.throwIfAborted();
            const { value, done } = await readChunk(reader, signal);
            if (done)
                break;
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
                await reservation.upgrade(newLength, signal);
            }
            length = newLength;
            chunks.push(value);
        }
        const result = new Uint8Array(length);
        let offset = 0;
        for (const chunk of chunks) {
            result.set(chunk, offset);
            offset += chunk.byteLength;
        }
        return result;
    }
    catch (error) {
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
        try {
            reader.releaseLock();
        }
        catch {
            // A non-cooperative stream can still have its cancelled read settling.
            // It no longer owns importer permits/reservations at this point.
        }
    }
}
