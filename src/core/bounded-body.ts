import type { BufferReservation } from './concurrency.js';
import { InvalidMediaError } from './retry-policy.js';

export interface ReadImageBodyOptions {
  maxBytes?: number;
  reservation?: BufferReservation;
  signal?: AbortSignal;
}

// Match the existing media validator's size limit before buffering the response.
// Supports streaming reservation upgrade to enforce memory backpressure for chunked streams.
export async function readImageBody(
  response: Response,
  optionsOrMaxBytes?: number | ReadImageBodyOptions
): Promise<Uint8Array> {
  const options: ReadImageBodyOptions =
    typeof optionsOrMaxBytes === 'number'
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
        await response.body?.cancel().catch(() => {});
        if (reservation && !reservation.isCommitted && !reservation.isReleased) {
          reservation.release();
        }
        throw new InvalidMediaError(
          url,
          'media',
          `Image declared content-length (${Math.round(declared / 1024 / 1024)}MB) exceeds maximum safe limit of ${Math.round(maxBytes / 1024 / 1024)}MB`
        );
      }
      if (reservation && declared > reservation.reservedBytes) {
        await reservation.upgrade(declared, signal);
      }
    }
  }

  if (!response.body) throw new Error('Image response has no body');
  const reader = response.body.getReader();
  const chunks: Uint8Array[] = [];
  let length = 0;

  try {
    while (true) {
      signal?.throwIfAborted();
      const { value, done } = await reader.read();
      if (done) break;

      const newLength = length + value.byteLength;
      if (newLength > maxBytes) {
        await reader.cancel().catch(() => {});
        if (reservation && !reservation.isCommitted && !reservation.isReleased) {
          reservation.release();
        }
        throw new InvalidMediaError(
          url,
          'media',
          `Image exceeds media byte limit of ${Math.round(maxBytes / 1024 / 1024)}MB`
        );
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
  } catch (error) {
    await reader.cancel().catch(() => {});
    if (reservation && !reservation.isCommitted && !reservation.isReleased) {
      reservation.release();
    }
    throw error;
  } finally {
    chunks.length = 0;
    reader.releaseLock();
  }
}
