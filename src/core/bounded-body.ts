import type { BufferReservation } from './concurrency.js';
import { Logger } from './logger.js';
import { InvalidMediaError } from './retry-policy.js';
import { performance } from 'node:perf_hooks';

export interface ReadImageBodyOptions {
  maxBytes?: number;
  reservation?: BufferReservation;
  signal?: AbortSignal;
  /**
   * Optional, opt-in lifecycle hooks used by the bounded request trace. They
   * receive counters only; no image data is retained or copied for tracing.
   */
  trace?: {
    onFirstChunk?: (metrics: { bytes: number; elapsedMs: number }) => void;
    onComplete?: (metrics: { bytes: number; chunks: number; elapsedMs: number }) => void;
    onReservationUpgrade?: (metrics: {
      phase: 'HEADER' | 'STREAM' | 'COMPLETION';
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

export const MAX_IMAGE_BODY_BYTES = 20 * 1024 * 1024;

function emitTrace(callback: (() => void) | undefined): void {
  try {
    callback?.();
  } catch {
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

type BodyMemorySnapshot = {
  rssBytes: number;
  externalBytes: number;
  arrayBuffersBytes: number;
};

type BodyMemoryProbe = {
  contentLengthBytes: number | null;
  chunkCount: number;
  chunkBytes: number;
  finalBytes: number;
  beforeConcat: BodyMemorySnapshot;
  afterAllocation?: BodyMemorySnapshot;
  afterCopy?: BodyMemorySnapshot;
  afterChunkRefsReleased?: BodyMemorySnapshot;
  concatDurationMs?: number;
};

function captureBodyMemorySnapshot(): BodyMemorySnapshot {
  const memory = process.memoryUsage();
  return {
    rssBytes: memory.rss,
    externalBytes: memory.external,
    arrayBuffersBytes: memory.arrayBuffers || 0,
  };
}

function maybeStartBodyMemoryProbe(
  contentLengthBytes: number | null,
  chunkCount: number,
  chunkBytes: number,
): BodyMemoryProbe | null {
  if (!bodyMemoryProbeEnabled || bodyMemoryProbeConsumed) return null;
  bodyMemoryProbeConsumed = true;
  return {
    contentLengthBytes,
    chunkCount,
    chunkBytes,
    finalBytes: chunkBytes,
    beforeConcat: captureBodyMemorySnapshot(),
  };
}

function emitBodyMemoryProbe(probe: BodyMemoryProbe): void {
  const snapshots = [
    probe.beforeConcat,
    probe.afterAllocation,
    probe.afterCopy,
    probe.afterChunkRefsReleased,
  ].filter((snapshot): snapshot is BodyMemorySnapshot => Boolean(snapshot));
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

function abortError(signal: AbortSignal): Error {
  const reason = signal.reason;
  if (reason instanceof Error) return reason;
  return new DOMException('Image response body read was aborted', 'AbortError');
}

/**
 * A fetch timeout only protects the response headers unless it is also raced
 * against body reads. Some upstream CDNs send headers/content-length and then
 * stop delivering bytes; leaving reader.read() pending would retain both the
 * download permit and its buffer reservation forever.
 */
async function readChunk(
  reader: ReadableStreamDefaultReader<Uint8Array>,
  signal?: AbortSignal,
): Promise<ReadableStreamReadResult<Uint8Array>> {
  if (!signal) return reader.read();
  signal.throwIfAborted();

  return new Promise<ReadableStreamReadResult<Uint8Array>>((resolve, reject) => {
    const onAbort = () => {
      // Do not await cancellation here: a broken upstream stream must not be
      // able to keep the importer slot/buffer hostage while cancel settles.
      void reader.cancel(signal.reason).catch(() => {});
      reject(abortError(signal));
    };
    signal.addEventListener('abort', onAbort, { once: true });
    reader.read().then(
      (result) => {
        signal.removeEventListener('abort', onAbort);
        resolve(result);
      },
      (error) => {
        signal.removeEventListener('abort', onAbort);
        reject(error);
      },
    );
  });
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

  const maxBytes = options.maxBytes ?? MAX_IMAGE_BODY_BYTES;
  const reservation = options.reservation;
  const signal = options.signal;
  const url = response.url || 'unknown';

  const declaredStr = response.headers.get('content-length');
  let contentLengthBytes: number | null = null;
  if (declaredStr) {
    const declared = parseInt(declaredStr, 10);
    if (!Number.isNaN(declared) && declared > 0) {
      contentLengthBytes = declared;
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
        try {
          await reservation.upgrade(declared, signal, { intent: 'COMPLETION' });
        } catch (error) {
          await response.body?.cancel().catch(() => {});
          if (!reservation.isCommitted && !reservation.isReleased) reservation.release();
          throw error;
        }
      }
    }
  }

  if (!response.body) throw new Error('Image response has no body');

  // With no usable final length, incremental growth can let several readers
  // fill the entire byte budget and then wait on one another forever. Promote
  // one reader to a bounded completion reservation before its body advances.
  // The autotuner accounts this in reservedBufferedBytes; it never borrows
  // beyond maxBufferedBytes.
  if (reservation && contentLengthBytes === null && maxBytes > reservation.reservedBytes) {
    const fromBytes = reservation.reservedBytes;
    emitTrace(() => options.trace?.onReservationUpgrade?.({
      phase: 'COMPLETION', state: 'STARTED', fromBytes, toBytes: maxBytes, elapsedMs: 0,
    }));
    try {
      await reservation.upgrade(maxBytes, signal, { intent: 'COMPLETION' });
    } catch (error) {
      await response.body.cancel().catch(() => {});
      if (!reservation.isCommitted && !reservation.isReleased) reservation.release();
      throw error;
    }
    emitTrace(() => options.trace?.onReservationUpgrade?.({
      phase: 'COMPLETION', state: 'COMPLETED', fromBytes, toBytes: reservation.reservedBytes, elapsedMs: 0,
    }));
  }

  const reader = response.body.getReader();
  const chunks: Uint8Array[] = [];
  let length = 0;
  let firstChunkSeen = false;
  const bodyStartedAt = performance.now();
  let waitingOnReservationUpgrade = false;
  let bodyMemoryProbe: BodyMemoryProbe | null = null;

  try {
    while (true) {
      signal?.throwIfAborted();
      const { value, done } = await readChunk(reader, signal);
      if (done) break;

      if (!firstChunkSeen) {
        firstChunkSeen = true;
        emitTrace(() => options.trace?.onFirstChunk?.({
          bytes: value.byteLength,
          elapsedMs: Math.round((performance.now() - bodyStartedAt) * 1000) / 1000,
        }));
      }

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
        const fromBytes = reservation.reservedBytes;
        const completionPromotion = contentLengthBytes === null || newLength > contentLengthBytes;
        const targetBytes = completionPromotion ? maxBytes : newLength;
        const phase = completionPromotion ? 'COMPLETION' : 'STREAM';
        waitingOnReservationUpgrade = true;
        emitTrace(() => options.trace?.onReservationUpgrade?.({
          phase, state: 'STARTED', fromBytes, toBytes: targetBytes,
          elapsedMs: Math.round((performance.now() - bodyStartedAt) * 1000) / 1000,
        }));
        await reservation.upgrade(
          targetBytes,
          signal,
          completionPromotion ? { intent: 'COMPLETION' } : undefined,
        );
        waitingOnReservationUpgrade = false;
        emitTrace(() => options.trace?.onReservationUpgrade?.({
          phase, state: 'COMPLETED', fromBytes, toBytes: reservation.reservedBytes,
          elapsedMs: Math.round((performance.now() - bodyStartedAt) * 1000) / 1000,
        }));
      }

      length = newLength;
      chunks.push(value);
    }

    bodyMemoryProbe = maybeStartBodyMemoryProbe(contentLengthBytes, chunks.length, length);
    const concatStartedAt = bodyMemoryProbe ? performance.now() : 0;
    const result = new Uint8Array(length);
    if (bodyMemoryProbe) bodyMemoryProbe.afterAllocation = captureBodyMemorySnapshot();
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
  } catch (error) {
    emitTrace(() => options.trace?.onError?.({
      bytes: length,
      chunks: chunks.length,
      elapsedMs: Math.round((performance.now() - bodyStartedAt) * 1000) / 1000,
      waitingOn: waitingOnReservationUpgrade ? 'RESERVATION_UPGRADE' : null,
      error,
    }));
    // Keep error recovery bounded too. The abort handler above already asked
    // the stream to cancel; a second cancel is best-effort only.
    void reader.cancel().catch(() => {});
    if (reservation && !reservation.isCommitted && !reservation.isReleased) {
      reservation.release();
    }
    throw error;
  } finally {
    chunks.length = 0;
    if (bodyMemoryProbe) {
      bodyMemoryProbe.afterChunkRefsReleased = captureBodyMemorySnapshot();
      emitBodyMemoryProbe(bodyMemoryProbe);
    }
    try {
      reader.releaseLock();
    } catch {
      // A non-cooperative stream can still have its cancelled read settling.
      // It no longer owns importer permits/reservations at this point.
    }
  }
}
