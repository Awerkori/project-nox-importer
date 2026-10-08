import { afterEach, describe, expect, it, vi } from 'vitest';
import { ImporterEngine } from '../src/core/engine.js';

function makeAbortableReservation() {
  let signal: AbortSignal | undefined;
  let upgradeStarted!: () => void;
  const started = new Promise<void>((resolve) => { upgradeStarted = resolve; });
  const reservation = {
    reservedBytes: 2 * 1024 * 1024,
    upgrade: vi.fn((_bytes: number, nextSignal?: AbortSignal) => {
      signal = nextSignal;
      upgradeStarted();
      if (!nextSignal) return Promise.reject(new Error('missing reservation abort signal'));
      return new Promise<void>((_resolve, reject) => {
        nextSignal.addEventListener('abort', () => reject(nextSignal.reason), { once: true });
      });
    }),
  };
  return { reservation, started, signal: () => signal };
}

function createEngine(config: Record<string, unknown> = {}): any {
  const engine: any = Object.create(ImporterEngine.prototype);
  engine.abortController = new AbortController();
  engine.config = config;
  engine.registry = new Map();
  engine.rateLimiter = { recordSuccess: vi.fn() };
  engine.logger = { warn: vi.fn() };
  return engine;
}

describe('header reservation upgrade cancellation', () => {
  afterEach(() => vi.unstubAllGlobals());

  it('cancels a direct Content-Length upgrade with the job signal', async () => {
    const controller = new AbortController();
    const tracked = makeAbortableReservation();
    vi.stubGlobal('fetch', vi.fn(async () => new Response('body', {
      status: 200,
      headers: { 'content-length': String(3 * 1024 * 1024) },
    })));

    const pending = createEngine().fetchImageBytes('https://cdn.example.test/page.jpg', 'unknown', {
      maxAttempts: 1,
      reservation: tracked.reservation,
      signal: controller.signal,
    });
    await tracked.started;
    expect(tracked.signal()).toBeDefined();

    controller.abort(new Error('job deadline'));
    await expect(pending).rejects.toThrow('job deadline');
  });

  it('traces headers and an aborted reservation upgrade without changing its cancellation path', async () => {
    const controller = new AbortController();
    const tracked = makeAbortableReservation();
    const events: Array<{ event: string; meta?: Record<string, unknown> }> = [];
    vi.stubGlobal('fetch', vi.fn(async () => new Response('body', {
      status: 200,
      headers: { 'content-length': String(3 * 1024 * 1024) },
    })));

    const pending = createEngine().fetchImageBytes('https://cdn.example.test/page.jpg', 'unknown', {
      maxAttempts: 1,
      reservation: tracked.reservation,
      signal: controller.signal,
      requestTrace: {
        jobId: 'job-1', source: 'megahentai', chapterNumber: 1, pageIndex: 0, totalPages: 13, producerAttempt: 1,
        emit: (event: string, meta?: Record<string, unknown>) => events.push({ event, meta }),
      },
    });
    await tracked.started;
    controller.abort(new Error('job deadline'));

    await expect(pending).rejects.toThrow('job deadline');
    expect(events.map(({ event }) => event)).toEqual([
      'DOWNLOAD_REQUEST_STARTED',
      'DOWNLOAD_REQUEST_HEADERS',
      'DOWNLOAD_REQUEST_RESERVATION_UPGRADE_STARTED',
      'DOWNLOAD_REQUEST_FAILED',
    ]);
    expect(events.at(-1)?.meta).toMatchObject({ stage: 'RESERVATION_UPGRADE', abortReason: 'JOB_OR_PIPELINE_ABORT' });
  });

  it('cancels a bridge Content-Length upgrade with the job signal', async () => {
    const controller = new AbortController();
    const tracked = makeAbortableReservation();
    vi.stubGlobal('fetch', vi.fn()
      .mockResolvedValueOnce(new Response('blocked', { status: 403 }))
      .mockResolvedValueOnce(new Response('body', {
        status: 200,
        headers: { 'content-length': String(3 * 1024 * 1024) },
      })));

    const pending = createEngine({ NOX_STORAGE_BRIDGE_TOKEN: 'test-token' }).fetchImageBytes(
      'https://kuromangas.com/page.jpg',
      'kuro',
      {
        maxAttempts: 1,
        reservation: tracked.reservation,
        signal: controller.signal,
      },
    );
    await tracked.started;
    expect(tracked.signal()).toBeDefined();

    controller.abort(new Error('job deadline'));
    await expect(pending).rejects.toThrow('job deadline');
  });

  it('traces the bridge headers, reservation upgrade and body lifecycle', async () => {
    const events: Array<{ event: string; meta?: Record<string, unknown> }> = [];
    const reservation = {
      reservedBytes: 2 * 1024 * 1024,
      upgrade: vi.fn(async function (this: { reservedBytes: number }, bytes: number) {
        this.reservedBytes = bytes;
      }),
    };
    vi.stubGlobal('fetch', vi.fn()
      .mockResolvedValueOnce(new Response('blocked', { status: 403 }))
      .mockResolvedValueOnce(new Response('body', {
        status: 200,
        headers: { 'content-length': String(3 * 1024 * 1024) },
      })));

    const body = await createEngine({ NOX_STORAGE_BRIDGE_TOKEN: 'test-token' }).fetchImageBytes(
      'https://kuromangas.com/page.jpg',
      'kuro',
      {
        maxAttempts: 1,
        reservation: reservation as any,
        requestTrace: {
          jobId: 'job-1', source: 'kuro', chapterNumber: 1, pageIndex: 0, totalPages: 1, producerAttempt: 1,
          emit: (event: string, meta?: Record<string, unknown>) => events.push({ event, meta }),
        },
      },
    );

    expect(body).toEqual(new TextEncoder().encode('body'));
    expect(events.map(({ event }) => event)).toEqual([
      'DOWNLOAD_REQUEST_STARTED',
      'DOWNLOAD_REQUEST_HEADERS',
      'DOWNLOAD_BRIDGE_REQUEST_STARTED',
      'DOWNLOAD_BRIDGE_REQUEST_HEADERS',
      'DOWNLOAD_BRIDGE_RESERVATION_UPGRADE_STARTED',
      'DOWNLOAD_BRIDGE_RESERVATION_UPGRADE_COMPLETED',
      'DOWNLOAD_BRIDGE_REQUEST_FIRST_BODY_CHUNK',
      'DOWNLOAD_BRIDGE_REQUEST_BODY_COMPLETED',
    ]);
    expect(reservation.upgrade).toHaveBeenCalledWith(
      3 * 1024 * 1024,
      expect.any(AbortSignal),
      { intent: 'COMPLETION' },
    );
  });
});
