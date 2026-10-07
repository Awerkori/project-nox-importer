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
});
