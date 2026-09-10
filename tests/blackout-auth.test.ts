import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { BlackoutComicsAdapter } from '../src/sources/blackoutcomics/blackoutcomics-adapter.js';
import { HostRateLimiter } from '../src/core/rate-limiter.js';

describe('BlackoutComicsAdapter Authentication & Mutex', () => {
  const originalEnv = process.env;

  beforeEach(() => {
    process.env = {
      ...originalEnv,
      BLACKOUT_EMAIL: 'test@example.com',
      BLACKOUT_PASSWORD: 'testpassword123',
    };
  });

  afterEach(() => {
    process.env = originalEnv;
  });

  it('executes exactly one login request when multiple concurrent callers invoke ensureAuthenticated', async () => {
    let homeFetchCount = 0;
    let loginFetchCount = 0;

    const mockFetch = vi.fn(async (input: any, init?: any) => {
      const url = typeof input === 'string' ? input : input.url;

      if (url === 'https://blackoutcomics.com') {
        homeFetchCount++;
        return new Response('<html><head><meta name="csrf-token" content="mock-csrf-token-123"></head><body></body></html>', {
          status: 200,
          headers: {
            'set-cookie': 'XSRF-TOKEN=csrf123; path=/',
          },
        });
      }

      if (url === 'https://blackoutcomics.com/entrar') {
        loginFetchCount++;
        // Simulate slight network delay
        await new Promise((r) => setTimeout(r, 20));
        return new Response('{"success":true}', {
          status: 200,
          headers: {
            'set-cookie': 'blackout-comics-session=valid-session-xyz; path=/',
          },
        });
      }

      return new Response('Not Found', { status: 404 });
    });

    const rateLimiter = new HostRateLimiter(100);
    const adapter = new BlackoutComicsAdapter(rateLimiter, mockFetch as unknown as typeof fetch);

    // Call ensureAuthenticated 5 times concurrently
    const results = await Promise.all([
      adapter.ensureAuthenticated(),
      adapter.ensureAuthenticated(),
      adapter.ensureAuthenticated(),
      adapter.ensureAuthenticated(),
      adapter.ensureAuthenticated(),
    ]);

    expect(results).toEqual([true, true, true, true, true]);
    expect(homeFetchCount).toBe(1);
    expect(loginFetchCount).toBe(1);

    // Calling again should use cached session and NOT trigger another login
    const cachedResult = await adapter.ensureAuthenticated();
    expect(cachedResult).toBe(true);
    expect(homeFetchCount).toBe(1);
    expect(loginFetchCount).toBe(1);
  });

  it('provides correct image headers with session cookies and referer', async () => {
    const mockFetch = vi.fn(async (input: any) => {
      const url = typeof input === 'string' ? input : input.url;
      if (url === 'https://blackoutcomics.com') {
        return new Response('<html><head><meta name="csrf-token" content="mock-token"></head></html>', {
          status: 200,
          headers: { 'set-cookie': 'XSRF-TOKEN=token123; path=/' },
        });
      }
      if (url === 'https://blackoutcomics.com/entrar') {
        return new Response('{"success":true}', {
          status: 200,
          headers: { 'set-cookie': 'blackout-comics-session=sess456; path=/' },
        });
      }
      return new Response('Not Found', { status: 404 });
    });

    const adapter = new BlackoutComicsAdapter(new HostRateLimiter(100), mockFetch as unknown as typeof fetch);
    await adapter.ensureAuthenticated();

    const headers = adapter.getImageHeaders('https://blackoutcomics.com/image/delivery/test-image.webp');
    expect(headers.Referer).toBe('https://blackoutcomics.com/');
    expect(headers.Cookie).toContain('blackout-comics-session=sess456');
    expect(headers.Cookie).toContain('age_gate_consent=');
  });

  it('gracefully returns false when credentials are missing', async () => {
    delete process.env.BLACKOUT_EMAIL;
    delete process.env.BLACKOUT_PASSWORD;

    const mockFetch = vi.fn();
    const adapter = new BlackoutComicsAdapter(new HostRateLimiter(100), mockFetch as unknown as typeof fetch);

    const result = await adapter.ensureAuthenticated();
    expect(result).toBe(false);
    expect(mockFetch).not.toHaveBeenCalled();
  });
});
