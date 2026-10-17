import { afterEach, describe, expect, it, vi } from 'vitest';
import { KuroAdapter } from '../kuro-adapter.js';

afterEach(() => vi.unstubAllEnvs());

describe('KuroAdapter bridge session recovery', () => {
  it('renews a cached session once when the bridge reports a Cloudflare challenge', async () => {
    vi.stubEnv('NOX_MANGA_URL', 'https://bridge.test');
    vi.stubEnv('NOX_STORAGE_BRIDGE_TOKEN', 'test-token');
    vi.stubEnv('KURO_EMAIL', 'importer@example.test');
    vi.stubEnv('KURO_PASSWORD', 'test-password');
    vi.stubEnv('KURO_SESSION', 'stale-session');
    vi.stubEnv('KURO_CLIENT_TOKEN', 'stale-context');

    let detailRequests = 0;
    let loginRequests = 0;
    const adapter = new KuroAdapter(undefined, async (_url, init) => {
      const target = JSON.parse(String(init?.body || '{}')).url || '';
      if (target.endsWith('/auth/login')) {
        loginRequests++;
        return Response.json({ status: 200, cookies: ['kuro_session=fresh-session', '_vx=fresh-context'] });
      }
      if (target.endsWith('/mangas/101')) {
        detailRequests++;
        if (detailRequests === 1) return Response.json({ status: 403, text: 'Just a moment...' });
        return Response.json({ status: 200, data: { manga: { id: 101, title: 'Recovered Work', genres: [], alternative_titles: [] } } });
      }
      throw new Error(`Unexpected bridge target: ${target}`);
    });

    await expect(adapter.fetchWorkDetails('101')).resolves.toMatchObject({ title: 'Recovered Work' });
    expect(loginRequests).toBe(1);
    expect(detailRequests).toBe(2);
  });

  it('retries login after a challenge when the initial cold-start login did not establish a session', async () => {
    vi.stubEnv('NOX_MANGA_URL', 'https://bridge.test');
    vi.stubEnv('NOX_STORAGE_BRIDGE_TOKEN', 'test-token');
    vi.stubEnv('KURO_EMAIL', 'importer@example.test');
    vi.stubEnv('KURO_PASSWORD', 'test-password');

    let bridgeLoginRequests = 0;
    let detailRequests = 0;
    const adapter = new KuroAdapter(undefined, async (url, init) => {
      const target = JSON.parse(String(init?.body || '{}')).url || '';
      if (target.endsWith('/auth/login')) {
        bridgeLoginRequests++;
        if (bridgeLoginRequests === 1) return new Response('temporary bridge failure', { status: 503 });
        return Response.json({ status: 200, cookies: ['kuro_session=fresh-session', '_vx=fresh-context'] });
      }
      if (String(url).endsWith('/auth/login')) return new Response('temporary direct failure', { status: 503 });
      if (target.endsWith('/mangas/101')) {
        detailRequests++;
        if (detailRequests === 1) return Response.json({ status: 403, text: 'Just a moment...' });
        return Response.json({ status: 200, data: { manga: { id: 101, title: 'Recovered Work', genres: [], alternative_titles: [] } } });
      }
      throw new Error(`Unexpected Kuro request: ${String(url)}`);
    });

    await expect(adapter.fetchWorkDetails('101')).resolves.toMatchObject({ title: 'Recovered Work' });
    expect(bridgeLoginRequests).toBe(2);
    expect(detailRequests).toBe(2);
  });
});
