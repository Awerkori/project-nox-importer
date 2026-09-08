import { describe, it, expect, vi } from 'vitest';
import { NoxWorkerStorageProvider, NoxWorkerStorageError } from '../src/storage/worker.js';

describe('NoxWorkerStorageProvider', () => {
  const baseUrl = 'https://manga-mock.workers.dev';
  const token = 'test-bridge-token-xyz';

  it('reports provider key as telegram for manga platform compatibility', () => {
    const provider = new NoxWorkerStorageProvider(baseUrl, token);
    expect(provider.getProviderKey()).toBe('telegram');
  });

  it('performs successful health check with valid Bearer token', async () => {
    const mockTransport = vi.fn().mockResolvedValue({
      ok: true,
      json: async () => ({ ok: true, provider: 'telegram' }),
    });

    const provider = new NoxWorkerStorageProvider(baseUrl, token, mockTransport as any);
    const result = await provider.healthCheck();

    expect(result).toBe(true);
    expect(mockTransport).toHaveBeenCalledWith(
      'https://manga-mock.workers.dev/api/internal/storage/upload',
      expect.objectContaining({
        method: 'GET',
        headers: expect.objectContaining({
          Authorization: `Bearer ${token}`,
        }),
      })
    );
  });

  it('returns false on healthCheck when response is not ok', async () => {
    const mockTransport = vi.fn().mockResolvedValue({
      ok: false,
      status: 401,
    });

    const provider = new NoxWorkerStorageProvider(baseUrl, token, mockTransport as any);
    const result = await provider.healthCheck();

    expect(result).toBe(false);
  });

  it('uploads binary image and returns providerKey', async () => {
    const mockTransport = vi.fn().mockResolvedValue({
      ok: true,
      status: 200,
      json: async () => ({
        providerKey: 'BQACAgEAAyEGAAMBA4O4cgAD...',
        mime: 'image/webp',
        width: 800,
        height: 1200,
        bytes: 12345,
      }),
    });

    const provider = new NoxWorkerStorageProvider(baseUrl, token, mockTransport as any);
    const bytes = new Uint8Array([1, 2, 3, 4, 5]);
    const fileId = await provider.upload(bytes, 'image/webp', '00000000-0000-0000-0000-000000000001');

    expect(fileId).toBe('BQACAgEAAyEGAAMBA4O4cgAD...');
    expect(mockTransport).toHaveBeenCalledWith(
      'https://manga-mock.workers.dev/api/internal/storage/upload?id=00000000-0000-0000-0000-000000000001',
      expect.objectContaining({
        method: 'POST',
        headers: expect.objectContaining({
          Authorization: `Bearer ${token}`,
          'Content-Type': 'image/webp',
          'Content-Length': '5',
        }),
      })
    );
  });

  it('throws NoxWorkerStorageError with auth stage on 401', async () => {
    const mockTransport = vi.fn().mockResolvedValue({
      ok: false,
      status: 401,
      text: async () => 'Unauthorized',
    });

    const provider = new NoxWorkerStorageProvider(baseUrl, token, mockTransport as any);
    const bytes = new Uint8Array([1, 2, 3]);

    await expect(provider.upload(bytes, 'image/png', '00000000-0000-0000-0000-000000000002'))
      .rejects.toThrow(NoxWorkerStorageError);
  });
});
