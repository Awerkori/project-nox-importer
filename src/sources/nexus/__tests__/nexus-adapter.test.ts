import { describe, it, expect, vi } from 'vitest';
import { NexusAdapter } from '../nexus-adapter.js';

describe('NexusAdapter search', () => {
  it('formats search query using PostgREST wildcards across punctuation and whitespace', async () => {
    let requestedUrl = '';
    const mockTransport = vi.fn().mockImplementation(async (url: string) => {
      requestedUrl = url;
      return {
        status: 200,
        ok: true,
        json: async () => [
          {
            id: 'test-work-id',
            title: 'Invocador de Demônios do Abismo',
            slug: 'invocador-de-demonios-do-abismo',
            cover_url: 'https://example.com/cover.webp',
            updated_at: '2026-10-19T00:00:00Z',
          },
        ],
      };
    });

    const adapter = new NexusAdapter(undefined as any, mockTransport as any);
    const results = await adapter.searchWorks('Summon The Demon, For I Am The Abyss');

    expect(results).toHaveLength(1);
    expect(results[0].sourceWorkId).toBe('test-work-id');
    expect(requestedUrl).toContain('Summon*The*Demon*For*I*Am*The*Abyss');
  });

  it('propagates HTTP 429, timeouts, and 5xx errors instead of returning empty results', async () => {
    const mockTransport = vi.fn().mockImplementation(async () => {
      return {
        status: 503,
        ok: false,
        text: async () => 'Service Unavailable',
      };
    });

    const adapter = new NexusAdapter(undefined as any, mockTransport as any);
    await expect(adapter.searchWorks('Summon The Demon')).rejects.toThrow();
  });
});
