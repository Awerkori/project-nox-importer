import { describe, it, expect } from 'vitest';
import { NexusToonsAdapter } from '../src/sources/nexustoons/nexustoons-adapter.js';
import { HostRateLimiter } from '../src/core/rate-limiter.js';
import { SourceRegistry } from '../src/sources/registry.js';
import { decryptNexusToonsPayload, isEncryptedNexusToons } from '../src/sources/nexustoons/nexustoons-decryptor.js';

describe('NexusToonsAdapter', () => {
  it('decrypts encrypted Orion payload matching Tachiyomi NexusDecrypt spec', () => {
    // Encrypted payload sample with v=2, k=4
    const encrypted = {
      d: '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',
      k: 4,
      v: 2,
    };

    expect(isEncryptedNexusToons(encrypted)).toBe(true);
    const decrypted = decryptNexusToonsPayload(encrypted);
    expect(decrypted.title).toBe('Solo Leveling');
    expect(decrypted.slug).toBe('solo-leveling');
    expect(decrypted.status).toBe('completed');
  });

  it('fetches updated works list with proper pagination and mapping', async () => {
    const mockTransport: typeof fetch = async (url) => {
      const u = url.toString();
      if (u.includes('/api/mangas?page=1')) {
        return {
          ok: true,
          status: 200,
          json: async () => ({
            page: 1,
            pages: 5,
            data: [
              {
                id: 101,
                slug: 'martial-god',
                title: 'Martial God',
                coverImage: 'https://img.nx-toons.xyz/covers/mg.webp',
                lastChapterAt: '2026-09-10T00:00:00Z',
              },
            ],
          }),
        } as any;
      }
      return { ok: false, status: 404 } as any;
    };

    const adapter = new NexusToonsAdapter(new HostRateLimiter(100), mockTransport);
    const res = await adapter.fetchUpdatedWorks('1');

    expect(res.works.length).toBe(1);
    expect(res.works[0].sourceWorkId).toBe('martial-god');
    expect(res.works[0].title).toBe('Martial God');
    expect(res.works[0].coverUrl).toBe('https://img.nx-toons.xyz/covers/mg.webp');
    expect(res.nextCursor).toBe('2');
  });

  it('fetches work details and correctly normalizes status, kind, and genres', async () => {
    const mockTransport: typeof fetch = async (url) => {
      const u = url.toString();
      if (u.includes('/api/manga/martial-god')) {
        return {
          ok: true,
          status: 200,
          json: async () => ({
            id: 101,
            slug: 'martial-god',
            title: 'Martial God',
            description: 'Journey to the martial pinnacle',
            author: 'Author San',
            artist: 'Artist Kun',
            status: 'ongoing',
            type: 'manhua',
            releaseYear: 2023,
            categories: [
              { name: 'Ação' },
              { category: { name: 'Cultivo' } },
            ],
            alternativeTitles: 'Martial Peak, God of War',
          }),
        } as any;
      }
      return { ok: false, status: 404 } as any;
    };

    const adapter = new NexusToonsAdapter(new HostRateLimiter(100), mockTransport);
    const details = await adapter.fetchWorkDetails('martial-god');

    expect(details.title).toBe('Martial God');
    expect(details.kind).toBe('MANHUA');
    expect(details.status).toBe('ONGOING');
    expect(details.genres).toEqual(['Ação', 'Cultivo']);
    expect(details.alternativeTitles).toEqual(['Martial Peak', 'God of War']);
    expect(details.year).toBe(2023);
  });

  it('fetches chapter list sorted in ascending order', async () => {
    const mockTransport: typeof fetch = async (url) => {
      const u = url.toString();
      if (u.includes('/api/manga/martial-god')) {
        return {
          ok: true,
          status: 200,
          json: async () => ({
            chapters: [
              { id: 202, number: '2.0', title: 'Chapter 2', createdAt: '2026-01-02' },
              { id: 201, number: 1, title: 'Chapter 1', createdAt: '2026-01-01' },
              { id: 203, number: '2.5', title: '', createdAt: '2026-01-03' },
            ],
          }),
        } as any;
      }
      return { ok: false, status: 404 } as any;
    };

    const adapter = new NexusToonsAdapter(new HostRateLimiter(100), mockTransport);
    const chapters = await adapter.fetchChapters('martial-god');

    expect(chapters.length).toBe(3);
    expect(chapters[0].number).toBe(1);
    expect(chapters[0].sourceChapterId).toBe('201');
    expect(chapters[1].number).toBe(2);
    expect(chapters[2].number).toBe(2.5);
  });

  it('fetches chapter pages resolving direct image URLs', async () => {
    const mockTransport: typeof fetch = async (url) => {
      const u = url.toString();
      if (u.includes('/api/read/201')) {
        return {
          ok: true,
          status: 200,
          json: async () => ({
            pages: [
              { pageNumber: 1, imageUrl: 'https://img.nx-toons.xyz/manga_pages/60/201/page_1.webp' },
              { pageNumber: 2, imageUrl: 'https://img.nx-toons.xyz/manga_pages/60/201/page_2.webp' },
            ],
          }),
        } as any;
      }
      return { ok: false, status: 404 } as any;
    };

    const adapter = new NexusToonsAdapter(new HostRateLimiter(100), mockTransport);
    const pages = await adapter.fetchChapterPages('201');

    expect(pages).toEqual([
      'https://img.nx-toons.xyz/manga_pages/60/201/page_1.webp',
      'https://img.nx-toons.xyz/manga_pages/60/201/page_2.webp',
    ]);
  });

  it('fetches chapter pages falling back to pageToken when imageUrl is absent', async () => {
    const mockTransport: typeof fetch = async (url) => {
      const u = url.toString();
      if (u.includes('/api/read/202')) {
        return {
          ok: true,
          status: 200,
          json: async () => ({
            pageToken: 'token_abc123',
            pages: [
              { pageNumber: 0 },
              { pageNumber: 1 },
            ],
          }),
        } as any;
      }
      return { ok: false, status: 404 } as any;
    };

    const adapter = new NexusToonsAdapter(new HostRateLimiter(100), mockTransport);
    const pages = await adapter.fetchChapterPages('202');

    expect(pages).toEqual([
      'https://nexustoons.com/api/p/token_abc123/0',
      'https://nexustoons.com/api/p/token_abc123/1',
    ]);
  });

  it('proves clear differentiation between Nexus Mangas and Nexus Toons in SourceRegistry', () => {
    const registry = new SourceRegistry(new HostRateLimiter(100));

    const nexusMangas = registry.get('nexus');
    const nexusToons = registry.get('nexus_toons');

    expect(nexusMangas).toBeDefined();
    expect(nexusToons).toBeDefined();

    // Distinct identity
    expect(nexusMangas!.id).toBe('nexus');
    expect(nexusMangas!.name).toBe('Nexus Mangas');
    expect(nexusMangas!.baseUrl).toBe('https://www.nexusmangas.com');

    expect(nexusToons!.id).toBe('nexus_toons');
    expect(nexusToons!.name).toBe('Nexus Toons');
    expect(nexusToons!.baseUrl).toBe('https://nexustoons.com');

    // Aliases
    expect(registry.get('nexus_mangas')).toBe(nexusMangas);
    expect(registry.get('nexustoons')).toBe(nexusToons);

    // Total 6 unique sources registered
    const all = registry.getAll();
    expect(all.length).toBe(6);
    expect(all.map((a) => a.id).sort()).toEqual([
      'kuro',
      'mangaflix',
      'mangotoons',
      'manhastro',
      'nexus',
      'nexus_toons',
    ]);
  });
});
