import { describe, it, expect } from 'vitest';
import { SourceRegistry } from '../src/sources/registry.js';
import { HostRateLimiter } from '../src/core/rate-limiter.js';
import { TaimuMangasAdapter } from '../src/sources/taimumangas/taimumangas-adapter.js';
import { EuphoriaScanAdapter } from '../src/sources/euphoriascan/euphoriascan-adapter.js';
import { FleurBlancheAdapter } from '../src/sources/fleurblanche/fleurblanche-adapter.js';
import { LittleTyrantAdapter } from '../src/sources/littletyrant/littletyrant-adapter.js';
import { MangaLivreToAdapter } from '../src/sources/mangalivreto/mangalivreto-adapter.js';
import { MonteTaiAdapter } from '../src/sources/montetai/montetai-adapter.js';
import { NebulosaScanAdapter } from '../src/sources/nebulosascan/nebulosascan-adapter.js';
import { NocturneSummerAdapter } from '../src/sources/nocturnesummer/nocturnesummer-adapter.js';
import { TankouHentaiAdapter } from '../src/sources/tankouhentai/tankouhentai-adapter.js';
import { CafeComYaoiAdapter } from '../src/sources/cafecomyaoi/cafecomyaoi-adapter.js';
import { SOURCE_CONCURRENCY_LIMITS } from '../src/core/concurrency.js';

describe('New PT-BR Sources Registration & Concurrency', () => {
  it('registers all 10 new adapters into SourceRegistry', () => {
    const rateLimiter = new HostRateLimiter(2.0);
    const registry = new SourceRegistry(rateLimiter);

    const expectedNewSources = [
      'taimumangas',
      'euphoriascan',
      'fleurblanche',
      'littletyrant',
      'mangalivreto',
      'montetai',
      'nebulosascan',
      'nocturnesummer',
      'tankouhentai',
      'cafecomyaoi',
    ];

    for (const sourceId of expectedNewSources) {
      const adapter = registry.get(sourceId);
      expect(adapter).toBeDefined();
      expect(adapter?.id).toBe(sourceId);
      expect(adapter?.name).toBeTruthy();
      expect(adapter?.baseUrl).toMatch(/^https:\/\//);
    }
  });

  it('defines valid concurrency limits for all new sources', () => {
    const expectedNewSources = [
      'taimumangas',
      'euphoriascan',
      'fleurblanche',
      'littletyrant',
      'mangalivreto',
      'montetai',
      'nebulosascan',
      'nocturnesummer',
      'tankouhentai',
      'cafecomyaoi',
    ];

    for (const sourceId of expectedNewSources) {
      const limit = SOURCE_CONCURRENCY_LIMITS[sourceId];
      expect(limit).toBeDefined();
      expect(limit.maxChapters).toBeGreaterThanOrEqual(4);
      expect(limit.maxPagesPerChapter).toBeGreaterThanOrEqual(4);
    }
  });

  it('verifies TaimuMangasAdapter metadata and URL formatting', () => {
    const adapter = new TaimuMangasAdapter();
    expect(adapter.id).toBe('taimumangas');
    expect(adapter.baseUrl).toBe('https://beta.taimumangas.com');
  });

  it('verifies Madara-based adapters inherit MadaraAdapter contract', () => {
    const adapters = [
      new EuphoriaScanAdapter(),
      new FleurBlancheAdapter(),
      new LittleTyrantAdapter(),
      new MangaLivreToAdapter(),
      new MonteTaiAdapter(),
      new NebulosaScanAdapter(),
      new NocturneSummerAdapter(),
      new TankouHentaiAdapter(),
      new CafeComYaoiAdapter(),
    ];

    for (const a of adapters) {
      expect(a.mangaSubString).toBe('manga');
      expect(typeof a.fetchUpdatedWorks).toBe('function');
      expect(typeof a.fetchWorkDetails).toBe('function');
      expect(typeof a.fetchChapters).toBe('function');
      expect(typeof a.fetchChapterPages).toBe('function');
      expect(typeof a.searchWorks).toBe('function');
    }
  });
});
