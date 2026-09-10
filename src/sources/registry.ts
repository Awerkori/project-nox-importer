import { SourceAdapter } from './types.js';
import { NexusAdapter } from './nexus/nexus-adapter.js';
import { MangaFlixAdapter } from './mangaflix/mangaflix-adapter.js';
import { ManhastroAdapter } from './manhastro/manhastro-adapter.js';
import { KuroAdapter } from './kuro/kuro-adapter.js';
import { MangoToonsAdapter } from './mangotoons/mangotoons-adapter.js';
import { HanamiHeavenAdapter } from './hanamiheaven/hanamiheaven-adapter.js';
import { HipercoolAdapter } from './hipercool/hipercool-adapter.js';
import { InstaHentaiAdapter } from './instahentai/instahentai-adapter.js';
import { MegaHentaiAdapter } from './megahentai/megahentai-adapter.js';
import { HostRateLimiter } from '../core/rate-limiter.js';

export class SourceRegistry {
  private adapters = new Map<string, SourceAdapter>();

  constructor(rateLimiter?: HostRateLimiter, bridgeToken?: string | null, mangaUrl?: string | null) {
    if (rateLimiter) {
      const bridgeUrl = bridgeToken && mangaUrl ? `${mangaUrl.replace(/\/$/, '')}/api/internal/importer/kuro-bridge` : undefined;
      // Register all supported adapters
      this.register(new NexusAdapter(rateLimiter));
      this.register(new MangaFlixAdapter(rateLimiter));
      this.register(new ManhastroAdapter(rateLimiter));
      this.register(new KuroAdapter(rateLimiter));
      this.register(new MangoToonsAdapter(rateLimiter));

      // Register adult sources (+18 / Adulto / Pornhwa)
      this.register(new HanamiHeavenAdapter(rateLimiter));
      this.register(new HipercoolAdapter(rateLimiter));
      this.register(new InstaHentaiAdapter(rateLimiter));
      this.register(new MegaHentaiAdapter(rateLimiter));
    }
  }

  register(adapter: SourceAdapter): void {
    this.adapters.set(adapter.id, adapter);

    // Register helpful canonical aliases
    if (adapter.id === 'nexus') {
      this.adapters.set('nexus_mangas', adapter);
      this.adapters.set('nexusmangas', adapter);
    }
  }

  clear(): void {
    this.adapters.clear();
  }

  get(id: string): SourceAdapter | undefined {
    return this.adapters.get(id);
  }

  getAll(): SourceAdapter[] {
    return Array.from(new Set(this.adapters.values()));
  }
}
