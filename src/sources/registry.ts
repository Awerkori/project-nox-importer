import { SourceAdapter } from './types.js';
import { NexusAdapter } from './nexus/nexus-adapter.js';
import { MangaFlixAdapter } from './mangaflix/mangaflix-adapter.js';
import { ManhastroAdapter } from './manhastro/manhastro-adapter.js';
import { ToonLivreAdapter } from './toonlivre/toonlivre-adapter.js';
import { KuroAdapter } from './kuro/kuro-adapter.js';
import { HostRateLimiter } from '../core/rate-limiter.js';

export class SourceRegistry {
  private adapters = new Map<string, SourceAdapter>();

  constructor(rateLimiter: HostRateLimiter) {
    // Register all supported adapters
    this.register(new NexusAdapter(rateLimiter));
    this.register(new MangaFlixAdapter(rateLimiter));
    this.register(new ManhastroAdapter(rateLimiter));
    this.register(new ToonLivreAdapter(rateLimiter));
    this.register(new KuroAdapter(rateLimiter));
  }

  register(adapter: SourceAdapter): void {
    this.adapters.set(adapter.id, adapter);
  }

  get(id: string): SourceAdapter | undefined {
    return this.adapters.get(id);
  }

  getAll(): SourceAdapter[] {
    return Array.from(this.adapters.values());
  }
}
