import { NexusAdapter } from './nexus/nexus-adapter.js';
import { MangaFlixAdapter } from './mangaflix/mangaflix-adapter.js';
import { ManhastroAdapter } from './manhastro/manhastro-adapter.js';
import { KuroAdapter } from './kuro/kuro-adapter.js';
import { MangoToonsAdapter } from './mangotoons/mangotoons-adapter.js';
export class SourceRegistry {
    adapters = new Map();
    constructor(rateLimiter) {
        // Register all supported adapters
        this.register(new NexusAdapter(rateLimiter));
        this.register(new MangaFlixAdapter(rateLimiter));
        this.register(new ManhastroAdapter(rateLimiter));
        this.register(new KuroAdapter(rateLimiter));
        this.register(new MangoToonsAdapter(rateLimiter));
    }
    register(adapter) {
        this.adapters.set(adapter.id, adapter);
    }
    get(id) {
        return this.adapters.get(id);
    }
    getAll() {
        return Array.from(this.adapters.values());
    }
}
