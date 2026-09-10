import { NexusAdapter } from './nexus/nexus-adapter.js';
import { MangaFlixAdapter } from './mangaflix/mangaflix-adapter.js';
import { ManhastroAdapter } from './manhastro/manhastro-adapter.js';
import { KuroAdapter } from './kuro/kuro-adapter.js';
import { MangoToonsAdapter } from './mangotoons/mangotoons-adapter.js';
export class SourceRegistry {
    adapters = new Map();
    constructor(rateLimiter, bridgeToken, mangaUrl) {
        if (rateLimiter) {
            const bridgeUrl = bridgeToken && mangaUrl ? `${mangaUrl.replace(/\/$/, '')}/api/internal/importer/kuro-bridge` : undefined;
            // Register all supported adapters
            this.register(new NexusAdapter(rateLimiter));
            this.register(new MangaFlixAdapter(rateLimiter));
            this.register(new ManhastroAdapter(rateLimiter));
            this.register(new KuroAdapter(rateLimiter));
            this.register(new MangoToonsAdapter(rateLimiter));
        }
    }
    register(adapter) {
        this.adapters.set(adapter.id, adapter);
        // Register helpful canonical aliases
        if (adapter.id === 'nexus') {
            this.adapters.set('nexus_mangas', adapter);
            this.adapters.set('nexusmangas', adapter);
        }
    }
    clear() {
        this.adapters.clear();
    }
    get(id) {
        return this.adapters.get(id);
    }
    getAll() {
        return Array.from(new Set(this.adapters.values()));
    }
}
