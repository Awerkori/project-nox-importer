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
import { TaimuMangasAdapter } from './taimumangas/taimumangas-adapter.js';
import { EuphoriaScanAdapter } from './euphoriascan/euphoriascan-adapter.js';
import { FleurBlancheAdapter } from './fleurblanche/fleurblanche-adapter.js';
import { LittleTyrantAdapter } from './littletyrant/littletyrant-adapter.js';
import { MangaLivreToAdapter } from './mangalivreto/mangalivreto-adapter.js';
import { MonteTaiAdapter } from './montetai/montetai-adapter.js';
import { NebulosaScanAdapter } from './nebulosascan/nebulosascan-adapter.js';
import { NocturneSummerAdapter } from './nocturnesummer/nocturnesummer-adapter.js';
import { TankouHentaiAdapter } from './tankouhentai/tankouhentai-adapter.js';
import { CafeComYaoiAdapter } from './cafecomyaoi/cafecomyaoi-adapter.js';
import { HotCabaretScanAdapter } from './hotcabaretscan/hotcabaretscan-adapter.js';
import { AmuyAdapter } from './amuy/amuy-adapter.js';
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

      // Register newly certified general PT-BR sources
      this.register(new TaimuMangasAdapter(rateLimiter));
      this.register(new EuphoriaScanAdapter(rateLimiter));
      this.register(new FleurBlancheAdapter(rateLimiter));
      this.register(new LittleTyrantAdapter(rateLimiter));
      this.register(new MangaLivreToAdapter(rateLimiter));
      this.register(new MonteTaiAdapter(rateLimiter));
      this.register(new NebulosaScanAdapter(rateLimiter));
      this.register(new NocturneSummerAdapter(rateLimiter));

      // Register adult sources (+18 / Adulto / Yaoi / Hentai)
      this.register(new HanamiHeavenAdapter(rateLimiter));
      this.register(new HipercoolAdapter(rateLimiter));
      this.register(new InstaHentaiAdapter(rateLimiter));
      this.register(new MegaHentaiAdapter(rateLimiter));
      this.register(new TankouHentaiAdapter(rateLimiter));
      this.register(new CafeComYaoiAdapter(rateLimiter));
      this.register(new HotCabaretScanAdapter(rateLimiter));
      this.register(new AmuyAdapter(rateLimiter));
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
