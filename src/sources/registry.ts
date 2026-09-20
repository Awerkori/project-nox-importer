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
import { ArthurScanAdapter } from './arthurscan/arthurscan-adapter.js';
import { BorutoExplorerAdapter } from './borutoexplorer/borutoexplorer-adapter.js';
import { CovenScanAdapter } from './covenscan/covenscan-adapter.js';
import { KamiSamaExplorerAdapter } from './kamisamaexplorer/kamisamaexplorer-adapter.js';
import { MrTenzusAdapter } from './mrtenzus/mrtenzus-adapter.js';
import { NinjaScanAdapter } from './ninjascan/ninjascan-adapter.js';
import { YuriVersoAdapter } from './yuriverso/yuriverso-adapter.js';
import { AcervoHentaiAdapter } from './acervohentai/acervohentai-adapter.js';
import { InkapkAdapter } from './inkapk/inkapk-adapter.js';
import { ApeComicsAdapter } from './apecomics/apecomics-adapter.js';
import { PizzariaScanAdapter } from './pizzariascan/pizzariascan-adapter.js';
import { YaoiFanClubAdapter } from './yaoifanclub/yaoifanclub-adapter.js';
import { MangaOnlineTvAdapter } from './mangaonlinetv/mangaonlinetv-adapter.js';
import { MangaOnlineAdapter } from './mangaonline/mangaonline-adapter.js';
import { PinkRosaAdapter } from './pinkrosa/pinkrosa-adapter.js';
import { GalaxScanlatorAdapter } from './galaxscanlator/galaxscanlator-adapter.js';
import { ApenasUmaFaAdapter } from './apenasumafa/apenasumafa-adapter.js';
import { Ler999Adapter } from './ler999/ler999-adapter.js';
import { OsakaScanAdapter } from './osakascan/osakascan-adapter.js';
import { MaidScanAdapter } from './maidscan/maidscan-adapter.js';
import { VegitoonsAdapter } from './vegitoons/vegitoons-adapter.js';
import { HentaiHomeAdapter } from './hentaihome/hentaihome-adapter.js';
import { MundoHentaiAdapter } from './mundohentai/mundohentai-adapter.js';
import { HentaiSeasonAdapter } from './hentaiseason/hentaiseason-adapter.js';
import { HentaiTokyoAdapter } from './hentaitokyo/hentaitokyo-adapter.js';
import { UniversoHentaiAdapter } from './universohentai/universohentai-adapter.js';
import { HentaiFusionAdapter } from './hentaifusion/hentaifusion-adapter.js';
import { ZettaHqAdapter } from './zettahq/zettahq-adapter.js';
import { NHentaiBrAdapter } from './nhentaibr/nhentaibr-adapter.js';
import { BrasilHentaiAdapter } from './brasilhentai/brasilhentai-adapter.js';
import { PointZeroToonsAdapter } from './pointzerotoons/pointzerotoons-adapter.js';
import { GeassComicsAdapter } from './geasscomics/geasscomics-adapter.js';
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

      // Register general PT-BR sources
      this.register(new TaimuMangasAdapter(rateLimiter));
      this.register(new EuphoriaScanAdapter(rateLimiter));
      this.register(new FleurBlancheAdapter(rateLimiter));
      this.register(new LittleTyrantAdapter(rateLimiter));
      this.register(new MangaLivreToAdapter(rateLimiter));
      this.register(new MonteTaiAdapter(rateLimiter));
      this.register(new NebulosaScanAdapter(rateLimiter));
      this.register(new NocturneSummerAdapter(rateLimiter));
      this.register(new ArthurScanAdapter(rateLimiter));
      this.register(new BorutoExplorerAdapter(rateLimiter));
      this.register(new CovenScanAdapter(rateLimiter));
      this.register(new KamiSamaExplorerAdapter(rateLimiter));
      this.register(new MrTenzusAdapter(rateLimiter));
      this.register(new NinjaScanAdapter(rateLimiter));
      this.register(new YuriVersoAdapter(rateLimiter));
      this.register(new ApeComicsAdapter(rateLimiter));
      this.register(new PizzariaScanAdapter(rateLimiter));
      this.register(new MangaOnlineTvAdapter(rateLimiter));
      this.register(new MangaOnlineAdapter(rateLimiter));
      this.register(new PinkRosaAdapter(rateLimiter));
      this.register(new GalaxScanlatorAdapter(rateLimiter));
      this.register(new ApenasUmaFaAdapter(rateLimiter));
      this.register(new Ler999Adapter(rateLimiter));
      this.register(new OsakaScanAdapter(rateLimiter));
      this.register(new MaidScanAdapter(rateLimiter));
      this.register(new VegitoonsAdapter(rateLimiter));
      this.register(new PointZeroToonsAdapter(rateLimiter));
      this.register(new GeassComicsAdapter(rateLimiter));

      // Register adult sources (+18 / Adulto / Yaoi / Hentai)
      this.register(new HanamiHeavenAdapter(rateLimiter));
      this.register(new HipercoolAdapter(rateLimiter));
      this.register(new InstaHentaiAdapter(rateLimiter));
      this.register(new MegaHentaiAdapter(rateLimiter));
      this.register(new TankouHentaiAdapter(rateLimiter));
      this.register(new CafeComYaoiAdapter(rateLimiter));
      this.register(new HotCabaretScanAdapter(rateLimiter));
      this.register(new AmuyAdapter(rateLimiter));
      this.register(new AcervoHentaiAdapter(rateLimiter));
      this.register(new InkapkAdapter(rateLimiter));
      this.register(new YaoiFanClubAdapter(rateLimiter));
      this.register(new HentaiHomeAdapter(rateLimiter));
      this.register(new MundoHentaiAdapter(rateLimiter));
      this.register(new HentaiSeasonAdapter(rateLimiter));
      this.register(new HentaiTokyoAdapter(rateLimiter));
      this.register(new UniversoHentaiAdapter(rateLimiter));
      this.register(new HentaiFusionAdapter(rateLimiter));
      this.register(new ZettaHqAdapter(rateLimiter));
      this.register(new NHentaiBrAdapter(rateLimiter));
      this.register(new BrasilHentaiAdapter(rateLimiter));
    }
  }

  register(adapter: SourceAdapter): void {
    this.adapters.set(adapter.id, adapter);

    // Register helpful canonical aliases
    if (adapter.id === 'nexus') {
      this.adapters.set('nexus_mangas', adapter);
      this.adapters.set('nexusmangas', adapter);
      this.adapters.set('nexus_toons', adapter);
      this.adapters.set('nexustoons', adapter);
    }
    if (adapter.id === 'kuro') {
      this.adapters.set('kuromangas', adapter);
    }
    if (adapter.id === 'hanamiheaven') {
      this.adapters.set('noindexscan', adapter);
    }
    if (adapter.id === 'pointzerotoons') {
      this.adapters.set('kitsuneyako', adapter);
      this.adapters.set('pointzero', adapter);
      this.adapters.set('point_zero_toons', adapter);
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
