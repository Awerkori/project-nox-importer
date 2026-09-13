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
import { TiaManhwaAdapter } from './tiamanhwa/tiamanhwa-adapter.js';
import { PointZeroToonsAdapter } from './pointzerotoons/pointzerotoons-adapter.js';
import { ApeComicsAdapter } from './apecomics/apecomics-adapter.js';
import { PizzariaScanAdapter } from './pizzariascan/pizzariascan-adapter.js';
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
            this.register(new TiaManhwaAdapter(rateLimiter));
            this.register(new PointZeroToonsAdapter(rateLimiter));
            this.register(new ApeComicsAdapter(rateLimiter));
            this.register(new PizzariaScanAdapter(rateLimiter));
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
        }
    }
    register(adapter) {
        this.adapters.set(adapter.id, adapter);
        // Register helpful canonical aliases
        if (adapter.id === 'nexus') {
            this.adapters.set('nexus_mangas', adapter);
            this.adapters.set('nexusmangas', adapter);
        }
        if (adapter.id === 'kuro') {
            this.adapters.set('kuromangas', adapter);
        }
        if (adapter.id === 'hanamiheaven') {
            this.adapters.set('noindexscan', adapter);
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
