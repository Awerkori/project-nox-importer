import { MadaraAdapter } from '../common/madara-adapter.js';
export class KamiSamaExplorerAdapter extends MadaraAdapter {
    constructor(rateLimiter, transport) {
        super({
            id: 'kamisamaexplorer',
            name: 'Kami Sama Explorer',
            baseUrl: 'https://leitor.kamisama.com.br',
            mangaSubString: 'manga',
            rateLimitRps: 2.0,
        }, rateLimiter, transport);
    }
}
