import { MadaraAdapter } from '../common/madara-adapter.js';
export class BorutoExplorerAdapter extends MadaraAdapter {
    constructor(rateLimiter, transport) {
        super({
            id: 'borutoexplorer',
            name: 'Boruto Explorer',
            baseUrl: 'https://leitor.borutoexplorer.com.br',
            mangaSubString: 'manga',
            rateLimitRps: 2.0,
        }, rateLimiter, transport);
    }
}
