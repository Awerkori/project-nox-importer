import { MadaraAdapter } from '../common/madara-adapter.js';
export class NebulosaScanAdapter extends MadaraAdapter {
    constructor(rateLimiter, transport) {
        super({
            id: 'nebulosascan',
            name: 'Nebulosa Scan',
            baseUrl: 'https://nebulosascan.com',
            mangaSubString: 'manga',
        }, rateLimiter, transport);
    }
}
