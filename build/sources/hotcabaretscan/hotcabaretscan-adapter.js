import { MadaraAdapter } from '../common/madara-adapter.js';
export class HotCabaretScanAdapter extends MadaraAdapter {
    constructor(rateLimiter, transport) {
        super({
            id: 'hotcabaretscan',
            name: 'Hot Cabaret Scan',
            baseUrl: 'https://hotcabaretscan.com',
            mangaSubString: 'manga',
            rateLimitRps: 2.0,
        }, rateLimiter, transport);
    }
}
