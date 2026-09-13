import { MadaraAdapter } from '../common/madara-adapter.js';
export class ArthurScanAdapter extends MadaraAdapter {
    constructor(rateLimiter, transport) {
        super({
            id: 'arthurscan',
            name: 'Arthur Scan',
            baseUrl: 'https://arthurscan.xyz',
            mangaSubString: 'manga',
            rateLimitRps: 2.0,
        }, rateLimiter, transport);
    }
}
