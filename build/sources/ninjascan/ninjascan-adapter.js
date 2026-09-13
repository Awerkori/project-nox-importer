import { MadaraAdapter } from '../common/madara-adapter.js';
export class NinjaScanAdapter extends MadaraAdapter {
    constructor(rateLimiter, transport) {
        super({
            id: 'ninjascan',
            name: 'Ninja Scan',
            baseUrl: 'https://ninjacomics.xyz',
            mangaSubString: 'manga',
            rateLimitRps: 2.0,
        }, rateLimiter, transport);
    }
}
