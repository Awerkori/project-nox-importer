import { MadaraAdapter } from '../common/madara-adapter.js';
export class CovenScanAdapter extends MadaraAdapter {
    constructor(rateLimiter, transport) {
        super({
            id: 'covenscan',
            name: 'Coven Scan',
            baseUrl: 'https://covendasbruxonas.com',
            mangaSubString: 'manga',
            rateLimitRps: 2.0,
        }, rateLimiter, transport);
    }
}
